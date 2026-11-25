"""协调服务的规则测试：联动退回、解禁调整、部分继续、公开事实与重连幂等。"""

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from coordinator import Coordinator, main

EXAMPLES = Path(__file__).with_name("examples")


def run_messages(coord, messages):
    """逐条送入消息并收集输出事件。"""
    events = []
    for at, kind, record in messages:
        coord.handle(at, kind, record)
        events.extend(coord.drain())
    return events


def run_scenario(path, state_dir, fresh=True):
    argv = [str(path), "--state-dir", str(state_dir)]
    if fresh:
        argv.append("--fresh")
    out = io.StringIO()
    with redirect_stdout(out):
        main(argv)
    return [json.loads(line) for line in out.getvalue().splitlines()]


def kinds(events, kind):
    return [e for e in events if e["kind"] == kind]


def release_msg(at, name, source, digest, embargo, members):
    return at, "release", {
        "release": name, "source": source, "digest": digest, "embargo": embargo,
        "members": [
            {"channel": ch, "region": region, "window": window, **({"group": group} if group else {})}
            for ch, region, window, group in members
        ],
    }


def ready_msg(at, name, ch, digest, window, retract_until):
    return at, "ready", {"release": name, "channel": ch, "digest": digest,
                         "window": window, "retract_until": retract_until}


def receipt_msg(at, name, ch, rev, state, published_at=None):
    record = {"release": name, "channel": ch, "rev": rev, "state": state}
    if published_at is not None:
        record["published_at"] = published_at
    return at, "receipt", record


class ExampleScenarioTests(unittest.TestCase):
    def test_late_embargo_flow(self):
        with tempfile.TemporaryDirectory() as directory:
            events = run_scenario(EXAMPLES / "late-embargo.jsonl", Path(directory) / "s")

        books = kinds(events, "book")
        self.assertEqual([(b["record"]["channel"], b["record"]["slot"]) for b in books[:2]],
                         [("A", 10), ("B", 10)])
        # 解禁推迟：A 已锁定无法撤单，B 被撤回；联动计划作废
        notes = [e["record"] for e in kinds(events, "note")]
        self.assertTrue(any("无法按新要求停止" in r.get("message", "")
                            for r in notes if r.get("channel") == "A"))
        cancels = kinds(events, "cancel")
        self.assertEqual([c["record"]["channel"] for c in cancels], ["B"])
        # 重启后状态恢复
        self.assertEqual(kinds(events, "restart")[0]["record"]["releases"], ["L"])
        # A 按作废计划提前公开：公开事实记录并标注违规
        public = kinds(events, "public")[0]["record"]
        self.assertEqual((public["channel"], public["published_at"]), ("A", 10))
        self.assertTrue(any("早于来源解禁" in v for v in public["violations"]))
        # 部分继续：B 重新预约到 14，A 被要求撤下
        decision = kinds(events, "decision")[0]["record"]
        self.assertEqual(decision["continued"], ["B"])
        self.assertEqual(decision["takedown"], ["A"])
        self.assertEqual(books[-1]["record"]["slot"], 14)
        self.assertEqual(kinds(events, "remove")[0]["record"]["channel"], "A")
        # 查询：有效安排、公开实况、原承诺、剩余补救责任
        report = kinds(events, "report")[0]["record"]
        self.assertEqual(report["channels"]["B"]["effective"]["slot"], 14)
        self.assertEqual(report["channels"]["A"]["public"][0]["published_at"], 10)
        self.assertTrue(report["channels"]["A"]["remediation"])
        self.assertTrue(any("removed" in a for a in report["channels"]["A"]["awaiting"]))
        # 撤下回执落地，公开事实仍保留
        removed = kinds(events, "receipt")[-1]["record"]
        self.assertEqual((removed["state"], removed["effect"]), ("removed", "已撤下；公开事实保留为历史"))

    def test_digest_change_and_independent_flow(self):
        with tempfile.TemporaryDirectory() as directory:
            events = run_scenario(EXAMPLES / "digest-and-independent.jsonl", Path(directory) / "s")
        # 联动组无共同档期被退回并指出阻碍，独立渠道 C 按原预约继续
        reject = kinds(events, "reject")[0]["record"]
        self.assertEqual(reject["group"], "g")
        self.assertTrue(any("无共同可发布时间" in b for b in reject["blockers"]))
        books = kinds(events, "book")
        self.assertEqual([(b["record"]["channel"], b["record"]["slot"]) for b in books], [("C", 3)])
        # 旧成片就绪不能复用到新成片
        notes = [e["record"].get("message", "") for e in kinds(events, "note")]
        self.assertTrue(any("不能复用旧成片确认" in m for m in notes))
        report = kinds(events, "report")[0]["record"]
        self.assertIsNone(report["channels"]["A"]["effective"])
        self.assertTrue(any("x2" in a for a in report["channels"]["A"]["awaiting"]))
        self.assertEqual(report["channels"]["C"]["public"][0]["digest"], "x1")


class RuleTests(unittest.TestCase):
    def test_late_cancel_after_public_keeps_fact(self):
        coord = Coordinator()
        events = run_messages(coord, [
            release_msg(0, "R", "s", "d", 5, [("A", "N", [5, 10], None)]),
            ready_msg(1, "R", "A", "d", [5, 10], 9),
            (2, "confirm", {"release": "R", "rev": 1}),
            receipt_msg(5, "R", "A", 1, "public", published_at=5),
            receipt_msg(6, "R", "A", 1, "cancelled"),
            (7, "inspect", {"release": "R"}),
        ])
        receipt = kinds(events, "receipt")[-1]["record"]
        self.assertIn("不能改成从未公开", receipt["effect"])
        report = kinds(events, "report")[0]["record"]
        self.assertEqual(len(report["channels"]["A"]["public"]), 1)
        self.assertEqual(report["channels"]["A"]["effective"]["status"], "public")

    def test_reconnect_duplicate_receipts_are_idempotent(self):
        coord = Coordinator()
        events = run_messages(coord, [
            release_msg(0, "R", "s", "d", 5, [("A", "N", [5, 10], None)]),
            ready_msg(1, "R", "A", "d", [5, 10], 9),
            (2, "confirm", {"release": "R", "rev": 1}),
            receipt_msg(5, "R", "A", 1, "public", published_at=5),
            receipt_msg(6, "R", "A", 1, "public", published_at=5),   # 重连补发
            receipt_msg(7, "R", "A", 1, "public", published_at=6),   # 冲突补发，保留首次
            (8, "inspect", {"release": "R"}),
        ])
        self.assertEqual(len(kinds(events, "public")), 1)
        dup_notes = [e["record"].get("message", "") for e in kinds(events, "note")]
        self.assertTrue(any("重复" in m for m in dup_notes))
        report = kinds(events, "report")[0]["record"]
        facts = report["channels"]["A"]["public"]
        self.assertEqual(len(facts), 1)
        self.assertEqual(facts[0]["published_at"], 5)

    def test_old_rev_cancel_does_not_close_new_rev_booking(self):
        coord = Coordinator()
        events = run_messages(coord, [
            release_msg(0, "R", "s", "d1", 5, [("A", "N", [5, 10], None)]),
            ready_msg(1, "R", "A", "d1", [5, 10], 20),
            (2, "confirm", {"release": "R", "rev": 1}),
            # 更换成片：旧预约撤回，重新核对就绪
            release_msg(3, "R", "s", "d2", 5, [("A", "N", [6, 10], None)]),
            receipt_msg(4, "R", "A", 1, "cancelled"),
            ready_msg(5, "R", "A", "d2", [6, 10], 20),
            (6, "confirm", {"release": "R", "rev": 2}),
            # 重连后旧代次的撤单确认补发，不能误关 rev2 的预约
            receipt_msg(7, "R", "A", 1, "cancelled"),
            (8, "inspect", {"release": "R"}),
        ])
        cancels = kinds(events, "cancel")
        self.assertEqual([c["record"]["rev"] for c in cancels], [1])
        books = kinds(events, "book")
        self.assertEqual([(b["record"]["rev"], b["record"]["slot"]) for b in books], [(1, 5), (2, 6)])
        report = kinds(events, "report")[0]["record"]
        self.assertEqual(report["channels"]["A"]["effective"]["status"], "booked")
        self.assertEqual(report["channels"]["A"]["effective"]["rev"], 2)

    def test_embargo_change_cancels_then_continue_subset_rebooks_together(self):
        coord = Coordinator()
        events = run_messages(coord, [
            release_msg(0, "G", "s", "d", 10, [("A", "N", [10, 20], "g"), ("B", "S", [10, 20], "g")]),
            ready_msg(1, "G", "A", "d", [10, 20], 30),
            ready_msg(1, "G", "B", "d", [10, 20], 30),
            (2, "confirm", {"release": "G", "rev": 1}),
            (3, "embargo", {"source": "s", "at": 12}),
            receipt_msg(4, "G", "A", 1, "cancelled"),
            receipt_msg(4, "G", "B", 1, "cancelled"),
            (5, "continue_subset", {"release": "G", "channels": ["A", "B"]}),
        ])
        books = kinds(events, "book")
        self.assertEqual([(b["record"]["channel"], b["record"]["slot"]) for b in books[-2:]],
                         [("A", 12), ("B", 12)])
        self.assertEqual(kinds(events, "decision")[0]["record"]["continued"], ["A", "B"])

    def test_rebooking_waits_for_cancel_receipt(self):
        coord = Coordinator()
        events = run_messages(coord, [
            release_msg(0, "G", "s", "d", 10, [("A", "N", [10, 20], None)]),
            ready_msg(1, "G", "A", "d", [10, 20], 30),
            (2, "confirm", {"release": "G", "rev": 1}),
            (3, "embargo", {"source": "s", "at": 12}),
            # 撤单回执未到就申请继续：应被拒绝并说明等待回执
            (4, "continue_subset", {"release": "G", "channels": ["A"]}),
        ])
        reject = kinds(events, "reject")[0]["record"]
        self.assertTrue(any("回执" in b for b in reject["blockers"]))
        self.assertEqual(kinds(events, "decision")[0]["record"]["rejected"], ["A"])

    def test_continue_subset_rejects_channel_that_no_longer_satisfies(self):
        coord = Coordinator()
        events = run_messages(coord, [
            release_msg(0, "G", "s", "d", 10, [("A", "N", [10, 12], "g"), ("B", "S", [10, 20], "g")]),
            ready_msg(1, "G", "A", "d", [10, 12], 30),
            ready_msg(1, "G", "B", "d", [10, 20], 30),
            (2, "confirm", {"release": "G", "rev": 1}),
            (3, "embargo", {"source": "s", "at": 15}),
            receipt_msg(4, "G", "A", 1, "cancelled"),
            receipt_msg(4, "G", "B", 1, "cancelled"),
            (5, "continue_subset", {"release": "G", "channels": ["A", "B"]}),
        ])
        decision = kinds(events, "decision")[0]["record"]
        self.assertEqual(decision["continued"], ["B"])
        self.assertEqual(decision["rejected"], ["A"])
        reject = kinds(events, "reject")[0]["record"]
        self.assertTrue(any("没有可用窗口" in b for b in reject["blockers"]))
        self.assertEqual(kinds(events, "book")[-1]["record"]["slot"], 15)

    def test_slot_uniqueness_across_releases(self):
        coord = Coordinator()
        events = run_messages(coord, [
            release_msg(0, "R1", "s1", "d1", 5, [("A", "N", [5, 10], None)]),
            ready_msg(1, "R1", "A", "d1", [5, 10], 20),
            release_msg(2, "R2", "s2", "d2", 5, [("A", "N", [5, 9], None)]),
            ready_msg(3, "R2", "A", "d2", [5, 9], 20),
            (4, "confirm", {"release": "R1", "rev": 1}),
            (5, "confirm", {"release": "R2", "rev": 1}),
        ])
        reject = kinds(events, "reject")[0]["record"]
        self.assertEqual(reject["release"], "R2")
        self.assertTrue(any("同一时段已有其他成片" in b for b in reject["blockers"]))
        self.assertEqual(len(kinds(events, "book")), 1)

    def test_prepared_is_not_public(self):
        coord = Coordinator()
        events = run_messages(coord, [
            release_msg(0, "R", "s", "d", 5, [("A", "N", [5, 10], None)]),
            ready_msg(1, "R", "A", "d", [5, 10], 9),
            (2, "confirm", {"release": "R", "rev": 1}),
            receipt_msg(3, "R", "A", 1, "prepared"),
            (4, "inspect", {"release": "R"}),
        ])
        report = kinds(events, "report")[0]["record"]
        self.assertEqual(report["channels"]["A"]["public"], [])
        self.assertEqual(report["channels"]["A"]["effective"]["status"], "prepared")

    def test_cancel_request_is_not_a_takedown(self):
        coord = Coordinator()
        events = run_messages(coord, [
            release_msg(0, "R", "s", "d", 5, [("A", "N", [5, 10], None)]),
            ready_msg(1, "R", "A", "d", [5, 10], 9),
            (2, "confirm", {"release": "R", "rev": 1}),
            (3, "embargo", {"source": "s", "at": 7}),
            (4, "inspect", {"release": "R"}),
        ])
        self.assertEqual(len(kinds(events, "cancel")), 1)
        report = kinds(events, "report")[0]["record"]
        # 撤单请求已发出，但渠道尚未确认，状态保留并说明还能等哪类确认
        self.assertEqual(report["channels"]["A"]["effective"]["status"], "cancelling")
        self.assertTrue(any("cancelled" in a for a in report["channels"]["A"]["awaiting"]))

    def test_unmatched_public_is_recorded_with_remediation(self):
        coord = Coordinator()
        events = run_messages(coord, [
            release_msg(0, "R", "s", "d", 5, [("A", "N", [5, 10], None)]),
            ready_msg(1, "R", "A", "d", [5, 10], 9),
            receipt_msg(6, "R", "A", 1, "public", published_at=5),  # 未确认预约就公开
            (7, "inspect", {"release": "R"}),
        ])
        public = kinds(events, "public")[0]["record"]
        self.assertIn("无对应预约的公开回报", public["violations"])
        report = kinds(events, "report")[0]["record"]
        self.assertEqual(report["channels"]["A"]["public"][0]["published_at"], 5)
        self.assertTrue(report["channels"]["A"]["remediation"])

    def test_removed_without_public_is_flagged(self):
        coord = Coordinator()
        events = run_messages(coord, [
            release_msg(0, "R", "s", "d", 5, [("A", "N", [5, 10], None)]),
            ready_msg(1, "R", "A", "d", [5, 10], 9),
            (2, "confirm", {"release": "R", "rev": 1}),
            receipt_msg(3, "R", "A", 1, "removed"),
        ])
        receipt = kinds(events, "receipt")[-1]["record"]
        self.assertIn("未公开", receipt["effect"])


class PersistenceTests(unittest.TestCase):
    def test_state_survives_process_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state"
            part1 = Path(directory) / "part1.jsonl"
            part2 = Path(directory) / "part2.jsonl"
            rows = [
                {"at": 0, "kind": "release", "record": {"release": "R", "source": "s", "digest": "d", "embargo": 5,
                                                        "members": [{"channel": "A", "region": "N", "window": [5, 10]}]}},
                {"at": 1, "kind": "ready", "record": {"release": "R", "channel": "A", "digest": "d",
                                                      "window": [5, 10], "retract_until": 9}},
                {"at": 2, "kind": "confirm", "record": {"release": "R", "rev": 1}},
            ]
            part1.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows), encoding="utf-8")
            part2.write_text(json.dumps({"at": 3, "kind": "inspect", "record": {"release": "R"}},
                                        ensure_ascii=False), encoding="utf-8")
            first = run_scenario(part1, state)
            self.assertEqual([b["record"]["slot"] for b in kinds(first, "book")], [5])
            # 第二次进程从状态目录恢复，继续处理
            second = run_scenario(part2, state, fresh=False)
            report = kinds(second, "report")[0]["record"]
            self.assertEqual(report["channels"]["A"]["effective"]["slot"], 5)
            self.assertEqual(report["channels"]["A"]["effective"]["status"], "booked")


if __name__ == "__main__":
    unittest.main()
