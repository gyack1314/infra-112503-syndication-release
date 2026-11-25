"""二创成片的多渠道预约发布协调服务。

节目团队通过本服务预约一组发布、修改解禁、接收渠道回执并作出部分处理决定。
输入为 README.md 描述的参与方消息（本地合成渠道），输出为 JSONL 协调事件。
状态持久化在本地目录，restart 消息或进程重启后都能接着处理。

协调规则要点：
- 同一渠道、同一地域、同一时段只公开一个成片版本；
- 联动成员必须能承诺同一个发布时间，否则整组退回并指出阻碍，独立渠道不受影响；
- 渠道确认备妥不代表公开，发布时间不能早于来源解禁；
- 更换成片后按新指纹重新核对就绪，不因相同预约号复用旧成片确认；
- 解禁调整后，可调整的成员先暂停并尝试撤回，待现场回执明确才允许再次预约；
  同组有成员无法停止时，其他成员不得继续按已作废的联动计划公开；
- 公开事实不可改写：晚到的撤销确认不能把已公开改成从未公开，
  撤单请求本身也不等于渠道实际撤下；
- 回执缺失时各渠道独立保留状态，查询会说明还能等哪类确认；
  重连后的重复回报幂等，保留一次公开事实，不误关其他成片的预约。
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from scenario_reader import iter_messages

SUPPORTED = {
    "confirm", "continue_subset", "embargo", "inspect",
    "ready", "receipt", "release", "restart",
}
RECEIPT_STATES = {"prepared", "locked", "public", "cancelled", "removed"}


def _channel_state():
    return {
        "readiness": {},        # digest -> {window, retract_until, at}
        "bookings": {},         # str(rev) -> [预约尝试, ...]
        "public_facts": [],     # 公开事实，一旦记录不可改写
        "unmatched_receipts": [],
        "flags": [],
        "remove_on_public": False,
    }


def _booking(rev, attempt, digest, region, group, window, slot, at):
    return {
        "rev": rev, "attempt": attempt, "digest": digest, "region": region,
        "group": group, "window": list(window), "slot": slot, "created_at": at,
        "prepared": False, "locked": False,
        "cancel_requested_at": None, "cancel_failed": None,
        "cancelled_at": None, "published_at": None,
        "remove_requested_at": None, "removed_at": None,
        "rejected": None, "receipts": [], "flags": [],
    }


class Coordinator:
    """发布协调核心：维护发布单与渠道状态，产出协调指令与查询报告。"""

    def __init__(self):
        self.releases = {}   # 发布单名 -> 状态
        self.sources = {}    # 来源 -> 当前解禁秒点
        self.events = []     # 待取走的输出事件
        self.seq = 0
        self.now = 0

    # ---------- 持久化 ----------

    def save(self, directory):
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        payload = {"seq": self.seq, "sources": self.sources, "releases": self.releases}
        tmp = directory / "state.json.tmp"
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, directory / "state.json")

    @classmethod
    def load(cls, directory):
        path = Path(directory) / "state.json"
        if not path.exists():
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        coord = cls()
        coord.seq = payload["seq"]
        coord.sources = payload["sources"]
        coord.releases = payload["releases"]
        return coord

    # ---------- 输出 ----------

    def emit(self, kind, record):
        self.seq += 1
        self.events.append({"seq": self.seq, "at": self.now, "kind": kind, "record": record})

    def drain(self):
        events, self.events = self.events, []
        return events

    # ---------- 消息入口 ----------

    def handle(self, at, kind, record):
        self.now = at
        handler = {
            "release": self._on_release,
            "ready": self._on_ready,
            "embargo": self._on_embargo,
            "confirm": self._on_confirm,
            "receipt": self._on_receipt,
            "continue_subset": self._on_continue_subset,
            "inspect": self._on_inspect,
            "restart": self._on_restart,
        }[kind]
        handler(record)

    # ---------- 工具 ----------

    def _channel(self, rel, ch):
        return rel["channels"].setdefault(ch, _channel_state())

    @staticmethod
    def _is_open(bk):
        return (bk["cancelled_at"] is None and bk["removed_at"] is None
                and not bk["rejected"] and bk["published_at"] is None)

    def _open_attempt(self, cs, rev):
        for bk in reversed(cs["bookings"].get(str(rev), [])):
            if self._is_open(bk):
                return bk
        return None

    def _route_attempt(self, cs, rev, state):
        """把回执定位到预约尝试；重连重复回报幂等，不误关其他尝试。"""
        attempts = cs["bookings"].get(str(rev), [])
        for bk in attempts:
            if any(r["state"] == state for r in bk["receipts"]):
                return bk, True
        for bk in reversed(attempts):
            if (bk["cancelled_at"] is None and bk["removed_at"] is None
                    and not bk["rejected"]):
                return bk, False
        return None, False

    # ---------- release ----------

    def _on_release(self, rec):
        name = rec["release"]
        rel = self.releases.get(name)
        if rel is None:
            rel = {"name": name, "source": None, "digest": None, "rev": 0,
                   "embargo": None, "arrangements": {}, "channels": {}}
            self.releases[name] = rel
        rev = rel["rev"] + 1
        source, digest, embargo = rec["source"], rec["digest"], rec["embargo"]
        rel.update(source=source, digest=digest, rev=rev, embargo=embargo)
        self.sources[source] = embargo
        members, groups = {}, {}
        for m in rec["members"]:
            members[m["channel"]] = {
                "region": m["region"], "window": list(m["window"]),
                "group": m.get("group"),
            }
        for ch, m in members.items():
            gkey = m["group"] if m["group"] else f"·{ch}"
            groups.setdefault(gkey, {"status": "pending", "slot": None,
                                     "blockers": [], "continuing": [],
                                     "members": []})["members"].append(ch)
        rel["arrangements"][str(rev)] = {
            "digest": digest, "submitted_at": self.now,
            "members": members, "groups": groups,
        }
        self.emit("arrangement", {
            "release": name, "rev": rev, "source": source, "digest": digest,
            "embargo": embargo,
            "members": [{"channel": ch, **m} for ch, m in members.items()],
        })
        if rev > 1:
            self._supersede(rel, rev)
            self._check_embargo(rel)

    def _supersede(self, rel, rev):
        """新安排生效：旧代次中仍可撤的预约若被取代（换片/退出/窗口不再覆盖）则撤回。"""
        arr = rel["arrangements"][str(rev)]
        digest = arr["digest"]
        for ch, cs in rel["channels"].items():
            for rev_s, attempts in list(cs["bookings"].items()):
                if int(rev_s) >= rev:
                    continue
                for bk in attempts:
                    if not self._is_open(bk):
                        continue
                    member = arr["members"].get(ch)
                    if member is None:
                        reason = "渠道不在新安排中"
                    elif digest != bk["digest"]:
                        reason = f"成片更换为 {digest}，需重新核对就绪"
                    elif not (member["window"][0] <= bk["slot"] <= member["window"][1]):
                        reason = "新安排窗口不再覆盖原档期"
                    else:
                        continue
                    self._request_cancel(rel, ch, bk, reason)

    # ---------- ready ----------

    def _on_ready(self, rec):
        rel = self.releases.get(rec.get("release"))
        if rel is None:
            self.emit("error", {"message": f"就绪回报指向未知发布单 {rec.get('release')}"})
            return
        ch, digest = rec["channel"], rec["digest"]
        cs = self._channel(rel, ch)
        cs["readiness"][digest] = {
            "window": list(rec["window"]),
            "retract_until": rec["retract_until"],
            "at": self.now,
        }
        self.emit("readiness", {
            "release": rel["name"], "channel": ch, "digest": digest,
            "window": list(rec["window"]), "retract_until": rec["retract_until"],
        })
        if digest != rel["digest"]:
            self.emit("note", {
                "release": rel["name"], "channel": ch,
                "message": (f"就绪回报对应成片 {digest}，当前安排需要 {rel['digest']}；"
                            "更换成片后须重新核对实际就绪，不能复用旧成片确认"),
            })

    # ---------- embargo ----------

    def _on_embargo(self, rec):
        source, new_at = rec["source"], rec["at"]
        old = self.sources.get(source)
        self.sources[source] = new_at
        self.emit("embargo", {"source": source, "at": new_at, "previous": old})
        for rel in self.releases.values():
            if rel["source"] != source:
                continue
            rel["embargo"] = new_at
            self._check_embargo(rel)

    def _check_embargo(self, rel):
        embargo = rel["embargo"]
        arr = rel["arrangements"][str(rel["rev"])]
        for gkey, grp in arr["groups"].items():
            if grp["status"] == "booked" and grp["slot"] is not None and grp["slot"] < embargo:
                grp["status"] = "voided"
                grp["blockers"] = [f"解禁调整为 {embargo}，共同档期 {grp['slot']} 失效"]
                label = "独立渠道" if gkey.startswith("·") else f"联动 {gkey}"
                self.emit("note", {
                    "release": rel["name"],
                    "message": (f"{label} 的原计划作废；可调整的成员先暂停并尝试撤回，"
                                "待现场回执明确才允许再次预约"),
                })
        for ch, cs in rel["channels"].items():
            for attempts in cs["bookings"].values():
                for bk in attempts:
                    if bk["cancelled_at"] is not None or bk["removed_at"] is not None or bk["rejected"]:
                        continue
                    if bk["published_at"] is not None:
                        if bk["published_at"] < embargo:
                            flag = f"公开时间 {bk['published_at']} 早于新解禁 {embargo}"
                            if flag not in bk["flags"]:
                                bk["flags"].append(flag)
                                self.emit("note", {
                                    "release": rel["name"], "channel": ch,
                                    "message": f"{flag}；等待运营决定部分继续或要求撤下",
                                })
                        continue
                    if bk["slot"] is not None and bk["slot"] < embargo:
                        self._request_cancel(rel, ch, bk,
                                             f"解禁调整为 {embargo}，原档期 {bk['slot']} 失效")

    # ---------- confirm ----------

    def _on_confirm(self, rec):
        rel = self.releases.get(rec.get("release"))
        if rel is None:
            self.emit("error", {"message": f"确认指向未知发布单 {rec.get('release')}"})
            return
        rev = rec["rev"]
        if str(rev) not in rel["arrangements"]:
            self.emit("note", {"release": rel["name"],
                               "message": f"未知的安排代次 rev{rev}，忽略确认"})
            return
        if rev != rel["rev"]:
            self.emit("note", {"release": rel["name"],
                               "message": f"确认的 rev{rev} 不是当前轮次 rev{rel['rev']}，忽略"})
            return
        arr = rel["arrangements"][str(rev)]
        for gkey, grp in arr["groups"].items():
            group = None if gkey.startswith("·") else gkey
            fixed, created, slot, rejected, blockers = self._book_channels(
                rel, rev, group, list(grp["members"]))
            if blockers:
                grp["status"] = "bounced"
                grp["blockers"] = blockers
            else:
                grp["status"] = "booked"
                grp["blockers"] = []
                if slot is not None:
                    grp["slot"] = slot

    # ---------- 预约 ----------

    def _book_channels(self, rel, rev, group, chs, partial=False):
        """尝试让一组渠道承诺同一发布时间。

        返回 (沿用, 新约, 档期, 退回渠道, 阻碍)。partial=False 时任一阻碍
        整组退回（确认预约）；partial=True 时只退回有阻碍的渠道，其余仍按
        共同档期继续（运营选择部分继续）。
        """
        arr = rel["arrangements"][str(rev)]
        digest, embargo = arr["digest"], rel["embargo"]
        fixed, eff, per_ch = {}, {}, {}
        for ch in chs:
            member = arr["members"].get(ch)
            if member is None:
                per_ch[ch] = f"{ch} 不是当前安排的成员"
                continue
            cs = self._channel(rel, ch)
            if any(f["rev"] == rev for f in cs["public_facts"]):
                fixed[ch] = None  # 已公开，视为继续
                continue
            bk = self._open_attempt(cs, rev)
            if bk is not None:
                if bk["cancel_requested_at"] is not None:
                    per_ch[ch] = f"{ch} 的撤单回执未到，待现场回执明确才允许再次预约"
                elif bk["slot"] is not None and bk["slot"] >= embargo:
                    fixed[ch] = bk["slot"]
                else:
                    per_ch[ch] = f"{ch} 原档期 {bk['slot']} 不满足当前解禁且尚未撤回"
                continue
            carried = self._carried_slot(cs, rev, member, digest, embargo)
            if carried is not None:
                fixed[ch] = carried
                continue
            ready = cs["readiness"].get(digest)
            if ready is None:
                per_ch[ch] = f"{ch} 缺少成片 {digest} 的就绪回报"
                continue
            lo = max(member["window"][0], ready["window"][0], embargo)
            hi = min(member["window"][1], ready["window"][1])
            if lo > hi:
                per_ch[ch] = f"{ch} 在解禁 {embargo} 之后没有可用窗口"
                continue
            conflict = self._slot_conflict(rel, ch, member["region"], digest, lo, hi, rev)
            if conflict is not None:
                per_ch[ch] = f"{ch}/{member['region']} 同一时段已有其他成片：{conflict}"
                continue
            eff[ch] = (lo, hi, ready)
        blockers = list(per_ch.values())
        feasible = [ch for ch in chs if ch not in per_ch]
        rejected = list(per_ch)
        if per_ch and not partial:
            feasible, rejected = [], list(chs)
        slot = None
        if feasible:
            pinned = [s for s in fixed.values() if s is not None]
            lo = max([eff[ch][0] for ch in feasible if ch in eff] + pinned, default=embargo)
            hi = min([eff[ch][1] for ch in feasible if ch in eff] + pinned, default=None)
            if hi is not None and lo > hi:
                detail = "，".join(
                    [f"{ch} [{eff[ch][0]},{eff[ch][1]}]" for ch in feasible if ch in eff]
                    + [f"{ch} 已约 {s}" for ch, s in fixed.items() if s is not None])
                blockers.append(f"联动成员无共同可发布时间：{detail}")
                rejected.extend(ch for ch in feasible if ch not in rejected)
                feasible = []
            else:
                slot = lo
        if blockers and rejected:
            self.emit("reject", {"release": rel["name"], "rev": rev, "group": group,
                                 "channels": sorted(rejected), "blockers": blockers})
        created = []
        for ch in feasible:
            if ch not in eff:
                continue
            lo_c, hi_c, ready = eff[ch]
            member = arr["members"][ch]
            cs = self._channel(rel, ch)
            attempts = cs["bookings"].setdefault(str(rev), [])
            attempts.append(_booking(rev, len(attempts) + 1, digest, member["region"],
                                     member["group"], member["window"], slot, self.now))
            created.append(ch)
            self.emit("book", {
                "release": rel["name"], "channel": ch, "rev": rev, "digest": digest,
                "region": member["region"], "slot": slot, "group": member["group"],
                "retract_until": ready["retract_until"],
            })
        return fixed, created, slot, rejected, blockers

    def _carried_slot(self, cs, rev, member, digest, embargo):
        """旧代次中同成片、仍有效的预约档期可沿用到新安排。"""
        for rev_s, attempts in cs["bookings"].items():
            if int(rev_s) >= rev:
                continue
            for bk in attempts:
                if not self._is_open(bk) or bk["cancel_requested_at"] is not None:
                    continue
                if bk["digest"] != digest or bk["slot"] is None:
                    continue
                if bk["slot"] < embargo:
                    continue
                if member["window"][0] <= bk["slot"] <= member["window"][1]:
                    return bk["slot"]
        return None

    def _slot_conflict(self, rel, ch, region, digest, lo, hi, rev):
        """同一渠道、同一地域、同一时段只公开一个成片版本。"""
        for other in self.releases.values():
            cs = other["channels"].get(ch)
            if cs is None:
                continue
            for rev_s, attempts in cs["bookings"].items():
                if other is rel and rev_s == str(rev):
                    continue
                for bk in attempts:
                    if bk["region"] != region or bk["digest"] == digest:
                        continue
                    if (bk["cancelled_at"] is not None or bk["removed_at"] is not None
                            or bk["rejected"]):
                        continue
                    instant = bk["published_at"] if bk["published_at"] is not None else bk["slot"]
                    if instant is not None and lo <= instant <= hi:
                        return f"{other['name']} rev{rev_s} 成片 {bk['digest']}（{instant}）"
            for f in cs["public_facts"]:
                if f["removed_at"] is not None or cs["bookings"].get(str(f["rev"])):
                    continue
                if f["region"] != region or f["digest"] == digest:
                    continue
                if f["published_at"] is not None and lo <= f["published_at"] <= hi:
                    return f"{other['name']} 已公开成片 {f['digest']}（{f['published_at']}）"
        return None

    # ---------- 撤单 / 撤下 ----------

    def _request_cancel(self, rel, ch, bk, reason):
        if (bk["cancel_requested_at"] is not None or bk["cancel_failed"] is not None
                or bk["cancelled_at"] is not None or bk["published_at"] is not None
                or bk["removed_at"] is not None):
            return
        cs = self._channel(rel, ch)
        ready = cs["readiness"].get(bk["digest"])
        if not bk["locked"] and ready is not None and self.now <= ready["retract_until"]:
            bk["cancel_requested_at"] = self.now
            self.emit("cancel", {"release": rel["name"], "channel": ch, "rev": bk["rev"],
                                 "slot": bk["slot"], "reason": reason})
            return
        why = ("渠道已锁定" if bk["locked"] else "缺少就绪信息" if ready is None
               else f"撤单期限 {ready['retract_until']} 已过")
        bk["cancel_failed"] = why
        bk["flags"].append(f"无法撤单（{why}）：{reason}")
        self.emit("note", {"release": rel["name"], "channel": ch,
                           "message": f"无法按新要求停止（{why}）"})
        self._void_group(rel, bk, reason)

    def _void_group(self, rel, bk, reason):
        group = bk["group"]
        if group is None:
            return
        arr = rel["arrangements"][str(bk["rev"])]
        grp = arr["groups"][group]
        if grp["status"] != "voided":
            grp["status"] = "voided"
            grp["blockers"] = [reason]
            self.emit("note", {
                "release": rel["name"],
                "message": f"联动 {group} 的计划作废，其他成员不得继续按已作废的联动计划公开",
            })
        for ch2 in grp["members"]:
            cs2 = self._channel(rel, ch2)
            bk2 = self._open_attempt(cs2, bk["rev"])
            if bk2 is not None and bk2 is not bk:
                self._request_cancel(rel, ch2, bk2, "联动计划作废，不得继续按已作废计划公开")

    def _request_remove(self, rel, ch, cs, bk, reason):
        if bk is not None and bk["remove_requested_at"] is not None:
            return
        if bk is not None:
            bk["remove_requested_at"] = self.now
        self.emit("remove", {"release": rel["name"], "channel": ch,
                             "rev": bk["rev"] if bk else None, "reason": reason})

    # ---------- receipt ----------

    def _on_receipt(self, rec):
        rel = self.releases.get(rec.get("release"))
        if rel is None:
            self.emit("error", {"message": f"回执指向未知发布单 {rec.get('release')}"})
            return
        ch, rev, state = rec["channel"], rec["rev"], rec["state"]
        if state not in RECEIPT_STATES:
            self.emit("error", {"release": rel["name"], "message": f"未知回执状态 {state}"})
            return
        cs = self._channel(rel, ch)
        bk, duplicate = self._route_attempt(cs, rev, state)
        if duplicate:
            self.emit("note", {
                "release": rel["name"], "channel": ch,
                "message": (f"rev{rev} 的 {state} 为重复回报（可能重连补发），"
                            "保留已有事实，不影响其他预约"),
            })
            return
        if bk is None:
            cs["unmatched_receipts"].append({
                "at": self.now, "rev": rev, "state": state,
                "published_at": rec.get("published_at"),
            })
            if state == "public":
                self._record_public(rel, ch, cs, rev, rec.get("published_at"), None,
                                    ["无对应预约的公开回报"])
            else:
                self.emit("note", {"release": rel["name"], "channel": ch,
                                   "message": f"rev{rev} 的 {state} 回执没有对应预约，已留存待查"})
            return
        bk["receipts"].append({"at": self.now, "state": state})
        if state == "prepared":
            bk["prepared"] = True
            effect = "已备妥（不代表公开）"
        elif state == "locked":
            bk["locked"] = True
            effect = "已锁定，撤单不再被接受"
        elif state == "public":
            self._record_public(rel, ch, cs, rev, rec.get("published_at"), bk, [])
            return
        elif state == "cancelled":
            if bk["published_at"] is not None:
                bk["flags"].append("公开后收到的撤销确认，公开事实保留")
                effect = "公开后晚到的撤销确认，不能改成从未公开"
            else:
                bk["cancelled_at"] = self.now
                effect = "撤单已确认，现场回执明确，之后允许再次预约"
        else:  # removed
            if bk["published_at"] is not None:
                bk["removed_at"] = self.now
                for f in cs["public_facts"]:
                    if f["rev"] == rev and f["removed_at"] is None:
                        f["removed_at"] = self.now
                effect = "已撤下；公开事实保留为历史"
            else:
                bk["flags"].append("未公开却回报 removed")
                effect = "未公开却回报已撤下，已记录"
        self.emit("receipt", {"release": rel["name"], "channel": ch, "rev": rev,
                              "state": state, "effect": effect})

    def _record_public(self, rel, ch, cs, rev, published_at, bk, extra):
        for f in cs["public_facts"]:
            if f["rev"] == rev:
                self.emit("note", {
                    "release": rel["name"], "channel": ch,
                    "message": f"rev{rev} 的重复公开回报，保留一次公开事实",
                })
                return
        arr = rel["arrangements"].get(str(rev))
        digest = arr["digest"] if arr else None
        fact = {"rev": rev, "digest": digest,
                "region": bk["region"] if bk else None,
                "published_at": published_at, "removed_at": None}
        cs["public_facts"].append(fact)
        if bk is not None:
            bk["published_at"] = published_at
        violations = list(extra)
        if published_at is not None and rel["embargo"] is not None and published_at < rel["embargo"]:
            violations.append(f"公开时间 {published_at} 早于来源解禁 {rel['embargo']}")
        if bk is not None and bk["cancel_requested_at"] is not None and bk["cancelled_at"] is None:
            violations.append("撤单请求未获确认前已公开")
        if bk is not None and bk["group"]:
            grp = rel["arrangements"][str(bk["rev"])]["groups"].get(bk["group"])
            if grp and grp["status"] == "voided":
                violations.append(f"按已作废的联动 {bk['group']} 计划公开")
        target = bk["flags"] if bk is not None else cs["flags"]
        target.extend(violations)
        self.emit("public", {"release": rel["name"], "channel": ch, "rev": rev,
                             "digest": digest, "published_at": published_at,
                             "violations": violations})
        if cs["remove_on_public"]:
            cs["remove_on_public"] = False
            self._request_remove(rel, ch, cs, bk, "未列入继续范围，公开后撤下")

    # ---------- continue_subset ----------

    def _on_continue_subset(self, rec):
        rel = self.releases.get(rec.get("release"))
        if rel is None:
            self.emit("error", {"message": f"部分继续指向未知发布单 {rec.get('release')}"})
            return
        rev = rel["rev"]
        arr = rel["arrangements"][str(rev)]
        listed = []
        for ch in rec.get("channels", []):
            if ch not in arr["members"]:
                self.emit("note", {"release": rel["name"], "channel": ch,
                                   "message": "不是当前安排的成员，忽略"})
            elif ch not in listed:
                listed.append(ch)
        keep = set(listed)
        stopped, takedown = [], []
        for ch in arr["members"]:
            if ch in keep:
                continue
            cs = self._channel(rel, ch)
            bk = self._open_attempt(cs, rev)
            if bk is not None:
                cs["remove_on_public"] = True
                self._request_cancel(rel, ch, bk, "未列入继续范围，停止预约")
                stopped.append(ch)
                continue
            pub = next((b for b in cs["bookings"].get(str(rev), [])
                        if b["published_at"] is not None and b["removed_at"] is None), None)
            if pub is not None:
                self._request_remove(rel, ch, cs, pub, "未列入继续范围，撤下已公开版本")
                takedown.append(ch)
        continued, rejected = [], []
        by_group = {}
        for ch in listed:
            by_group.setdefault(arr["members"][ch]["group"], []).append(ch)
        for group, chs in by_group.items():
            fixed, created, slot, refused, blockers = self._book_channels(
                rel, rev, group, chs, partial=True)
            continued.extend(list(fixed) + created)
            rejected.extend(refused)
            if group is not None and (fixed or created):
                grp = arr["groups"][group]
                grp["continuing"] = sorted(set(grp["continuing"]) | set(fixed) | set(created))
                grp["status"] = "partial" if set(grp["continuing"]) != set(grp["members"]) else "booked"
                if slot is not None:
                    grp["slot"] = slot
        self.emit("decision", {
            "release": rel["name"], "rev": rev,
            "continued": sorted(continued), "stopped": sorted(stopped),
            "takedown": sorted(takedown), "rejected": sorted(set(rejected)),
        })

    # ---------- restart / inspect ----------

    def _on_restart(self, rec):
        self.emit("restart", {
            "restored": True,
            "releases": sorted(self.releases),
            "sources": dict(sorted(self.sources.items())),
        })

    def _on_inspect(self, rec):
        rel = self.releases.get(rec.get("release"))
        if rel is None:
            self.emit("error", {"message": f"查询指向未知发布单 {rec.get('release')}"})
            return
        self.emit("report", self._report(rel))

    def _report(self, rel):
        rev = rel["rev"]
        arr = rel["arrangements"][str(rev)]
        channels = {}
        for ch in sorted(set(rel["channels"]) | set(arr["members"])):
            cs = rel["channels"].get(ch, _channel_state())
            member = arr["members"].get(ch, {})
            attempts = sorted((bk for bl in cs["bookings"].values() for bk in bl),
                              key=lambda b: (b["rev"], b["attempt"]))
            open_bk = next((bk for bk in reversed(attempts) if self._is_open(bk)), None)
            show = open_bk or (attempts[-1] if attempts else None)
            channels[ch] = {
                "region": member.get("region"),
                "group": member.get("group"),
                "effective": ({"rev": show["rev"], "slot": show["slot"],
                               "digest": show["digest"], "status": self._status_of(show)}
                              if show else None),
                "commitments": [{"rev": bk["rev"], "slot": bk["slot"], "digest": bk["digest"],
                                 "status": self._status_of(bk)}
                                for bk in attempts if bk["slot"] is not None],
                "public": [dict(f) for f in cs["public_facts"]],
                "awaiting": self._awaiting(rel, cs, arr, ch),
                "remediation": self._remediation(rel, cs, arr, ch),
            }
        return {
            "release": rel["name"], "source": rel["source"], "embargo": rel["embargo"],
            "rev": rev, "digest": rel["digest"],
            "groups": {g: {k: v for k, v in grp.items()} for g, grp in arr["groups"].items()},
            "channels": channels,
        }

    @staticmethod
    def _status_of(bk):
        if bk["rejected"]:
            return "rejected"
        if bk["removed_at"] is not None:
            return "removed"
        if bk["published_at"] is not None:
            return "public（撤下请求已发出）" if bk["remove_requested_at"] is not None else "public"
        if bk["cancelled_at"] is not None:
            return "cancelled"
        if bk["cancel_requested_at"] is not None:
            return "cancelling"
        if bk["locked"]:
            return "locked"
        if bk["prepared"]:
            return "prepared"
        return "booked"

    def _awaiting(self, rel, cs, arr, ch):
        out = []
        for attempts in cs["bookings"].values():
            for bk in attempts:
                if bk["rejected"] or bk["cancelled_at"] is not None or bk["removed_at"] is not None:
                    continue
                if bk["remove_requested_at"] is not None:
                    out.append(f"rev{bk['rev']} 的 removed 回执（撤下待确认）")
                elif bk["cancel_requested_at"] is not None and bk["published_at"] is None:
                    out.append(f"rev{bk['rev']} 的 cancelled 回执；若撤单未生效，可能改来 locked 或 public")
                elif bk["published_at"] is None:
                    out.append(f"rev{bk['rev']} 的 prepared/locked/public 回执")
        member = arr["members"].get(ch)
        if member is not None and arr["digest"] not in cs["readiness"]:
            if not any(self._is_open(bk) for bl in cs["bookings"].values() for bk in bl):
                out.append(f"成片 {arr['digest']} 的 ready 回报（更换成片后须重新核对）")
        return out

    def _remediation(self, rel, cs, arr, ch):
        out = list(cs["flags"])
        for attempts in cs["bookings"].values():
            for bk in attempts:
                out.extend(bk["flags"])
        for f in cs["public_facts"]:
            if f["removed_at"] is not None:
                continue
            if f["published_at"] is not None and f["published_at"] < rel["embargo"]:
                out.append(f"公开时间 {f['published_at']} 早于当前解禁 {rel['embargo']}，需撤下或运营确认")
            bk = next((b for b in cs["bookings"].get(str(f["rev"]), [])
                       if b["published_at"] is not None), None)
            if bk is not None and bk["remove_requested_at"] is not None:
                out.append("撤下请求已发出，等待 removed 回执")
        member = arr["members"].get(ch)
        if member and member["group"]:
            grp = arr["groups"].get(member["group"])
            if grp and grp["status"] == "voided" and not grp["continuing"]:
                out.append(f"联动 {member['group']} 计划已作废，等待运营选择部分继续或要求撤下")
        seen, dedup = set(), []
        for item in out:
            if item not in seen:
                seen.add(item)
                dedup.append(item)
        return dedup


def main(argv=None):
    parser = argparse.ArgumentParser(description="多渠道预约发布协调服务")
    parser.add_argument("scenario", type=Path, help="参与方消息 JSONL")
    parser.add_argument("--state-dir", type=Path, required=True, help="状态目录（持久化）")
    parser.add_argument("--fresh", action="store_true", help="忽略已有状态重新开始")
    args = parser.parse_args(argv)

    coord = None if args.fresh else Coordinator.load(args.state_dir)
    if coord is None:
        coord = Coordinator()
    for row in iter_messages(args.scenario, SUPPORTED):
        if row["kind"] == "restart":
            coord.save(args.state_dir)
            coord = Coordinator.load(args.state_dir) or coord
        try:
            coord.handle(row["at"], row["kind"], row["record"])
        except Exception as exc:  # 输入违反契约时报告并继续
            coord.emit("error", {"message": f"{row['kind']} 处理失败：{exc}"})
        for event in coord.drain():
            print(json.dumps(event, ensure_ascii=False))
        coord.save(args.state_dir)


if __name__ == "__main__":
    main()
