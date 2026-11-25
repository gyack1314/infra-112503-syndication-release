# 二创成片的多渠道预约发布协调

联动成员在同一轮安排使用同一成片指纹。渠道回执通过发布单、渠道和预约代次关联，地区是合成标签。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **release**　release、source、digest、embargo 与 members，每个成员有 channel、region、window 及 group，group 缺省表示独立。 同一个 release 再提交表示新的安排，不移除原有回执。各渠道独立使用已确认的安排代次。
- **ready**　渠道回报其可发布对应 digest 的 window，retract_until 是还能接收撤单的最后秒点。
- **embargo**　来源声明新的解禁秒点。
- **confirm**　运营批准当前预约轮次，rev 为发布单从 1 起的安排代次。
- **receipt**　渠道回报对应 rev 的 prepared、locked、public、cancelled 或 removed；public 记录实际 published_at。
- **continue_subset**　运营申请只继续所列仍满足约束的渠道。
- **restart**　协调服务重启。
- **inspect**　查看发布单。

## 示例

- late-embargo.jsonl
- digest-and-independent.jsonl

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。

## 运行协调服务

`coordinator.py` 是持续可用的发布协调入口：读入参与方消息流，输出 JSONL 协调事件，
状态持久化在 `--state-dir` 指定的目录，restart 消息或进程重启后都能接着处理。

    python3 coordinator.py examples/late-embargo.jsonl --state-dir .state/late --fresh
    python3 coordinator.py 下一段消息.jsonl --state-dir .state/late   # 不带 --fresh 即继续

输出事件：

- **arrangement / readiness / embargo**　发布单登记、渠道就绪记录、解禁更新。
- **book**　发给渠道的预约指令（含共同档期与撤单期限）。联动成员承诺同一时间；
  无法共同满足窗口时整组出现在 **reject** 中并附阻碍，独立渠道不受影响。
- **cancel / remove**　发给渠道的撤单、撤下请求。请求不等于结果：
  只有渠道的 cancelled / removed 回执才改变状态。
- **receipt / public**　回执受理结果与公开事实。公开后晚到的撤销确认不改写公开事实；
  重连后的重复回报幂等，保留一次公开事实，不误关其他成片的预约。
- **decision**　continue_subset 的处理结果：继续、停止、撤下与退回的渠道。
- **report**　inspect 的查询报告：各渠道当前有效的安排、公开实况、原承诺、
  剩余补救责任，以及回执缺失时还能等哪类确认（awaiting）。
- **note / error / restart**　计划作废、无法停止、旧成片就绪不可复用等说明，
  输入契约问题，以及重启恢复摘要。

更换成片（digest）后以新指纹重新核对就绪；同一渠道、同一地域、同一时段只公开一个成片版本。
