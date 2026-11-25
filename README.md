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
