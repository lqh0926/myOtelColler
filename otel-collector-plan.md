# 简易 OTLP Collector 项目

> 定位：对口通信设备 telemetry 工作经历的独立项目。不复刻官方 Collector，
> 做一个能讲清核心链路 + 一个背压亮点的最小系统。目标 2-3 周（早9晚11 强度下）。
> 核心卖点：**按数据语义分级的背压设计**——这是工作经历直接投射的差异化。

## 边界（先划死，防膨胀）

**做：** OTLP receiver → pipeline → 背压 → Prometheus exporter，一条链路走通。
**不做：** 多种 receiver/exporter、插件化架构、复杂配置系统、性能极致优化、生产级鲁棒性。
**灵魂：** 有界队列 + 高低水位滞回 + 按数据重要性分级丢弃。其余都是陪衬。

## 分工

- 🟣 自己写：设计决策 + 背压核心（面试深挖区）
- 🟢 AI 写·你 review：样板 / proto / exporter（能讲"为什么"即可）

---

## Week 1 · 链路跑通

- [ ] 🟣 定义 pipeline 骨架：`Receiver → Processor → Exporter`，用 consumer 接口串联
      （自己设计接口，想清楚数据怎么流、谁调谁）
- [ ] 🟢 OTLP gRPC receiver：用官方 OTel proto，接收 metrics
- [ ] 🟢 一个最简 batch processor：先把链路端到端跑通
- [ ] 🟢 Prometheus exporter：把 metrics 暴露出去
- 验收：一条 OTLP 数据能从 receiver 进、Prometheus 出，链路通。

## Week 2 · 背压核心 ⭐（你的亮点，重点自己写）

- [ ] 🟣 有界队列：固定容量，讲清楚为什么必须有界（无界 = OOM）
- [ ] 🟣 高低水位滞回：高水位触发降级、低水位恢复，防临界抖动
- [ ] 🟣 分级丢弃策略：按数据重要性丢（对应工作里"周期可丢、事件/告警不可丢"）
      —— 做成可配置：丢最旧 / 丢最新 / 按标签优先级
- [ ] 🟣 记录设计决策：每个取舍为什么这么选，写进 README（面试素材）
- 验收：模拟 exporter 变慢，队列到高水位触发分级丢弃，降到低水位恢复，
        全程不 OOM，日志能看出丢了哪些、留了哪些。

## Week 3 · 桥接 + 收尾

- [ ] 🟢 桥接 Raft 项目：Raft 埋点吐 OTLP → 本 Collector 接收
- [ ] 🟢 Grafana dashboard：展示 Raft 集群指标 + Collector 自身的队列水位/丢弃率
- [ ] 🟢 Dockerfile + docker-compose：一键起 Raft + Collector + Prometheus + Grafana
- [ ] 🟣 README：架构图 + 背压设计决策 + 与工作经历的对照
- 验收：docker-compose 一把起全套，Grafana 看得到 Raft 指标和 Collector 背压行为。

---

## 面试话术锚点（做的时候就想好怎么讲）

1. **背压双层叙事**：项目里做了可配置的水位滞回 + 分级丢弃；工作中在真实高频遥测流
   （周期采样 + onchange）里做过"丢周期、保事件/告警"的取舍，依据是数据语义不是工具。
2. **为什么有界**：拿可控的丢弃换不可控的 OOM。
3. **为什么滞回**：单阈值在临界点抖动，高低水位拉开缓冲区间才稳（施密特触发器同理）。
4. **数据同源判断**：resource 相同 = 同一实体产出，是关联 metric 和 trace 的基础。

---

## 贴给 Claude Code 的启动提示词

```
我要做一个简易 OTLP Collector 项目（Go），对口我在通信设备做 telemetry 采集的工作经历。
这是求职项目，不是复刻官方 Collector，目标 2-3 周，边界要卡死别膨胀。

范围：OTLP gRPC receiver → pipeline → 背压处理 → Prometheus exporter，一条链路。
核心亮点是背压：有界队列 + 高低水位滞回 + 按数据重要性分级丢弃。

分工要求：
- pipeline 接口设计、背压核心逻辑，我自己写，你只跟我讨论方案、挑刺，不要直接给完整实现。
- proto 接入、gRPC receiver 样板、Prometheus exporter、Dockerfile 这些样板，你可以写，
  但要能让我理解每块"为什么这么做"。
- 任何你写的代码，假设面试官问"这里为什么这么设计"，我要能自己讲出来，
  所以边写边跟我讲决策理由和其他选项。

我的背景：做过 6.5840（Raft 全通过），Go 基础还行；工作里处理过高频指标流的背压，
用单 buf + 优先级丢弃（周期数据可丢，onchange/告警优先发）。这个项目里我想把它
升级成有界队列 + 水位滞回 + 可配置分级丢弃。

第一步：先跟我一起把 pipeline 骨架的接口定下来（Receiver/Processor/Exporter 和
consumer 接口怎么串）。这步我自己设计，你负责挑毛病、问我为什么，别替我写。
从这里开始。
```
