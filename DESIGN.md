# 简易 OTLP Collector 设计

## 目标与边界

本项目实现一条最小可运行链路：

```text
OTLP/gRPC Receiver → Queue Processor → Prometheus Exporter
```

核心是用有界队列、高低水位滞回和按语义分级的丢弃策略处理背压，而不是复刻官方 OpenTelemetry Collector。第一版仅处理 Metrics，支持 Gauge 和单调累计 Counter；不实现 Logs、Traces、插件系统、持久化队列和多路 fan-out。

## 数据流与协议

OTLP 客户端通过标准 unary `MetricsService.Export` 主动推送数据。Receiver 在协议边界将 OTLP protobuf 转换为内部模型，避免 pipeline 依赖传输格式。一个 OTLP 请求可按 `Resource + Scope` 拆成多个 `MetricBatch`。

第一版约定每个 Export 请求只有一种优先级，并通过 gRPC metadata 传递；缺失时使用默认值。SDK 必须在 batching 前区分周期数据和事件/告警数据。该扩展不改变标准 OTLP RPC，但属于项目自定义约定。

## 最小数据模型

内部结构包含：

```text
MetricBatch
├── Priority: Low | High
├── ResourceAttributes
├── ScopeName / ScopeVersion
└── []Metric
    ├── Name / Description / Unit
    ├── Type: Gauge | Counter
    └── []DataPoint
        ├── Attributes
        ├── Timestamp
        └── Value: float64
```

值统一为 `float64`，attributes 暂只支持字符串。这样符合 Prometheus 输出模型并控制实现规模。Histogram、非字符串 attributes 等未支持输入必须明确拒绝或记录后跳过，不能静默错误转换。

## Consumer 与所有权

pipeline 只需要一个核心接口：

```go
type MetricsConsumer interface {
	ConsumeMetrics(context.Context, *MetricBatch) error
}
```

Receiver 持有下游 Consumer；Processor 实现 Consumer 并持有下一个 Consumer；Exporter 实现 Consumer 并作为终点。

调用 `ConsumeMetrics` 后，无论返回成功或错误，所有权都转移给 Consumer，调用方不得继续读取或修改 batch。返回 `nil` 表示 Consumer 已接管数据，或已按显式配置的策略完成丢弃；不保证数据已经导出或持久化。Queue Processor 因而可以在成功入队后返回。

当前是单链路，不实现官方 fan-out 所需的 `MutatesData` capability。以后真正增加 1:N 分支时，再由 fan-out 根据 Consumer 是否修改数据决定共享或深拷贝。

## 背压策略

队列元素是整个 `MetricBatch`，容量是不可突破的硬约束：

- 到达高水位后进入降级状态，低优先级新 batch 被策略性丢弃并返回 `nil`。
- 高优先级到达满队列时，优先驱逐已有低优先级 batch。
- 如果满队列中全是高优先级 batch，拒绝最新 batch，记录原因并返回可识别的 `ErrQueueFull`。
- 队列下降到低水位后退出降级状态，使用滞回避免阈值附近反复切换。

策略性丢弃记录 `dropped_batches_total`；未接管的过载请求记录 `rejected_batches_total`。队列长度、背压状态和丢弃原因通过独立 self-telemetry 旁路暴露，不重新进入受背压的业务队列。标签只使用有限集合，例如 `reason`、`priority` 和 `policy`，避免高基数。

## 生命周期

组件按需实现小接口：

```go
type Starter interface {
	Start(context.Context) error
}

type Shutdowner interface {
	Shutdown(context.Context) error
}
```

启动顺序为 Exporter → Processor → Receiver，确保开放入口前下游已经就绪；关闭顺序相反，先停止接收，再让 Processor 在超时范围内尽力排空，最后关闭 Exporter。无状态同步 Processor 不需要提供空生命周期方法。

## 计划目录

```text
cmd/collector/           程序入口与组件装配
internal/model/          最小 Metrics 数据模型
internal/pipeline/       Consumer、生命周期和错误契约
internal/receiver/otlp/  OTLP gRPC 接入与模型转换
internal/processor/queue 有界队列和背压策略
internal/exporter/prom/  Prometheus 业务指标输出
internal/telemetry/      Collector 自监控
```

接口设计与背压核心由项目所有者主导；协议接入、exporter 和部署文件可以作为外围样板实现，但每项取舍都应可解释和测试。
