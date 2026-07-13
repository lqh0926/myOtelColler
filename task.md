# 简易 OTLP Collector 任务清单

本文根据 `DESIGN.md` 拆分实施任务，并以 `otel-collector-plan.md` 的 2–3 周节奏安排顺序。第一版只交付一条 Metrics 链路：

```text
OTLP/gRPC Receiver → Queue Processor → Prometheus Exporter
```

## 完成标准

- OTLP 客户端可通过标准 `MetricsService.Export` 发送 Gauge 和单调累计 Counter。
- 数据经过内部模型和有界队列后，可从 Prometheus scrape endpoint 读取。
- 慢速 exporter 场景下，队列不突破容量，并正确执行高低水位滞回和优先级策略。
- Collector 自监控可观察队列长度、背压状态、策略性丢弃和拒绝。
- 启停顺序、排空超时、错误语义和 batch 所有权符合 `DESIGN.md`。
- `go test ./...`、`go test -race ./...`、`go vet ./...` 均通过。

## 0. 项目脚手架

- [x] 初始化 Go module，并固定 Go 版本。
- [x] 创建计划目录：
  - [x] `cmd/collector/`
  - [x] `internal/model/`
  - [x] `internal/pipeline/`
  - [x] `internal/receiver/otlp/`
  - [x] `internal/processor/queue/`
  - [x] `internal/exporter/prom/`
  - [x] `internal/telemetry/`
- [x] 引入最小依赖：OTLP protobuf/gRPC 与 Prometheus client。
- [x] 提供基础配置，至少包含监听地址、队列容量、高低水位、排空超时和默认优先级。
- [x] 增加 `.gitignore`，忽略构建产物和本地运行文件。

验收：`go build ./...` 能在空实现阶段成功执行，目录和依赖不包含额外 receiver、exporter 或插件框架。

## 1. Pipeline 契约与内部模型（所有者主导）

- [x] 在 `internal/model/` 定义 `MetricBatch`、`Metric`、`DataPoint`、`Priority` 和指标类型。
- [x] 将 value 统一为 `float64`，attributes 限定为 `map[string]string`。
- [x] 在 `internal/pipeline/` 定义：
  - [x] `MetricsConsumer.ConsumeMetrics(context.Context, *MetricBatch) error`
  - [x] `Starter.Start(context.Context) error`
  - [x] `Shutdowner.Shutdown(context.Context) error`
  - [x] 可识别的 `ErrQueueFull`
- [x] 用包注释明确 batch 所有权：调用 `ConsumeMetrics` 后所有权无条件转移，调用方不得再次访问。
- [x] 用包注释明确返回语义：`nil` 可代表接管或策略性丢弃，不代表已导出或持久化。
- [x] 补充模型与错误契约的单元测试。

验收：Receiver、Processor、Exporter 仅通过小接口串联；pipeline 不依赖 OTLP protobuf 或 Prometheus 类型。

## 2. Prometheus Exporter

- [x] 实现 `MetricsConsumer`，将内部 Gauge 转换为可抓取的 Prometheus 指标。
- [x] 实现单调累计 Counter 的输出语义，并明确处理重启、回退值和重复序列的第一版约束。
- [x] 将 resource、scope 和 datapoint 字符串 attributes 映射为标签，并处理非法名称和标签冲突。
- [x] 明确同名指标但类型、描述、单位或标签集合不一致时的错误策略。
- [x] 提供独立 HTTP scrape endpoint 和生命周期管理。
- [x] 为 Gauge、Counter、多 datapoint、冲突输入和 context cancellation 编写测试。

验收：直接向 exporter 传入内部 batch 后，HTTP endpoint 输出合法的 Prometheus exposition text。

## 3. OTLP/gRPC Receiver 与模型转换

- [ ] 使用官方 OTLP MetricsService protobuf 实现 unary `Export` RPC。
- [ ] 从 gRPC metadata 读取请求级 `Low | High` 优先级；缺失时使用配置的默认值。
- [ ] 按 `Resource + Scope` 将请求拆成一个或多个 `MetricBatch`。
- [ ] 转换 Gauge 和单调累计 Counter 的数值、时间戳、描述、单位与字符串 attributes。
- [ ] 对未支持数据显式处理：
  - [ ] Histogram、非单调 Sum 和其他指标类型明确拒绝或记录后跳过。
  - [ ] 非字符串 attributes 明确拒绝或记录后跳过。
  - [ ] 禁止静默错误转换。
- [ ] 将下游可识别错误映射为明确的 gRPC 状态码；策略性丢弃仍返回成功。
- [ ] 实现 gRPC server 的 Start/Shutdown，停止时先关闭入口。
- [ ] 编写转换表驱动测试和 receiver 集成测试。

验收：OTLP 请求可被正确拆分、转换并交给 mock consumer；非法输入行为可观察且有测试覆盖。

## 4. 有界队列与背压（所有者主导）

- [ ] 先记录并评审队列并发模型、锁粒度、消费者循环与关闭状态机。
- [ ] 实现以整个 `MetricBatch` 为元素的固定容量队列，任何路径都不得突破容量。
- [ ] 校验配置约束：`0 ≤ low watermark < high watermark ≤ capacity`。
- [ ] 实现高低水位滞回：
  - [ ] 队列到达高水位后进入降级状态。
  - [ ] 降级状态下拒绝接纳新的 Low batch，记为策略性丢弃并返回 `nil`。
  - [ ] 队列下降到低水位后退出降级状态。
- [ ] 实现满队列优先级策略：
  - [ ] High batch 到达时优先驱逐队列中已有的 Low batch。
  - [ ] 满队列全部为 High 时拒绝最新 batch，记录拒绝并返回 `ErrQueueFull`。
  - [ ] 明确 Low batch 在未进入降级但队列已满这一边界路径的处理结果。
- [ ] 消费循环将 batch 交给下游；定义下游出错后的处理方式，避免隐式无限重试。
- [ ] 实现关闭时停止接纳、在超时内尽力排空、取消后退出消费者 goroutine。
- [ ] 编写单元测试：
  - [ ] 容量 0/1 和阈值边界。
  - [ ] 高水位进入、低水位退出及阈值间不抖动。
  - [ ] Low 策略性丢弃。
  - [ ] High 驱逐 Low。
  - [ ] 全 High 满队列返回 `ErrQueueFull`。
  - [ ] 慢速/失败 exporter。
  - [ ] 并发生产、消费、取消和关闭。
  - [ ] 所有权转移后无数据竞争。

验收：压力测试中队列长度从不超过容量；`go test -race ./...` 通过；每种丢弃或拒绝路径都有确定返回值和原因。

## 5. Collector 自监控

- [ ] 通过独立旁路暴露 self-telemetry，不将其重新放入业务队列。
- [ ] 提供至少以下指标：
  - [ ] 当前队列长度。
  - [ ] 当前背压/降级状态。
  - [ ] `dropped_batches_total`。
  - [ ] `rejected_batches_total`。
- [ ] 丢弃指标只使用有限枚举标签，如 `reason`、`priority`、`policy`。
- [ ] 为所有队列决策建立稳定、低基数的 reason 集合。
- [ ] 验证业务指标与 Collector 自监控指标不会命名冲突。

验收：人为阻塞 exporter 后，可从 self-telemetry 观察水位变化、状态切换、Low 丢弃和 High 拒绝。

## 6. 装配与生命周期

- [ ] 在 `cmd/collector/` 装配 Exporter → Queue Processor → Receiver。
- [ ] 按 Exporter → Processor → Receiver 的顺序启动。
- [ ] 启动失败时按相反方向回滚已启动组件。
- [ ] 监听 SIGINT/SIGTERM，按 Receiver → Processor → Exporter 的顺序关闭。
- [ ] 为 Processor 设置排空超时，并记录未排空数据量。
- [ ] 返回和记录带上下文的错误，避免隐藏全局状态。
- [ ] 编写生命周期测试，覆盖部分启动失败、正常关闭、排空成功和排空超时。

验收：入口开放前下游已经就绪；停止接收后队列能在超时内尽力排空；进程无 goroutine 泄漏。

## 7. 端到端验证

- [ ] 编写端到端测试：发送 OTLP Gauge，最终从 Prometheus endpoint 读取对应样本。
- [ ] 编写端到端测试：发送单调累计 Counter，验证输出值和标签。
- [ ] 验证一个请求按多个 Resource/Scope 拆分后均可输出。
- [ ] 构造慢 exporter 或高并发输入，验证滞回和分级丢弃。
- [ ] 验证满队列全 High 时 RPC 收到可识别的过载错误。
- [ ] 运行并记录：
  - [ ] `go fmt ./...`
  - [ ] `go vet ./...`
  - [ ] `go test ./...`
  - [ ] `go test -race ./...`

验收：核心链路、异常输入、过载和优雅关闭均有自动化证据。

## 8. 部署与演示

- [ ] 编写 Collector `Dockerfile`。
- [ ] 提供 Prometheus scrape 配置。
- [ ] 提供 `docker-compose.yml`，至少启动 Collector、Prometheus 和 Grafana。
- [ ] 创建 Grafana dashboard，展示业务指标、队列水位、背压状态、丢弃率和拒绝率。
- [ ] 准备可重复的负载脚本或示例客户端，能分别发送 Low 和 High 数据并制造慢消费。
- [ ] 可选：将现有 Raft 项目埋点接入本 Collector；此项不得阻塞 Collector 核心交付。

验收：`docker compose up --build` 后可演示正常链路以及“丢周期数据、保事件/告警”的背压行为。

## 9. 文档收尾

- [ ] 更新 `README.md`：目标、边界、快速启动、配置、架构图和演示步骤。
- [ ] 记录关键设计决策：有界队列、滞回、语义优先级、所有权和错误语义。
- [ ] 记录第一版限制：仅 Metrics、Gauge/单调 Counter、字符串 attributes、单链路、无持久化。
- [ ] 记录数据丢弃/拒绝矩阵和对应 self-telemetry reason。
- [ ] 更新 `AGENTS.md` 中已经落地的构建和运行命令。

验收：新参与者只读 README 即可启动系统、发送样例数据、观察指标并解释背压策略。

## 明确不在第一版范围

- Logs、Traces、Histogram 和非字符串 attributes。
- 插件系统、动态组件发现和通用配置框架。
- 持久化队列、重放、Exactly-once 语义。
- 多 receiver、多 exporter 和 1:N fan-out。
- 官方 Collector 的 capability/`MutatesData` 机制。
- 无限制重试、生产级 HA 和极致性能优化。

## 建议里程碑

- **M1：链路跑通** — 完成 0–3、基础装配和 Gauge 端到端测试。
- **M2：背压核心** — 完成 4–7，重点验证滞回、分级丢弃、错误语义和 race safety。
- **M3：可演示交付** — 完成 8–9，形成一键启动、dashboard 和设计说明。
