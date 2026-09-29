# Delta: chained-logger-api

## ADDED Requirements

### Requirement: 各级别全局日志入口
系统 SHALL 提供 `logx` 根包全局函数入口：

- 直调形式（无修饰）：`logx.Debug(msg string, fields ...any)`、`logx.Info(...)`、`logx.Warn(...)`、`logx.Error(...)`、`logx.DPanic(...)`，以各级别直接输出到当前全局 Logger 的输出目标。
- 链式入口：`logx.L()` SHALL 返回 `*Builder`；调用方 SHALL 通过链式中间方法（Service/Operation/Tags/WithContext）修饰后，调用末端各级别方法（`Debug/Info/Warn/Error/DPanic(msg, fields ...any)`）输出，无需显式持有 Logger 实例。

#### Scenario: 无修饰直接输出
- **WHEN** 调用 `logx.Warn("慢查询", "latency", 800.0)` 且未配置默认值
- **THEN** 日志以 Warn 级别输出到配置的输出目标（stdout/file），并携带 `log.service` 字段（取 `cfg.ServiceName` 兜底）

#### Scenario: 链式指定后输出
- **WHEN** 调用 `logx.L().Service("order").Operation("/api/v1/order").Info("curd错误", "db", "127.0.0.1:3306")`
- **THEN** 输出一条 Info 日志，消息为 "curd错误"，字段含 `db` 与 `log.service="order"`、`log.operation="/api/v1/order"`

### Requirement: 键值交替变参字段语法
日志输出方法 SHALL 接受 `msg string, fields ...any` 形式的参数：string 与任意值成对出现时，string 为字段 key、紧随其后的值（任意类型）为该 key 的 value。类型映射规则 SHALL 为：string→字符串、float64/float32→浮点、int 系→整数、bool→布尔、time.Duration→时长、time.Time→时间、error→错误、其它→反射兜底（不可编码值 stringify）。该语法 SHALL 允许中文 key。

#### Scenario: 基本键值对
- **WHEN** 调用 `logx.Info("curd错误", "db", "127.0.0.1:3306", "sql语句", "select * from order")`
- **THEN** 输出日志包含字段 `db="127.0.0.1:3306"` 与 `sql语句="select * from order"`

#### Scenario: 混合类型值
- **WHEN** 调用 `logx.Warn("慢查询", "latency", 800.0, "count", 5, "retry", true)`
- **THEN** 输出字段 `latency` 为浮点 800、`count` 为整数 5、`retry` 为布尔 true

#### Scenario: 落单字符串追加到消息
- **WHEN** 调用 `logx.Warn("慢查询", "latency", 800.0, "2012.11.22")` 且最后一个字符串后无值
- **THEN** 该字符串追加到日志消息正文（如 `慢查询 2012.11.22`），不作为字段

### Requirement: 非法键值对不中断输出
当作为 key 位置的值不是 string（无法作为字段名）时，系统 SHALL 不 panic、不丢弃整条日志；该键值对 SHALL 以 `bad-field.<N>` 字段记录并附原始值，日志继续正常输出。

#### Scenario: 非 string 键
- **WHEN** 调用 `logx.Info("m", 42, "x")`（42 在 key 位置）
- **THEN** 不 panic；输出含 `bad-field.0` 字段（值含 42 与 "x" 的原始信息），消息 "m" 正常记录

### Requirement: 链式中间修饰方法
`logx.L()` 返回的 Builder SHALL 提供中间方法 `Service(name string)`、`Operation(op string)`、`Tags(tags ...string)` 与 `WithContext(ctx context.Context)`。这些方法 SHALL 不产生日志输出，仅累积调用级属性，返回 Builder 供继续链式调用。末端输出口 SHALL 为 Builder 上的各级别方法（`Debug/Info/Warn/Error/DPanic(msg, fields ...any)`），末端级别即输出级别。

#### Scenario: 链式末端指定级别输出
- **WHEN** 调用 `logx.L().Service("order").Tags("slow","mysql").Warn("慢查询", "latency", 800.0)`
- **THEN** 输出一条 Warn 日志，字段含 `log.service="order"`、`log.tags="slow,mysql"`（逗号拼接）与 `latency`

### Requirement: 属性回退链
属性解析 SHALL 遵循优先级：链式显式指定 > 配置默认值（`Config.DefaultService` / `Config.DefaultOperation` / `Config.DefaultTags`）> `Config.ServiceName` 兜底。Tags SHALL 为链式与配置默认值的并集并去重。

#### Scenario: 配置默认值兜底
- **WHEN** 配置 `default_service: "order"`、`default_tags: ["slow"]`，调用 `logx.Warn("慢查询", "latency", 800.0)`（无链式修饰）
- **THEN** 输出字段含 `log.service="order"` 与 `log.tags="slow"`

#### Scenario: 显式覆盖默认
- **WHEN** 配置 `default_service: "order"`，调用 `logx.L().Service("pay").Info("m")`
- **THEN** `log.service="pay"`（显式优先）

### Requirement: Logger 实例级链式入口
`logx.Logger` 接口 SHALL 新增 `Chain() *Builder` 方法（zap 后端实现），返回绑定该实例输出目标与配置（Default* 回退用实例 cfg）的 Builder；`Chain().WithContext(ctx)` 携带 ctx。现有 `DebugContext`/`InfoContext` 等 Context 方法与 `With`/`Named` 方法 SHALL 保持行为不变。

#### Scenario: 实例链式
- **WHEN** 通过 `logx.New(cfg)` 获得 logger，调用 `logger.Chain().Service("order").Info("m", "k", "v")`
- **THEN** 输出走该 logger 的输出目标，字段与全局入口语义一致

#### Scenario: 实例链式带 ctx 注入调用链
- **WHEN** 在 OpenTelemetry 活动 span 内调用 `logger.Chain().WithContext(ctx).Service("order").Operation("/api/v1/order").Warn("慢查询", "latency", 800.0)`
- **THEN** 该 span 携带属性 `service.name="order"`、`operation="/api/v1/order"`、`log.tags`（若有）；Jaeger 中该 span 可按这些属性过滤

### Requirement: 活动 span 属性注入（方案 A）
带 context 的日志输出（Builder `WithContext` 或现有 `*Context` 方法路径）SHALL 在 context 含活动 recording span 时，将解析后的 `service.name`、`operation`、`log.tags`（数组）通过 `span.SetAttributes` 注入该 span。context 无活动 span 时 SHALL 静默降级为纯日志输出，不创建 span、不报错。

#### Scenario: 无 span 静默降级
- **WHEN** 使用空 context（无 span）调用 `logger.Chain().WithContext(context.Background()).Info("m")`
- **THEN** 日志正常输出，无 panic、无错误返回，不产生新 span

### Requirement: 兼容现有构造函数
`logx.Fields`（`map[string]any`）元素与现有 `zap.String`/`zap.Int` 等构造函数（`zap.Field`）SHALL 可出现在 `...any` 变参中并被正确展开/编码，既有调用代码（如 `InfoContext(ctx, msg, zap.String("k","v"))`）行为不变。

#### Scenario: 混用构造函数
- **WHEN** 调用 `logx.Info("m", "k", "v", zap.String("legacy", "x"))`
- **THEN** 输出含 `k="v"` 与 `legacy="x"` 字段

#### Scenario: Fields map 展开
- **WHEN** 调用 `logx.Warn("m", logx.Fields{"a": 1, "b": "x"})`
- **THEN** 输出含 `a=1` 与 `b="x"` 字段（map 遍历序不定，可接受）

### Requirement: 导出目标配置化
`Config.OTel.Exporter` 合法值 SHALL 扩展为 `jaeger` / `kafka` / `otlp` / `stdout`，其中 `kafka` 表示将 span 直推 Kafka topic（复用 `Config.MQ` 的 Brokers/Topic/GroupID），`mq` SHALL 作为 `kafka` 的别名向后兼容。`SetupTracerProvider` SHALL 作为统一装配入口支持全部取值；Kafka 目标缺 Broker 配置时 SHALL 返回明确错误。现有 `NewMQTracerProvider` SHALL 保留。

#### Scenario: Kafka 直推
- **WHEN** 配置 `otel.exporter: "kafka"` 与 `mq.brokers` 并调用 `SetupTracerProvider`
- **THEN** 返回的 TracerProvider 将 span 写入 Kafka topic `cfg.MQ.Topic`

#### Scenario: Kafka 缺配置 fail fast
- **WHEN** 配置 `otel.exporter: "kafka"` 但 `mq.brokers` 为空
- **THEN** `SetupTracerProvider` 返回包含 "brokers" 信息的错误，不返回 provider

#### Scenario: 别名兼容
- **WHEN** 配置 `otel.exporter: "mq"`
- **THEN** 行为与 `"kafka"` 完全一致
