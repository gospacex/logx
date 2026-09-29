# Design: add-chained-logger-api

## 1. 架构总览（不变层）

现有链路：`zap.Logger` → `OTelCore`（日志 → Span Event）→ 基础 Core（stdout/file）；TracerProvider 独立装配导出。

本次只在**入口层**与**属性/导出层**扩展，Core 结构不变：

```
调用方
  │  logx.Warn("慢查询", "latency", 800.0)                     # 全局直调（无 ctx）
  │  logger.Warn(ctx).Service("order").Run("m", "k", "v")     # 实例链式（带 ctx）
  ▼
Builder（根包 logx）
  ① 参数解析：[]any 键值交替 → []zapcore.Field（见 §2）
  ② 属性合并：显式 > cfg.Default* > cfg.ServiceName；Tags 并集去重（见 §3）
  ③ 落链（方案 A）：ctx 活动 recording span → SetAttributes(service.name, operation, log.tags)
  ▼
zap.Logger.write（现有方法扩展）
  │  字段 = 属性字段 + 解析业务字段 + trace_id/span_id（现有逻辑）
  ▼
OTelCore（不动）：*Context 方法日志 → Span Event
  ▼
TracerProvider（exporter.go 统一装配）：jaeger | kafka | otlp | stdout
```

## 2. 字段解析规则（Builder / 各级别方法共用）

输入：`fields ...any`，扫描规则（按下标配对）：

| 位置形态 | 规则 |
|---|---|
| `string` 后跟任意值 | 键值对：key=该 string，value 按 Go 类型映射 |
| 任意值后跟 string | 键值对（同上，key 必为 string） |
| 奇数位非 string（无法作 key） | 该对编码为 `bad-field.N`（stringify 原值），不 panic、不丢日志 |
| 末尾落单 string（后无值） | 追加到消息正文（空格连接） |
| 末尾落单非 string | 编码为 `arg.N` 字段兜底 |
| `logx.Fields`（`map[string]any`） | 展开为若干键值对（map 遍历序不保证，日志字段序随之不定，属可接受） |
| `zap.Field` | 原样保留（兼容现有调用） |

类型映射（value → zapcore.Field / OTel 属性）：

| Go 类型 | zapcore | 备注 |
|---|---|---|
| string | String | 中文 key 天然支持 |
| int/int8/16/32/64 | Int / Int64 | 按宽度 |
| float32/float64 | Float32/Float64 | |
| bool | Bool | |
| time.Duration | Duration | |
| time.Time | Time | |
| error | Error | |
| 其它 | Any | 编码时走 stringify 兜底（复用 otel.go 现有逻辑） |

实现位置：`logx` 根包新增 `builder.go`（Builder、Fields、解析器 `parseFields(msg string, fields ...any) (string, []zap.Field)`）；解析结果以 `zap.Field` 列表交给现有 `Logger.write`，不新增 Core 层代码。

## 3. 属性与回退链

- `service` = `builder.service != "" ? builder.service : cfg.DefaultService`；仍空 → `cfg.ServiceName`
- `operation` = `builder.operation != "" ? builder.operation : cfg.DefaultOperation`；空则省略字段
- `tags` = `dedup(append(builder.tags, cfg.DefaultTags...))`；空则省略
- 日志字段 key 固定：`log.service` / `log.operation` / `log.tags`（逗号拼接）
- span 属性：`service.name`、`operation`、`log.tags`（`attribute.StringSlice`）；仅当 `span.IsRecording()` 时注入

## 4. 导出目标统一（exporter.go）

- `buildSpanExporter` switch 增加：`case "kafka", "mq":` → `buildProducer(cfg.MQ)` + `NewMQExporter`（复用 exporter_mq.go 既有类型）；`cfg.MQ.Brokers` 为空 → 返回 `fmt.Errorf("otel kafka exporter: no brokers configured; set logx.mq.brokers")`
- `SetupTracerProvider` 签名不变；Kafka 路径在装配阶段即建立 producer（fail fast）
- `NewMQTracerProvider` 内部转调统一装配逻辑（保留导出符号，行为不变）
- 其余（jaeger/otlp/stdout、采样器、Resource）不动

## 5. 风险与权衡

| 决策 | 取舍 |
|---|---|
| 方案 A（不自动建 span） | 无 ctx span 的属性只进日志字段；trace 覆盖依赖调用方传 ctx。避免 span 爆炸，YAGNI |
| 键值交替而非 map 必填 | 最接近用户期望的字典写法；代价是坏键运行时降级为 `bad-field.N`（已有场景约定） |
| `logx.Fields` 展开序不定 | map 遍历天然无序；日志字段序无契约，不影响检索（字段按 key 查询） |
| 全局直调无 ctx | 与现状一致（无 Span Event）；需要入链的调用走 `WithContext` 或 Logger 实例方法 |
