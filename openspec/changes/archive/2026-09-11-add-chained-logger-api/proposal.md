# add-chained-logger-api

## Why

当前 logx 使用方式有两处摩擦：字段必须逐条写 `zap.String(...)`/`zap.Float64(...)` 构造函数（样板重、期望的字典形态写不出来，中文 key 也不便）；调用上下文（Service / Operation / Tags）无法便捷标注且不能靠配置初始化缺省值；导出目标在 `SetupTracerProvider`（jaeger/otlp/stdout）与 `NewMQTracerProvider`（kafka）之间割裂，无法一份配置切换。

## What Changes

- **新增** 根包 `logx` 各级别全局直调 `logx.Debug()/Info()/Warn()/Error()/DPanic()(msg, fields ...any)`；链式入口 `logx.L()` 返回 `*Builder`，中间方法 `Service/Operation/Tags/WithContext` 修饰，末端各级别方法（如 `.Info(msg, fields...)`）输出
- **新增** 键值交替变参字段语法（`logx.Warn("慢查询", "latency", 800.0, "sql语句", "...")`），无构造器；`logx.Fields`（map 字面量）与现有 `zap.String/Int` 构造函数作为兼容并存
- **新增** `Logger` 接口 `Chain() *Builder` 实例级链式入口（绑定实例输出目标与配置）；属性双写：日志字段（`log.service/log.operation/log.tags`）+ ctx 活动 span `SetAttributes`（方案 A，无 span 静默降级）
- **新增** 配置缺省段 `default_service` / `default_operation` / `default_tags`，回退链：显式链式 > 配置默认 > `cfg.ServiceName`
- **修改** `Config.OTel.Exporter` 合法值扩展 `kafka`（直推 Kafka topic，复用 `Config.MQ`），`mq` 为别名；`SetupTracerProvider` 统一装配，`NewMQTracerProvider` 保留转调
- 现有 `*Context` 方法、`OTelCore` 事件注入、`With/Named` 行为不变（向后兼容）
- 不包含（本次）：无 span 时自动建 span（方案 B）、Pulsar、OTLP Logs 通道

## Capabilities

### New Capabilities

- `chained-logger-api`: 各级别全局/实例 Builder 入口、键值交替变参字段、Service/Operation/Tags 链式与配置回退、活动 span 属性注入、导出目标（kafka/jaeger/otlp/stdout）统一配置化

### Modified Capabilities

（无 — `openspec/specs/` 当前为空，尚无既有 spec 需要修改）

## Impact

- **代码**：`logx.go`（Builder/Fields/全局函数/Config 扩展）、`zap/zap.go`（实例 Builder 入口 + 参数解析 + 属性注入）、`zap/otel.go`（属性双写辅助，如需）、`zap/exporter.go`（exporter=kafka 统一装配 + mq 别名）、`config.go`（DefaultService/DefaultOperation/DefaultTags）
- **API**：纯增量；现有接口与行为不破坏
- **依赖**：无新增外部依赖
- **示例**：`example/main.go` 更新演示新 API
- **测试**：新增解析/回退/属性注入单测（表驱动）；Kafka 直推用 example/docker-compose 环境验证；现有 `logx_test.go`/`zap_test.go` 保持绿
