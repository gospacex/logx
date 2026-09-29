# Close Issues: add-chained-logger-api

> 生成于 close 阶段 2（代码审查）。close 阶段不修改代码；需修复项切 `/sddflow build` 后重新 close。

## 代码审查结论

- 审查范围：本次变更全部 12 个文件（config/builder/logx/zap 各模块 + 测试 + example/README）
- 测试证据：`go test -race -count=1 ./...` 全绿
- 无 CRITICAL

## IMPORTANT

### I-1 `NewMQTracerProvider` 委托后路由语义变化（真 bug，建议修复）
- 位置：`zap/exporter_mq.go:234`
- 问题：改为转调 `SetupTracerProviderWithMQ(cfg, mqCfg)` 后，路由由 `cfg.Exporter` 决定。旧实现无条件装配 MQ exporter；现在若调用方传 `Exporter: ""` 或 `"jaeger"`，`FillDefaults` 会把空值填为 `jaeger`，`NewMQTracerProvider` 实际装配出 Jaeger exporter —— 与函数名语义（"MQ TracerProvider"）及旧行为不符。
- 影响面：库内唯一调用方 `example/main.go:230` 显式传了 `Exporter: "mq"`，不受影响；但外部既有调用方若未设置 Exporter 会静默改道。
- 建议修复（1 行）：`NewMQTracerProvider` 委托前强制 `cfg.Exporter = "mq"`（保持旧行为）。
- 处理状态：**已修复**（close 阶段 2 用户决策"修复后归档"）：`zap/exporter_mq.go` 委托前 `cfg.Exporter = "mq"`，补回归测试 `TestNewMQTracerProviderForcesMQRouting`（Exporter ∈ {"", "jaeger", "stdout", "mq"} 均断言 `*mqSpanExporter`），`go test -race -count=1 ./...` 全绿

### I-2 `Builder` 全局副作用（设计建议，不阻塞）
- 位置：`builder.go`（`RegisterFieldNormalizer` 全局注入 + `L()` 依赖 GetGlobal）
- 问题：归一化器为进程级全局变量，多后端注册场景存在覆盖风险；`L()` 语义依赖全局 Logger。
- 建议：后续版本提供 `BuilderFor(l Logger)` 纯实例化入口；当前使用模式下（单一 zap 后端）风险可接受。

### I-3 `Logger` 接口扩展的版本策略（发布注意项）
- 位置：`logx.go`（接口新增 6 个方法）
- 问题：所有 `logx.Logger` 实现方需补 6 个方法，属 breaking change，发版时应走 MAJOR 或提供 adapter。
- 处理状态：记录为发布说明，不阻塞归档。

## MINOR（10 项，均不阻塞，摘要）
- `dedupeTags` 对大小写敏感（"Slow" 与 "slow" 视为不同 tag）—— 记录即可
- `TypedField` 对非导出结构体走 `Any` 反射兜底，输出为 `zap.Reflected` 字符串 —— 符合 spec "其它→反射兜底"
- `bad-field.N` 值格式（fmt 拼接 key 与 value）可读性一般 —— spec 仅要求记录原始信息，合规
- `TestKafkaDirectPushIntegration` 条件测试默认 skip，CI 无 Kafka 覆盖 —— 已知边界（docker-compose 手动验证）
- README「导出目标」表中 jaeger 标注为默认，与 `FillDefaults` 一致，但 `Config.FillDefaults` 与 `OTelConfig.FillDefaults` 两处默认值需保持同步 —— 已确认一致
- example 中 `runMQDemo` 默认不启动（开关控制），kafka 演示需手动开 —— 文档已说明
- （其余 MINOR 项为命名/注释风格建议，见审查输出记录）

## 规格一致性（阶段 3 预检）
- `openspec validate add-chained-logger-api --strict`：valid
- spec 9 条 requirement 核对见 `verification.md`（全部满足）
- 三方文档 checkbox 全 `[x]`（1.2 补勾于 close 阶段，证据：`config_test.go TestFillDefaults_ServiceDefaults` PASS）

## 阶段 3：规格一致性三维对照（close 执行记录）

| 维度 | 结果 |
|------|------|
| **Completeness** | ✅ tasks.md 1.1–8.2 全 `[x]`；plan-ready.md Task 1–8 全"任务完成"；superpowers plan 各 Step 全 `[x]`；9 条 spec requirement 均有实现文件 + 测试证据（verification.md 逐条列出） |
| **Correctness** | ✅ 实现与 spec.md 各 Scenario 一致：无修饰直调/链式输出/中文 key/混合类型/落单字符串追加消息/坏键 `bad-field.N`/回退链/实例 Chain/ctx span 注入（方案 A 静默降级）/`logx.Fields` + `zap.*` 混用/kafka-mq 别名/缺 broker fail fast（错误含 "brokers"）——均有对应 PASS 测试 |
| **Coherence** | ✅ 代码遵循 design.md 决策：根包不 import `zap`（归一化器经 `RegisterFieldNormalizer` 注入，zap init 注册）；span 注入走 `InjectSpanAttrs` 接口断言（noopLogger 无实现静默跳过）；导出统一在 `SetupTracerProviderWithMQ`；`NewMQTracerProvider` 强制 mq 路由保持语义（I-1 修复后） |

CLI 校验：`openspec validate add-chained-logger-api --strict` → **valid**（修复 I-1 后复跑确认）
