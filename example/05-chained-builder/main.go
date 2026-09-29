// logx/example/05-chained-builder/main.go
// 用例 05：链式 Builder API 全特性（零依赖，stdout exporter 可见 span 属性注入）
//
// 前置条件：无
// 运行：go run .
//
// 覆盖功能矩阵 §5（builder.go + zap/fields.go）：
//   - logx.L() 全局入口 / logger.Chain() 实例入口 / Attach 绑定
//   - 键值交替 ...any：中文 key、Go 类型映射、logx.Fields 展开、zap.Field 混用
//   - 归一化边界：落单 string 追加消息、落单非 string → arg.N、坏键 → bad-field.N（不 panic）
//   - Service/Operation/Tags 回退链（显式 > default_* > ServiceName）+ Tags 并集去重
//   - span 属性注入：ctx 含活动 span 时 SetAttributes（service.name/operation/log.tags），
//     exporter=stdout 时 span 以 pretty JSON dump，可见注入的属性
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/gospacex/logx"
	_ "github.com/gospacex/logx/zap" // 注册 provider + 字段归一化器
	zapimpl "github.com/gospacex/logx/zap"
	"go.uber.org/zap"
)

func main() {
	cfg := logx.DefaultConfig()
	cfg.Level = "debug"
	cfg.Format = "console"
	cfg.Output = "stdout"
	cfg.ServiceName = "logx-demo-builder"

	// 配置默认值兜底（Builder 回退链中间环节）
	cfg.DefaultService = "demo-svc"
	cfg.DefaultOperation = "/api"
	cfg.DefaultTags = []string{"demo"}

	// OTel stdout exporter：span 以 JSON dump 到终端，肉眼可见 SetAttributes 注入的属性
	cfg.OTel.Enabled = true
	cfg.OTel.Exporter = "stdout"
	cfg.FillDefaults()

	logger, err := logx.New(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "logx.New failed: %v\n", err)
		return
	}
	defer logger.Close()
	logx.SetGlobal(logger)

	setup, err := zapimpl.SetupTracerProvider(cfg.OTel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "setup tracer provider failed: %v\n", err)
		return
	}
	defer func() { _ = setup.Provider.Shutdown(context.Background()) }()

	// 1) 全局直调 + 中文键 + 配置默认值兜底
	//    未显式 Service/Tags → log.service=demo-svc、log.operation=/api、log.tags=demo
	logx.L().Info("下单成功", "订单号", "1001", "金额", 99.5)

	// 2) 全局链式：显式 Service/Operation/Tags（与 DefaultTags 并集去重，链式在前保序）
	logx.L().Service("order").Operation("/api/v1/order").Tags("demo", "slow", "mysql").
		Info("慢查询", "latency", 800.0, "sql语句", "select * from order")

	// 3) 归一化边界：logx.Fields 展开 + zap.Field 混用 + 落单 string 追加消息 + 坏键不 panic
	logx.L().Warn("边界场景",
		logx.Fields{"user": "u-1", "amount": 99.5},
		zap.String("legacy", "x"),
		42, "落单非string会编码为arg字段", // 坏键 42（非 string 位置）→ bad-field.2
		"落单string追加到消息",
	)

	// 4) 实例 Chain + 活动 span 属性注入（span dump 中应见 service.name/operation/log.tags）
	ctx, span := setup.Provider.Tracer("builder-demo").Start(context.Background(), "chained-demo")
	logger.Chain().WithContext(ctx).Service("order").Tags("slow").
		Warn("链式实例演示", "k", "v", "sql语句", "select * from order")
	span.End()
	_ = setup.Provider.ForceFlush(context.Background())

	// 5) 无 span 静默降级：WithContext(context.Background()) 不报错、不阻塞，日志照常输出
	logger.Chain().WithContext(context.Background()).Info("无span静默降级", "k", "v")
}
