// logx/example/03-otel-tracing/main.go
// 用例 03：OTel 调用链集成（日志 → Span Event，零代码依赖 docker，只需 compose 起 jaeger）
//
// 前置条件：
//
//	cd logx/example && docker compose up -d   # 仅需要 Jaeger (UI 26686 / collector 24268)
//
// 运行（双模式初始化，演示 ReadConfig）：
//
//	go run .                                    # 默认：程序内构造 cfg（无需 yaml）
//	HUBX_CONFIG=$PWD/config.yaml go run .       # 或：读取 config.yaml（logx: 段）
//	LOGX_JAEGER_ENDPOINT=http://host:24268/api/traces go run .   # 覆盖 Jaeger 端点
//
// 覆盖功能矩阵：
//
//	§1 双模式配置：HUBX_CONFIG 环境变量存在 → logx.ReadConfig() + GetGlobal()；否则程序内构造
//	§2 TracerProvider 装配（jaeger exporter）+ 各级别日志入链（Span Event）
//	§3 子 span（deduct-stock）内日志随父链一并导出
//
// 验证：open http://localhost:26686 搜索服务 logx-demo-otel
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gospacex/logx"
	_ "github.com/gospacex/logx/zap" // 注册默认 provider（空白导入）
	zapimpl "github.com/gospacex/logx/zap"
	"go.uber.org/zap"
)

func main() {
	cfg := loadConfig()

	// 可选覆盖：LOGX_JAEGER_ENDPOINT
	if ep := os.Getenv("LOGX_JAEGER_ENDPOINT"); ep != "" {
		cfg.OTel.Jaeger.Endpoint = ep
	}

	logger, err := logx.New(cfg)
	if err != nil {
		log.Fatalf("logx.New failed: %v", err)
	}
	defer logger.Close()
	logx.SetGlobal(logger)
	logger = logx.GetGlobal() // 演示：一切经由全局句柄

	logger.Info("logx initialized",
		zap.String("service", cfg.ServiceName),
		zap.String("config_source", configSource),
		zap.String("jaeger.endpoint", cfg.OTel.Jaeger.Endpoint),
	)

	// ===== TracerProvider 装配（jaeger 直推，defer Shutdown）=====
	setup, err := zapimpl.SetupTracerProvider(cfg.OTel)
	if err != nil {
		log.Fatalf("SetupTracerProvider failed: %v", err)
	}
	defer func() { _ = setup.Provider.Shutdown(context.Background()) }()

	// ===== 各级别日志入链（root span + 子 span）=====
	demoAllLevels(logger, setup)

	// ===== 强制刷出并提示查看 =====
	_ = setup.Provider.ForceFlush(context.Background())
	fmt.Printf("spans exported to Jaeger; open http://localhost:26686 and search service %q\n", cfg.ServiceName)
}

// configSource 记录本次 cfg 来源（程序内构造 / config.yaml）
var configSource = "in-program"

// loadConfig 双模式初始化：
//   - HUBX_CONFIG 指向 config.yaml → logx.ReadConfig() + Unmarshal("logx", &cfg)
//   - 否则 → 程序内构造（保证无 yaml 也能跑）
func loadConfig() logx.Config {
	if os.Getenv("HUBX_CONFIG") != "" {
		configSource = "HUBX_CONFIG config.yaml"
		if err := logx.ReadConfig(); err != nil {
			log.Fatalf("logx.ReadConfig failed (HUBX_CONFIG=%s): %v", os.Getenv("HUBX_CONFIG"), err)
		}
		var cfg logx.Config
		if err := logx.GetConfig().Unmarshal("logx", &cfg); err != nil {
			log.Fatalf("unmarshal logx config section: %v", err)
		}
		cfg.FillDefaults()
		return cfg
	}

	// 程序内构造（默认路径）
	cfg := logx.DefaultConfig()
	cfg.Level = "debug"
	cfg.ServiceName = "logx-demo-otel"
	cfg.OTel.Enabled = true
	cfg.OTel.Exporter = "jaeger"
	cfg.OTel.Jaeger.Endpoint = "http://localhost:24268/api/traces"
	cfg.OTel.Sampler = logx.SamplerConfig{Type: "parentbased_traceidratio", Ratio: 1.0}
	cfg.FillDefaults()
	return cfg
}

// demoAllLevels 演示 Debug/Info/Warn/Error/DPanic 各级别日志入链 + 子 span
func demoAllLevels(logger logx.Logger, setup *zapimpl.TracerSetup) {
	tracer := setup.Provider.Tracer("logx-demo-otel")

	// root span
	ctx, span := tracer.Start(context.Background(), "all-levels-demo")

	// Debug — 调试信息
	logger.DebugContext(ctx, "debug: validating order request",
		zap.String("order_id", "1001"),
		zap.String("user_id", "u-2001"),
	)

	// Info — 正常业务流程
	logger.InfoContext(ctx, "info: order placed",
		zap.String("order_id", "1001"),
		zap.Float64("amount", 99.5),
		zap.String("db.system", "mysql"),
		zap.String("db.statement", "INSERT INTO orders ..."),
	)
	time.Sleep(50 * time.Millisecond)

	// Warn — 可恢复异常
	logger.WarnContext(ctx, "warn: slow query detected",
		zap.String("db.system", "mysql"),
		zap.String("db.statement", "SELECT * FROM stock WHERE ..."),
		zap.Duration("db.latency", 800*time.Millisecond),
	)

	// 子 span：扣减库存（子 span 内打 2 条日志）
	childCtx, childSpan := tracer.Start(ctx, "deduct-stock")
	logger.DebugContext(childCtx, "debug: cache hit, key=stock:1001",
		zap.String("cachex.redis.command", "GET"),
		zap.String("cachex.redis.key", "stock:1001"),
	)
	logger.InfoContext(childCtx, "info: stock deducted",
		zap.String("cachex.redis.command", "DECR"),
		zap.String("cachex.redis.key", "stock:1001"),
		zap.Int("remaining", 49),
	)
	childSpan.End()

	// Error — 不可恢复错误（仍入链，便于排查）
	logger.ErrorContext(ctx, "error: payment gateway timeout",
		zap.String("payment.gateway", "stripe"),
		zap.String("error.message", "context deadline exceeded"),
	)

	// DPanic — 开发模式 panic（生产模式降级为 Error，仍入链）
	logger.DPanicContext(ctx, "dpanic: unexpected nil pointer in validator",
		zap.String("component", "order-validator"),
		zap.String("field", "shipping_address"),
	)

	span.End()
}
