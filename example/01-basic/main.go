// logx/example/01-basic/main.go
// 用例 01：零依赖基础日志（级别 / 字段构造器 / *Context / 派生 / 全局函数）
//
// 前置条件：无（纯 stdout 输出，不需要 docker）
// 运行：go run .          （从本目录）
//
//	或 go run ./example/01-basic （从 logx 根目录）
//
// 覆盖功能矩阵：
//
//	§1 统一接口：7 级别 + 各级 *Context、With/Named 派生、Level/Sync/Close
//	§2 输出：console 格式 + 字段构造器 String/Int/Any/Err
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/gospacex/logx"
	_ "github.com/gospacex/logx/zap" // 注册默认 provider（空白导入）
)

func main() {
	cfg := logx.DefaultConfig()
	cfg.Level = "debug"
	cfg.Format = "console" // 彩色级别 + 短 caller
	cfg.Output = "stdout"
	logger, err := logx.New(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "logx.New failed: %v\n", err)
		return
	}
	defer logger.Close()
	logx.SetGlobal(logger) // 全局函数（logx.Info 等）与 logx.L() 走该实例

	fmt.Println("== 1. 字段构造器（String/Int/Any/Err）==")
	logger.Info("order created",
		logx.String("order_id", "1001"),
		logx.Int("amount_cents", 9950),
		logx.Any("payload", map[string]any{"sku": "A-1", "qty": 2}),
		logx.Err(errors.New("mock error: stock insufficient")),
	)

	fmt.Println("\n== 2. 各级别（debug→error，dpanic 生产降级）==")
	ctx := context.Background()
	logger.DebugContext(ctx, "debug: checking cache", logx.String("cache.key", "hello"))
	logger.InfoContext(ctx, "info: request ok", logx.Int("status", 200))
	logger.WarnContext(ctx, "warn: slow query", logx.Any("latency", 800*time.Millisecond))
	logger.ErrorContext(ctx, "error: payment timeout", logx.String("gateway", "stripe"))
	logger.DPanicContext(ctx, "dpanic: unexpected nil", logx.String("field", "addr"))
	// Panic / Fatal 会终止进程，不可在正常流程演示：
	// logger.Panic("panic: 不可恢复")   // 输出后 panic
	// logger.Fatal("fatal: 退出进程")   // 输出后 os.Exit(1)

	fmt.Println("\n== 3. 派生 Logger（With 固定字段 / Named 前缀）==")
	derived := logger.With(logx.String("component", "order")).Named("order-svc")
	derived.Info("derived log line", logx.String("order_id", "1002"))

	fmt.Println("\n== 4. 全局便捷函数（使用全局 Logger）==")
	logx.Info("global info", logx.String("k", "v"))
	logx.Warn("global warn")

	fmt.Println("\n== 5. 资源管理（Level / Sync / Close）==")
	fmt.Printf("current level: %s\n", logger.Level())
	if err := logger.Sync(); err != nil {
		// stdout sink 不支持 Sync（"sync /dev/stdout: bad file descriptor" 属预期），
		// 文件输出（lumberjack）的 Sync 才真正有缓冲可刷
		fmt.Printf("sync skipped for stdout (expected): %v\n", err)
	}
	// defer logger.Close() 已在上方注册
}
