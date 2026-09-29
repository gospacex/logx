// logx/example/example01_basic_test.go
// 用例 01 测试：零依赖基础日志（级别 / 字段构造器 / *Context / 派生 / 全局函数）
//
// 镜像 example/01-basic/main.go 的用法断言，使用共享捕获桩做零 IO 验证。
//
//	覆盖：SetGlobal/GetGlobal、String/Int/Any/Err 构造器、7 级别 *Context、
//	With/Named 派生、全局便捷函数、Level/Sync/Close、Panic/Fatal 安全契约、
//	logx.New 默认通路（zap provider 已注册时装配成功）。
package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap/zapcore"

	"github.com/gospacex/logx"
	_ "github.com/gospacex/logx/zap" // 注册默认 provider（空白导入，与用例 01 一致）
)

// TestExample01BasicUseCase 用例 01：镜像 main.go 的 API 使用序列并断言行为契约
func TestExample01BasicUseCase(t *testing.T) {
	// §1 统一接口：桩实现 logx.Logger 契约 + 全局句柄装配
	logger := newCaptureLogger()
	logx.SetGlobal(logger)
	defer logx.CloseGlobal()
	if got := logx.GetGlobal(); got != logx.Logger(logger) {
		t.Fatalf("GetGlobal 与 SetGlobal 注入的实例不一致: %T", got)
	}

	// §2 字段构造器：String/Int/Any/Err（与 main.go 第一条日志同字段）
	logger.Info("order created",
		logx.String("order_id", "1001"),
		logx.Int("amount_cents", 9950),
		logx.Any("payload", map[string]any{"sku": "A-1", "qty": 2}),
		logx.Err(errors.New("mock error: stock insufficient")),
	)

	ctx := context.Background()
	logger.DebugContext(ctx, "debug: checking cache", logx.String("cache.key", "hello"))
	logger.InfoContext(ctx, "info: request ok", logx.Int("status", 200))
	logger.WarnContext(ctx, "warn: slow query", logx.Any("latency", 800*time.Millisecond))
	logger.ErrorContext(ctx, "error: payment timeout", logx.String("gateway", "stripe"))
	logger.DPanicContext(ctx, "dpanic: unexpected nil", logx.String("field", "addr"))

	// §3 派生 Logger：With 固定字段 + Named 前缀（桩 Named 返回共享底层记录的派生桩）
	derived := logger.With(logx.String("component", "order")).Named("order-svc")
	derived.Info("derived log line", logx.String("order_id", "1002"))

	// §4 全局便捷函数（经 SetGlobal 桩）
	logx.Info("global info", logx.String("k", "v"))
	logx.Warn("global warn")

	// §5 资源管理：Level / Sync / Close
	if logger.Level() != zapcore.DebugLevel {
		t.Errorf("Level() = %v, want %v", logger.Level(), zapcore.DebugLevel)
	}
	if err := logger.Sync(); err != nil {
		t.Errorf("Sync: %v", err)
	}
	if err := logger.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if logger.closed != 1 || logger.synced != 1 {
		t.Errorf("Sync/Close 调用次数 = synced:%d closed:%d, want 各 1", logger.synced, logger.closed)
	}

	// ---- 断言：级别、名称、字段 ----
	byMsg := entriesByMsg(logger.entriesSnapshot())
	cases := []struct {
		msg       string
		wantLevel zapcore.Level
		wantName  string
		wantKeys  []string
	}{
		{"order created", zapcore.InfoLevel, "", []string{"order_id", "amount_cents", "payload", "error"}},
		{"debug: checking cache", zapcore.DebugLevel, "", []string{"cache.key"}},
		{"info: request ok", zapcore.InfoLevel, "", []string{"status"}},
		{"warn: slow query", zapcore.WarnLevel, "", []string{"latency"}},
		{"error: payment timeout", zapcore.ErrorLevel, "", []string{"gateway"}},
		{"dpanic: unexpected nil", zapcore.DPanicLevel, "", []string{"field"}},
		{"global info", zapcore.InfoLevel, "", []string{"k"}},
		{"global warn", zapcore.WarnLevel, "", nil},
	}
	for _, tc := range cases {
		e, ok := byMsg[tc.msg]
		if !ok {
			t.Fatalf("日志 %q 未出现在桩记录中", tc.msg)
		}
		if e.level != tc.wantLevel {
			t.Errorf("%q: 级别 = %v, want %v", tc.msg, e.level, tc.wantLevel)
		}
		if e.name != tc.wantName {
			t.Errorf("%q: 名称 = %q, want %q", tc.msg, e.name, tc.wantName)
		}
		for _, k := range tc.wantKeys {
			requireKey(t, e.keys, k)
		}
	}

	// §3 断言：派生桩自身记录，name 前缀生效
	dentries := derived.(*captureLogger).entriesSnapshot()
	de, ok := entriesByMsg(dentries)["derived log line"]
	if !ok {
		t.Fatalf("派生桩未记录 %q", "derived log line")
	}
	if de.name != "order-svc" {
		t.Errorf("派生日志名称 = %q, want %q", de.name, "order-svc")
	}
	if de.level != zapcore.InfoLevel {
		t.Errorf("派生日志级别 = %v, want %v", de.level, zapcore.InfoLevel)
	}
	requireKey(t, de.keys, "order_id")
}

// TestExample01PanicFatalSafe 用例 01 边界：Panic/Fatal 在桩上记录而不终止测试进程
// （main.go 中二者被注释——真实 zap 实现会 panic/os.Exit；此测试验证桩的契约边界）
func TestExample01PanicFatalSafe(t *testing.T) {
	logger := newCaptureLogger()
	logger.Panic("panic: 不可恢复")
	logger.Fatal("fatal: 退出进程")

	msgs := logger.panicMessages()
	if len(msgs) != 2 {
		t.Fatalf("Panic/Fatal 记录数 = %d, want 2: %v", len(msgs), msgs)
	}
	for i, want := range []string{"panic: panic: 不可恢复", "fatal: fatal: 退出进程"} {
		if msgs[i] != want {
			t.Errorf("记录[%d] = %q, want %q", i, msgs[i], want)
		}
	}
}

// TestExample01NewDefaultPath 用例 01 默认通路：zap provider 已注册时 New 装配成功
func TestExample01NewDefaultPath(t *testing.T) {
	cfg := logx.DefaultConfig()
	cfg.Level = "debug"
	cfg.Format = "json"
	cfg.Output = "file"
	cfg.FilePath = t.TempDir() + "/app.log" // 防污染工作目录
	logger, err := logx.New(cfg)
	if err != nil {
		t.Fatalf("logx.New(zap provider 已注册) 应成功: %v", err)
	}
	if err := logger.Sync(); err != nil {
		t.Errorf("Sync: %v", err)
	}
	if err := logger.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
