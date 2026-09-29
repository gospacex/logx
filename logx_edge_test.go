// logx/logx_edge_test.go
// 根包边界补全：Int/Err 字段构造器、noopLogger 全接口、全局便捷函数
// 终止级别（stub 实现下安全）、Builder 末端 Debug/DPanic 与 resolveAttrs 回退链
package logx

import (
	"context"
	"errors"
	"testing"

	"go.uber.org/zap/zapcore"
)

// TestFieldConstructorsIntErr Int/Err 构造器契约（对齐 zap 语义）
func TestFieldConstructorsIntErr(t *testing.T) {
	f := Int("n", 42)
	if f.Key != "n" || f.Type != zapcore.Int64Type || f.Integer != 42 {
		t.Errorf("Int = %+v, want key=n Int64Type=42", f)
	}

	e := Err(errors.New("boom"))
	// zap.Error 契约：键固定 "error"，值存 Interface
	if e.Key != "error" || e.Type != zapcore.ErrorType {
		t.Errorf("Err = key=%s type=%v, want key=error ErrorType", e.Key, e.Type)
	}
	if e.Interface.(error).Error() != "boom" {
		t.Errorf("Err.Interface = %v, want 底层 error", e.Interface)
	}
}

// TestGetConfigUninitialized 未 ReadConfig 时 GetConfig 返回零值句柄（不 panic）
func TestGetConfigUninitialized(t *testing.T) {
	_ = GetConfig() // 契约：nil/零值句柄，不 panic
}

// TestNoopLoggerFull noopLogger 全接口调用（含 Chain/Close/InjectSpanAttrs 零覆盖方法）
func TestNoopLoggerFull(t *testing.T) {
	var _ Logger = noopLogger{}
	n := noopLogger{}
	n.Debug("m")
	n.Info("m")
	n.Warn("m")
	n.Error("m")
	n.DPanic("m")
	n.Panic("m")
	n.Fatal("m")
	n.DebugContext(context.Background(), "m")
	n.InfoContext(context.Background(), "m")
	n.WarnContext(context.Background(), "m")
	n.ErrorContext(context.Background(), "m")
	n.DPanicContext(context.Background(), "m")
	n.PanicContext(context.Background(), "m")
	n.FatalContext(context.Background(), "m")
	if l := n.With(Int("k", 1)); l == nil {
		t.Error("With 应返回非 nil Logger")
	}
	if l := n.Named("x"); l == nil {
		t.Error("Named 应返回非 nil Logger")
	}
	if n.Level() != zapcore.ErrorLevel {
		t.Errorf("noop Level = %v, want ErrorLevel", n.Level())
	}
	if err := n.Sync(); err != nil {
		t.Errorf("noop Sync = %v, want nil", err)
	}
	if err := n.Close(); err != nil {
		t.Errorf("noop Close = %v, want nil", err)
	}
	if n.Zap() != nil {
		t.Error("noop Zap = 非 nil, want nil")
	}
	if b := n.Chain(); b == nil {
		t.Error("noop Chain = nil, want 可用 Builder")
	}
	// 无副作用调用
	n.InjectSpanAttrs(context.Background(), "s", "o", nil)
}

// TestGlobalFunctionsTerminatingLevels 全局便捷函数 终止级别 Context 通路
// （全局为 stubLogger 时终止级别也是 no-op，测试进程安全）
func TestGlobalFunctionsTerminatingLevels(t *testing.T) {
	SetGlobal(&stubLogger{})
	defer CloseGlobal()

	ctx := context.Background()
	DPanicContext(ctx, "dpanic m")
	PanicContext(ctx, "panic m")
	FatalContext(ctx, "fatal m")
}

// TestGlobalFunctionsNonTerminatingContext 全局便捷函数非终止级别 Context 通路：
// DebugContext/WarnContext/ErrorContext（InfoContext 由 TestGlobalFunctions 覆盖，
// 终止级别 Context 由 TestGlobalFunctionsTerminatingLevels 覆盖）
// 全局为 stubLogger 时纯转发、无副作用，测试进程安全
func TestGlobalFunctionsNonTerminatingContext(t *testing.T) {
	SetGlobal(&stubLogger{})
	defer CloseGlobal()

	ctx := context.Background()
	DebugContext(ctx, "dbg m", String("k", "v"))
	WarnContext(ctx, "warn m", Int("code", 5))
	ErrorContext(ctx, "err m")
}

// TestBuilderTerminalsDebugDPanic 末端 Debug/DPanic 方法 → emit 级别分支
func TestBuilderTerminalsDebugDPanic(t *testing.T) {
	SetGlobal(&stubLogger{})
	defer CloseGlobal()

	L().Debug("debug terminal")
	L().DPanic("dpanic terminal")
}

// TestBuilderTerminalError 末端 Error 方法 → emit 的 ErrorLevel 分支
func TestBuilderTerminalError(t *testing.T) {
	SetGlobal(&stubLogger{})
	defer CloseGlobal()

	L().Error("error terminal", "k", "v")
}

// TestResolveAttrsNilLogger resolveAttrs 的 l==nil 早退路径：
// 显式 Service/Operation 与 Tags 去重结果原样返回（不回退链）
func TestResolveAttrsNilLogger(t *testing.T) {
	b := &Builder{ctx: context.Background(), service: "s", operation: "o", tags: []string{"a", "a"}}
	svc, op, tags := b.resolveAttrs()
	if svc != "s" || op != "o" {
		t.Errorf("resolveAttrs(l=nil) = svc=%q op=%q, want 显式值 s/o", svc, op)
	}
	if len(tags) != 1 || tags[0] != "a" {
		t.Errorf("tags = %v, want 去重后 [a]", tags)
	}

	// 全缺省 + l=nil：三返回均零值
	b2 := &Builder{ctx: context.Background()}
	if svc, op, tags := b2.resolveAttrs(); svc != "" || op != "" || len(tags) != 0 {
		t.Errorf("全缺省 resolveAttrs = %q/%q/%v, want 全空", svc, op, tags)
	}
}

// fullStubLogger 回退链数据源：DefaultOperation/ServiceName 有值
type fullStubLogger struct{ stubLogger }

func (*fullStubLogger) DefaultOperation() string { return "op-x" }
func (*fullStubLogger) ServiceName() string      { return "svc-x" }

// TestBuilderResolveAttrsFallbacks resolveAttrs 回退链：
// 显式空 → DefaultService("") → ServiceName 兜底；operation → DefaultOperation
func TestBuilderResolveAttrsFallbacks(t *testing.T) {
	b := &Builder{ctx: context.Background(), l: &fullStubLogger{}}
	svc, op, tags := b.resolveAttrs()
	if svc != "svc-x" {
		t.Errorf("service = %q, want ServiceName 兜底 svc-x", svc)
	}
	if op != "op-x" {
		t.Errorf("operation = %q, want DefaultOperation 兜底 op-x", op)
	}
	if len(tags) != 0 {
		t.Errorf("tags = %v, want 空（全缺省）", tags)
	}
}
