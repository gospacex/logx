// logx/common/logger_edge_test.go
// common 包注入/提取边界形态单测（nil 接口、typed-nil logger、错类型值、嵌套注入）
package common

import (
	"context"
	"testing"

	"github.com/gospacex/logx"
)

// TestGetLoggerUninjectedFallsBackGlobal 未注入 / 错类型值 → 回退全局（非 nil）
func TestGetLoggerUninjectedFallsBackGlobal(t *testing.T) {
	// 未注入
	if l := GetLogger(context.Background()); l == nil {
		t.Error("GetLogger(未注入) = nil, want 全局 logger")
	}
	// 错类型值（loggerKey 下存了非 logx.Logger）→ 断言失败回退全局
	ctx := context.WithValue(context.Background(), loggerKey{}, 42)
	if l := GetLogger(ctx); l == nil {
		t.Error("GetLogger(错类型值) = nil, want 全局 logger")
	}
}

// TestGetLoggerNilInterfaceLogger 注入了 nil 接口 → 视为未注入，回退全局
// （logx.Logger(nil) 存入 ctx 后 any.(logx.Logger) 断言失败——nil 接口不匹配具体类型断言）
func TestGetLoggerNilInterfaceLogger(t *testing.T) {
	var l logx.Logger = nil
	ctx := WithLogger(context.Background(), l)
	if got, ok := TryGetLogger(ctx); ok || got != nil {
		t.Errorf("TryGetLogger(nil 接口) = (%v, %v), want (nil, false)", got, ok)
	}
	if gl := GetLogger(ctx); gl == nil {
		t.Error("GetLogger(nil 接口) = nil, want 全局 logger（nil 视为未注入）")
	}
}

// TestGetLoggerTypedNilLogger typed-nil（具体 nil 指针装入接口）：Go 语义下接口持有
// 「类型+零值」，类型断言成功且 l != nil 为 true——无法在不引入反射的前提下
// 识别 typed-nil，契约范围外；测试锁定当前行为，调用方应避免注入具体 nil 指针。
func TestGetLoggerTypedNilLogger(t *testing.T) {
	var nilStub *stubLogger // typed nil
	var l logx.Logger = nilStub
	ctx := WithLogger(context.Background(), l)
	if got, ok := TryGetLogger(ctx); !ok || got == nil {
		t.Errorf("TryGetLogger(typed-nil) = (%v, %v), want 视为已注入（Go 接口语义，无法区分 typed-nil）", got, ok)
	}
	if gl := GetLogger(ctx); gl == nil {
		t.Error("GetLogger(typed-nil) = nil, want 与 TryGetLogger 一致的注入值")
	}
}

// TestTryGetLoggerNilCtx nil ctx 直接 (nil, false)，不 panic
func TestTryGetLoggerNilCtx(t *testing.T) {
	if got, ok := TryGetLogger(nil); ok || got != nil {
		t.Errorf("TryGetLogger(nil) = (%v, %v), want (nil, false)", got, ok)
	}
}

// TestWithLoggerNested 嵌套注入：内层值覆盖外层；未再注入时取最近一层
func TestWithLoggerNested(t *testing.T) {
	outer := &stubLogger{name: "outer"}
	inner := &stubLogger{name: "inner"}
	ctx := WithLogger(context.Background(), outer)
	ctx = WithLogger(ctx, inner)

	if got, _ := TryGetLogger(ctx); got != logx.Logger(inner) {
		t.Errorf("嵌套后 = %v, want inner（最近注入优先）", got)
	}
	// 外层 ctx 不受内层影响（context 不可变）
	outerCtx := WithLogger(context.Background(), outer)
	if got, _ := TryGetLogger(outerCtx); got != logx.Logger(outer) {
		t.Errorf("outer ctx = %v, want outer（不应被嵌套影响）", got)
	}
}
