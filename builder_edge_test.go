// logx/builder_edge_test.go
// Builder 边界形态单测：回退链 nil 分支、Attach/WithContext 边界、
// NormalizeFields 未注册回退、dedupeTags 去重语义
package logx

import (
	"context"
	"testing"
)

// TestDedupeTags 并集去重语义：保序、跳过空串、nil 输入安全
func TestDedupeTags(t *testing.T) {
	cases := []struct {
		name string
		in   []string
		want []string
	}{
		{"nil", nil, []string{}},
		{"empty", []string{}, []string{}},
		{"dedupe-keep-order", []string{"b", "a", "b"}, []string{"b", "a"}},
		{"skip-empty-string", []string{"", "a", ""}, []string{"a"}},
		{"mixed", []string{"a", "", "b", "a", "c", "b"}, []string{"a", "b", "c"}},
	}
	for _, tc := range cases {
		got := dedupeTags(tc.in)
		if len(got) != len(tc.want) {
			t.Errorf("%s: dedupeTags = %v, want %v", tc.name, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("%s: dedupeTags = %v, want %v（保序去重）", tc.name, got, tc.want)
				break
			}
		}
	}
}

// TestBuilderEmitNilLoggerNoPanic 零值 Builder（l=nil）末端输出静默返回，不 panic
// （noopLogger.Chain() 即返回该形态）
func TestBuilderEmitNilLoggerNoPanic(t *testing.T) {
	b := &Builder{}            // l == nil
	b.Info("silent", "k", "v") // emit 首行 b.l==nil 直接返回
	b.Warn("silent")
	// 经 noopLogger.Chain() 同源验证
	n := noopLogger{}
	n.Chain().Error("silent")
}

// TestBuilderAttach 绑定与 nil 忽略
func TestBuilderAttach(t *testing.T) {
	a := &captureLogger{cfg: DefaultConfig()}
	b := &captureLogger{cfg: DefaultConfig()}

	// 先绑 a，再绑 b：末端输出走 b
	bd := (&Builder{ctx: context.Background()}).Attach(a).Attach(b)
	bd.Info("m", "k", "v")
	if a.msg != "" {
		t.Errorf("attach 覆盖后 a 仍收到输出: %q", a.msg)
	}
	if b.msg != "m" {
		t.Errorf("b.msg = %q, want m", b.msg)
	}

	// Attach(nil) 忽略，保留既有目标
	_ = bd.Attach(nil)
	b.msg = ""
	bd.Warn("m2")
	if b.msg != "m2" {
		t.Errorf("attach(nil) 不应改变目标: b.msg = %q, want m2", b.msg)
	}
}

// TestBuilderWithContextNil 传 nil ctx 保留原 ctx 不污染
func TestBuilderWithContextNil(t *testing.T) {
	orig := context.Background()
	b := (&Builder{ctx: orig}).WithContext(nil)
	if b.ctx != orig {
		t.Error("WithContext(nil) 不应替换既有 ctx")
	}
}

// TestNormalizeFieldsFallback 未注册归一化器时退化：每个值以 Any("arg") 包装
// （zap 包 init 已注册，测试中临时置 nil 再恢复，验证根包兜底分支）
func TestNormalizeFieldsFallback(t *testing.T) {
	old := normalizeFieldsFn
	normalizeFieldsFn = nil
	defer func() { normalizeFieldsFn = old }()

	f, extra := NormalizeFields("k", "v", 42)
	if extra != "" {
		t.Errorf("fallback extra = %q, want empty", extra)
	}
	if len(f) != 3 {
		t.Fatalf("fallback fields = %d, want 3（逐值 Any 包装）", len(f))
	}
	for i, fl := range f {
		if fl.Key != "arg" {
			t.Errorf("fallback field[%d] key = %q, want arg", i, fl.Key)
		}
	}
}

// TestNoopLoggerBasics noopLogger 各方法安全：级别 ErrorLevel、Zap() 为 nil、
// Sync/Close 无错误
func TestNoopLoggerBasics(t *testing.T) {
	n := noopLogger{}
	if lvl := n.Level(); lvl.String() != "error" {
		t.Errorf("noopLevel = %v, want error", lvl)
	}
	if n.Zap() != nil {
		t.Error("noopLogger.Zap() = non-nil, want nil")
	}
	if err := n.Sync(); err != nil {
		t.Errorf("noopLogger.Sync() = %v, want nil", err)
	}
	if err := n.Close(); err != nil {
		t.Errorf("noopLogger.Close() = %v, want nil", err)
	}
	if d := n.DefaultTags(); d != nil {
		t.Errorf("noopLogger.DefaultTags() = %v, want nil", d)
	}
}
