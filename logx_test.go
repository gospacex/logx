// logx/logx_test.go
// logx 根包单元测试
package logx

import (
	"context"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// stubProvider 测试用提供者
type stubProvider struct{}

func (stubProvider) Name() string { return "stub" }

func (stubProvider) New(cfg Config) (Logger, error) {
	return &stubLogger{}, nil
}

// stubLogger 最小 Logger 实现
type stubLogger struct{}

func (s *stubLogger) Debug(msg string, f ...Field)                                                {}
func (s *stubLogger) Info(msg string, f ...Field)                                                 {}
func (s *stubLogger) Warn(msg string, f ...Field)                                                 {}
func (s *stubLogger) Error(msg string, f ...Field)                                                {}
func (s *stubLogger) DPanic(msg string, f ...Field)                                               {}
func (s *stubLogger) Panic(msg string, f ...Field)                                                {}
func (s *stubLogger) Fatal(msg string, f ...Field)                                                {}
func (s *stubLogger) DebugContext(c context.Context, m string, f ...Field)                        {}
func (s *stubLogger) InfoContext(c context.Context, m string, f ...Field)                         {}
func (s *stubLogger) WarnContext(c context.Context, m string, f ...Field)                         {}
func (s *stubLogger) ErrorContext(c context.Context, m string, f ...Field)                        {}
func (s *stubLogger) DPanicContext(c context.Context, m string, f ...Field)                       {}
func (s *stubLogger) PanicContext(c context.Context, m string, f ...Field)                        {}
func (s *stubLogger) FatalContext(c context.Context, m string, f ...Field)                        {}
func (s *stubLogger) With(f ...Field) Logger                                                      { return s }
func (s *stubLogger) Named(n string) Logger                                                       { return s }
func (s *stubLogger) Level() zapcore.Level                                                        { return 0 }
func (s *stubLogger) Sync() error                                                                 { return nil }
func (s *stubLogger) Close() error                                                                { return nil }
func (s *stubLogger) Zap() *zap.Logger                                                            { return nil }
func (s *stubLogger) Chain() *Builder                                                             { return &Builder{ctx: context.Background()} }
func (s *stubLogger) DefaultService() string                                                      { return "" }
func (s *stubLogger) DefaultOperation() string                                                    { return "" }
func (s *stubLogger) DefaultTags() []string                                                       { return nil }
func (s *stubLogger) ServiceName() string                                                         { return "" }
func (s *stubLogger) InjectSpanAttrs(c context.Context, service, operation string, tags []string) {}

// TestRegisterProviderAndNew 测试 provider 注册与 New
func TestRegisterProviderAndNew(t *testing.T) {
	p := stubProvider{}
	RegisterProvider(p)
	if err := SetDefaultProvider("stub"); err != nil {
		t.Fatalf("SetDefaultProvider: %v", err)
	}

	l, err := New(DefaultConfig())
	if err != nil {
		t.Fatalf("New with stub provider: %v", err)
	}
	if l == nil {
		t.Fatal("New returned nil logger")
	}
}

// TestSetDefaultProviderUnknown 测试未注册 provider 报错
func TestSetDefaultProviderUnknown(t *testing.T) {
	if err := SetDefaultProvider("does-not-exist"); err == nil {
		t.Error("expected error for unknown provider")
	}
}

// TestGlobalFunctions 测试全局便捷函数不 panic
func TestGlobalFunctions(t *testing.T) {
	// 默认 provider 未注册（除非 zap 包被导入）——这里直接使用 stub
	RegisterProvider(stubProvider{})
	_ = SetDefaultProvider("stub")

	Debug("global debug")
	Info("global info", Field{Key: "k", String: "v"})
	Warn("global warn")
	Error("global error")
	InfoContext(context.Background(), "global ctx info")
	CloseGlobal()
}
