// logx/common/logger_test.go
// logx/common 单元测试
package common

import (
	"context"
	"testing"

	"github.com/gospacex/logx"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// stubLogger 测试用最小 Logger
type stubLogger struct{ name string }

func (s *stubLogger) Debug(msg string, f ...logx.Field)                                           {}
func (s *stubLogger) Info(msg string, f ...logx.Field)                                            {}
func (s *stubLogger) Warn(msg string, f ...logx.Field)                                            {}
func (s *stubLogger) Error(msg string, f ...logx.Field)                                           {}
func (s *stubLogger) DPanic(msg string, f ...logx.Field)                                          {}
func (s *stubLogger) Panic(msg string, f ...logx.Field)                                           {}
func (s *stubLogger) Fatal(msg string, f ...logx.Field)                                           {}
func (s *stubLogger) DebugContext(c context.Context, m string, f ...logx.Field)                   {}
func (s *stubLogger) InfoContext(c context.Context, m string, f ...logx.Field)                    {}
func (s *stubLogger) WarnContext(c context.Context, m string, f ...logx.Field)                    {}
func (s *stubLogger) ErrorContext(c context.Context, m string, f ...logx.Field)                   {}
func (s *stubLogger) DPanicContext(c context.Context, m string, f ...logx.Field)                  {}
func (s *stubLogger) PanicContext(c context.Context, m string, f ...logx.Field)                   {}
func (s *stubLogger) FatalContext(c context.Context, m string, f ...logx.Field)                   {}
func (s *stubLogger) With(f ...logx.Field) logx.Logger                                            { return s }
func (s *stubLogger) Named(n string) logx.Logger                                                  { return &stubLogger{name: n} }
func (s *stubLogger) Level() zapcore.Level                                                        { return 0 }
func (s *stubLogger) Sync() error                                                                 { return nil }
func (s *stubLogger) Close() error                                                                { return nil }
func (s *stubLogger) Zap() *zap.Logger                                                            { return nil }
func (s *stubLogger) Chain() *logx.Builder                                                        { return &logx.Builder{} }
func (s *stubLogger) DefaultService() string                                                      { return "" }
func (s *stubLogger) DefaultOperation() string                                                    { return "" }
func (s *stubLogger) DefaultTags() []string                                                       { return nil }
func (s *stubLogger) ServiceName() string                                                         { return "" }
func (s *stubLogger) InjectSpanAttrs(c context.Context, service, operation string, tags []string) {}

func TestWithLoggerAndGetLogger(t *testing.T) {
	l := &stubLogger{name: "req"}
	ctx := WithLogger(context.Background(), l)

	got := GetLogger(ctx)
	if got == nil {
		t.Fatal("GetLogger returned nil")
	}
	if s, ok := got.(*stubLogger); !ok || s.name != "req" {
		t.Errorf("GetLogger = %v, want stubLogger{req}", got)
	}
}

func TestGetLoggerNilCtx(t *testing.T) {
	if l := GetLogger(nil); l == nil {
		t.Error("GetLogger(nil) should return global logger (non-nil)")
	}
}

func TestTryGetLogger(t *testing.T) {
	if _, ok := TryGetLogger(context.Background()); ok {
		t.Error("TryGetLogger on empty ctx should be false")
	}

	l := &stubLogger{name: "x"}
	ctx := WithLogger(context.Background(), l)
	if got, ok := TryGetLogger(ctx); !ok || got != logx.Logger(l) {
		t.Errorf("TryGetLogger = (%v, %v), want (%v, true)", got, ok, l)
	}
}
