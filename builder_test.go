// logx/builder_test.go
// 链式 Builder 与回退链单元测试
package logx

import (
	"context"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// 注：...any 归一化器（zap.NormalizedAny）由 zap_test 外部测试包 blank import 触发注册，
// 内部测试（package logx）不能直接 import logx/zap（测试导入环），见 builder_ext_test.go

// captureLogger 记录末端输出（msg, fields）供断言
type captureLogger struct {
	msg  string
	flds []Field
	cfg  Config
}

func (c *captureLogger) Debug(m string, f ...Field)                             { c.msg, c.flds = m, f }
func (c *captureLogger) Info(m string, f ...Field)                              { c.msg, c.flds = m, f }
func (c *captureLogger) Warn(m string, f ...Field)                              { c.msg, c.flds = m, f }
func (c *captureLogger) Error(m string, f ...Field)                             { c.msg, c.flds = m, f }
func (c *captureLogger) DPanic(m string, f ...Field)                            { c.msg, c.flds = m, f }
func (c *captureLogger) Panic(m string, f ...Field)                             {}
func (c *captureLogger) Fatal(m string, f ...Field)                             {}
func (c *captureLogger) DebugContext(ctx context.Context, m string, f ...Field) { c.msg, c.flds = m, f }
func (c *captureLogger) InfoContext(ctx context.Context, m string, f ...Field)  { c.msg, c.flds = m, f }
func (c *captureLogger) WarnContext(ctx context.Context, m string, f ...Field)  { c.msg, c.flds = m, f }
func (c *captureLogger) ErrorContext(ctx context.Context, m string, f ...Field) { c.msg, c.flds = m, f }
func (c *captureLogger) DPanicContext(ctx context.Context, m string, f ...Field) {
	c.msg, c.flds = m, f
}
func (c *captureLogger) PanicContext(ctx context.Context, m string, f ...Field) {}
func (c *captureLogger) FatalContext(ctx context.Context, m string, f ...Field) {}
func (c *captureLogger) With(f ...Field) Logger                                 { return c }
func (c *captureLogger) Named(n string) Logger                                  { return c }
func (c *captureLogger) Level() zapcore.Level                                   { return zapcore.DebugLevel }
func (c *captureLogger) Sync() error                                            { return nil }
func (c *captureLogger) Close() error                                           { return nil }
func (c *captureLogger) Zap() *zap.Logger                                       { return nil }
func (c *captureLogger) Chain() *Builder                                        { return &Builder{l: c, ctx: context.Background()} }
func (c *captureLogger) DefaultService() string                                 { return c.cfg.DefaultService }
func (c *captureLogger) DefaultOperation() string                               { return c.cfg.DefaultOperation }
func (c *captureLogger) DefaultTags() []string                                  { return c.cfg.DefaultTags }
func (c *captureLogger) ServiceName() string                                    { return c.cfg.ServiceName }
func (c *captureLogger) InjectSpanAttrs(ctx context.Context, service, operation string, tags []string) {
}

func fieldStr(fld []Field, key string) (string, bool) {
	for _, f := range fld {
		if f.Key == key {
			return f.String, true
		}
	}
	return "", false
}

func TestBuilderServiceOperationTags(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ServiceName = "svc-x"
	cfg.DefaultTags = []string{"slow"}
	c := &captureLogger{cfg: cfg}
	SetGlobal(c)
	defer SetGlobal(nil)

	L().Service("order").Operation("/api/v1/order").Tags("db").Info("curd错误", "db", "127.0.0.1:3306")
	if c.msg != "curd错误" {
		t.Fatalf("msg = %q", c.msg)
	}
	if got, _ := fieldStr(c.flds, "log.service"); got != "order" {
		t.Errorf("log.service = %q, want order", got)
	}
	if got, _ := fieldStr(c.flds, "log.operation"); got != "/api/v1/order" {
		t.Errorf("log.operation = %q", got)
	}
	if got, _ := fieldStr(c.flds, "log.tags"); got != "db,slow" {
		t.Errorf("log.tags = %q, want db,slow（并集去重，链式在前）", got)
	}
	if got, _ := fieldStr(c.flds, "db"); got != "127.0.0.1:3306" {
		t.Errorf("kv field db = %q", got)
	}
}

func TestBuilderFallbackChain(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ServiceName = "svc-x"
	cfg.DefaultService = "order"

	// 无链式修饰 → 回退 DefaultService
	c := &captureLogger{cfg: cfg}
	SetGlobal(c)
	L().Warn("慢查询", "latency", 800.0)
	if got, _ := fieldStr(c.flds, "log.service"); got != "order" {
		t.Errorf("fallback log.service = %q, want order", got)
	}

	// 全空 → cfg.ServiceName 兜底
	c2 := &captureLogger{cfg: DefaultConfig()}
	c2.cfg.ServiceName = "svc-x"
	SetGlobal(c2)
	defer SetGlobal(nil)
	L().Info("m")
	if got, _ := fieldStr(c2.flds, "log.service"); got != "svc-x" {
		t.Errorf("service-name fallback log.service = %q, want svc-x", got)
	}
}

func TestBuilderTrailingStringJoinsMessage(t *testing.T) {
	c := &captureLogger{cfg: DefaultConfig()}
	SetGlobal(c)
	defer SetGlobal(nil)
	L().Warn("慢查询", "latency", 800.0, "2012.11.22")
	if c.msg != "慢查询 2012.11.22" {
		t.Errorf("msg = %q, want 慢查询 2012.11.22", c.msg)
	}
}

func TestBuilderBadKeyNoPanic(t *testing.T) {
	c := &captureLogger{cfg: DefaultConfig()}
	SetGlobal(c)
	defer SetGlobal(nil)
	L().Info("m", 42, "x")
	if c.msg != "m" {
		t.Fatalf("msg = %q", c.msg)
	}
	if _, ok := fieldStr(c.flds, "bad-field.0"); !ok {
		t.Errorf("bad-field.0 missing in %+v", c.flds)
	}
}
