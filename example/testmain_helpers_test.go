// logx/example/testmain_helpers_test.go
// 用例测试共享辅助：捕获桩 Logger + 断言工具（各用例测试文件共用）
package main

import (
	"context"
	"sync"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/gospacex/logx"
)

// ---------- 捕获桩 Logger（记录各级别调用，零 IO） ----------

type capturedEntry struct {
	level  zapcore.Level
	name   string
	msg    string
	keys   []string
	fields []logx.Field
}

type captureLogger struct {
	mu      sync.Mutex
	entries []capturedEntry
	name    string
	closed  int
	synced  int
	panics  []string
}

var _ logx.Logger = (*captureLogger)(nil)

func newCaptureLogger() *captureLogger { return &captureLogger{} }

// capture 以 name/msg 记录一条日志（桩各级别方法均转发至此）
func (c *captureLogger) capture(lvl zapcore.Level, msg string, fields ...logx.Field) {
	keys := make([]string, 0, len(fields))
	for _, f := range fields {
		keys = append(keys, f.Key)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	fldCopy := make([]logx.Field, len(fields))
	copy(fldCopy, fields)
	c.entries = append(c.entries, capturedEntry{level: lvl, name: c.name, msg: msg, keys: keys, fields: fldCopy})
}

// entriesSnapshot 快照（避免并发读写）
func (c *captureLogger) entriesSnapshot() []capturedEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]capturedEntry, len(c.entries))
	copy(out, c.entries)
	return out
}

func (c *captureLogger) Debug(msg string, fields ...logx.Field) {
	c.capture(zapcore.DebugLevel, msg, fields...)
}
func (c *captureLogger) Info(msg string, fields ...logx.Field) {
	c.capture(zapcore.InfoLevel, msg, fields...)
}
func (c *captureLogger) Warn(msg string, fields ...logx.Field) {
	c.capture(zapcore.WarnLevel, msg, fields...)
}
func (c *captureLogger) Error(msg string, fields ...logx.Field) {
	c.capture(zapcore.ErrorLevel, msg, fields...)
}
func (c *captureLogger) DPanic(msg string, fields ...logx.Field) {
	c.capture(zapcore.DPanicLevel, msg, fields...)
}
func (c *captureLogger) Panic(msg string, fields ...logx.Field) {
	c.recordPanic("panic: " + msg)
}
func (c *captureLogger) Fatal(msg string, fields ...logx.Field) {
	c.recordPanic("fatal: " + msg)
}

func (c *captureLogger) recordPanic(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.panics = append(c.panics, s)
}

func (c *captureLogger) DebugContext(ctx context.Context, msg string, fields ...logx.Field) {
	c.Debug(msg, fields...)
}
func (c *captureLogger) InfoContext(ctx context.Context, msg string, fields ...logx.Field) {
	c.Info(msg, fields...)
}
func (c *captureLogger) WarnContext(ctx context.Context, msg string, fields ...logx.Field) {
	c.Warn(msg, fields...)
}
func (c *captureLogger) ErrorContext(ctx context.Context, msg string, fields ...logx.Field) {
	c.Error(msg, fields...)
}
func (c *captureLogger) DPanicContext(ctx context.Context, msg string, fields ...logx.Field) {
	c.DPanic(msg, fields...)
}
func (c *captureLogger) PanicContext(ctx context.Context, msg string, fields ...logx.Field) {
	c.Panic(msg, fields...)
}
func (c *captureLogger) FatalContext(ctx context.Context, msg string, fields ...logx.Field) {
	c.Fatal(msg, fields...)
}

func (c *captureLogger) With(fields ...logx.Field) logx.Logger {
	clone := newCaptureLogger()
	clone.entries = c.entriesSnapshot()
	clone.name = c.name
	clone.synced = c.synced
	return clone
}
func (c *captureLogger) Named(name string) logx.Logger {
	clone := newCaptureLogger()
	clone.entries = c.entriesSnapshot()
	clone.name = name
	clone.synced = c.synced
	return clone
}
func (c *captureLogger) Level() zapcore.Level { return zapcore.DebugLevel }
func (c *captureLogger) Sync() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.synced++
	return nil
}
func (c *captureLogger) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed++
	return nil
}
func (c *captureLogger) Zap() *zap.Logger { return nil }
func (c *captureLogger) Chain() *logx.Builder {
	return (&logx.Builder{}).Attach(c)
}
func (c *captureLogger) DefaultService() string   { return "" }
func (c *captureLogger) DefaultOperation() string { return "" }
func (c *captureLogger) DefaultTags() []string    { return nil }
func (c *captureLogger) ServiceName() string      { return "capture" }
func (c *captureLogger) InjectSpanAttrs(ctx context.Context, service, operation string, tags []string) {
}

// panicMessages 已记录的 panic/fatal 消息快照
func (c *captureLogger) panicMessages() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.panics))
	copy(out, c.panics)
	return out
}

// ---------- 断言辅助 ----------

// hasKey keys 中是否包含 want
func hasKey(keys []string, want string) bool {
	for _, k := range keys {
		if k == want {
			return true
		}
	}
	return false
}

// requireKey 断言 keys 含 want，否则 t.Errorf
func requireKey(t *testing.T, keys []string, want string) {
	t.Helper()
	if !hasKey(keys, want) {
		t.Errorf("字段 %q 未出现在日志中（现有 keys: %v）", want, keys)
	}
}

// byMsg 按消息建立索引
func entriesByMsg(entries []capturedEntry) map[string]capturedEntry {
	out := make(map[string]capturedEntry, len(entries))
	for _, e := range entries {
		out[e.msg] = e
	}
	return out
}
