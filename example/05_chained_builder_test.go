// logx/example/05_chained_builder_test.go
// 用例 05 测试：链式 Builder API 全特性（回退链 / 并集去重 / 归一化边界 / 静默降级）
//
// 使用共享捕获桩 + logx.Builder 真实实现（零外部依赖）验证 main.go 演示的契约：
//
//   - 全局 L() 直调：中文键 + 配置默认值兜底（log.service/log.operation/log.tags）
//   - 显式 Service/Operation/Tags 覆盖 + Tags 并集去重（链式在前保序）
//   - 归一化边界：logx.Fields 展开、zap.Field 混用、落单 string 追加消息、
//     坏键（非 string 键位）编码 bad-field.N 且不 panic
//   - 实例 Chain()：无 span 静默降级（不报错、不阻塞、日志照常输出）
//
// 注：活动 span 属性注入（main.go 步骤 4）依赖 TracerProvider 装配，
// 属用例 03 集成范畴，此处仅验证无 span 降级路径。
package main

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/gospacex/logx"
	_ "github.com/gospacex/logx/zap" // 注册 provider + 字段归一化器（与用例 05 一致）
)

// cfgStub 定制捕获桩：可配置 DefaultService/DefaultOperation/DefaultTags（Builder 回退链中间环节）
type cfgStub struct {
	*captureLogger
	svc string
	op  string
	tgs []string
}

func newCfgStub(svc, op string, tgs ...string) *cfgStub {
	return &cfgStub{captureLogger: newCaptureLogger(), svc: svc, op: op, tgs: tgs}
}

func (s *cfgStub) DefaultService() string   { return s.svc }
func (s *cfgStub) DefaultOperation() string { return s.op }
func (s *cfgStub) DefaultTags() []string    { return s.tgs }

// TestExample05GlobalFallbackChain §1：未显式 Service/Tags → 回退配置默认值
func TestExample05GlobalFallbackChain(t *testing.T) {
	stub := newCfgStub("demo-svc", "/api", "demo")
	logx.SetGlobal(stub)
	defer logx.CloseGlobal()

	// main.go 步骤 1：全局直调 + 中文键，未显式 Service/Tags → 走配置默认值兜底
	logx.L().Info("下单成功", "订单号", "1001", "金额", 99.5)

	e := stub.entriesSnapshot()[0]
	if e.msg != "下单成功" {
		t.Fatalf("日志 msg = %q, want 下单成功", e.msg)
	}
	if got := lookupVal(e, "log.service"); got != "demo-svc" {
		t.Errorf("log.service = %q, want demo-svc（DefaultService 兜底）", got)
	}
	if got := lookupVal(e, "log.operation"); got != "/api" {
		t.Errorf("log.operation = %q, want /api（DefaultOperation 兜底）", got)
	}
	if got := lookupVal(e, "log.tags"); got != "demo" {
		t.Errorf("log.tags = %q, want demo（DefaultTags 兜底）", got)
	}
	if got := lookupVal(e, "订单号"); got != "1001" {
		t.Errorf("中文键 订单号 = %q, want 1001", got)
	}
	if got := lookupVal(e, "金额"); got != "99.5" {
		t.Errorf("金额 = %q, want 99.5（float64 映射）", got)
	}
}

// TestExample05ExplicitOverrideAndTagDedup §2：显式 Service/Operation/Tags 覆盖 + 并集去重
func TestExample05ExplicitOverrideAndTagDedup(t *testing.T) {
	stub := newCfgStub("demo-svc", "/api", "demo")
	logx.SetGlobal(stub)
	defer logx.CloseGlobal()

	// main.go 步骤 2：Tags("demo","slow","mysql") 与 DefaultTags["demo"] 并集去重，链式在前保序
	logx.L().Service("order").Operation("/api/v1/order").Tags("demo", "slow", "mysql").
		Info("慢查询", "latency", 800.0, "sql语句", "select * from order")

	e := stub.entriesSnapshot()[0]
	if e.msg != "慢查询" {
		t.Fatalf("日志 msg = %q, want 慢查询", e.msg)
	}
	if got := lookupVal(e, "log.service"); got != "order" {
		t.Errorf("log.service = %q, want order（显式覆盖 DefaultService）", got)
	}
	if got := lookupVal(e, "log.operation"); got != "/api/v1/order" {
		t.Errorf("log.operation = %q, want /api/v1/order", got)
	}
	if got := lookupVal(e, "log.tags"); got != "demo,slow,mysql" {
		t.Errorf("log.tags = %q, want demo,slow,mysql（并集去重 + 链式保序）", got)
	}
}

// TestExample05NormalizationEdges §3：归一化边界（main.go 步骤 3 同序列）
func TestExample05NormalizationEdges(t *testing.T) {
	stub := newCfgStub("demo-svc", "/api", "demo")
	logx.SetGlobal(stub)
	defer logx.CloseGlobal()

	var panicked bool
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
			}
		}()
		logx.L().Warn("边界场景",
			logx.Fields{"user": "u-1", "amount": 99.5},
			zap.String("legacy", "x"),
			42, "落单非string会编码为arg字段", // 坏键 42（非 string 键位）→ bad-field.N
			"落单string追加到消息",
		)
	}()
	if panicked {
		t.Fatal("坏键触发 panic（契约：不 panic，编码为 bad-field.N）")
	}

	e := stub.entriesSnapshot()[0]
	// 落单 string 追加消息：emit 以 " " 连接进 msg
	if e.msg != "边界场景 落单string追加到消息" {
		t.Errorf("追加消息后 msg = %q, want %q", e.msg, "边界场景 落单string追加到消息")
	}
	// logx.Fields 展开
	if got := lookupVal(e, "user"); got != "u-1" {
		t.Errorf("Fields 展开 user = %q, want u-1", got)
	}
	// zap.Field 原样混用
	if got := lookupVal(e, "legacy"); got != "x" {
		t.Errorf("zap.Field legacy = %q, want x", got)
	}
	// 坏键：非 string 键位 → bad-field.N
	foundBad := false
	for _, k := range e.keys {
		if len(k) > 10 && k[:10] == "bad-field." {
			foundBad = true
			break
		}
	}
	if !foundBad {
		t.Errorf("坏键 42 未编码为 bad-field.N（keys: %v）", e.keys)
	}
}

// TestExample05NoSpanSilentFallback §4：无 span 静默降级——WithContext(背景 ctx) 不报错、
// 不阻塞、日志照常输出（main.go 步骤 5；真实 zap logger 走完整装配）
func TestExample05NoSpanSilentFallback(t *testing.T) {
	cfg := logx.DefaultConfig()
	cfg.Level = "debug"
	cfg.Output = "stdout"
	cfg.ServiceName = "logx-demo-builder"
	cfg.OTel.Enabled = true
	cfg.OTel.Exporter = "stdout" // 零外部依赖
	cfg.FillDefaults()

	logger, err := logx.New(cfg)
	if err != nil {
		t.Fatalf("logx.New: %v", err)
	}
	defer logger.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		// 无活动 span 的 ctx：属性注入静默降级（InjectSpanAttrs 契约），日志输出不受影响
		logger.Chain().WithContext(context.Background()).Info("无span静默降级", "k", "v")
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("无 span 路径阻塞（契约：静默降级，不阻塞）")
	}
}

// ---------- 辅助 ----------

// lookupVal 取桩记录某 key 的字段值字符串视图
func lookupVal(e capturedEntry, key string) string {
	for _, f := range e.fields {
		if f.Key == key {
			return fieldValString(f)
		}
	}
	return "<missing>"
}

// fieldValString 按 zapcore.Field 类型提取值的字符串视图（测试断言用）
func fieldValString(f logx.Field) string {
	switch f.Type {
	case zapcore.StringType:
		return f.String
	case zapcore.Float64Type:
		return strconv.FormatFloat(math.Float64frombits(uint64(f.Integer)), 'g', -1, 64)
	case zapcore.Float32Type:
		return strconv.FormatFloat(float64(math.Float32frombits(uint32(f.Integer))), 'g', -1, 32)
	case zapcore.Int8Type, zapcore.Int16Type, zapcore.Int32Type, zapcore.Int64Type,
		zapcore.Uint8Type, zapcore.Uint16Type, zapcore.Uint32Type, zapcore.Uint64Type:
		return strconv.FormatInt(f.Integer, 10)
	case zapcore.BoolType:
		return strconv.FormatBool(f.Integer == 1)
	default:
		return fmt.Sprintf("%v", f.Interface)
	}
}
