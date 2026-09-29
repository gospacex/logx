// logx/zap/otel_edge_test.go
// fieldToAttribute 全类型映射 + parseLevel 全取值补全
package zap

import (
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap/zapcore"
)

// strVal 自定义 Stringer（StringerType 分支经 interface{ String() string } 断言）
type strVal string

func (s strVal) String() string { return string(s) }

// TestFieldToAttributeAllTypes 全类型分支：数值窄化（Int32/16/8、Uint8/16/32/64）、
// 浮点位数（Float64/Float32）、时间（Time/TimeFull）、字符串族（Stringer/Error/ByteString）、
// 反射兜底（string 与非 string 走 stringify）、未知类型不转换
func TestFieldToAttributeAllTypes(t *testing.T) {
	// TimeType 分支经 time.Unix(0, nano) 还原为本地时区，期望值走同路径计算
	tsNano := time.Date(2026, 9, 13, 1, 2, 3, 0, time.UTC).UnixNano()
	wantTS := time.Unix(0, tsNano).Format(time.RFC3339Nano)
	cases := []struct {
		name   string
		f      zapcore.Field
		want   string // Emit 期望值；wantOK=false 时断言不转换
		wantOK bool
	}{
		{"duration", zapcore.Field{Key: "d", Type: zapcore.DurationType, Integer: int64(2 * time.Second)}, "2000000000", true},
		{"float32", zapcore.Field{Key: "f32", Type: zapcore.Float32Type, Integer: 0x40000000}, "2", true},
		{"int32-narrow", zapcore.Field{Key: "i32", Type: zapcore.Int32Type, Integer: 42}, "42", true},
		{"int16-narrow", zapcore.Field{Key: "i16", Type: zapcore.Int16Type, Integer: 42}, "42", true},
		{"int8-narrow", zapcore.Field{Key: "i8", Type: zapcore.Int8Type, Integer: 42}, "42", true},
		{"uint64", zapcore.Field{Key: "u64", Type: zapcore.Uint64Type, Integer: 42}, "42", true},
		{"uint32", zapcore.Field{Key: "u32", Type: zapcore.Uint32Type, Integer: 42}, "42", true},
		{"uint16", zapcore.Field{Key: "u16", Type: zapcore.Uint16Type, Integer: 42}, "42", true},
		{"uint8", zapcore.Field{Key: "u8", Type: zapcore.Uint8Type, Integer: 42}, "42", true},
		{"time-rfc3339", zapcore.Field{Key: "ts", Type: zapcore.TimeType, Integer: tsNano}, wantTS, true},
		{"time-full", zapcore.Field{Key: "tsf", Type: zapcore.TimeFullType, Interface: time.Date(2026, 9, 13, 1, 2, 3, 0, time.UTC)}, "2026-09-13T01:02:03Z", true},
		{"bytestring", zapcore.Field{Key: "bs", Type: zapcore.ByteStringType, Interface: []byte("bin")}, "bin", true},
		{"stringer", zapcore.Field{Key: "sg", Type: zapcore.StringerType, Interface: strVal("sg-val")}, "sg-val", true},
		{"error", zapcore.Field{Key: "e", Type: zapcore.ErrorType, Interface: errors.New("boom")}, "boom", true},
		{"reflect-string", zapcore.Field{Key: "rs", Type: zapcore.ReflectType, Interface: "str-val"}, "str-val", true},
		// 非 string 的反射值走 stringify 兜底：无 String() 方法 → 字面量 "nil"
		{"reflect-nonstring", zapcore.Field{Key: "rn", Type: zapcore.ReflectType, Interface: 12345}, "nil", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kv, ok := fieldToAttribute(tc.f)
			if !ok {
				t.Fatalf("fieldToAttribute(%s) = (zero, false), want 可转换", tc.name)
			}
			if got := kv.Value.Emit(); got != tc.want {
				t.Errorf("%s emit = %q, want %q", tc.name, got, tc.want)
			}
			if kv.Key != attribute.Key(tc.f.Key) {
				t.Errorf("%s key = %v, want %v", tc.name, kv.Key, tc.f.Key)
			}
		})
	}

	// 未知类型 / 无法转换的类型 → (zero, false)
	negCases := []struct {
		name string
		f    zapcore.Field
	}{
		{"unknown-type", zapcore.Field{Key: "x", Type: zapcore.UnknownType, String: "v"}},
		{"timefull-wrong-iface", zapcore.Field{Key: "x", Type: zapcore.TimeFullType, Interface: "not-time"}},
		{"error-wrong-iface", zapcore.Field{Key: "x", Type: zapcore.ErrorType, Interface: 42}},
		{"stringer-nil", zapcore.Field{Key: "x", Type: zapcore.StringerType, Interface: 42}},
		{"reflect-nil-iface", zapcore.Field{Key: "x", Type: zapcore.ReflectType, Interface: nil}},
	}
	for _, nc := range negCases {
		if _, ok := fieldToAttribute(nc.f); ok {
			t.Errorf("%s: 应不可转换（want ok=false）", nc.name)
		}
	}
}

// TestParseLevelAllValues 补全既有 TestParseLevel 未覆盖的 dpanic/panic/fatal + 大小写
func TestParseLevelAllValues(t *testing.T) {
	cases := []struct {
		in   string
		want zapcore.Level
	}{
		{"dpanic", zapcore.DPanicLevel},
		{"panic", zapcore.PanicLevel},
		{"fatal", zapcore.FatalLevel},
		{"", zapcore.InfoLevel},
		{"DEBUG", zapcore.InfoLevel}, // 大小写敏感：未知值回退 info
		{"Debug", zapcore.InfoLevel},
	}
	for _, tc := range cases {
		if got := parseLevel(tc.in); got != tc.want {
			t.Errorf("parseLevel(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}
