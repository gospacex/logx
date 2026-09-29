// logx/zap/fields_test.go
// 键值交替 ...any → []zap.Field 归一化单元测试（typedField 类型映射 + 边界形态）
package zap

import (
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/gospacex/logx"
)

// TestTypedField 按 value 的 Go 类型映射到 zap.Field（表驱动）。
// 契约（zap v1.27 + zap/fields.go 的 typedField）：
//   - 各整型保留独立宽度（Int8Type..Int64Type / Uint8Type..Uint64Type），
//     仅 int/uint 无宽度 → zap.Int/zap.Uint → Int64Type/Uint64Type
//   - error → zap.Error 改写键名为 "error"（zap.Error 的语义，非透传 key）
//   - nil → zap.Skip() 空键（编码时跳过，属预期 no-op 字段）
//   - 未知类型 → zap.Any 的快捷分支：[]int 具体类型 → ArrayMarshalerType；
//     匿名 struct → 落入 default → ReflectType
func TestTypedField(t *testing.T) {
	key := "k"
	now := time.Date(2026, 9, 13, 0, 0, 0, 0, time.UTC)
	errVal := errors.New("boom")
	keyed := func(k string, val any) zap.Field { return typedField(k, val) }
	cases := []struct {
		name string
		val  any
		typ  zapcore.FieldType
	}{
		{"string", "v", zapcore.StringType},
		{"int", 1, zapcore.Int64Type},
		{"int8", int8(1), zapcore.Int8Type},
		{"int16", int16(1), zapcore.Int16Type},
		{"int32", int32(1), zapcore.Int32Type},
		{"int64", int64(1), zapcore.Int64Type},
		{"uint", uint(1), zapcore.Uint64Type},
		{"uint8", uint8(1), zapcore.Uint8Type},
		{"uint16", uint16(1), zapcore.Uint16Type},
		{"uint32", uint32(1), zapcore.Uint32Type},
		{"uint64", uint64(1), zapcore.Uint64Type},
		{"float32", float32(1.5), zapcore.Float32Type},
		{"float64", 1.5, zapcore.Float64Type},
		{"bool", true, zapcore.BoolType},
		{"duration", time.Second, zapcore.DurationType},
		{"time", now, zapcore.TimeType},
		{"struct", struct{ A int }{1}, zapcore.ReflectType},
	}
	for _, tc := range cases {
		f := typedField(key, tc.val)
		if f.Key != key {
			t.Errorf("%s: key = %q, want %q", tc.name, f.Key, key)
		}
		if f.Type != tc.typ {
			t.Errorf("%s: type = %v, want %v", tc.name, f.Type, tc.typ)
		}
	}
	if f := typedField(key, errVal); f.Key != "error" || f.Type != zapcore.ErrorType {
		t.Errorf("error = key %q type %v, want key \"error\" + ErrorType（zap.Error 改写键名）", f.Key, f.Type)
	}
	if f := typedField(key, nil); f.Key != "" || f.Type != zapcore.SkipType {
		t.Errorf("nil = key %q type %v, want Skip 空键", f.Key, f.Type)
	}
	if f := keyed("k", "v"); f.Type != zapcore.StringType {
		t.Errorf("string via keyed = %v, want StringType", f.Type)
	}
	if f := keyed("k", []int{1}); f.Type != zapcore.ArrayMarshalerType {
		t.Errorf("[]int via keyed = %v, want ArrayMarshalerType（zap.Any 快捷分支）", f.Type)
	}
}

// TestNormalizedAnyMixed 全形态混用：键值对 + 中文 key + Fields 展开 + zap.Field 混用
// + 坏键 + 落单 string，验证相互之间不串位
func TestNormalizedAnyMixed(t *testing.T) {
	f, extra := NormalizedAny(
		"db", "127.0.0.1:3306",
		"sql语句", "select 1",
		logx.Fields{"m1": "a", "m2": 2},
		logx.String("legacy", "x"),
		42, "badpair", // 坏键（int 在 key 位）
		"trailing", // 落单 string
	)
	if extra != "trailing" {
		t.Fatalf("extra = %q, want trailing", extra)
	}
	have := map[string]zap.Field{}
	for _, ff := range f {
		have[ff.Key] = ff
	}
	for _, want := range []string{"db", "sql语句", "m1", "m2", "legacy", "bad-field.6"} {
		if _, ok := have[want]; !ok {
			t.Errorf("field %q missing: %v", want, keys(f))
		}
	}
	// 坏键值格式：<key 值> -> <value 值>（编号用原始下标 i；
	// 字符串值经 zap.Any 的 string 快捷分支，存入 .String 而非 .Interface）
	if s := have["bad-field.6"].String; s != "42 -> badpair" {
		t.Errorf("bad-field.6 = %q, want %q", s, "42 -> badpair")
	}
}

// TestNormalizedAnyLoneNonString 末尾落单非 string → arg.<i> 兜底
func TestNormalizedAnyLoneNonString(t *testing.T) {
	cases := []struct {
		name string
		in   []any
		key  string
	}{
		{"lone int", []any{42}, "arg.0"},
		{"pair then lone int", []any{"k", "v", 7}, "arg.2"},
		{"lone duration", []any{time.Second}, "arg.0"},
	}
	for _, tc := range cases {
		f, extra := NormalizedAny(tc.in...)
		if extra != "" {
			t.Errorf("%s: extra = %q, want empty", tc.name, extra)
		}
		if len(f) == 0 || f[len(f)-1].Key != tc.key {
			t.Errorf("%s: last key = %v, want %s", tc.name, keys(f), tc.key)
		}
	}
}

// TestNormalizedAnyEmpty 空输入不产字段不产消息
func TestNormalizedAnyEmpty(t *testing.T) {
	f, extra := NormalizedAny()
	if len(f) != 0 || extra != "" {
		t.Errorf("empty = fields %v extra %q, want 全空", keys(f), extra)
	}
}

// TestNormalizedAnyTrailingAfterBadKey 坏键后紧跟落单 string：坏键成对消费（i+=2），
// 落单 string 才追加消息——验证不串位
func TestNormalizedAnyTrailingAfterBadKey(t *testing.T) {
	f, extra := NormalizedAny(42, "x", "tail")
	if extra != "tail" {
		t.Fatalf("extra = %q, want tail（坏键对消费后落单 string 应作消息）", extra)
	}
	if len(f) != 1 || f[0].Key != "bad-field.0" {
		t.Fatalf("fields = %v, want 仅 bad-field.0", keys(f))
	}
}

func keys(fs []zap.Field) []string {
	out := make([]string, 0, len(fs))
	for _, f := range fs {
		out = append(out, f.Key)
	}
	return out
}
