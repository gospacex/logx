// logx/zap/fields.go
// 键值交替变参 → []zap.Field 归一化（无构造器字段语法）
// 规则：
//   - string 与任意值成对：string 为 key，值按 Go 类型映射
//   - logx.Fields（map[string]any）元素：展开为各键值对
//   - zap.Field 元素：原样保留（兼容既有构造函数写法）
//   - key 位置非 string：该对编码为 bad-field.<N>，不 panic
//   - 末尾落单 string：作为追加消息片段返回（调用方以 " " 连接进 msg）
//   - 末尾落单非 string：编码为 arg.<N> 兜底
package zap

import (
	"fmt"
	"time"

	"github.com/gospacex/logx"
	"go.uber.org/zap"
)

// NormalizedAny 将 ...any 键值变参归一化为 []zap.Field
// 返回 (fields, 追加消息片段)
func NormalizedAny(vv ...any) ([]zap.Field, string) {
	fields := make([]zap.Field, 0, len(vv)/2+1)
	i := 0
	for i < len(vv) {
		switch t := vv[i].(type) {
		case string:
			if i+1 < len(vv) {
				fields = append(fields, typedField(t, vv[i+1]))
				i += 2
				continue
			}
			return fields, t // 落单 string → 追加消息片段
		case logx.Fields:
			for k, val := range t {
				fields = append(fields, typedField(k, val))
			}
			i++
		case zap.Field:
			fields = append(fields, t)
			i++
		default:
			if i+1 < len(vv) {
				fields = append(fields, zap.Any(
					fmt.Sprintf("bad-field.%d", i),
					fmt.Sprintf("%v -> %v", vv[i], vv[i+1])))
				i += 2
				continue
			}
			fields = append(fields, zap.Any(fmt.Sprintf("arg.%d", i), vv[i]))
			i++
		}
	}
	return fields, ""
}

// typedField 按 value 的 Go 类型映射到 zap.Field
func typedField(key string, v any) zap.Field {
	switch x := v.(type) {
	case string:
		return zap.String(key, x)
	case int:
		return zap.Int(key, x)
	case int8:
		return zap.Int8(key, x)
	case int16:
		return zap.Int16(key, x)
	case int32:
		return zap.Int32(key, x)
	case int64:
		return zap.Int64(key, x)
	case uint:
		return zap.Uint(key, x)
	case uint8:
		return zap.Uint8(key, x)
	case uint16:
		return zap.Uint16(key, x)
	case uint32:
		return zap.Uint32(key, x)
	case uint64:
		return zap.Uint64(key, x)
	case float32:
		return zap.Float32(key, x)
	case float64:
		return zap.Float64(key, x)
	case bool:
		return zap.Bool(key, x)
	case time.Duration:
		return zap.Duration(key, x)
	case time.Time:
		return zap.Time(key, x)
	case error:
		return zap.Error(x)
	case nil:
		return zap.Skip()
	default:
		return zap.Any(key, v)
	}
}
