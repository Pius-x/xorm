package utils

import (
	"reflect"
	"sort"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/pkg/errors"
	"golang.org/x/exp/constraints"
)

var ComplexType = []reflect.Kind{reflect.Struct, reflect.Map, reflect.Slice, reflect.Array}

// Concat 字符串拼接
func Concat(strArr ...string) string {

	var builder strings.Builder
	for i := 0; i < len(strArr); i++ {
		builder.WriteString(strArr[i])
	}

	return builder.String()
}

// Indirect 若是指针 返回指针指向的 Value Type ; 不是指针直接返回 Value Type
func Indirect(args any) (reflect.Value, reflect.Type) {
	val := reflect.ValueOf(args)
	typ := reflect.TypeOf(args)

	// 若是指针则取指针指向的值
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
		typ = val.Type()
	}

	return val, typ
}

// InSlice 判断指定值是否包含在切片中
func InSlice[T constraints.Ordered | reflect.Kind](needle T, haystack []T) bool {
	for i := 0; i < len(haystack); i++ {
		if haystack[i] == needle {
			return true
		}
	}

	return false
}

// StructToMap 利用反射将结构体转化为map
func StructToMap(structInfo any, tag string, stringify bool) (map[string]any, error) {

	val := reflect.Indirect(reflect.ValueOf(structInfo))

	smap := make(map[string]any)
	if err := ReflectToMap(smap, val, tag, stringify); err != nil {
		return nil, err
	}

	return smap, nil
}

// ReflectToMap 结构体反射转化成Map
func ReflectToMap(smap map[string]any, val reflect.Value, tag string, stringify bool) error {
	typ := val.Type()
	if typ.Kind() != reflect.Struct {
		return errors.New("expect struct")

	}
	for i := 0; i < typ.NumField(); i++ {
		typField := typ.Field(i)
		if !typField.IsExported() {
			continue
		}

		if typField.Anonymous && typField.Type.Kind() == reflect.Struct {
			if err := ReflectToMap(smap, val.Field(i), tag, stringify); err != nil {
				return err
			}
		}

		tagName := typ.Field(i).Tag.Get(tag)
		if tagName == "" {
			continue
		}

		if stringify && IsComplexType(typField.Type) {
			marshal, err := sonic.MarshalString(val.Field(i).Interface())
			if err != nil {
				return errors.WithStack(err)
			}
			smap[tagName] = marshal
		} else {
			smap[tagName] = val.Field(i).Interface()
		}
	}

	return nil
}

// IsComplexType 判断是否为复杂数据结构
func IsComplexType(typ reflect.Type) bool {
	kind := typ.Kind()
	if !InSlice(kind, ComplexType) {
		return false
	}

	// []byte 不作为复杂类型
	if (kind == reflect.Slice || kind == reflect.Array) && typ.Elem().Kind() == reflect.Uint8 {
		return false
	}

	return true
}

// MapKeys 获取Map中所有的Value,作为切片返回,可排序
func MapKeys[K constraints.Ordered, V any](m map[K]V, order bool) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	if order {
		sort.SliceStable(keys, func(i, j int) bool {
			return keys[i] < keys[j]
		})
	}
	return keys
}
