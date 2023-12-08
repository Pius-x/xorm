package zapkey

import (
	"fmt"

	"go.uber.org/zap"
)

const (
	ErrMsg     = "err_msg"    // string 错误信息
	Stacktrace = "stacktrace" // string 错误堆栈
	Latency    = "latency"    // int 延时
	Query      = "query"      // string Sql query
	Args       = "args"       // any Sql query args
)

func ErrField(err error) []zap.Field {
	return []zap.Field{
		zap.String(ErrMsg, err.Error()),
		zap.String(Stacktrace, fmt.Sprintf("%+v", err)),
	}
}
