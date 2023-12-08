package hook

import (
	"context"
	"time"

	"github.com/Pius-x/xorm/zapkey"
	"github.com/qustavo/sqlhooks/v2"
	"go.uber.org/zap"
)

// make sure ZapHook implement all sqlhooks interface.
var _ interface {
	sqlhooks.Hooks
	sqlhooks.OnErrorer
} = (*ZapHook)(nil)

// ZapHook using zap log sql query and args.
type ZapHook struct {
	*zap.Logger
}

var PrintSql bool

// durationKey is context.valueCtx Key.
type durationKey struct{}

func buildSqlLog(query string, args ...any) []zap.Field {
	if len(args) == 0 {
		return []zap.Field{zap.String(zapkey.Query, query)}
	}
	return []zap.Field{zap.String(zapkey.Query, query), zap.Any(zapkey.Args, args)}
}

func (z *ZapHook) Before(ctx context.Context, _ string, _ ...any) (context.Context, error) {
	if z == nil || z.Logger == nil {
		return ctx, nil
	}

	ctx = context.WithValue(ctx, (*durationKey)(nil), time.Now())
	return ctx, nil
}

func (z *ZapHook) After(ctx context.Context, query string, args ...any) (context.Context, error) {
	if z == nil || z.Logger == nil {
		return ctx, nil
	}

	var latency time.Duration
	var durationField = zap.Skip()
	if v, ok := ctx.Value((*durationKey)(nil)).(time.Time); ok {
		latency = time.Since(v)
		durationField = zap.Int64(zapkey.Latency, latency.Milliseconds())
	}

	// 慢sql
	if latency > time.Second {
		z.With(durationField).Warn("Sql Exec Slow", buildSqlLog(query, args...)...)
	} else if PrintSql {
		z.With(durationField).Info("Sql Exec", buildSqlLog(query, args...)...)
	}

	return ctx, nil
}

func (z *ZapHook) OnError(_ context.Context, err error, query string, args ...any) error {
	if z == nil || z.Logger == nil {
		return nil
	}

	z.With(zapkey.ErrField(err)...).Error("Sql Exec Err", buildSqlLog(query, args...)...)
	return nil
}
