package xorm

import (
	dbSql "database/sql"
	"fmt"
	"reflect"

	"github.com/Pius-x/xorm/sqlx_inherit"
	"github.com/Pius-x/xorm/utils"
	"github.com/bytedance/sonic"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/types"
	"github.com/pkg/errors"
)

type SqlxTabler interface {
	TableName() string
}

func (cli *Cli) toSqlxTablers(record any) ([]SqlxTabler, error) {
	if record == nil {
		return nil, errors.New("records is empty")
	}

	switch r := record.(type) {
	case SqlxTabler:
		return []SqlxTabler{r}, nil
	default:
		val, typ := utils.Indirect(record)
		if typ.Kind() != reflect.Slice {
			return nil, errors.New("expect slice")
		}

		// 遍历切片中的元素
		st := make([]SqlxTabler, 0, val.Len())
		for i := 0; i < val.Len(); i++ {
			if one, ok := val.Index(i).Interface().(SqlxTabler); !ok {
				return nil, errors.New("slice elem expect SqlxTabler")
			} else {
				st = append(st, one)
			}
		}
		return st, nil
	}
}

func (cli *Cli) toTbAndTags(record any) (string, []string, error) {
	if record == nil {
		return "", nil, errors.New("records is nil")
	}

	val, typ := utils.Indirect(record)
	if val.Kind() == reflect.Slice {
		typ = typ.Elem()
		val = reflect.Zero(typ)
	}

	st, ok := val.Interface().(SqlxTabler)
	if !ok {
		return "", nil, errors.New("slice elem expect SqlxTabler")
	}

	smap := make(map[string]any)
	if err := utils.ReflectToMap(smap, val, Tag, false); err != nil {
		return "", nil, err
	}

	return st.TableName(), utils.MapKeys(smap, false), nil
}

func (cli *Cli) insert(records []SqlxTabler) (dbSql.Result, error) {
	mapSlice, tags, err := cli.toMapSlice(records)
	if err != nil {
		return nil, err
	}

	query := cli.buildInsetQuery(records[0].TableName(), tags)

	result, err := cli.NamedExec(query, mapSlice)
	if err != nil {
		return nil, errors.WithMessage(err, "Insert 语句执行出错")
	}

	return result, nil
}

func (cli *Cli) upsert(records []SqlxTabler) (dbSql.Result, error) {
	mapSlice, tags, err := cli.toMapSlice(records)
	if err != nil {
		return nil, err
	}

	query := cli.buildUpsertQuery(records[0].TableName(), tags)

	result, err := cli.NamedExec(query, mapSlice)
	if err != nil {
		return nil, errors.WithMessage(err, "Upsert 语句执行出错")
	}

	return result, nil
}

func (cli *Cli) toMapSlice(records []SqlxTabler) ([]map[string]any, []string, error) {

	mmp := make([]map[string]any, 0, len(records))
	for _, record := range records {
		smp, err := utils.StructToMap(record, Tag, true)
		if err != nil {
			return nil, nil, err
		}
		mmp = append(mmp, smp)
	}

	return mmp, utils.MapKeys(mmp[0], false), nil
}

func (cli *Cli) stringifyMap(m map[string]any) (map[string]any, error) {
	hm := make(map[string]any, len(m))
	for k, v := range m {
		if utils.IsComplexType(reflect.TypeOf(v)) {
			marshal, err := sonic.MarshalString(v)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			v = marshal
		}

		hm[k] = v
	}
	return hm, nil
}

// 为字符串添加单引号
func (cli *Cli) wrapSingleQuotes(field any) string {

	value := fmt.Sprintf("%v", field)
	switch val := field.(type) {
	case string:
		value = utils.Concat("'", val, "'")
	case []byte:
		value = utils.Concat("'", string(val), "'")
	case types.JSONText:
		value = utils.Concat("'", val.String(), "'")
	case types.NullJSONText:
		value = utils.Concat("'", val.String(), "'")
	}

	return value
}

// 序列化Map切片中的值
func (cli *Cli) stringifyMapSlice(updateMap []map[string]any) ([]map[string]any, error) {

	mapSlice := make([]map[string]any, 0, len(updateMap))
	for i := 0; i < len(updateMap); i++ {
		if hm, err := cli.stringifyMap(updateMap[i]); err != nil {
			return nil, err
		} else {
			mapSlice = append(mapSlice, hm)
		}
	}

	return mapSlice, nil
}

// 查询封装
func (cli *Cli) search(dest any, query string, args ...any) error {
	r, err := cli.Query(query, args...)
	if err != nil {
		return err
	}
	rows := &sqlx_inherit.Rows{Rows: r, Mapper: cli.Mapper}
	defer rows.Close()

	if cli.isSearchSlice(dest) {
		return sqlx_inherit.ScanAll(rows, dest, false)
	}
	return sqlx_inherit.ScanAny(rows, dest, false)
}

// map查询封装
func (cli *Cli) mapSearch(dest any, query string, args ...any) error {
	rows, err := cli.Queryx(query, args...)
	if err != nil {
		return errors.WithStack(err)
	}
	defer rows.Close()

	if cli.isSearchSlice(dest) {
		return sqlx_inherit.ScanMap(rows, dest)
	}
	return sqlx_inherit.ScanMapOnce(rows, dest)
}

// 查询指定字段
func (cli *Cli) searchField(dest any, tb string, fieldName string, where string, args []any) (err error) {

	where, args, err = sqlx.In(where, args...)
	if err != nil {

		return errors.Wrap(err, "参数解析失败")
	}

	query, err := cli.buildSearchQuery(tb, []string{fieldName}, where)
	if err != nil {
		return errors.WithMessage(err, "构建查询语句出错")
	}

	// 过滤没有记录的正常情况
	if err = cli.search(dest, query, args...); err != nil && !errors.Is(err, dbSql.ErrNoRows) {
		return errors.WithMessage(err, fmt.Sprintf("语句执行出错, sql:%s", query))
	}

	return nil
}

// searchOne 查询单个字段
func (cli *Cli) searchOne(dest any, tb string, fieldName string, where string, args []any) (err error) {

	where, args, err = sqlx.In(where, args...)
	if err != nil {
		return errors.Wrap(err, "参数解析失败")
	}

	query, err := cli.buildSearchQuery(tb, []string{fieldName}, where)
	if err != nil {
		return errors.WithMessage(err, "构建查询语句出错")
	}

	// 过滤没有记录的正常情况
	if err = cli.search(dest, query, args...); err != nil && !errors.Is(err, dbSql.ErrNoRows) {
		return errors.WithMessage(err, fmt.Sprintf("语句执行出错, sql:%s", query))
	}

	return nil
}

// isSearchSlice 是否查询单字段切片
func (cli *Cli) isSearchSlice(dest any) bool {
	kind := reflect.Indirect(reflect.ValueOf(dest)).Kind()
	if kind == reflect.Slice || kind == reflect.Array {
		return true
	}
	return false
}
