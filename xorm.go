package xorm

import (
	dbSql "database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/Pius-x/xorm/hook"
	"github.com/Pius-x/xorm/utils"
	"github.com/bytedance/sonic"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

func WithPrintSql() {
	hook.PrintSql = true
}

const Tag = "db"

type Cli struct {
	*sqlx.DB
}

// Get 查询单条数据
func (cli *Cli) Get(dest any, query string, args ...any) error {
	err := cli.DB.Get(dest, query, args...)
	if err != nil && !errors.Is(err, dbSql.ErrNoRows) {
		return errors.WithStack(err)

	}
	return nil
}

// Select 查询多行数据
func (cli *Cli) Select(dest any, query string, args ...any) error {
	err := cli.DB.Select(dest, query, args...)
	if err != nil && !errors.Is(err, dbSql.ErrNoRows) {
		return errors.WithStack(err)
	}
	return nil
}

// Query 查询
func (cli *Cli) Query(query string, args ...any) (*dbSql.Rows, error) {
	rows, err := cli.DB.Query(query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return rows, nil
}

// Exec 执行查询而不返回任何行
func (cli *Cli) Exec(query string, args ...any) (dbSql.Result, error) {
	result, err := cli.DB.Exec(query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return result, nil
}

// NamedExec 执行查询而不返回任何行
func (cli *Cli) NamedExec(query string, args any) (dbSql.Result, error) {
	result, err := cli.DB.NamedExec(query, args)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return result, nil
}

// JointFieldsIn 多字段的 Where In 语句构建
func (cli *Cli) JointFieldsIn(fields []string, args ...any) (string, []any) {
	fieldWraps := make([]string, 0, len(fields))
	for _, field := range fields {
		fieldWraps = append(fieldWraps, utils.Concat("`", field, "`"))
	}

	var placeholders string
	filterArgs := make([]any, 0, len(args))
	for _, arg := range args {
		val, _ := utils.Indirect(arg)
		if val.Kind() == reflect.Slice && val.Len() != len(fields) {
			continue
		}
		placeholders = utils.Concat(placeholders, "(?),")
		filterArgs = append(filterArgs, arg)
	}
	placeholders = placeholders[:len(placeholders)-1]

	return utils.Concat("Where (", strings.Join(fieldWraps, ","), ") In (", placeholders, ")"), filterArgs
}

// region Key 查

// Count 统计记录数
// tb 数据库表名
// where 条件语句 如: "WHERE id = 1" 或者 "WHERE id = ?" 参数放在args中
func (cli *Cli) Count(dest any, tb string, where string, args ...any) (err error) {

	where, args, err = sqlx.In(where, args...)
	if err != nil {
		return errors.Wrap(err, "参数解析失败")
	}

	query := cli.buildCountQuery(tb, where)

	return cli.Get(dest, query, args...)
}

// Search 查询 (支持嵌套查询,嵌套结构体,切片,数组,Map)
// dest 若是结构体指针 则为单行查询; 若是结构体切片指针,则为多行查询
// where 条件语句 如: "WHERE id = 1" 或者 "WHERE id = ?" 参数放在args中
// args 条件语句使用占位符?时的可变参数
func (cli *Cli) Search(dest any, where string, args ...any) error {

	tb, tags, err := cli.toTbAndTags(dest)
	if err != nil {
		return errors.WithMessage(err, "获取结构体表名和Tags出错")
	}

	where, args, err = sqlx.In(where, args...)
	if err != nil {
		return errors.Wrap(err, "参数解析失败")
	}

	query, err := cli.buildSearchQuery(tb, tags, where)
	if err != nil {
		return errors.WithMessage(err, "构建查询语句出错")
	}

	// 过滤没有记录的正常情况
	err = cli.search(dest, query, args...)
	if err != nil && !errors.Is(err, dbSql.ErrNoRows) {
		return errors.WithMessage(err, fmt.Sprintf("语句执行出错, sql:%s", query))
	}

	return nil
}

// SearchOneField 查询单个字段
// dest 基础类型 以及支持直接查询结构体,切片,数组,Map
// where 条件语句 如: "WHERE id = 1" 或者 "WHERE id = ?" 参数放在args中
// args 条件语句使用占位符?时的可变参数
func (cli *Cli) SearchOneField(dest any, tb string, fieldName string, where string, args ...any) error {
	if cli.isSearchSlice(dest) {
		var tmpDest string

		if err := cli.searchOne(&tmpDest, tb, fieldName, where, args); err != nil {
			return err
		}

		if err := sonic.UnmarshalString(tmpDest, dest); err != nil {
			return errors.WithStack(err)
		}
		return nil
	}

	return cli.searchOne(dest, tb, fieldName, where, args)
}

// SearchOneFieldMulti 批量查询单个字段 (支持直接查询结构体,切片,数组,Map)
// dest 若是结构体指针 则为单行查询; 若是结构体切片指针,则为多行查询
// where 条件语句 如: "WHERE id = 1" 或者 "WHERE id = ?" 参数放在args中
// args 条件语句使用占位符?时的可变参数
func (cli *Cli) SearchOneFieldMulti(dest any, tb string, fieldName string, where string, args ...any) error {
	if !cli.isSearchSlice(dest) {
		return errors.New("expected pass slice or array")
	}

	return cli.searchOne(dest, tb, fieldName, where, args)
}

// SearchFields 查询多个字段
// dest Map(单行)或Map切片(多行)
// where 条件语句 如: "WHERE id = 1" 或者 "WHERE id = ?" 参数放在args中
// args 条件语句使用占位符?时的可变参数
func (cli *Cli) SearchFields(dest any, tb string, fields []string, where string, args ...any) error {

	var err error
	where, args, err = sqlx.In(where, args...)
	if err != nil {
		return errors.Wrap(err, "参数解析失败")
	}

	query, err := cli.buildSearchQuery(tb, fields, where)
	if err != nil {
		return errors.Wrap(err, "构建查询语句出错")
	}

	// 过滤没有记录的正常情况
	if err = cli.mapSearch(dest, query, args...); err != nil && !errors.Is(err, dbSql.ErrNoRows) {
		return errors.WithMessage(err, fmt.Sprintf("语句执行出错, sql:%s", query))
	}

	return nil
}

// endregion

// region Key 增

// Insert 插入 (支持嵌套插入,嵌套结构体,切片,数组,Map 会转换成字符串插入)
// record 输入结构体或结构体指针
// 批量插入时 Result.LastInsertId 为第一条插入的自增ID或最后条记录插入的Id
func (cli *Cli) Insert(record any) (dbSql.Result, error) {
	records, err := cli.toSqlxTablers(record)
	if err != nil {
		return nil, err
	}

	return cli.insert(records)
}

// endregion

// region Key 改

// UpdateByStruct 结构体更新(单行)
// record 输入实现 SqlxTabler 接口的结构体 (需要填充所有的结构体字段,不填写默认为零值)
// fields 需要判断的字段
func (cli *Cli) UpdateByStruct(record any, fields ...string) (dbSql.Result, error) {
	if len(fields) == 0 {
		return nil, errors.New("update joint field empty")
	}

	records, err := cli.toSqlxTablers(record)
	if err != nil {
		return nil, err
	}
	tb := records[0].TableName()

	var query string
	var args []any
	switch len(records) {
	case 1:
		updateMap, err := utils.StructToMap(records[0], Tag, true)
		if err != nil {
			return nil, errors.WithMessage(err, "转化成Map切片出错")
		}

		query, args, err = cli.buildUpdateQuery(tb, updateMap, fields)
		if err != nil {
			return nil, errors.WithMessage(err, "构建更新语句出错")
		}
	default:
		mapSlice, _, err := cli.toMapSlice(records)
		if err != nil {
			return nil, err
		}

		query, args, err = cli.buildUpdateBatchQuery(tb, mapSlice, fields...)
		if err != nil {
			return nil, errors.WithMessage(err, "构建更新语句出错")
		}
	}

	result, err := cli.Exec(query, args...)
	if err != nil {
		return nil, errors.WithMessage(err, "update 语句执行出错")
	}

	return result, nil
}

// UpdateByMap 部分更新(单行)
// tb 数据库表名
// updateMap 输入需要更新的字段的Map
// fields 需要判断的字段
func (cli *Cli) UpdateByMap(tb string, record any, fields ...string) (dbSql.Result, error) {
	if len(fields) == 0 {
		return nil, errors.New("update joint field empty")
	}

	var query string
	var args []any
	switch data := record.(type) {
	case map[string]any:
		updateMap, err := cli.stringifyMap(data)
		if err != nil {
			return nil, errors.WithMessage(err, "序列化UpdateMap出错")
		}

		query, args, err = cli.buildUpdateQuery(tb, updateMap, fields)
		if err != nil {
			return nil, errors.WithMessage(err, "构建单行更新语句出错")
		}
	case []map[string]any:
		mapSlice, err := cli.stringifyMapSlice(data)
		if err != nil {
			return nil, errors.WithMessage(err, "序列化UpdateMap切片出错")
		}

		query, args, err = cli.buildUpdateBatchQuery(tb, mapSlice, fields...)
		if err != nil {
			return nil, errors.WithMessage(err, "构建多行更新语句出错")
		}
	default:
		return nil, errors.New(fmt.Sprintf("unexpected type %T", record))
	}

	result, err := cli.Exec(query, args...)
	if err != nil {
		return nil, errors.WithMessage(err, "update 语句执行出错")
	}

	return result, nil
}

// endregion

// region Key 删

// Delete 删除
// tb 数据库表名
// where 条件语句 如: "WHERE id = ?"
func (cli *Cli) Delete(tb string, where string, args ...any) (dbSql.Result, error) {

	where, args, err := sqlx.In(where, args...)
	if err != nil {
		return nil, errors.Wrap(err, "参数解析失败")
	}

	query := cli.buildDeleteQuery(tb, where)
	result, err := cli.Exec(query, args...)
	if err != nil {
		return nil, errors.WithMessage(err, "Delete 语句执行失败")
	}
	return result, nil
}

// endregion

// region Key 增或改

// Upsert 插入或更新 (不存在则插入,存在则更新) (支持嵌套插入,嵌套结构体,切片,数组,Map 会转换成字符串插入)
// arg 批量插入时 输入结构体切片; 单条插入时 输入结构体或结构体指针
// 批量插入时 Result.LastInsertId 为第一条插入的自增ID或最后条记录插入的Id
func (cli *Cli) Upsert(record any) (dbSql.Result, error) {
	records, err := cli.toSqlxTablers(record)
	if err != nil {
		return nil, err
	}

	return cli.upsert(records)
}

// endregion
