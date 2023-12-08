package mysql

import (
	"fmt"
	"strings"

	"github.com/Pius-x/xorm/utils"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// 构建更新语句
func (cli *Cli) buildUpdateQuery(tb string, updateMap map[string]any, fields []string) (string, []any, error) {
	if len(updateMap) == 0 {
		return "", nil, errors.New("updateMap is empty")
	}

	query := utils.Concat("UPDATE ", tb, " SET ")
	args := make([]any, 0, len(updateMap)+len(fields))

	var fieldStr string
	for tag, val := range updateMap {
		if !utils.InSlice(tag, fields) {
			args = append(args, val)
			fieldStr = utils.Concat(fieldStr, "`", tag, "` = ?,")
		}
	}

	where := "WHERE true"
	for _, field := range fields {
		if keyVal, Ok := updateMap[field]; !Ok {
			return "", nil, errors.New(fmt.Sprintf("field:%s not find in update map", field))
		} else {
			args = append(args, keyVal)
			where = utils.Concat(where, " AND `", field, "`= ?")
		}
	}

	fieldStr = fieldStr[:len(fieldStr)-1]

	query = utils.Concat(query, fieldStr, " ", where)

	return query, args, nil
}

// 构建批量更新语句
func (cli *Cli) buildUpdateBatchQuery(tb string, mapSlice []map[string]any, fields ...string) (string, []any, error) {

	if len(mapSlice) == 0 {
		return "", nil, errors.New("updateMaps is empty")
	}

	if len(fields) == 0 {
		return "", nil, errors.New("update fields is empty")
	}

	for _, updateMap := range mapSlice {
		for _, field := range fields {
			if _, Ok := updateMap[field]; !Ok {
				return "", nil, errors.New(fmt.Sprintf("field:%s not find in map slice", field))
			}
		}
	}

	updates, args := cli.updateCaseWhenThen(mapSlice, fields...)

	fieldArgs := make([]any, 0, len(mapSlice))
	for _, datum := range mapSlice {
		anySlice := make([]any, 0, len(fields))
		for _, field := range fields {
			anySlice = append(anySlice, datum[field])
		}
		fieldArgs = append(fieldArgs, anySlice)
	}

	var err error
	where, args2 := cli.JointFieldsIn(fields, fieldArgs...)
	if where, args2, err = sqlx.In(where, args2...); err != nil {
		return "", nil, errors.WithStack(err)
	}
	args = append(args, args2...)

	query := utils.Concat("UPDATE ", tb, " SET \n", updates, " \n", where)

	return query, args, nil
}

// 构建统计计数语句
func (cli *Cli) buildCountQuery(tb string, where string) string {
	return utils.Concat("SELECT COUNT(1)", " FROM ", tb, " ", where)
}

// 构建查询语句
func (cli *Cli) buildSearchQuery(tb string, tags []string, where string) (string, error) {
	if len(tags) == 0 {
		return "", errors.New("tags is empty")
	}

	query := "SELECT "
	for _, tag := range tags {
		query = utils.Concat(query, "`", tag, "`", ",")
	}

	query = query[:len(query)-1]

	query = utils.Concat(query, " FROM ", tb, " ", where)
	return query, nil
}

// 构建插入语句
func (cli *Cli) buildInsetQuery(tb string, tags []string) string {

	query := "INSERT INTO"
	query = utils.Concat(query, " ", tb, " (")
	var fieldStr string
	var nameStr string
	for _, tag := range tags {
		fieldStr = utils.Concat(fieldStr, "`", tag, "`,")
		nameStr = utils.Concat(nameStr, ":", tag, ",")
	}
	fieldStr = fieldStr[:len(fieldStr)-1]
	nameStr = nameStr[:len(nameStr)-1]

	query = utils.Concat(query, fieldStr, ") VALUES (", nameStr, ")")

	return query
}

// 构建插入或更新语句
func (cli *Cli) buildUpsertQuery(tb string, tags []string) string {
	insetQuery := cli.buildInsetQuery(tb, tags)

	updateTail := ""
	for _, tag := range tags {
		updateTail = utils.Concat(updateTail, "`", tag, "` = VALUES(`", tag, "`),")
	}
	updateTail = updateTail[:len(updateTail)-1]

	query := utils.Concat(insetQuery, " ON DUPLICATE KEY UPDATE ", updateTail)

	return query
}

// 构建删除语句
func (cli *Cli) buildDeleteQuery(tb string, where string) string {
	return utils.Concat("DELETE FROM", " ", tb, " ", where)
}

func (cli *Cli) updateCaseWhenThen(updateData []map[string]any, fields ...string) (string, []any) {

	var fs = make([]string, 0, len(fields))
	for _, field := range fields {
		fs = append(fs, utils.Concat(" `", field, "` = ? "))
	}

	subCase := utils.Concat("when", strings.Join(fs, "And"), "then ? ")

	args := make([]any, 0, len(updateData[0])*len(updateData))

	var updateClauses []string
	for field := range updateData[0] {
		if utils.InSlice(field, fields) {
			continue
		}
		var subCases = make([]string, 0, len(updateData))
		for _, updateMap := range updateData {
			subCases = append(subCases, subCase)

			for _, key := range fields {
				args = append(args, updateMap[key])
			}
			args = append(args, updateMap[field])
		}
		updateClauses = append(updateClauses, fmt.Sprintf("%s = \n\tCASE \n\t\t%s \n\tEND", field, strings.Join(subCases, "\n\t\t")))
	}

	return strings.Join(updateClauses, ",\n"), args
}
