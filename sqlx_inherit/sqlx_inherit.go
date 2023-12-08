package sqlx_inherit

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/Pius-x/xorm/utils"
	"github.com/bytedance/sonic"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
	"github.com/pkg/errors"
)

type Rows struct {
	*sql.Rows
	Mapper *reflectx.Mapper
}

var (
	_scannerInterface = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
	mpr               *reflectx.Mapper
	mprMu             sync.Mutex
	NameMapper        = strings.ToLower
	origMapper        = reflect.ValueOf(NameMapper)
)

func ScanAny(r *Rows, dest interface{}, structOnly bool) error {
	if r.Rows == nil {
		return errors.WithStack(sql.ErrNoRows)
	}

	defer r.Rows.Close()

	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr {
		return errors.New("must pass a pointer, not a value, to StructScan destination")
	}
	if v.IsNil() {
		return errors.New("nil pointer passed to StructScan destination")
	}

	base := reflectx.Deref(v.Type())
	scannable := isScannable(base)

	if structOnly && scannable {
		return errors.WithStack(structOnlyError(base))
	}

	columns, err := r.Columns()
	if err != nil {
		return err
	}

	if scannable && len(columns) > 1 {
		return errors.WithStack(fmt.Errorf("scannable dest type %s with >1 columns (%d) in result", base.Kind(), len(columns)))
	}

	if scannable {
		if !utils.IsComplexType(v.Elem().Type()) {
			return scan(r, dest)
		}

		values := new([]byte)
		if err = scan(r, values); err != nil {
			return err
		}

		if err = sonic.Unmarshal(*values, v.Interface()); err != nil {
			return errors.Wrap(err, "check if the struct matches\n")
		}

		return nil
	}

	fields := r.Mapper.TraversalsByName(v.Type(), columns)

	// if we are not unsafe and are missing fields, return an error
	if f, err := missingFields(fields); err != nil {
		return fmt.Errorf("missing destination name %s in %T", columns[f], dest)
	}

	values := make([]interface{}, len(columns))
	if err = fieldsByTraversal(v, fields, values); err != nil {
		return err
	}

	// scan into the struct field pointers and append to our results
	if err = scan(r, values...); err != nil {
		return err
	}

	// 解析复杂数据格式
	if err = parseComplexField(v.Elem(), fields, values); err != nil {
		return err
	}

	return nil
}

func ScanAll(rows *Rows, dest interface{}, structOnly bool) error {
	var v, vp reflect.Value

	value := reflect.ValueOf(dest)

	// json.Unmarshal returns errors for these
	if value.Kind() != reflect.Ptr {
		return errors.New("must pass a pointer, not a value, to StructScan destination")
	}
	if value.IsNil() {
		return errors.New("nil pointer passed to StructScan destination")
	}
	direct := reflect.Indirect(value)

	slice, err := baseType(value.Type(), reflect.Slice)
	if err != nil {
		return errors.WithStack(err)
	}
	direct.SetLen(0)

	isPtr := slice.Elem().Kind() == reflect.Ptr
	base := reflectx.Deref(slice.Elem())
	scannable := isScannable(base)

	if structOnly && scannable {
		return errors.WithStack(structOnlyError(base))
	}

	columns, err := rows.Columns()
	if err != nil {
		return errors.WithStack(err)
	}

	// if it's a base type make sure it only has 1 column;  if not return an error
	if scannable && len(columns) > 1 {
		return errors.WithStack(fmt.Errorf("non-struct dest type %s with >1 columns (%d)", base.Kind(), len(columns)))
	}

	if !scannable {
		var values []interface{}

		fields := rows.Mapper.TraversalsByName(base, columns)
		// if we are not unsafe and are missing fields, return an error
		if f, err := missingFields(fields); err != nil {
			return errors.WithStack(fmt.Errorf("missing destination name %s in %T", columns[f], dest))
		}
		values = make([]interface{}, len(columns))

		for rows.Next() {
			// create a new struct type (which returns PtrTo) and indirect it
			vp = reflect.New(base)
			v = reflect.Indirect(vp)

			if err = fieldsByTraversal(v, fields, values); err != nil {
				return err
			}

			// scan into the struct field pointers and append to our results
			if err = rows.Scan(values...); err != nil {
				return errors.WithStack(err)
			}

			// 解析复杂数据格式
			if err = parseComplexField(v, fields, values); err != nil {
				return err
			}

			if isPtr {
				direct.Set(reflect.Append(direct, vp))
			} else {
				direct.Set(reflect.Append(direct, v))
			}
		}
	} else {
		var values any

		for rows.Next() {
			vp = reflect.New(base)
			v = reflect.Indirect(vp)
			if utils.IsComplexType(v.Type()) {
				values = new([]byte)
			} else {
				values = vp.Interface()
			}

			if err = rows.Scan(values); err != nil {
				return errors.WithStack(err)
			}

			if utils.IsComplexType(v.Type()) {
				if err = sonic.Unmarshal(*values.(*[]byte), v.Addr().Interface()); err != nil {
					return errors.Wrap(err, "check if the struct matches")
				}
			}

			// append
			if isPtr {
				direct.Set(reflect.Append(direct, vp))
			} else {
				direct.Set(reflect.Append(direct, v))
			}
		}
	}

	return errors.WithStack(rows.Err())
}

func ScanMap(rows *sqlx.Rows, dest any) error {

	value := reflect.ValueOf(dest)

	// json.Unmarshal returns errors for these
	if value.Kind() != reflect.Ptr {
		return errors.New("must pass a pointer, not a value, to StructScan destination")
	}
	if value.IsNil() {
		return errors.New("nil pointer passed to StructScan destination")
	}
	direct := reflect.Indirect(value)
	direct.SetLen(0)

	for rows.Next() {
		values := map[string]any{}
		if err := rows.MapScan(values); err != nil {
			return errors.WithStack(err)
		}
		v := reflect.ValueOf(values)

		// append
		direct.Set(reflect.Append(direct, v))
	}

	return errors.WithStack(rows.Err())
}

func ScanMapOnce(rows *sqlx.Rows, dest any) error {

	value := reflect.ValueOf(dest)

	// json.Unmarshal returns errors for these
	if value.Kind() != reflect.Ptr {
		return errors.New("must pass a pointer, not a value, to StructScan destination")
	}
	if value.IsNil() {
		return errors.New("nil pointer passed to StructScan destination")
	}
	direct := reflect.Indirect(value)
	rows.Next()

	values := map[string]any{}
	if err := rows.MapScan(values); err != nil {
		return errors.WithStack(err)
	}
	v := reflect.ValueOf(values)
	direct.Set(v)

	return errors.WithStack(rows.Err())
}

func missingFields(transversals [][]int) (field int, err error) {
	for i, t := range transversals {
		if len(t) == 0 {
			return i, errors.New("missing field")
		}
	}
	return 0, nil
}

func fieldsByTraversal(v reflect.Value, traversals [][]int, values []interface{}) error {
	v = reflect.Indirect(v)
	if v.Kind() != reflect.Struct {
		return errors.New("argument not a struct")
	}

	for i, traversal := range traversals {
		if len(traversal) == 0 {
			values[i] = new(interface{})
			continue
		}
		f := reflectx.FieldByIndexes(v, traversal)

		if utils.IsComplexType(f.Type()) {
			values[i] = new([]byte)
		} else {
			values[i] = f.Addr().Interface()
		}

	}
	return nil
}

// mapper returns a valid mapper using the configured NameMapper func.
func mapper() *reflectx.Mapper {
	mprMu.Lock()
	defer mprMu.Unlock()

	if mpr == nil {
		mpr = reflectx.NewMapperFunc("db", NameMapper)
	} else if origMapper != reflect.ValueOf(NameMapper) {
		// if NameMapper has changed, create a new mapper
		mpr = reflectx.NewMapperFunc("db", NameMapper)
		origMapper = reflect.ValueOf(NameMapper)
	}
	return mpr
}

func isScannable(t reflect.Type) bool {
	if reflect.PtrTo(t).Implements(_scannerInterface) {
		return true
	}
	if t.Kind() != reflect.Struct {
		return true
	}

	// it's not important that we use the right mapper for this particular object,
	// we're only concerned on how many exported fields this struct has
	if len(mapper().TypeMap(t).Index) == 0 {
		return true
	}

	toMap, _ := utils.StructToMap(reflect.New(t).Interface(), "db", false)
	return len(toMap) == 0
}

func baseType(t reflect.Type, expected reflect.Kind) (reflect.Type, error) {
	t = reflectx.Deref(t)
	if t.Kind() != expected {
		return nil, fmt.Errorf("expected %s but got %s", expected, t.Kind())
	}
	return t, nil
}

func structOnlyError(t reflect.Type) error {
	isStruct := t.Kind() == reflect.Struct
	isScanner := reflect.PtrTo(t).Implements(_scannerInterface)
	if !isStruct {
		return fmt.Errorf("expected %s but got %s", reflect.Struct, t.Kind())
	}
	if isScanner {
		return fmt.Errorf("structscan expects a struct dest but the provided struct type %s implements scanner", t.Name())
	}
	return fmt.Errorf("expected a struct, but struct %s has no exported fields", t.Name())
}

func scan(r *Rows, dest ...interface{}) error {

	defer r.Close()
	for _, dp := range dest {
		if _, ok := dp.(*sql.RawBytes); ok {
			return errors.New("sql: RawBytes isn't allowed on Row.Scan")
		}
	}

	if !r.Next() {
		if err := r.Err(); err != nil {
			return err
		}
		return errors.WithStack(sql.ErrNoRows)
	}
	err := r.Scan(dest...)
	if err != nil {
		return err
	}
	// Make sure the query can be processed to completion with no errors.
	if err := r.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func parseComplexField(val reflect.Value, fields [][]int, values []any) error {
	for i, traversa := range fields {
		if len(traversa) == 0 {
			values[i] = new(interface{})
			continue
		}
		f := reflectx.FieldByIndexes(val, traversa)

		if utils.IsComplexType(f.Type()) {
			if err := sonic.Unmarshal(*values[i].(*[]byte), f.Addr().Interface()); err != nil {
				return errors.Wrap(err, "check if the struct matches")
			}
		}
	}

	return nil
}
