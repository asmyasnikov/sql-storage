package internal

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
)

var (
	_ driver.Conn           = (*conn)(nil)
	_ driver.QueryerContext = (*conn)(nil)
	_ driver.ExecerContext  = (*conn)(nil)
)

var (
	selectAllRe    = regexp.MustCompile(`[sS][eE][lL][eE][cC][tT]\s+\*\s+[fF][rR][oO][mM]\s+(memtable)\s*\;`)
	selectLookupRe = regexp.MustCompile(`[sS][eE][lL][eE][cC][tT]\s+(value)\s+[fF][rR][oO][mM]\s+(memtable)\s*[wW][hH][eE][rR][eE]\s(id)\=\'(.+)\'\;`)
	insertRe       = regexp.MustCompile(`[iI][nN][sS][eE][rR][tT]\s+[iI][nN][tT][oO]\s+(memtable)\s+\(id,\s+value\)\s+[vV][aA][lL][uU][eE][sS]\s+\('(.+)',\s*'(.+)'\)\s*\;`)
)

type memStorage interface {
	Get(key string) (value string, err error)
	Set(key, value string) (err error)
	Keys() ([]string, error)
}

type conn struct {
	data memStorage
}

type setter interface {
	Set(key, value string) (err error)
}

var errNotMatched = errors.New("query not matched")

func insert(data setter, query string) error {
	if !insertRe.MatchString(query) {
		return fmt.Errorf("%q: %w", query, errNotMatched)
	}
	ss := insertRe.FindAllStringSubmatch(query, -1)
	if len(ss) != 1 || len(ss[0]) != 4 {
		return fmt.Errorf("query %q corrupted: %v", query, ss)
	}
	key, value := ss[0][2], ss[0][3]
	if err := data.Set(key, value); err != nil {
		return fmt.Errorf("set failed: %w", err)
	}
	return nil
}

func insert2(data setter, query string) error {
	if insertRe.MatchString(query) {
		ss := insertRe.FindAllStringSubmatch(query, -1)
		if len(ss) == 1 && len(ss[0]) == 4 {
			key, value := ss[0][2], ss[0][3]
			if err := data.Set(key, value); err == nil {
				return nil
			} else {
				return fmt.Errorf("set failed: %w", err)
			}
		} else {
			return fmt.Errorf("query %q corrupted: %v", query, ss)
		}
	} else {
		return fmt.Errorf("%q: %w", query, errNotMatched)
	}
}

func (c conn) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	err := insert(c.data, query)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (c conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch {
	case selectAllRe.MatchString(query):
		keys, err := c.data.Keys()
		if err != nil {
			return nil, err
		}
		r := &rows{
			columns: []string{"id", "value"},
		}
		for _, k := range keys {
			v, err := c.data.Get(k)
			if err != nil {
				return nil, err
			}
			r.rows = append(r.rows, []string{k, v})
		}
		return r, nil
	case selectLookupRe.MatchString(query):
		ss := selectLookupRe.FindAllStringSubmatch(query, -1)
		if len(ss) != 1 || len(ss[0]) != 5 {
			return nil, fmt.Errorf("query %q corrupted: %v", query, ss)
		}
		key := ss[0][4]
		if v, err := c.data.Get(key); err != nil {
			return nil, fmt.Errorf("get failed: %w", err)
		} else {
			return &rows{
				columns: []string{"value"},
				rows: [][]string{
					[]string{v},
				},
			}, nil
		}
	default:
		return nil, fmt.Errorf("query %q not matched", query)
	}
}

func (c conn) Prepare(query string) (driver.Stmt, error) {
	//TODO implement me
	panic("implement me")
}

func (c conn) Close() error {
	return nil
}

func (c conn) Begin() (driver.Tx, error) {
	//TODO implement me
	panic("implement me")
}

func Conn(data memStorage) *conn {
	return &conn{data: data}
}
