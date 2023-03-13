package xsql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
)

var (
	ErrUnknownQuery = errors.New("unknown query")
)

var (
	_ driver.Conn           = (*conn)(nil)
	_ driver.ExecerContext  = (*conn)(nil)
	_ driver.QueryerContext = (*conn)(nil)
)

var (
	selectAllRe    = regexp.MustCompile(`[sS][eE][lL][eE][cC][tT]\s+\*\s+[fF][rR][oO][mM]\s+(memtable)\s*\;`)
	selectLookupRe = regexp.MustCompile(`[sS][eE][lL][eE][cC][tT]\s+(value)\s+[fF][rR][oO][mM]\s+(memtable)\s*[wW][hH][eE][rR][eE]\s(id)\=\'(.+)\'\;`)
	upsertRe       = regexp.MustCompile(`UPSERT\s+INTO\s+(memtable)\s+\(id,\s+value\)\s+VALUES\s+\('(.+)',\s*'(.+)'\)\s*\;`)
)

type conn struct {
	memtable storage
}

// SELECT
func (c conn) QueryContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch {
	case selectAllRe.MatchString(query):
		keys, err := c.memtable.Keys()
		if err != nil {
			return nil, WithStackTrace(err)
		}
		r := &rows{
			nextRow: 0,
			columns: []string{
				"id",
				"value",
			},
			rows: make([][]string, 0, len(keys)),
		}
		for i := range keys {
			value, err := c.memtable.Get(keys[i])
			if err != nil {
				return nil, WithStackTrace(err)
			}
			r.rows = append(r.rows, []string{
				keys[i],
				value,
			})
		}
		return r, nil
	case selectLookupRe.MatchString(query):
		ss := selectLookupRe.FindAllStringSubmatch(query, -1)
		if len(ss) == 0 {
			return nil, WithStackTrace(fmt.Errorf("query %q is not UPSERT query", query))
		}
		if len(ss) != 1 || len(ss[0]) != 5 {
			return nil, WithStackTrace(fmt.Errorf("parsing of query %q failed: %v", query, ss))
		}
		id := ss[0][4]
		value, err := c.memtable.Get(id)
		if err != nil {
			return nil, WithStackTrace(err)
		}
		return &rows{
			nextRow: 0,
			columns: []string{
				"value",
			},
			rows: [][]string{
				{value},
			},
		}, nil
	default:
		return nil, WithStackTrace(
			WithStackTrace(
				WithStackTrace(
					WithStackTrace(
						WithStackTrace(
							WithStackTrace(
								ErrUnknownQuery,
							),
						),
					),
				),
			),
		)
	}
}

// UPSERT
func (c conn) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ss := upsertRe.FindAllStringSubmatch(query, -1)
	if len(ss) == 0 {
		return nil, WithStackTrace(fmt.Errorf("query %q is not UPSERT query", query))
	}
	if len(ss) != 1 || len(ss[0]) != 4 {
		return nil, WithStackTrace(fmt.Errorf("parsing of query %q failed: %v", query, ss))
	}
	id := ss[0][2]
	value := ss[0][3]
	if err := c.memtable.Set(id, value); err != nil {
		return nil, WithStackTrace(err)
	}
	return &result{
		lastInsertId: 0,
		rowsAffected: 1,
	}, nil
}

func (c conn) Prepare(query string) (driver.Stmt, error) {
	panic("implement me")
}

func (c conn) Close() error {
	return nil
}

func (c conn) Begin() (driver.Tx, error) {
	panic("implement me")
}

type storage interface {
	Get(key string) (value string, err error)
	Set(key, value string) (err error)
	Keys() (keys []string, err error)
}

func New(memtable storage) *conn {
	return &conn{
		memtable: memtable,
	}
}
