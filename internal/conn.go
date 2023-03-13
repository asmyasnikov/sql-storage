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
	keysRe = regexp.MustCompile(`KEYS;`)
	getRe  = regexp.MustCompile(`GET (.+)\;`)
	setRe  = regexp.MustCompile(`SET (.*)\s+=\s+(.+);`)
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

type getter interface {
	Get(key string) (value string, err error)
}

type keyer interface {
	Keys() ([]string, error)
}

var errNotMatched = errors.New("query not matched")

func set(data setter, query string) error {
	if !setRe.MatchString(query) {
		return fmt.Errorf("%q: %w", query, errNotMatched)
	}
	ss := setRe.FindAllStringSubmatch(query, -1)
	if len(ss) != 1 || len(ss[0]) != 3 {
		return fmt.Errorf("query %q corrupted: %v", query, ss)
	}
	key, value := ss[0][1], ss[0][2]
	if err := data.Set(key, value); err != nil {
		return fmt.Errorf("set failed: %w", err)
	}
	return nil
}

func (c conn) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	err := set(c.data, query)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func get(data getter, query string) (value string, _ error) {
	if !getRe.MatchString(query) {
		return "", fmt.Errorf("%q: %w", query, errNotMatched)
	}
	ss := getRe.FindAllStringSubmatch(query, -1)
	if len(ss) != 1 || len(ss[0]) != 2 {
		return "", fmt.Errorf("query %q corrupted: %v", query, ss)
	}
	v, err := data.Get(ss[0][1])
	if err != nil {
		return "", fmt.Errorf("get failed: %w", err)
	}
	return v, nil
}

func keys(data keyer) ([]string, error) {
	v, err := data.Keys()
	if err != nil {
		return nil, fmt.Errorf("keys failed: %w", err)
	}
	return v, nil
}

func (c conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch {
	case keysRe.MatchString(query):
		keys, err := keys(c.data)
		if err != nil {
			return nil, err
		}
		r := &rows{
			columns: []string{"key", "value"},
		}
		for _, k := range keys {
			v, err := c.data.Get(k)
			if err != nil {
				return nil, err
			}
			r.rows = append(r.rows, []string{k, v})
		}
		return r, nil
	case getRe.MatchString(query):
		if v, err := get(c.data, query); err != nil {
			return nil, err
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
