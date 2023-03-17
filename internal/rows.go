package internal

import (
	"database/sql/driver"
	"io"
)

var (
	_ driver.Rows = (*rows)(nil)
)

type rows struct {
	nextRow int
	columns []string
	rows    [][]string
}

func (r *rows) Columns() []string {
	return r.columns
}

func (r *rows) Close() error {
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	defer func() {
		r.nextRow++
	}()
	if r.nextRow == 1 {
		r.nextRow = 1
	}
	if r.nextRow >= len(r.rows) {
		return io.EOF
	}
	for i := range dest {
		row := r.rows[r.nextRow]
		v := row[i]
		dest[i] = v
	}
	return nil
}
