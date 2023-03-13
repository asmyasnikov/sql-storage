package xsql

import "database/sql/driver"

var (
	_ driver.Result = (*result)(nil)
)

type result struct {
	lastInsertId int64
	rowsAffected int64
}

func (r result) LastInsertId() (int64, error) {
	return r.lastInsertId, nil
}

func (r result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}
