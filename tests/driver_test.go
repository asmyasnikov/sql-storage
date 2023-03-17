package tests

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os/signal"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	_ "github.com/asmyasnikov/sql-storage"
	"github.com/asmyasnikov/sql-storage/internal/storage"
)

func TestAzazaDriver(t *testing.T) {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer cancel()

	db, err := sql.Open("kambodja", "")
	require.NoError(t, err)
	t.Run("INSERT", func(t *testing.T) {
		_, err = db.ExecContext(ctx, "INSERT INTO memtable (id, value) VALUES ('1234', '5678');")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "INSERT INTO memtable (id, value) VALUES ('key', 'value');")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "INSERT INTO memtable (id, value) VALUES ('key1', 'value1');")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "INSERT INTO memtable (id, value) VALUES ('key2', 'value2');")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "INSERT INTO memtable (id, value) VALUES ('key3', 'value3');")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "INSERT INTO memtable (id, value) VALUES ('key4', 'value4');")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "INSERT INTO memtable (id, value) VALUES ('key5', 'value5');")
		require.NoError(t, err)
	})
	t.Run("FULL-SCAN", func(t *testing.T) {
		rows, err := db.QueryContext(ctx, "SELECT * FROM memtable;")
		require.NoError(t, err)
		for rows.Next() {
			var id, value string
			err = rows.Scan(&id, &value)
			require.NoError(t, err)
			fmt.Printf("%s, %s\n", id, value)
		}
		err = rows.Err()
		require.NoError(t, err)
	})
	t.Run("LOOKUP", func(t *testing.T) {
		row := db.QueryRowContext(ctx, "SELECT value FROM memtable WHERE id='123';")
		err := row.Err()
		t.Log(err)
		errors.Is(err, storage.ErrNoData)
		require.ErrorIs(t, err, storage.ErrNoData)
		row = db.QueryRowContext(ctx, "SELECT value FROM memtable WHERE id='1234';")
		require.NoError(t, row.Err())
		var value string
		err = row.Scan(&value)
		require.NoError(t, err)
		fmt.Printf("value(1234) = %s\n", value)
		err = row.Err()
		require.NoError(t, err)
	})
	err = db.Close()
	require.NoError(t, err)
}
