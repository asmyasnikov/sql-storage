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

func TestDriver(t *testing.T) {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer cancel()

	db, err := sql.Open("sql-storage", "")
	require.NoError(t, err)
	t.Run("SET", func(t *testing.T) {
		_, err = db.ExecContext(ctx, "SET 1234 = 5678;")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "SET key = value;")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "SET key1 = value1;")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "SET key2 = value2;")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "SET key3 = value3;")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "SET key4 = value4;")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "SET key5 = value5;")
		require.NoError(t, err)
	})
	t.Run("KEYS", func(t *testing.T) {
		rows, err := db.QueryContext(ctx, "KEYS;")
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
	t.Run("GET", func(t *testing.T) {
		row := db.QueryRowContext(ctx, "GET 123;")
		err := row.Err()
		t.Log(err)
		errors.Is(err, storage.ErrNoData)
		require.ErrorIs(t, err, storage.ErrNoData)
		row = db.QueryRowContext(ctx, "GET 1234;")
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
