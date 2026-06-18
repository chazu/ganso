package ganso

import (
	"context"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// Exec runs a single positional-parameter (`?`) write statement inside an
// IMMEDIATE write transaction on the dedicated writer connection. Added for
// the Procyon Park tuple-store binding, which uses positional params.
func (db *Database) Exec(query string, args ...any) error {
	return db.WithTx(func(tx *Tx) error {
		return sqlitex.Execute(tx.conn, query, &sqlitex.ExecOptions{Args: args})
	})
}

// QueryArgs runs a positional-parameter read on a pooled reader connection and
// collects all rows as []map[string]any (column name -> value).
func (db *Database) QueryArgs(ctx context.Context, query string, args ...any) ([]map[string]any, error) {
	if db.closed.Load() {
		return nil, ErrClosed
	}
	conn, err := db.pool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer db.pool.Put(conn)

	var rows []map[string]any
	err = sqlitex.Execute(conn, query, &sqlitex.ExecOptions{
		Args: args,
		ResultFunc: func(stmt *sqlite.Stmt) error {
			row := make(map[string]any, stmt.ColumnCount())
			for i := 0; i < stmt.ColumnCount(); i++ {
				name := stmt.ColumnName(i)
				switch stmt.ColumnType(i) {
				case sqlite.TypeInteger:
					row[name] = stmt.ColumnInt64(i)
				case sqlite.TypeFloat:
					row[name] = stmt.ColumnFloat(i)
				case sqlite.TypeText:
					row[name] = stmt.ColumnText(i)
				case sqlite.TypeBlob:
					buf := make([]byte, stmt.ColumnLen(i))
					stmt.ColumnBytes(i, buf)
					row[name] = buf
				case sqlite.TypeNull:
					row[name] = nil
				}
			}
			rows = append(rows, row)
			return nil
		},
	})
	return rows, err
}
