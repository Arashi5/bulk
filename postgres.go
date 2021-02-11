package bulk

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"strings"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
)

type Connection struct {
	Master      *pgxpool.Pool
	Transaction transaction
}

// Подключение к базе
func Connect(ctx context.Context, cfg *Config) (*Connection, error) {
	res, err := conn(ctx, cfg.Conn)
	if err != nil {
		return nil, errors.Wrap(err, "DB connect")
	}

	return &Connection{Master: res}, nil
}

// Подключаемся к базе
func conn(ctx context.Context, db DataBase) (*pgxpool.Pool, error) {
	return pgxpool.Connect(ctx, fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		db.User,
		db.Password,
		db.Host,
		db.Port,
		db.DataBaseName,
		db.Secure,
	))
}

// Закрытие коннектов
func Close(_ context.Context, c *Connection) {
	c.Master.Close()
}

// Получение соединения c мастером
func GetMasterConn(ctx context.Context, c *Connection) (*pgxpool.Conn, error) {
	conn, err := c.Master.Acquire(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get master connection")
	}
	return conn, nil
}

// Получение соединения c мастером
func GetMasterConnPool(ctx context.Context, c *Connection) (*pgxpool.Pool, error) {
	conn := c.Master
	if conn != nil {
		return nil, errors.New("get master pool connection")
	}
	return conn, nil
}

// Возвращает название constraint по его колонкам.
// Огранчение - если создано 2 и более constraint с одинаковыми колонками, то вернет один из constraint
func GetConstraintName(ctx context.Context, conn pgx.Tx, tableName string, columnNames []string) string {
	empty := ""
	if len(columnNames) == 0 {
		return empty
	}

	columnNamesQuoted := make([]string, len(columnNames))
	for i, v := range columnNames {
		columnNamesQuoted[i] = fmt.Sprintf("'%v'", v)
	}

	q := `SELECT kcu1.constraint_name
		  FROM information_schema.key_column_usage as kcu1
		  WHERE kcu1.table_name = '%v' AND kcu1.constraint_name IN (
				SELECT DISTINCT kcu.constraint_name
				FROM information_schema.key_column_usage as kcu
				WHERE kcu.table_name = '%v' AND kcu.column_name IN (%v)
		  )
		  GROUP BY kcu1.constraint_name
		  HAVING COUNT(kcu1.constraint_name) = %v`

	query := fmt.Sprintf(q, tableName, tableName, strings.Join(columnNamesQuoted, ", "), len(columnNamesQuoted))

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return empty
	}
	defer rows.Close()

	for rows.Next() {
		constraintName := ""

		err := rows.Scan(&constraintName)
		if err != nil {
			return empty
		}

		return constraintName
	}

	return empty
}
