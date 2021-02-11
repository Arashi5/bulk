package bulk

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"sync/atomic"
)

const (
	Tx   = "tx"
	Conn = "conn"
)

type Transaction interface {
	Begin(ctx context.Context, c Connection) (context.Context, error)
	Rollback(context.Context) (context.Context, error)
	Commit(context.Context) (context.Context, error)
}

type transaction struct {
	count uint32
}

func (t *transaction) Begin(ctx context.Context, c *Connection) (*pgxpool.Conn, context.Context, error) {
	if t.count > 0 && ctx.Value(Tx) != nil {
		atomic.AddUint32(&t.count, 1)
		pool := ctx.Value(Conn).(*pgxpool.Conn)

		return pool, ctx, nil
	}

	conn, err := GetMasterConn(ctx, c)
	if err != nil {
		return nil, ctx, err
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}

	atomic.AddUint32(&t.count, 1)

	ctx = context.WithValue(ctx, Tx, tx)
	ctx = context.WithValue(ctx, Conn, conn)

	return conn, ctx, nil
}

func (t *transaction) Rollback(ctx context.Context) (context.Context, error) {
	if t.count < 0 {
		return nil, errors.New("there is no connection to Postgres")
	}

	tx := ctx.Value(Tx).(pgx.Tx)
	if err := tx.Rollback(ctx); err != nil {
		return nil, err
	}

	atomic.SwapUint32(&t.count, t.count-1)
	return ctx, nil
}

func (t *transaction) Commit(ctx context.Context) (context.Context, error) {
	if t.count < 0 {
		return nil, errors.New("there is no connection to Postgres")
	}

	conn := ctx.Value(Tx).(pgx.Tx)
	if err := conn.Commit(ctx); err != nil {
		return nil, err
	}

	atomic.SwapUint32(&t.count, t.count-1)

	return ctx, nil
}
