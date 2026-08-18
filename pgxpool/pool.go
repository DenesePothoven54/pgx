package pgxpool

import (
	"context"
	"sync"
	"github.com/jackc/pgx/v5/pgconn"
)

type Pool struct {
	ctx    context.Context
	cancel context.CancelFunc
	// ... existing fields
}

func ConnectConfig(ctx context.Context, config *Config) (*Pool, error) {
	ctx, cancel := context.WithCancel(ctx)
	p := &Pool{
		ctx:    ctx,
		cancel: cancel,
	}
	// ... existing initialization
	return p, nil
}

func (p *Pool) backgroundMinConnRefill() {
	// Use p.ctx instead of context.Background()
	p.createConnection(p.ctx)
}

func (p *Pool) Close() {
	p.cancel()
	// ... existing close logic
}