// Package pgxpool provides a minimal connection pool that propagates its
// lifecycle context to background connection-establishment routines so that
// BeforeConnect hooks can read context-bound metadata (tracing spans, tenant
// IDs, dynamic credentials) during background maintenance.
package pgxpool

import (
	"context"
	"sync"
	"time"
)

// ConnConfig represents configuration for a single connection.
type ConnConfig struct {
	// Host is the database host.
	Host string
}

// BeforeConnectFunc is invoked before establishing a connection. It receives
// the context of the operation that triggered the connection so callers can
// attach metadata.
type BeforeConnectFunc func(ctx context.Context, cfg *ConnConfig) error

// Config configures a Pool.
type Config struct {
	// MinConns is the minimum number of connections the pool maintains.
	MinConns int
	// BeforeConnect, if set, is called before each connection is established.
	BeforeConnect BeforeConnectFunc
}

// Conn represents a single pooled connection.
type Conn struct {
	cfg *ConnConfig
}

// Pool is a minimal connection pool. It owns a lifecycle context derived from
// the context passed to NewPool and uses it for all background work.
type Pool struct {
	mu           sync.Mutex
	config       *Config
	lifecycleCtx context.Context
	cancel       context.CancelFunc
	conns        []*Conn
}

// NewPool creates a Pool from ctx and config and starts background
// maintenance (min-conn refill) using the pool's lifecycle context.
func NewPool(ctx context.Context, config *Config) *Pool {
	lifecycleCtx, cancel := context.WithCancel(ctx)
	p := &Pool{
		config:       config,
		lifecycleCtx: lifecycleCtx,
		cancel:       cancel,
		conns:        make([]*Conn, 0),
	}
	if config != nil && config.MinConns > 0 {
		go p.backgroundMinConnRefill()
	}
	return p
}

// backgroundMinConnRefill maintains MinConns using the pool lifecycle context
// rather than context.Background(), so BeforeConnect hooks can read
// context-bound metadata during background establishment.
func (p *Pool) backgroundMinConnRefill() {
	for {
		select {
		case <-p.lifecycleCtx.Done():
			return
		default:
		}

		p.mu.Lock()
		need := p.config.MinConns - len(p.conns)
		p.mu.Unlock()

		if need > 0 {
			if err := p.createConnection(p.lifecycleCtx); err != nil {
				select {
				case <-p.lifecycleCtx.Done():
					return
				case <-time.After(10 * time.Millisecond):
				}
				continue
			}
		}

		select {
		case <-p.lifecycleCtx.Done():
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// createConnection establishes a connection, invoking BeforeConnect with ctx.
func (p *Pool) createConnection(ctx context.Context) error {
	if p.config.BeforeConnect != nil {
		if err := p.config.BeforeConnect(ctx, &ConnConfig{}); err != nil {
			return err
		}
	}
	p.mu.Lock()
	p.conns = append(p.conns, &Conn{cfg: &ConnConfig{}})
	p.mu.Unlock()
	return nil
}

// Acquire returns a connection, propagating the caller's context to
// BeforeConnect for explicit acquisition paths.
func (p *Pool) Acquire(ctx context.Context) (*Conn, error) {
	if err := p.createConnection(ctx); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.conns) == 0 {
		return nil, nil
	}
	c := p.conns[len(p.conns)-1]
	p.conns = p.conns[:len(p.conns)-1]
	return c, nil
}

// Close cancels the pool lifecycle context and stops background maintenance.
func (p *Pool) Close() {
	p.cancel()
}
