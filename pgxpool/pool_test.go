package pgxpool

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestBeforeConnectReceivesLifecycleContextDuringBackgroundRefill verifies that
// the BeforeConnect hook receives the pool's lifecycle context (not a blank
// context.Background()) during background min-conn refills, so context-bound
// metadata is available.
func TestBeforeConnectReceivesLifecycleContextDuringBackgroundRefill(t *testing.T) {
	type ctxKey string
	const testKey ctxKey = "trace-id"
	const expectedVal = "bg-refill-123"

	ctx := context.WithValue(context.Background(), testKey, expectedVal)

	var (
		mu       sync.Mutex
		hookCalls int
	)
	config := &Config{
		MinConns: 1,
		BeforeConnect: func(ctx context.Context, cfg *ConnConfig) error {
			mu.Lock()
			hookCalls++
			mu.Unlock()
			val := ctx.Value(testKey)
			if val != expectedVal {
				return fmt.Errorf("expected context value %q, got %v", expectedVal, val)
			}
			return nil
		},
	}

	pool := NewPool(ctx, config)
	defer pool.Close()

	// Give the background refill goroutine time to run and establish a conn.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		pool.mu.Lock()
		n := len(pool.conns)
		pool.mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	calls := hookCalls
	mu.Unlock()
	if calls == 0 {
		t.Fatal("BeforeConnect was never invoked during background refill")
	}
}

// TestAcquirePropagatesCallerContext verifies that explicit acquisition paths
// propagate the caller's context to BeforeConnect.
func TestAcquirePropagatesCallerContext(t *testing.T) {
	type ctxKey string
	const testKey ctxKey = "tenant"
	const expectedVal = "acme"

	ctx := context.WithValue(context.Background(), testKey, expectedVal)

	config := &Config{
		BeforeConnect: func(ctx context.Context, cfg *ConnConfig) error {
			if val := ctx.Value(testKey); val != expectedVal {
				return fmt.Errorf("expected %q, got %v", expectedVal, val)
			}
			return nil
		},
	}

	pool := NewPool(context.Background(), config)
	defer pool.Close()

	if _, err := pool.Acquire(ctx); err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
}
