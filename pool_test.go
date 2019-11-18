package pool_test

import (
	"context"
	"testing"
	"time"

	pool "github.com/xttty/client-pool"
	"google.golang.org/grpc"
)

func TestPool(t *testing.T) {
	pool, err := pool.Init(factor, 5, 8, 3*time.Second, 10*time.Second, time.Second, pool.PoolGetModeLoose)
	if err != nil {
		t.Log(err)
	}
	now := time.Now()
	for i := 0; i < 20; i++ {
		time.Sleep(50 * time.Millisecond)
		go func(idx int) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			conn, err := pool.Get(ctx)
			if err != nil {
				t.Logf("connection occurred error: %s, pool size: %d\n, pool conn cnt: %d", err.Error(), pool.Size(), pool.ConnCnt())
				return
			}
			defer conn.Close()

			t.Logf("idx: %d, conn init: %d, conn last used: %d, cnt: %d, size: %d", idx, conn.TimeInit().Nanosecond()-now.Nanosecond(), conn.TimeUsed().Nanosecond()-now.Nanosecond(), pool.ConnCnt(), pool.Size())
			time.Sleep(500 * time.Millisecond)
		}(i)
	}
	time.Sleep(5 * time.Second)
	t.Log("-------------------")
	for i := 0; i < 20; i++ {
		time.Sleep(50 * time.Millisecond)
		go func(idx int) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			conn, err := pool.Get(ctx)
			if err != nil {
				t.Logf("connection occurred error: %s, pool size: %d\n, pool conn cnt: %d", err.Error(), pool.Size(), pool.ConnCnt())
				return
			}
			defer conn.Close()

			t.Logf("idx: %d, conn init: %d, conn last used: %d, cnt: %d, size: %d", idx, conn.TimeInit().Nanosecond()-now.Nanosecond(), conn.TimeUsed().Nanosecond()-now.Nanosecond(), pool.ConnCnt(), pool.Size())
			time.Sleep(500 * time.Millisecond)
		}(i)
	}
	time.Sleep(5 * time.Second)
	pool.Close()
	time.Sleep(time.Second)
	t.Logf("pool closed: %v, pool conn cnt: %d", pool.IsClose(), pool.ConnCnt())
}

func factor() (*grpc.ClientConn, error) {
	return grpc.Dial("127.0.0.1:8080", grpc.WithInsecure())
}
