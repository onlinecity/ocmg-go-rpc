package main

import (
	"context"
	"flag"
	"sync"
	"time"

	"github.com/onlinecity/ocmg-go-rpc/pkg/rpc"
	"go.uber.org/zap"
)

func test(pool *rpc.ConnPool, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var foo string
	if ok, err := pool.Call(ctx, &foo, "TestSingleEcho", "foo"); err != nil {
		zap.S().Fatalw("TestSingleEcho failed", "err", err, "ok", ok)
	}
}

func loop(pool *rpc.ConnPool, wg *sync.WaitGroup, iterations, id int) {
	timeout := time.Duration(2) * time.Second
	for i := 0; i < iterations; i++ {
		test(pool, timeout)

		stats := pool.Stats()
		zap.L().Info("pool stats",
			zap.Uint32("idle", stats.IdleConns),
			zap.Uint32("stale", stats.StaleConns),
			zap.Uint32("total", stats.TotalConns),
			zap.Uint32("hits", stats.Hits),
			zap.Uint32("miss", stats.Misses),
			zap.Uint32("timeouts", stats.Timeouts),
		)
		zap.L().Info("test", zap.Int("worker", id))
		time.Sleep(time.Duration(id) * time.Millisecond)
	}
	wg.Done()
}

func main() {
	logger, _ := zap.NewDevelopment()
	zap.ReplaceGlobals(logger)
	defer logger.Sync() // nolint:errcheck

	endpoint := flag.String("endpoint", "tcp://localhost:5507", "where to connect")
	flag.Parse()
	zap.S().Infof("connecting to %q\n", *endpoint)

	servPool := rpc.NewConnPool(&rpc.PoolOptions{
		Resolver: func(ctx context.Context) (string, error) {
			return *endpoint, nil
		},
		PoolSize:           10,
		IdleTimeout:        time.Duration(30) * time.Second,
		IdleCheckFrequency: time.Duration(10) * time.Second,
		PoolTimeout:        time.Duration(30) * time.Second,
	})

	concurrency := 40
	iterations := 30
	wg := &sync.WaitGroup{}
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		zap.L().Info("launch", zap.Int("worker", i))
		go loop(servPool, wg, iterations, i)
	}
	wg.Wait()
}
