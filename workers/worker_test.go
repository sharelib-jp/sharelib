package workers

import (
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/neilotoole/slogt"
)

// ワーカーが 0 以下で開始できないことを確認するテスト
func TestStartInvalidWorkerCount(t *testing.T) {
	slog.SetDefault(slogt.New(t))

	pool, err := NewPriorityPool(1)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	err = pool.Start(0)
	if err != ErrInvalidWorkerCount {
		t.Errorf("Expected ErrInvalidWorkerCount, got %v", err)
	}
}

// 低優先度でジョブが順に実行されることを確認するテスト
func TestSingleExecuteOnlyLow(t *testing.T) {
	slog.SetDefault(slogt.New(t))

	pool, err := NewPriorityPool(100)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	err = pool.Start(1)
	if err != nil {
		t.Fatalf("Failed to start pool: %v", err)
	}
	defer pool.Stop()

	for i := range 100 {
		count := i
		err := pool.SubmitLow(Job{
			Fn: func() {
				t.Logf("Executing job %d", count)
				time.Sleep(100 * time.Millisecond)
			},
			Comment: fmt.Sprintf("Low priority job #%d", count),
			TimeOut: 500 * time.Millisecond,
		})
		if err != nil {
			t.Errorf("Failed to submit job: %v", err)
		}
	}

	// Wait for all jobs to complete
	time.Sleep(11 * time.Second)
}

// 高優先度ジョブが低優先度ジョブよりも先に実行されることを確認するテスト
func TestHighPriorityPreemptsLow(t *testing.T) {
	slog.SetDefault(slogt.New(t))

	pool, err := NewPriorityPool(100)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	if err := pool.Start(1); err != nil {
		t.Fatalf("Failed to start pool: %v", err)
	}

	var mu sync.Mutex
	var order []string
	var wg sync.WaitGroup

	for i := range 10 {
		idx := i
		wg.Add(1)
		if err := pool.SubmitLow(Job{
			Fn: func() {
				mu.Lock()
				order = append(order, fmt.Sprintf("low%d", idx))
				mu.Unlock()
				time.Sleep(200 * time.Millisecond)
				wg.Done()
			},
			Comment: fmt.Sprintf("Low #%d", idx),
			TimeOut: 1 * time.Second,
		}); err != nil {
			t.Fatalf("SubmitLow failed: %v", err)
		}
	}

	// wait a bit so first low job starts
	time.Sleep(50 * time.Millisecond)

	// Submit a high-priority job which should run before the remaining low jobs
	wg.Add(1)
	if err := pool.SubmitHigh(Job{
		Fn: func() {
			mu.Lock()
			order = append(order, "high")
			mu.Unlock()
			wg.Done()
		},
		Comment: "High",
		TimeOut: 1 * time.Second,
	}); err != nil {
		t.Fatalf("SubmitHigh failed: %v", err)
	}

	// Wait for all to finish
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for jobs to complete")
	}

	if len(order) != 11 {
		t.Fatalf("expected 11 executions, got %d: %v", len(order), order)
	}
	// Expect order: low0, high, low1, low2
	if order[1] != "high" {
		t.Fatalf("expected high to run second, got order=%v", order)
	}
	defer pool.Stop()
}

// タイムアウトが正しく動作することを確認するテスト
func TestTimeoutBehavior(t *testing.T) {
	slog.SetDefault(slogt.New(t))

	pool, err := NewPriorityPool(10)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	if err := pool.Start(1); err != nil {
		t.Fatalf("Failed to start pool: %v", err)
	}

	started := make(chan struct{}, 1)
	completed := make(chan struct{}, 1)

	// Submit a job that sleeps longer than TimeOut
	if err := pool.SubmitLow(Job{
		Fn: func() {
			started <- struct{}{}
			time.Sleep(300 * time.Millisecond)
			completed <- struct{}{}
		},
		Comment: "TimeoutTest",
		TimeOut: 100 * time.Millisecond,
	}); err != nil {
		t.Fatalf("SubmitLow failed: %v", err)
	}

	// Wait for job to start
	select {
	case <-started:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("job did not start in time")
	}

	// completed should NOT be received within TimeOut + small slack, indicating timeout occurred
	select {
	case <-completed:
		t.Fatalf("job completed before expected timeout")
	case <-time.After(150 * time.Millisecond):
		// no completed -> likely timed out (expected)
	}

	// eventually the job goroutine will finish; wait to avoid leak
	select {
	case <-completed:
	case <-time.After(500 * time.Millisecond):
	}

	defer pool.Stop()
}

// 並列実行が正しく動作することを確認するテスト
func TestParallelExecuteOnlyLow(t *testing.T) {
	slog.SetDefault(slogt.New(t))

	pool, err := NewPriorityPool(100)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	err = pool.Start(5)
	if err != nil {
		t.Fatalf("Failed to start pool: %v", err)
	}

	for i := range 100 {
		count := i
		err := pool.SubmitLow(Job{
			Fn: func() {
				t.Logf("Executing job %d", count)
				time.Sleep(100 * time.Millisecond)
			},
			Comment: fmt.Sprintf("Low priority job #%d", count),
			TimeOut: 500 * time.Millisecond,
		})
		if err != nil {
			t.Errorf("Failed to submit job: %v", err)
		}
	}

	// Wait for all jobs to complete
	time.Sleep(5 * time.Second)

	defer pool.Stop()
}

// 並列実行が正しく動作することを確認するテスト
func TestParallelExecute(t *testing.T) {
	slog.SetDefault(slogt.New(t))

	pool, err := NewPriorityPool(100)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	err = pool.Start(5)
	if err != nil {
		t.Fatalf("Failed to start pool: %v", err)
	}

	for i := range 100 {
		count := i
		err := pool.SubmitLow(Job{
			Fn: func() {
				t.Logf("Executing job %d", count)
				time.Sleep(100 * time.Millisecond)
			},
			Comment: fmt.Sprintf("Low priority job #%d", count),
			TimeOut: 500 * time.Millisecond,
		})
		if err != nil {
			t.Errorf("Failed to submit job: %v", err)
		}
	}

	time.Sleep(1 * time.Second)

	if err := pool.SubmitHigh(Job{
		Fn: func() {
			time.Sleep(500 * time.Millisecond)
		},
		Comment: "High",
		TimeOut: 1 * time.Second,
	}); err != nil {
		t.Fatalf("SubmitHigh failed: %v", err)
	}

	// Wait for all jobs to complete
	time.Sleep(5 * time.Second)

	defer pool.Stop()
}
