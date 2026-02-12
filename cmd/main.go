package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/sharelib-jp/sharelib/workers"
)

func main() {
	file, err := os.OpenFile("app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	logger := slog.New(slog.NewJSONHandler(file, nil))
	slog.SetDefault(logger)

	// 3並列で実行するプールを作成
	pool, err := workers.NewPriorityPool(100)
	if err != nil {
		fmt.Println("failed to create pool:", err)
		return
	}
	if err := pool.Start(3); err != nil {
		fmt.Println("failed to start pool:", err)
		return
	}

	// Submit a lot of low priority jobs (book registration)
	for i := 1; i <= 5; i++ {
		count := i
		if err := pool.SubmitLow(workers.Job{
			Fn: func() {
				fmt.Printf("  -> 蔵書登録 %d 完了\n", count)
				time.Sleep(1 * time.Second)
			},
			Comment: fmt.Sprintf("蔵書登録 job #%d", count),
		}); err != nil {
			fmt.Println("submit low failed:", err)
		}
	}
	if err := pool.SubmitLow(workers.Job{
		Fn: func() {
			fmt.Println("エラーを吐くであろうタイムアウト")
			time.Sleep(30 * time.Second)
		},
		Comment: fmt.Sprintf("Error Test Timeout #%d\n", 999),
		TimeOut: 5 * time.Second,
	}); err != nil {
		fmt.Println("submit low failed:", err)
	}

	// 少し遅れて高優先度（貸出処理）を投入
	time.Sleep(500 * time.Millisecond)
	l, err := pool.ListJobs()
	if err != nil {
		fmt.Println("list jobs failed:", err)
	} else {
		for i, jobInfo := range l {
			fmt.Printf("Queued Job %d - Priority: %s, Comment: %s, Time: %s, TimeOut: %s\n", i, jobInfo.Priority, jobInfo.Comment, jobInfo.Time.Format(time.RFC3339), jobInfo.TimeOut)
		}
		fmt.Printf("Current queued jobs: %d\n", len(l))
	}
	if err := pool.SubmitHigh(workers.Job{
		Fn: func() {
			fmt.Println("  !!! 貸出処理（特急）完了 !!!")
		},
		Comment: "貸出処理（特急）",
		TimeOut: 2 * time.Second,
	}); err != nil {
		fmt.Println("submit high failed:", err)
	}

	time.Sleep(5 * time.Second)
	pool.Stop()
}
