package main

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/mrLexx/go-concurrency-lab/internal/logger"
	"github.com/mrLexx/go-concurrency-lab/internal/stats"
	wp "github.com/mrLexx/go-concurrency-lab/internal/workerpool"
)

func processResult(resultCh <-chan wp.Result) {
	for r := range resultCh {
		slog.Info("Обработка результата", "воркер№", r.JobID)
	}
}

func processErrors(panicsCh <-chan error) {
	for e := range panicsCh {
		if poolErr, ok := errors.AsType[*wp.Error](e); ok {
			slog.Error(
				"Ошибка...",
				"WorkerID", poolErr.WorkerID(),
				"JobID", poolErr.JobID(),
				"Техт", poolErr.Error(),
			)
		} else {
			slog.Error("Ошибка", "неизвестная ошибка", e)
		}
	}
}

func extService(_ wp.Job) (wp.Result, error) {
	//nolint:gosec // псевдослучайность достаточна
	// delay := time.Duration(5000+rand.IntN(7)*50) * time.Millisecond
	delay := time.Duration(1000+rand.IntN(7)*50) * time.Millisecond

	time.Sleep(delay)

	//nolint:gosec // псевдослучайность достаточна
	if rand.Float64() < 0.3 {
		panic("чтото там")
	}
	return wp.Result{}, nil
}

func sendJobs(pool *wp.WorkerPool, n int) {
	slog.Debug("Начинаем рассылать задачи")
	for i := range n {
		time.Sleep(1 * time.Second)
		jobID := i + 1
		slog.Debug("Задача", "id", jobID)
		if err := pool.Put(wp.Job{ID: jobID, Value: wp.Task{}}); err != nil {
			break
		}
	}

	slog.Debug("Закрываем канал с работой")
	pool.CloseJobs()
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Init(
		logger.LevelInfo,
		logger.HandlerText,
	)

	n := 10

	workers := 10
	limit := 10
	jobCap := 0
	resultCap := 0
	errCap := 0

	pool := wp.NewWorkerPool(
		stats.NewStats(),
		workers,
		limit,
		jobCap,
		resultCap,
		errCap,
	)

	var resultsWg sync.WaitGroup
	var errsWg sync.WaitGroup
	var jobsWg sync.WaitGroup

	resultsWg.Add(1)
	go func() {
		defer resultsWg.Done()
		processResult(pool.Results())
	}()

	errsWg.Add(1)
	go func() {
		defer errsWg.Done()
		processErrors(pool.Errs())
	}()

	pool.Start(extService)

	// Начинаем рассылать задачи
	jobsWg.Add(1)
	go func() {
		defer jobsWg.Done()
		sendJobs(pool, n)
	}()

	// Ждем когда задачи все отошлются и отработает воркерпул, для штатного завершения
	poolFinished := make(chan struct{})
	go func() {
		defer close(poolFinished)
		jobsWg.Wait()
		pool.Wait()
	}()

	// Смотрим что вперед завершится: или воркерпул или прилетит отмена контекста
	select {
	case <-ctx.Done():
		slog.Info("Получен сигнал остановки")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := pool.Shutdown(shutdownCtx); err != nil {
			slog.Error("Остановка WorkPool", "err", err)
		}
	case <-poolFinished:
		slog.Info("Все job обработаны штатно")
	}

	slog.Info("Ждем обработки вспомогательных каналов")
	resultsWg.Wait()
	errsWg.Wait()

	slog.Info("Работа завершена")

	s := pool.GetStatsSnapshot()

	slog.Info(
		"Лог",
		"fail", s.Fails,
		"success", s.Success,
		"success", s.WorkerJobs,
	)

	time.Sleep(2 * time.Second)
}
