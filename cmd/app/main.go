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

func processResult(logger *slog.Logger, resultCh <-chan wp.Result) {
	for r := range resultCh {
		logger.Info("Обработка результата", "воркер№", r.JobID)
	}
}

func processErrors(logger *slog.Logger, panicsCh <-chan error) {
	for e := range panicsCh {
		if poolErr, ok := errors.AsType[*wp.Error](e); ok {
			logger.Error(
				"Ошибка...",
				"WorkerID", poolErr.WorkerID(),
				"JobID", poolErr.JobID(),
				"Техт", poolErr.Error(),
			)
		} else {
			logger.Error("Ошибка", "неизвестная ошибка", e)
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

func sendJobs(logger *slog.Logger, pool *wp.WorkerPool, n int) {
	logger.Debug("Начинаем рассылать задачи")
	for i := range n {
		time.Sleep(1 * time.Second)
		jobID := i + 1
		logger.Debug("Задача", "id", jobID)
		if err := pool.Put(wp.Job{ID: jobID, Value: wp.Task{}}); err != nil {
			break
		}
	}
	logger.Debug("Закрываем канал с работой")
	pool.CloseJobs()
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := logger.NewLogger(
		logger.TypeHandlerText,
		logger.LevelInfo,
	)
	slog.SetDefault(logger)

	n := 5000

	workers := 10
	limit := 10
	jobCap := 0
	resultCap := 0
	errCap := 0

	pool := wp.NewWorkerPool(
		logger,
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
		processResult(logger, pool.Results())
	}()

	errsWg.Add(1)
	go func() {
		defer errsWg.Done()
		processErrors(logger, pool.Errs())
	}()

	pool.Start(extService)

	// Начинаем рассылать задачи
	jobsWg.Add(1)
	go func() {
		defer jobsWg.Done()
		sendJobs(logger, pool, n)
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
		logger.Info("Получен сигнал остановки")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := pool.Shutdown(shutdownCtx); err != nil {
			logger.Error("Остановка WorkPool", "err", err)
		}
	case <-poolFinished:
		logger.Info("Все job обработаны штатно")
	}

	logger.Info("Ждем обработки вспомогательных каналов")
	resultsWg.Wait()
	errsWg.Wait()

	logger.Info("Работа завершена")

	s := pool.GetStatsSnapshot()

	logger.Info(
		"Лог",
		"fail", s.Fails,
		"success", s.Success,
		"success", s.WorkerJobs,
	)

	time.Sleep(2 * time.Second)
}
