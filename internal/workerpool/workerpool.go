package workerpool

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// ExternalServiceFunc сигнатура внешней функции обработки задачи
type ExternalServiceFunc func(j Job) (Result, error)

// Task структура самой задача
type Task struct{}

// Job структура
type Job struct {
	ID    int
	Value Task
}

// Result структура резульатат выполенения работы
type Result struct {
	JobID int
	Value string
}

// workerPool структура самого воркпула
type workerPool struct {
	logger *slog.Logger
	stats  Stats

	workers int
	limit   int

	jobs      chan Job
	results   chan Result
	errs      chan error
	semaphore chan struct{}

	wg sync.WaitGroup
}

// StatsSnapshot структура среза по статистике успех/не успех
type StatsSnapshot struct {
	Success    int
	Fails      int
	WorkerJobs map[int]int
}

// NewWorkerPool создание экземпляра воркпула
func NewWorkerPool(
	logger *slog.Logger,
	stats Stats,
	workers,
	limit,
	jobCap,
	resultCap,
	errCap int,
) *workerPool {
	if limit == 0 {
		limit = workers
	}

	return &workerPool{
		logger: logger,
		stats:  stats,

		workers: workers,
		limit:   limit,

		jobs:      make(chan Job, jobCap),
		results:   make(chan Result, resultCap),
		errs:      make(chan error, errCap),
		semaphore: make(chan struct{}, limit),
	}
}

// callExternalService вызов внешнего сервиса, с поддержкой контекста отмены
func (p *workerPool) callExternalService(
	ctx context.Context,
	job Job,
	extService ExternalServiceFunc,
) (Result, error) {
	type out struct {
		result Result
		err    error
	}
	logger := p.logger.With("jobID", job.ID)

	done := make(chan out, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("%v", r)
				done <- out{result: Result{}, err: err}
			}
		}()
		result, err := extService(job)
		done <- out{result: result, err: err}
	}()

	select {
	case o := <-done:
		return o.result, o.err
	case <-ctx.Done():
		logger.Debug(
			"Не удалось отправить результат callExternalService: контекст отменён",
		)
		return Result{}, ctx.Err()
	}
}

// processJob обработка "работы"
func (p *workerPool) processJob(
	ctx context.Context,
	workerID int,
	job Job,
	extService ExternalServiceFunc,
) {
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("%v", r)

			select {
			case p.errs <- NewError(workerID, job.ID, err):
				p.stats.IncFail()
			case <-ctx.Done():
				p.logger.Error("Ошибка после panic потеряна из-за отмены контекста",
					"workerID", workerID, "jobID", job.ID, "err", err)
			}
		}
	}()

	result, err := p.callExternalService(ctx, job, extService)
	if err != nil {
		select {
		case p.errs <- NewError(workerID, job.ID, err):
			p.stats.IncFail()
			return
		case <-ctx.Done():
			p.logger.Error("Ошибка после отработки extService потеряна из-за отмены контекста",
				"workerID", workerID, "jobID", job.ID, "err", err)
			return
		}
	}

	result.JobID = job.ID

	select {
	case p.results <- result:
		p.stats.IncWorkerJobs(workerID)
	case <-ctx.Done():
		p.logger.Debug(
			"Не удалось отправить результат job: контекст отменён",
			"workerID", workerID,
			"jobID", job.ID,
		)
		return
	}
}

// worker сам воркер
func (p *workerPool) worker(
	ctx context.Context,
	workerID int,
	extService ExternalServiceFunc,
) {
	logger := p.logger.With("workerID", workerID)
	for {
		select {
		case job, ok := <-p.jobs:

			if !ok {
				logger.Debug("Канал jobs закрыт, worker завершает работу")
				return
			}
			logger.Debug("Ждем открытия канала")
			p.semaphore <- struct{}{}
			logger.Debug("Бронируем канал")
			p.processJob(ctx, workerID, job, extService)
			<-p.semaphore
			logger.Debug("Канал открыт")

		case <-ctx.Done():
			logger.Debug("Worker остановлен по отмене контекста", "workerID", workerID)
			return
		}
	}
}

// Start запускает воркеры в работу
func (p *workerPool) Start(
	ctx context.Context,
	extService ExternalServiceFunc,
) {
	p.wg.Add(p.workers)
	for i := range p.workers {
		go func() {
			defer func() {
				p.wg.Done()
			}()

			workerID := i + 1
			p.worker(ctx, workerID, extService)
		}()
	}
}

// Put размещает работу в очередь на обработку
func (p *workerPool) Put(ctx context.Context, job Job) error {
	select {

	case p.jobs <- job:
		return nil
	case <-ctx.Done():
		p.logger.Debug(
			"Не удалось отправить job на обработку: контекст отменён",
			"jobID", job.ID,
		)
		return ctx.Err()
	}
}

// Results дает доступ к каналу с результатами
func (p *workerPool) Results() <-chan Result {
	return p.results
}

// Errs Дает доступ к каналу с ошибками
func (p *workerPool) Errs() <-chan error {
	return p.errs
}

// CloseJobs закрывает канал с работами
func (p *workerPool) CloseJobs() {
	close(p.jobs)
}

// Wait Ожидает завершения работы всех воркеров и каналов
func (p *workerPool) Wait() {
	p.wg.Wait()
	close(p.results)
	close(p.errs)
	close(p.semaphore)
}

// GetStatsSnapshot получает статистику по ошибкам/количеству успешных работа на задачу
func (p *workerPool) GetStatsSnapshot() StatsSnapshot {
	return StatsSnapshot{
		Success: int(p.stats.SuccessCount()),
		Fails:   int(p.stats.FailCount()),

		WorkerJobs: p.stats.WorkerJobCounts(),
	}
}
