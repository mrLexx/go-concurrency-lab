package workerpool

import (
	"context"
	"errors"
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

// StatsSnapshot структура среза по статистике успех/не успех
type StatsSnapshot struct {
	Success    int
	Fails      int
	WorkerJobs map[int]int
}

// WorkerPool структура самого воркпула
type WorkerPool struct {
	stats Stats

	workers int
	limit   int

	done      chan struct{}
	jobs      chan Job
	results   chan Result
	errs      chan error
	semaphore chan struct{}

	wg   sync.WaitGroup
	once sync.Once
}

// NewWorkerPool создание экземпляра воркпула
func NewWorkerPool(
	stats Stats,

	workers,
	limit,

	jobCap,
	resultCap,
	errCap int,
) *WorkerPool {
	if limit == 0 {
		limit = workers
	}

	return &WorkerPool{
		stats: stats,

		workers: workers,
		limit:   limit,

		jobs:      make(chan Job, jobCap),
		results:   make(chan Result, resultCap),
		errs:      make(chan error, errCap),
		semaphore: make(chan struct{}, limit),
		done:      make(chan struct{}),
	}
}

func (p *WorkerPool) closeDone() {
	p.once.Do(func() {
		close(p.done)
	})
}

// callExternalService вызов внешнего сервиса, с поддержкой контекста отмены
func (p *WorkerPool) callExternalService(
	job Job,
	extService ExternalServiceFunc,
) (Result, error) {
	type out struct {
		result Result
		err    error
	}

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
	case <-p.done:
		slog.Warn(
			"Не удалось отправить результат callExternalService: контекст отменён",
			"jobID", job.ID,
		)
		return Result{}, errors.New("не удалось отправить результат callExternalService: контекст отменён")
	}
}

// processJob обработка "работы"
func (p *WorkerPool) processJob(
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
			case <-p.done:
				slog.Warn("Ошибка после panic потеряна из-за отмены контекста",
					"workerID", workerID, "jobID", job.ID, "err", err)
			}
		}
	}()

	result, err := p.callExternalService(job, extService)
	if err != nil {
		select {
		case p.errs <- NewError(workerID, job.ID, err):
			p.stats.IncFail()
			return
		case <-p.done:
			slog.Warn("Ошибка после отработки extService потеряна из-за отмены контекста",
				"workerID", workerID, "jobID", job.ID, "err", err)
			return
		}
	}

	result.JobID = job.ID

	select {
	case p.results <- result:
		p.stats.IncWorkerJobs(workerID)
	case <-p.done:
		slog.Warn(
			"Не удалось отправить результат job: контекст отменён",
			"workerID", workerID,
			"jobID", job.ID,
		)
		return
	}
}

// worker сам воркер
func (p *WorkerPool) worker(
	workerID int,
	extService ExternalServiceFunc,
) {
	sl := slog.With("workerID", workerID)

	for {
		select {
		case job, ok := <-p.jobs:

			if !ok {
				sl.Debug("Канал jobs закрыт, worker завершает работу")
				return
			}
			sl.Debug("Ждем открытия канала")
			p.semaphore <- struct{}{}
			sl.Debug("Бронируем канал")
			p.processJob(workerID, job, extService)
			<-p.semaphore
			sl.Debug("Канал открыт")

		case <-p.done:
			sl.Warn("Worker остановлен по отмене контекста", "workerID", workerID)
			return
		}
	}
}

// Start запускает воркеры в работу
func (p *WorkerPool) Start(
	extService ExternalServiceFunc,
) {
	p.wg.Add(p.workers)
	for i := range p.workers {
		go func() {
			defer func() {
				p.wg.Done()
			}()

			workerID := i + 1
			p.worker(workerID, extService)
		}()
	}
}

// Shutdown остнавливает воркерпул
func (p *WorkerPool) Shutdown(ctx context.Context) error {
	slog.Debug("Начало graceful shutdown")

	workersDone := make(chan struct{})
	go func() {
		defer close(workersDone)
		p.wg.Wait()
	}()

	var err error

	select {
	case <-workersDone:
	case <-ctx.Done():
		slog.Warn("Таймаут shutdown, принудительная остановка воркеров")
		p.closeDone()
		err = ctx.Err()
	}

	return err
}

// Put размещает работу в очередь на обработку
func (p *WorkerPool) Put(job Job) error {
	select {

	case p.jobs <- job:
		return nil
	case <-p.done:
		slog.Warn(
			"Не удалось отправить job на обработку: контекст отменён",
			"jobID", job.ID,
		)
		return fmt.Errorf("не удалось отправить job на обработку: контекст отменён: jobID: %d", job.ID)
	}
}

// Results дает доступ к каналу с результатами
func (p *WorkerPool) Results() <-chan Result {
	return p.results
}

// Errs Дает доступ к каналу с ошибками
func (p *WorkerPool) Errs() <-chan error {
	return p.errs
}

// CloseJobs закрывает канал с работами
func (p *WorkerPool) CloseJobs() {
	close(p.jobs)
}

// Wait Ожидает завершения работы всех воркеров и каналов
func (p *WorkerPool) Wait() {
	wait := make(chan struct{})

	go func() {
		defer close(wait)

		p.wg.Wait()
		close(p.results)
		close(p.errs)
		close(p.semaphore)
	}()

	<-wait
}

// GetStatsSnapshot получает статистику по ошибкам/количеству успешных работа на задачу
func (p *WorkerPool) GetStatsSnapshot() StatsSnapshot {
	return StatsSnapshot{
		Success: int(p.stats.SuccessCount()),
		Fails:   int(p.stats.FailCount()),

		WorkerJobs: p.stats.WorkerJobCounts(),
	}
}
