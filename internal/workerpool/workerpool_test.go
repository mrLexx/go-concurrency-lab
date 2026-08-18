package workerpool

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrLexx/go-concurrency-lab/internal/stats"
)

func asPoolError(err error) (*Error, bool) {
	if poolErr, ok := errors.AsType[*Error](err); ok {
		return poolErr, true
	}
	return nil, false
}

// syncBuffer — потокобезопасная обёртка над bytes.Buffer для перехвата
// вывода slog из горутин воркеров.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func NewLogBuf(t *testing.T, level slog.Level) *syncBuffer {
	t.Helper()

	logBuf := &syncBuffer{}

	prevLogger := slog.Default()

	slog.SetDefault(slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: level})))

	t.Cleanup(func() { slog.SetDefault(prevLogger) })

	return logBuf
}

func TestError(t *testing.T) {
	t.Parallel()

	cause := errors.New("error")
	err := NewError(3, 42, cause)

	poolErr, ok := asPoolError(err)
	if !ok {
		t.Fatalf("ожидалась *Error, получено: %T", err)
	}

	if got := poolErr.Error(); got != cause.Error() {
		t.Errorf("Error() = %q, ожидалось %q", got, cause.Error())
	}

	if !errors.Is(poolErr, cause) {
		t.Errorf("Unwrap(): errors.Is(poolErr, cause) = false, ожидалось true")
	}

	if got := poolErr.WorkerID(); got != 3 {
		t.Errorf("WorkerID() = %d, ожидалось 3", got)
	}

	if got := poolErr.JobID(); got != 42 {
		t.Errorf("JobID() = %d, ожидалось 42", got)
	}
}

func drainWithTimeout[T any](t *testing.T, ch <-chan T, timeout time.Duration) []T {
	t.Helper()

	var items []T
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	for {
		select {
		case value, ok := <-ch:
			if !ok {
				return items
			}
			items = append(items, value)

		case <-deadline.C:
			t.Fatalf("drainWithTimeout: таймаут %s истёк, канал не закрылся (получено значений: %d)", timeout, len(items))
			return items
		}
	}
}

type ExtServiceBehavior struct {
	Result Result
	Err    error
	Panic  any
	Delay  time.Duration
	WaitCh <-chan struct{}
}

func fakeExternalService(behavior ExtServiceBehavior) ExternalServiceFunc {
	return func(_ Job) (Result, error) {
		switch {
		case behavior.WaitCh != nil:
			<-behavior.WaitCh
		case behavior.Delay > 0:
			time.Sleep(behavior.Delay)
		}

		if behavior.Panic != nil {
			panic(behavior.Panic)
		}

		return behavior.Result, behavior.Err
	}
}

func TestNewWorkerPool(t *testing.T) {
	testCases := []struct {
		name string

		workers   int
		limit     int
		jobCap    int
		resultCap int
		errCap    int

		expectedWorkers int
		expectedLimit   int
	}{
		{
			name: "limit == 0, должен стать равным workers",

			workers:   10,
			limit:     0,
			jobCap:    0,
			resultCap: 0,
			errCap:    0,

			expectedWorkers: 10,
			expectedLimit:   10,
		},
		{
			name: "llimit явно задан и меньше workers",

			workers:   10,
			limit:     5,
			jobCap:    0,
			resultCap: 0,
			errCap:    0,

			expectedWorkers: 10,
			expectedLimit:   5,
		},
		{
			name: "limit явно задан и больше workers",

			workers:   10,
			limit:     15,
			jobCap:    0,
			resultCap: 0,
			errCap:    0,

			expectedWorkers: 10,
			expectedLimit:   15,
		},
		{
			name: "емкость канала jobCap явно задана",

			workers:   10,
			limit:     15,
			jobCap:    10,
			resultCap: 0,
			errCap:    0,

			expectedWorkers: 10,
			expectedLimit:   15,
		},
		{
			name: "емкость канала resultCap явно задана",

			workers:   10,
			limit:     15,
			jobCap:    0,
			resultCap: 5,
			errCap:    0,

			expectedWorkers: 10,
			expectedLimit:   15,
		},
		{
			name: "емкость канала errCap явно задана",

			workers:   10,
			limit:     15,
			jobCap:    0,
			resultCap: 0,
			errCap:    7,

			expectedWorkers: 10,
			expectedLimit:   15,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			workerPool := NewWorkerPool(
				stats.NewStats(),
				tc.workers,
				tc.limit,
				tc.jobCap,
				tc.resultCap,
				tc.errCap,
			)
			if workerPool == nil {
				t.Fatal("NewWorkerPool() вернул nil")
			}

			assertWorkerPoolFields(t, workerPool, tc.expectedWorkers, tc.expectedLimit, tc.jobCap, tc.resultCap, tc.errCap)
		})
	}
}

func assertWorkerPoolFields(
	t *testing.T,
	wp *WorkerPool,
	expectedWorkers int,
	expectedLimit int,
	expectedJobCap int,
	expectedResultCap int,
	expectedErrCap int,
) {
	t.Helper()

	if wp.workers != expectedWorkers {
		t.Errorf("workers = %d, ожидалось %d", wp.workers, expectedWorkers)
	}

	if wp.limit != expectedLimit {
		t.Errorf("limit = %d, ожидалось %d", wp.limit, expectedLimit)
	}

	if cap(wp.jobs) != expectedJobCap {
		t.Errorf("cap(jobs) = %d, ожидалось %d", cap(wp.jobs), expectedJobCap)
	}

	if cap(wp.results) != expectedResultCap {
		t.Errorf("cap(results) = %d, ожидалось %d", cap(wp.results), expectedResultCap)
	}

	if cap(wp.errs) != expectedErrCap {
		t.Errorf("cap(errs) = %d, ожидалось %d", cap(wp.errs), expectedErrCap)
	}
}

func TestWorkerPool_GetStatsSnapshot(t *testing.T) {
	t.Run("GetStatsSnapshot при пустой работе возвращает все по нулям", func(t *testing.T) {
		t.Parallel()

		pool := NewWorkerPool(
			stats.NewStats(),
			1,
			0,
			0,
			0,
			0,
		)

		pool.Start(fakeExternalService(ExtServiceBehavior{}))

		pool.closeDone()
		s := pool.GetStatsSnapshot()

		if s.Fails != 0 {
			t.Errorf("Fails: получено %d, ожидалось 0", s.Fails)
		}
		if s.Success != 0 {
			t.Errorf("Success: получено %d, ожидалось 0", s.Success)
		}
		if len(s.WorkerJobs) != 0 {
			t.Errorf("WorkerJobs: получено %d значений, ожидалось 0", len(s.WorkerJobs))
		}
	})

	t.Run("GetStatsSnapshot возвращает одну ошибку", func(t *testing.T) {
		t.Parallel()

		pool := NewWorkerPool(
			stats.NewStats(),
			1,
			0,
			0,
			0,
			0,
		)

		pool.Start(fakeExternalService(ExtServiceBehavior{Err: errors.New("")}))
		_ = pool.Put(Job{})

		<-pool.Errs()
		pool.CloseJobs()

		waitDone := make(chan struct{})
		go func() {
			defer close(waitDone)
			pool.Wait()
		}()

		select {
		case <-waitDone:
			// ok: воркер отработал job и вышел
		case <-time.After(2 * time.Second):
			t.Fatal("worker не завершился вовремя после CloseJobs()")
		}

		pool.closeDone()
		s := pool.GetStatsSnapshot()

		if s.Fails != 1 {
			t.Errorf("Fails: получено %d, ожидалось 1", s.Fails)
		}
		if s.Success != 0 {
			t.Errorf("Success: получено %d, ожидалось 0", s.Success)
		}
		if len(s.WorkerJobs) != 0 {
			t.Errorf("WorkerJobs: получено %d значений, ожидалось 0", len(s.WorkerJobs))
		}
	})

	t.Run("GetStatsSnapshot возвращает один успех", func(t *testing.T) {
		t.Parallel()

		pool := NewWorkerPool(
			stats.NewStats(),
			1,
			0,
			0,
			0,
			0,
		)

		pool.Start(fakeExternalService(ExtServiceBehavior{}))

		_ = pool.Put(Job{})
		<-pool.Results()

		_ = pool.Put(Job{})
		<-pool.Results()

		pool.CloseJobs()

		waitDone := make(chan struct{})
		go func() {
			defer close(waitDone)
			pool.Wait()
		}()

		select {
		case <-waitDone:
			// ok: воркер отработал job и вышел
		case <-time.After(2 * time.Second):
			t.Fatal("worker не завершился вовремя после CloseJobs()")
		}

		pool.closeDone()
		s := pool.GetStatsSnapshot()

		if s.Fails != 0 {
			t.Errorf("Fails: получено %d, ожидалось 0", s.Fails)
		}
		if s.Success != 2 {
			t.Errorf("Success: получено %d, ожидалось 2", s.Success)
		}
		if len(s.WorkerJobs) != 1 {
			t.Errorf("WorkerJobs: получено %d значений, ожидалось 1", len(s.WorkerJobs))
		}
	})
}

func TestWorkerPool_processJob_lostError(t *testing.T) {
	logBuf := NewLogBuf(t, slog.LevelDebug)

	pool := NewWorkerPool(
		stats.NewStats(),
		1,
		0,
		0,
		0,
		0,
	)

	pool.Start(fakeExternalService(ExtServiceBehavior{Err: errors.New("boom")}))

	if err := pool.Put(Job{ID: 1}); err != nil {
		t.Fatalf("Put вернул ошибку: %v", err)
	}

	pool.closeDone()

	waitDone := make(chan struct{})
	go func() {
		defer close(waitDone)
		pool.wg.Wait()
	}()

	select {
	case <-waitDone:
		// ok: worker завершился
	case <-time.After(2 * time.Second):
		t.Fatal("worker не завершился вовремя после closeDone()")
	}

	const wantMsg = "Ошибка после отработки extService потеряна из-за отмены контекста"
	if got := logBuf.String(); !strings.Contains(got, wantMsg) {
		t.Errorf("лог не содержит ожидаемое предупреждение %q, получено:\n%s", wantMsg, got)
	}
}

func TestWorkerPool_worker(t *testing.T) {
	t.Run("CloseJobs при пустом канале jobs завершает всех воркеров", func(t *testing.T) {
		t.Parallel()

		const workers = 5

		pool := NewWorkerPool(
			stats.NewStats(),
			workers,
			workers,
			0,
			0,
			0,
		)

		pool.Start(fakeExternalService(ExtServiceBehavior{}))

		// Ни одной job не отправляем — сразу закрываем канал.
		pool.CloseJobs()

		waitDone := make(chan struct{})
		go func() {
			defer close(waitDone)
			pool.Wait()
		}()

		select {
		case <-waitDone:
			// ok: все воркеры вышли из worker() штатно
		case <-time.After(2 * time.Second):
			t.Fatal("Wait() не завершился за отведённое время — воркеры зависли при пустом jobs")
		}

		// Wait() вернулся, results и errs должны быть закрыты и пусты.
		gotResults := drainWithTimeout(t, pool.Results(), 500*time.Millisecond)
		if len(gotResults) != 0 {
			t.Errorf("results: получено %d значений, ожидалось 0", len(gotResults))
		}

		gotErrs := drainWithTimeout(t, pool.Errs(), 500*time.Millisecond)
		if len(gotErrs) != 0 {
			t.Errorf("errs: получено %d значений, ожидалось 0", len(gotErrs))
		}
	})

	t.Run("закрытие done прерывает воркеров, ожидающих job", func(t *testing.T) {
		t.Parallel()

		const workers = 5

		pool := NewWorkerPool(
			stats.NewStats(),
			workers,
			workers,
			0,
			0,
			0,
		)

		pool.Start(fakeExternalService(ExtServiceBehavior{}))

		pool.closeDone()

		waitDone := make(chan struct{})
		go func() {
			defer close(waitDone)
			pool.wg.Wait()
		}()

		select {
		case <-waitDone:
			// ok: все воркеры вышли из worker() штатно
		case <-time.After(2 * time.Second):
			t.Fatal("Wait() не завершился за отведённое время — воркеры зависли при пустом jobs")
		}
	})

	t.Run("закрытие done прерывает воркера в середине обработки job", func(t *testing.T) {
		t.Parallel()

		var (
			extSvcStartedCh = make(chan struct{}) // сигнал: extService реально начал выполняться
			waitCh          = make(chan struct{}) // "зависание" extService, управляемое тестом
		)

		extSvc := func(_ Job) (Result, error) {
			// стартуем
			close(extSvcStartedCh)

			<-waitCh
			return Result{}, nil
		}

		pool := NewWorkerPool(
			stats.NewStats(),
			1, // workers
			1, // limit
			0, // jobCap
			1, // resultCap — с запасом, чтобы processJob не заблокировался на отправке
			1, // errCap
		)

		pool.Start(extSvc)

		t.Cleanup(func() { close(waitCh) })

		if err := pool.Put(Job{ID: 1}); err != nil {
			t.Fatalf("Put вернул ошибку: %v", err)
		}

		select {
		case <-extSvcStartedCh:
			// ok: extService запущен, worker сейчас внутри processJob
		case <-time.After(2 * time.Second):
			t.Fatal("extService не был вызван — job не дошла до worker'а")
		}

		// extService уже стартовал, семафор должен быть занят
		if got := len(pool.semaphore); got != 1 {
			t.Fatalf("semaphore: занято %d слотов, ожидался 1 (job в процессе обработки)", got)
		}

		pool.closeDone()

		// callExternalService должна вернуться немедленно через
		// case <-p.done, не дожидаясь реального завершения extService
		// (который всё ещё стоит на waitCh). Проверяем это через wg.Wait(),
		// как и в предыдущих тестах группы.
		waitDone := make(chan struct{})
		go func() {
			defer close(waitDone)
			pool.wg.Wait()
		}()

		select {
		case <-waitDone:
			// ok: worker завершился, не дожидаясь extService
		case <-time.After(2 * time.Second):
			t.Fatal("worker завис в середине обработки job после closeDone() — вероятно, не отработал case <-p.done в callExternalService")
		}

		// worker завершился штатно, семафор должен быть освобождён
		if got := len(pool.semaphore); got != 0 {
			t.Errorf("semaphore: занято %d слотов после завершения worker'а, ожидалось 0", got)
		}
	})
}

func TestWorkerPool_Put_ContextCancelled(t *testing.T) {
	t.Run("отправка работы при done", func(t *testing.T) {
		t.Parallel()

		const workers = 1

		pool := NewWorkerPool(
			stats.NewStats(),
			workers,
			0,
			0,
			0,
			0,
		)

		pool.Start(fakeExternalService(ExtServiceBehavior{}))

		pool.closeDone()

		testJob := Job{ID: 42, Value: Task{}}
		err := pool.Put(testJob)

		if err == nil {
			t.Fatal("ожидалась ошибка, но метод вернул nil")
		}

		expectedSubstring := "не удалось отправить job на обработку"
		expectedID := "jobID: 42"
		if !strings.Contains(err.Error(), expectedSubstring) {
			t.Errorf("ошибка не содержит базовый текст. Получено: %v", err)
		}

		if !strings.Contains(err.Error(), expectedID) {
			t.Errorf("ошибка не содержит правильный job.ID. Получено: %v", err)
		}

		pool.CloseJobs()

		waitDone := make(chan struct{})
		go func() {
			defer close(waitDone)
			pool.Wait()
		}()

		gotErrs := drainWithTimeout(t, pool.Errs(), 500*time.Millisecond)
		if len(gotErrs) != 0 {
			t.Errorf("errs: получено %d значений, ожидалось 0", len(gotErrs))
		}
	})
}

func TestWorkerPool_Shutdown(t *testing.T) {
	t.Run("воркеры укладываются в таймаут — Shutdown возвращает nil", func(t *testing.T) {
		t.Parallel()

		pool := NewWorkerPool(
			stats.NewStats(),
			1, // workers
			0, // limit
			0, // jobCap
			0, // resultCap
			0, // errCap
		)

		pool.Start(fakeExternalService(ExtServiceBehavior{}))

		pool.CloseJobs()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		if err := pool.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown() = %v, ожидался nil", err)
		}
	})

	t.Run("таймаут ctx принудительно останавливает воркеров", func(t *testing.T) {
		t.Parallel()

		waitCh := make(chan struct{})
		t.Cleanup(func() { close(waitCh) })

		extSvc := func(_ Job) (Result, error) {
			<-waitCh // "зависает", пока тест сам не отпустит
			return Result{}, nil
		}

		pool := NewWorkerPool(
			stats.NewStats(),
			1, // workers
			1, // limit
			0, // jobCap
			1, // resultCap — с запасом, чтобы processJob не заблокировался на отправке
			1, // errCap
		)

		pool.Start(extSvc)

		if err := pool.Put(Job{ID: 1}); err != nil {
			t.Fatalf("Put вернул ошибку: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		err := pool.Shutdown(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Shutdown() = %v, ожидалась context.DeadlineExceeded", err)
		}

		// Shutdown при таймауте обязан принудительно закрыть done
		if err := pool.Put(Job{ID: 2}); err == nil {
			t.Error("Put после Shutdown-таймаута вернул nil — ожидалась ошибка (done должен быть закрыт)")
		}
	})
}
