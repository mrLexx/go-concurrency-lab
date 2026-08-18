package stats

import (
	"sync"
	"testing"
)

func TestNewStats(t *testing.T) {
	statistics := NewStats()

	if statistics == nil {
		t.Fatal("NewStats() вернул nil")
	}
	if statistics.SuccessCount() != 0 {
		t.Errorf("SuccessCount() = %d, ожидалось 0", statistics.SuccessCount())
	}
	if statistics.FailCount() != 0 {
		t.Errorf("FailCount() = %d, ожидалось 0", statistics.FailCount())
	}
	if statistics.WorkerJobCounts() == nil {
		t.Error("WorkerJobCounts() вернул nil, ожидалась инициализированная карта")
	}
	if len(statistics.WorkerJobCounts()) != 0 {
		t.Errorf("WorkerJobCounts() длина = %d, ожидалось 0", len(statistics.WorkerJobCounts()))
	}
}

func TestIncFail_And_FailCount(t *testing.T) {
	testCases := []struct {
		name          string
		incrementsNum int
		wantFailCount int64
	}{
		{name: "без инкрементов", incrementsNum: 0, wantFailCount: 0},
		{name: "один инкремент", incrementsNum: 1, wantFailCount: 1},
		{name: "несколько инкрементов", incrementsNum: 10, wantFailCount: 10},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			statistics := NewStats()
			for range testCase.incrementsNum {
				statistics.IncFail()
			}

			if got := statistics.FailCount(); got != testCase.wantFailCount {
				t.Errorf("FailCount() = %d, ожидалось %d", got, testCase.wantFailCount)
			}
			// IncFail не должен затрагивать счётчик успехов
			if got := statistics.SuccessCount(); got != 0 {
				t.Errorf("SuccessCount() = %d, ожидалось 0 (IncFail не должен на него влиять)", got)
			}
		})
	}
}

func TestIncSuccess_And_SuccessCount(t *testing.T) {
	testCases := []struct {
		name             string
		incrementsNum    int
		wantSuccessCount int64
	}{
		{name: "без инкрементов", incrementsNum: 0, wantSuccessCount: 0},
		{name: "один инкремент", incrementsNum: 1, wantSuccessCount: 1},
		{name: "несколько инкрементов", incrementsNum: 25, wantSuccessCount: 25},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			statistics := NewStats()
			for range testCase.incrementsNum {
				statistics.IncSuccess()
			}

			if got := statistics.SuccessCount(); got != testCase.wantSuccessCount {
				t.Errorf("SuccessCount() = %d, ожидалось %d", got, testCase.wantSuccessCount)
			}

			if got := statistics.FailCount(); got != 0 {
				t.Errorf("FailCount() = %d, ожидалось 0 (IncFail не должен на него влиять)", got)
			}
		})
	}
}

func TestIncWorkerJobs(t *testing.T) {
	const (
		workerOneID = 1
		workerTwoID = 2
	)

	statistics := NewStats()

	statistics.IncWorkerJobs(workerOneID)
	statistics.IncWorkerJobs(workerOneID)
	statistics.IncWorkerJobs(workerTwoID)

	jobCounts := statistics.WorkerJobCounts()

	if got := jobCounts[workerOneID]; got != 2 {
		t.Errorf("jobCounts[%d] = %d, ожидалось 2", workerOneID, got)
	}
	if got := jobCounts[workerTwoID]; got != 1 {
		t.Errorf("jobCounts[%d] = %d, ожидалось 1", workerTwoID, got)
	}

	wantTotalSuccess := int64(3)
	if got := statistics.SuccessCount(); got != wantTotalSuccess {
		t.Errorf("SuccessCount() = %d, ожидалось %d (побочный эффект IncWorkerJobs)", got, wantTotalSuccess)
	}
}

func TestWorkerJobCounts_UnknownWorker(t *testing.T) {
	statistics := NewStats()
	statistics.IncWorkerJobs(1)

	const unknownWorkerID = 999
	if got := statistics.WorkerJobCounts()[unknownWorkerID]; got != 0 {
		t.Errorf("WorkerJobCounts()[%d] = %d, ожидалось 0 для несуществующего воркера", unknownWorkerID, got)
	}
}

func TestStats_ConcurrentIncrements(t *testing.T) {
	const (
		goroutinesNum        = 100
		incrementsPerRoutine = 1000
		workersNum           = 5
	)

	statistics := NewStats()

	var wg sync.WaitGroup
	wg.Add(goroutinesNum)

	for goroutineIdx := range goroutinesNum {
		go func(goroutineIdx int) {
			defer wg.Done()

			workerID := goroutineIdx % workersNum
			for range incrementsPerRoutine {
				statistics.IncSuccess()
				statistics.IncFail()
				statistics.IncWorkerJobs(workerID)
			}
		}(goroutineIdx)
	}

	wg.Wait()

	wantFailCount := int64(goroutinesNum * incrementsPerRoutine)
	if got := statistics.FailCount(); got != wantFailCount {
		t.Errorf("FailCount() = %d, ожидалось %d", got, wantFailCount)
	}

	wantSuccessCount := int64(goroutinesNum*incrementsPerRoutine) * 2
	if got := statistics.SuccessCount(); got != wantSuccessCount {
		t.Errorf("SuccessCount() = %d, ожидалось %d", got, wantSuccessCount)
	}

	jobCounts := statistics.WorkerJobCounts()
	wantJobsPerWorker := (goroutinesNum / workersNum) * incrementsPerRoutine
	for workerID := range workersNum {
		if got := jobCounts[workerID]; got != wantJobsPerWorker {
			t.Errorf("jobCounts[%d] = %d, ожидалось %d", workerID, got, wantJobsPerWorker)
		}
	}
}

func TestWorkerJobCounts_ReturnsSafeCopy(t *testing.T) {
	statistics := NewStats()
	statistics.IncWorkerJobs(1)

	jobCounts := statistics.WorkerJobCounts()
	jobCounts[1] = 999 // мутируем полученную карту

	// Внутреннее состояние stats должно остаться нетронутым.
	if got := statistics.WorkerJobCounts()[1]; got != 1 {
		t.Fatalf("WorkerJobCounts()[1] = %d, ожидалось 1 — "+
			"внешняя мутация не должна влиять на internal map", got)
	}
}

func TestWorkerJobCounts_MutationDoesNotRaceWithWrites(t *testing.T) {
	const iterationsNum = 1000

	statistics := NewStats()
	statistics.IncWorkerJobs(1)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for range iterationsNum {
			statistics.IncWorkerJobs(1)
		}
	}()

	go func() {
		defer wg.Done()
		for i := range iterationsNum {
			jobCounts := statistics.WorkerJobCounts()
			jobCounts[1] = i // безопасная мутация собственной копии
		}
	}()

	wg.Wait()
}
