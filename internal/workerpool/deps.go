package workerpool

// Stats и интерфейс по сбору статистики успех/не успех
type Stats interface {
	IncSuccess()
	SuccessCount() int64

	IncFail()
	FailCount() int64

	IncWorkerJobs(workerID int)
	WorkerJobCounts() map[int]int
}
