package workerpool

// Error workpool ошибка
type Error struct {
	workerID int
	jobID    int
	err      error
}

// NewError конструктор
func NewError(workerID, jobID int, err error) error {
	return &Error{
		workerID: workerID,
		jobID:    jobID,
		err:      err,
	}
}

func (err *Error) Error() string {
	return err.err.Error()
}

func (err *Error) Unwrap() error {
	return err.err
}

// WorkerID получение WorkerID
func (err *Error) WorkerID() int {
	return err.workerID
}

// JobID получение JobID
func (err *Error) JobID() int {
	return err.jobID
}
