package sqlretry

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"
)

// RetryError represents an error received for an operation that will be retried.
type RetryError struct {
	err     error
	retry   int
	retries int
	next    time.Duration
}

// Internal constructor
func newRetryError(err error, retry, retries int, next time.Duration) *RetryError {
	return &RetryError{err, retry, retries, next}
}

// Err returns the underlying error
func (e *RetryError) Err() error {
	return e.err
}

// Error returns a string representation of the error including retrying information.
// The original error string is at the end after "; ".
func (e *RetryError) Error() string {
	if e.retries > 0 || e.retry < e.retries {
		return fmt.Sprintf("retry %d of %d, next in %s; %s", e.retry, e.retries, e.next.String(), e.err)
	}
	return fmt.Sprintf("retry %d of %d; %s", e.retry, e.retries, e.err)
}

// Retry returns the current retry and the total numbers of retries to be performed.
// If the total number of retries is zero, infinte replies are performed.
func (e *RetryError) Retry() (retry int, retries int) {
	return e.retry, e.retries
}

// Next returns the time after which the operation will be repeated.  A zero value can
// have different meanings depending on whether there are more retries to be performed.
func (e *RetryError) Next() time.Duration {
	return e.next
}

// Temporary always returns true
func (e *RetryError) Temporary() bool {
	return true
}

type RetryDB struct {
	*sql.DB
	retries    int
	multiplier float64
	backoff    time.Duration
	maxwait    time.Duration
	sem        chan struct{}
	canRetry   func(error) bool
}

// TODO: generate open params to be able to reconnect to a pool of servers.
func NewRetryDB(driver, dsn string) (*RetryDB, error) {
	db, err := sql.Open(driver, dsn)
	return &RetryDB{DB: db, canRetry: CanRetry}, err
}

func (db *RetryDB) SetRetries(retries int) {
	db.retries = retries
}

func (db *RetryDB) SetBackoff(backoff time.Duration) {
	db.backoff = backoff
}

func (db *RetryDB) SetMultiplier(multiplier float64) {
	db.multiplier = multiplier
}

func (db *RetryDB) SetMaxWait(maxwait time.Duration) {
	db.maxwait = maxwait
}

func (db *RetryDB) SetParallel(n int) {
	db.sem = make(chan struct{}, n)
}

func (db *RetryDB) SetCanRetry(f func(error) bool) {
	db.canRetry = f
}

func (db *RetryDB) QueryErrCh(errCh chan<- error, query string, args ...interface{}) (rs *sql.Rows, err error) {
	if db.sem != nil {
		db.sem <- struct{}{}
	}
	backoff := db.backoff

	var i int
	for {
		if db.retries > 0 && i >= db.retries {
			break
		} else {
			i++
		}

		rs, err = db.DB.Query(query, args...)
		if err == nil {
			break
		}
		if !db.canRetry(err) {
			break
		}

		if errCh != nil {
			errCh <- newRetryError(err, i, db.retries, backoff)
		}

		time.Sleep(backoff)

		backoff = time.Duration(float64(backoff) * db.multiplier)
		if db.maxwait > 0 && backoff > db.maxwait {
			backoff = db.maxwait
		}
	}
	if errCh != nil {
		close(errCh)
	}
	if db.sem != nil {
		<-db.sem
	}
	return
}

func (db *RetryDB) Query(query string, args ...interface{}) (rs *sql.Rows, err error) {
	return db.QueryErrCh(nil, query, args...)
}

// Errors that we know we can retry on.
var retryErrs = []error{driver.ErrBadConn}

// Errors that have the Temporary method can be retired if they are temporary (net.Error for example.)
type temporary interface {
	Temporary() bool
}

// CanRetry returns true if err is an error upon which we can retry the operation.
func CanRetry(err error) bool {
	for _, e := range retryErrs {
		if err == e {
			return true
		}
	}
	if err, isTemporary := err.(temporary); isTemporary {
		return err.Temporary()
	}
	return false
}

// TODO:
//
//    func (db *DB) Begin() (*Tx, error)
//    + wrap Tx
//    func (db *DB) Close() error
//    func (db *DB) Exec(query string, args ...interface{}) (Result, error)
//    func (db *DB) Ping() error
//    func (db *DB) Prepare(query string) (*Stmt, error)
//    func (db *DB) QueryRow(query string, args ...interface{}) *Row
//
