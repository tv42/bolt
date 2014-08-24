// Package batch is a batching wrapper for Bolt transactions.
package batch

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/boltdb/bolt"
)

// Default values for batch size and delay before it is started.
const (
	DefaultMaxSize  = 1000
	DefaultMaxDelay = 10 * time.Millisecond
)

// New returns a new Batcher.
func New(db *bolt.DB, options ...Option) *Batcher {
	b := &Batcher{
		db:       db,
		maxSize:  DefaultMaxSize,
		maxDelay: DefaultMaxDelay,
	}
	for _, opt := range options {
		opt.fn(b)
	}
	return b
}

// Option is used to configure the Batcher.
type Option struct {
	// hide the actual function so calling code can't mutate a Batcher
	// after New
	fn func(*Batcher)
}

// MaxSize sets the maximum size of a batch.
func MaxSize(size int) Option {
	if size <= 0 {
		panic(fmt.Errorf("batch.MaxSize is impossibly low: %v", size))
	}
	return Option{
		fn: func(b *Batcher) {
			b.maxSize = size
		},
	}
}

// MaxDelay sets the maximum delay before a batch starts.
func MaxDelay(delay time.Duration) Option {
	if delay <= 0 {
		panic(fmt.Errorf("batch.MaxDelay is impossibly low: %v", delay))
	}
	return Option{
		fn: func(b *Batcher) {
			b.maxDelay = delay
		},
	}
}

// Batcher executes multiple mutators in a single transaction.
type Batcher struct {
	db       *bolt.DB
	maxSize  int
	maxDelay time.Duration

	cur unsafe.Pointer
}

// Update calls fn as part of a batch.. It behaves similar to
// bolt.DB.Update, except:
//
// 1. multiple Update function calls can be combined into a single
// Bolt transaction.
//
// 2. the function passed to Batcher.Update may be called multiple
// times, regardless of whether it returns error or not.
//
// This means that Update function side effects must be idempotent and
// take permanent effect only after a successful return is seen in
// caller.
func (batcher *Batcher) Update(fn func(*bolt.Tx) error) error {
	b := batch{
		batcher: batcher,
	}
	b.mu.Lock()
	for {
		var cur = (*batch)(atomic.LoadPointer(&batcher.cur))
		if cur != nil {
			// another call is cur
			if ch := cur.merge(fn); ch != nil {
				// cur will call our fn
				err := <-ch
				if p, ok := err.(panicked); ok {
					panic(p.reason)
				}
				return err
			}
			// this batch refused to accept more work
		}

		// try to become cur
		if atomic.CompareAndSwapPointer(&batcher.cur, unsafe.Pointer(cur), unsafe.Pointer(&b)) {
			// we are now cur
			return b.master(batcher.db, fn)
		}
	}
}

type call struct {
	fn  func(*bolt.Tx) error
	err chan<- error
}

type batch struct {
	batcher *Batcher
	mu      sync.Mutex
	calls   []call
	full    chan struct{}
	started bool
}

// caller has locked batch.mu for us
func (b *batch) master(db *bolt.DB, fn func(*bolt.Tx) error) error {
	b.full = make(chan struct{}, 1)
	ch := make(chan error, 1)
	b.calls = append(b.calls, call{fn: fn, err: ch})
	b.mu.Unlock()

	t := time.NewTimer(b.batcher.maxDelay)
	select {
	case <-t.C:
	case <-b.full:
		t.Stop()
	}

	b.mu.Lock()
	b.started = true
	b.mu.Unlock()

retry:
	for len(b.calls) > 0 {
		var failIdx = -1
		err := db.Update(func(tx *bolt.Tx) error {
			for i, c := range b.calls {
				if err := safelyCall(c.fn, tx); err != nil {
					failIdx = i
					return err
				}
			}
			return nil
		})

		if failIdx >= 0 {
			// take the failing transaction out of the batch. it's safe to
			// shorten b.calls here because b.started has been set.
			c := b.calls[failIdx]
			b.calls[failIdx], b.calls = b.calls[len(b.calls)-1], b.calls[:len(b.calls)-1]
			// run it solo, report result, continue with the rest of the batch
			c.err <- db.Update(func(tx *bolt.Tx) error {
				return safelyCall(fn, tx)
			})
			continue retry
		}

		// pass success, or bolt internal errors, to all callers
		for _, c := range b.calls {
			if c.err != nil {
				c.err <- err
			}
		}
		break retry
	}

	err := <-ch
	if p, ok := err.(panicked); ok {
		panic(p.reason)
	}
	return err
}

type panicked struct {
	reason interface{}
}

func (p panicked) Error() string {
	if err, ok := p.reason.(error); ok {
		return err.Error()
	}
	return fmt.Sprintf("panic: %v", p.reason)
}

func safelyCall(fn func(*bolt.Tx) error, tx *bolt.Tx) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = panicked{p}
		}
	}()
	return fn(tx)
}

func (b *batch) merge(fn func(*bolt.Tx) error) chan error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.started {
		return nil
	}

	var ch chan error
	if len(b.calls) < b.batcher.maxSize {
		ch = make(chan error, 1)
		c := call{
			fn:  fn,
			err: ch,
		}
		b.calls = append(b.calls, c)
	}

	if len(b.calls) >= b.batcher.maxSize {
		// wake up batch, it's ready to run
		select {
		case b.full <- struct{}{}:
		default:
		}
	}

	return ch
}
