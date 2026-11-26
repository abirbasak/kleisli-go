// parallel.go bridges Seq[T] to async.Pipe[T] — the one thing this
// package deliberately doesn't do itself: running an operation across
// goroutines.
package stream

import (
	"context"

	"github.com/azuiktech/kleisli-go/async"
)

// ToPipe hands s's elements to async.Pipe without materializing the Seq
// first — items are produced lazily into the channel as the downstream
// consumer reads them.
func (s Seq[T]) ToPipe() async.Pipe[T] { return async.FromIter(s.seq) }

// Parallel applies fn to s's elements on n worker goroutines, lazily:
// nothing runs until the result is iterated. Output order is completion
// order. Stopping early cancels the workers. Panics if n < 1.
func (s Seq[T]) Parallel[U any](n int, fn func(T) U) Seq[U] {
	return FromSeq(func(yield func(U) bool) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		async.FromIterContext(ctx, s.seq).Parallel(n, fn).All()(yield)
	})
}
