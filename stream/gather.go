package stream

import (
	"github.com/azuiktech/kleisli-go/adt"
	"github.com/azuiktech/kleisli-go/fn"
)

// --- Gatherer: general stateful intermediate operations ---
//
// Seq's methods are fixed operations this package had to hardcode.
// Gatherer (Java's java.util.stream.Gatherer, JEP 485, minus the
// parallel-combiner half — Seq is sequential) is the escape
// hatch for the operation nobody hardcoded: a small, named, reusable value
// — init state, an integrator that consumes one element and emits
// zero-or-more outputs (and reports whether to keep going), and a
// finisher for trailing state attached with Then — that plugs into Gather
// exactly like a built-in op. Most of what follows in this section is
// itself just a Gatherer.

// Gatherer is a custom, stateful intermediate operation. A written-once
// Gatherer is a reusable named combinator, same status as a built-in one —
// this is the sanctioned place for a loop to live when nothing built-in
// fits, not license to reach for one in business code. Build one with
// GathererOf; attach a finisher with Then.
type Gatherer[T, A, U any] struct {
	init      func() A
	integrate func(state A, item T, emit func(U)) (A, bool)
	finish    func(state A, emit func(U))
}

// GathererOf builds a Gatherer with nothing to flush at the end. integrate
// processes one item against the current state, may emit zero or more
// outputs, and returns the next state plus whether to keep consuming
// (false stops early, same as Java's integrator returning false).
func GathererOf[T, A, U any](init func() A, integrate func(state A, item T, emit func(U)) (A, bool)) Gatherer[T, A, U] {
	return Gatherer[T, A, U]{init: init, integrate: integrate, finish: func(A, func(U)) {}}
}

// Then returns g with finish run on the final state once integration
// stops — after g's own finisher, and also after an early stop.
func (g Gatherer[T, A, U]) Then(finish func(state A, emit func(U))) Gatherer[T, A, U] {
	return Gatherer[T, A, U]{init: g.init, integrate: g.integrate, finish: func(state A, emit func(U)) {
		g.finish(state, emit)
		finish(state, emit)
	}}
}

func statelessGatherer[T, U any](integrate func(item T, emit func(U))) Gatherer[T, struct{}, U] {
	return GathererOf(
		func() struct{} { return struct{}{} },
		func(s struct{}, item T, emit func(U)) (struct{}, bool) {
			integrate(item, emit)
			return s, true
		})
}

// takeWhileGatherer is Seq.TakeWhile's engine.
func takeWhileGatherer[T any](fn func(T) bool) Gatherer[T, struct{}, T] {
	return GathererOf(
		func() struct{} { return struct{}{} },
		func(state struct{}, item T, emit func(T)) (struct{}, bool) {
			if !fn(item) {
				return state, false
			}
			emit(item)
			return state, true
		})
}

// dropWhileGatherer is DropWhile's shared engine.
func dropWhileGatherer[T any](fn func(T) bool) Gatherer[T, bool, T] {
	return GathererOf(
		func() bool { return true }, // still dropping
		func(dropping bool, item T, emit func(T)) (bool, bool) {
			if dropping && fn(item) {
				return true, true
			}
			emit(item)
			return false, true
		})
}

// enumerateGatherer is Enumerate's shared engine.
func enumerateGatherer[T any]() Gatherer[T, int, adt.Indexed[T]] {
	return GathererOf(
		func() int { return 0 },
		func(i int, item T, emit func(adt.Indexed[T])) (int, bool) {
			emit(adt.Indexed[T]{Index: i, Value: item})
			return i + 1, true
		})
}

// distinctByGatherer is DistinctBy's shared engine.
func distinctByGatherer[T any, K comparable](fn func(T) K) Gatherer[T, map[K]struct{}, T] {
	return GathererOf(
		func() map[K]struct{} { return make(map[K]struct{}) },
		func(seen map[K]struct{}, item T, emit func(T)) (map[K]struct{}, bool) {
			k := fn(item)
			if _, ok := seen[k]; !ok {
				seen[k] = struct{}{}
				emit(item)
			}
			return seen, true
		})
}

// Scan emits every intermediate accumulator value — a running Reduce, as
// opposed to Reduce's single final value. Rust's Iterator::scan.
func Scan[T, A any](initial A, fn func(A, T) A) Gatherer[T, A, A] {
	return GathererOf(
		func() A { return initial },
		func(acc A, item T, emit func(A)) (A, bool) {
			acc = fn(acc, item)
			emit(acc)
			return acc, true
		})
}

// Fold is Reduce's non-terminal twin: the same single final accumulator,
// but emitted once at the end instead of returned as a bare value — so
// it keeps flowing as a one-element Seq[U] into FlatMap/Map rather
// than ending the pipeline. Scan's opposite in that sense: Scan emits on
// every step and never at the end; Fold emits only at the end.
func Fold[T, U any](initial U, fn func(U, T) U) Gatherer[T, U, U] {
	return GathererOf(
		func() U { return initial },
		func(acc U, item T, _ func(U)) (U, bool) { return fn(acc, item), true }).
		Then(func(acc U, emit func(U)) { emit(acc) })
}

// WindowFixed partitions into non-overlapping chunks of n — the last chunk
// may be shorter. Java 24's Gatherers.windowFixed. Panics if n < 1.
func WindowFixed[T any](n int) Gatherer[T, []T, []T] {
	if n < 1 {
		panic("stream: WindowFixed size must be at least 1")
	}
	return GathererOf(
		func() []T { return make([]T, 0, n) },
		func(window []T, item T, emit func([]T)) ([]T, bool) {
			window = append(window, item)
			if len(window) == n {
				emit(window)
				return make([]T, 0, n), true
			}
			return window, true
		}).
		Then(func(window []T, emit func([]T)) {
			if len(window) > 0 {
				emit(window)
			}
		})
}

// WindowSliding produces every overlapping window of size n — fewer than n
// elements at the very start are not emitted. Java 24's
// Gatherers.windowSliding. Each window is a defensive copy, not a
// zero-copy aliased subslice the way Rust's slice::windows works — a
// deliberate safety-over-performance choice (mutating one window can
// never corrupt its neighbors or the source). Total copying is O(n ×
// size), not O(n) — revisit only if profiling shows it actually matters.
// Panics if n < 1.
func WindowSliding[T any](n int) Gatherer[T, []T, []T] {
	if n < 1 {
		panic("stream: WindowSliding size must be at least 1")
	}
	return GathererOf(
		func() []T { return make([]T, 0, n) },
		func(window []T, item T, emit func([]T)) ([]T, bool) {
			window = append(window, item)
			if len(window) > n {
				window = window[1:]
			}
			if len(window) == n {
				snapshot := make([]T, n)
				copy(snapshot, window)
				emit(snapshot)
			}
			return window, true
		})
}

// ChunkBy groups consecutive elements sharing key into one slice each,
// emitting a run as soon as the key changes. Rust's chunk_by.
func ChunkBy[T any, K comparable](key func(T) K) Gatherer[T, adt.Pair[K, []T], []T] {
	return GathererOf(
		func() adt.Pair[K, []T] { return adt.PairOf(*new(K), []T{}) },
		func(run adt.Pair[K, []T], item T, emit func([]T)) (adt.Pair[K, []T], bool) {
			k := key(item)
			if len(run.Second()) > 0 && run.First() != k {
				emit(run.Second())
				return adt.PairOf(k, []T{item}), true
			}
			return adt.PairOf(k, append(run.Second(), item)), true
		}).
		Then(func(run adt.Pair[K, []T], emit func([]T)) {
			if len(run.Second()) > 0 {
				emit(run.Second())
			}
		})
}

// DedupBy drops each element whose key equals the previous element's key,
// keeping the first of every run. Duplicates that are not adjacent stay —
// DistinctBy drops those.
func DedupBy[T any, K comparable](key func(T) K) Gatherer[T, adt.Option[K], T] {
	return GathererOf(
		adt.None[K],
		func(prev adt.Option[K], item T, emit func(T)) (adt.Option[K], bool) {
			k := key(item)
			if !adt.Options.Contains(prev, k) {
				emit(item)
			}
			return adt.Some(k), true
		})
}

// Dedup drops each element equal to the previous one.
func Dedup[T comparable]() Gatherer[T, adt.Option[T], T] {
	return DedupBy(fn.Identity[T])
}

// Intersperse places sep between every pair of elements.
func Intersperse[T any](sep T) Gatherer[T, bool, T] {
	return GathererOf(
		func() bool { return false },
		func(started bool, item T, emit func(T)) (bool, bool) {
			if started {
				emit(sep)
			}
			emit(item)
			return true, true
		})
}

// StepBy keeps the first element and then every nth one. Rust's step_by.
// Panics if n < 1.
func StepBy[T any](n int) Gatherer[T, int, T] {
	if n < 1 {
		panic("stream: StepBy step must be at least 1")
	}
	return GathererOf(
		func() int { return 0 },
		func(i int, item T, emit func(T)) (int, bool) {
			if i%n == 0 {
				emit(item)
			}
			return i + 1, true
		})
}

// KeepFirst is a merge policy for ToMapBy that keeps the first value seen for a key.
func KeepFirst[V any](existing, _ V) V { return existing }

// KeepLast is a merge policy for ToMapBy that keeps the latest value seen for a key.
func KeepLast[V any](_, incoming V) V { return incoming }

// MapWhile returns a Gatherer that maps to Option[U] and stops on None.
// Compose with Seq.Gather for use without a convenience method.
func MapWhile[T, U any](fn func(T) adt.Option[U]) Gatherer[T, struct{}, U] {
	return mapWhileGatherer[T, U](fn)
}

// ScanWhile returns a Gatherer that accumulates state and stops on None.
// Compose with Seq.Gather for use without a convenience method.
func ScanWhile[T, A, U any](initial A, fn func(A, T) (A, adt.Option[U])) Gatherer[T, A, U] {
	return scanWhileGatherer[T, A, U](initial, fn)
}

func mapWhileGatherer[T, U any](fn func(T) adt.Option[U]) Gatherer[T, struct{}, U] {
	return GathererOf(
		func() struct{} { return struct{}{} },
		func(s struct{}, item T, emit func(U)) (struct{}, bool) {
			opt := fn(item)
			if opt.IsNone() {
				return s, false
			}
			emit(opt.MustGet())
			return s, true
		})
}

func scanWhileGatherer[T, A, U any](initial A, fn func(A, T) (A, adt.Option[U])) Gatherer[T, A, U] {
	return GathererOf(
		func() A { return initial },
		func(acc A, item T, emit func(U)) (A, bool) {
			next, opt := fn(acc, item)
			if opt.IsNone() {
				return next, false
			}
			emit(opt.MustGet())
			return next, true
		})
}
