# Chapter 3: Data Pipelines (`stream`)

The `stream` package provides `Seq[T]`: a lazy, pull-based view wrapping the Go standard library's `iter.Seq[T]`. A `Seq` owns nothing — it views a container or a generator, never copies elements, and can process infinite sources without allocating intermediate slices.

```go
import "github.com/azuiktech/kleisli-go/stream"
```

---

## 1. Seq[T] (Lazy Pull Iterator)

`Seq[T]` wraps standard Go `iter.Seq[T]` functions. Every intermediate operation executes lazily on demand.

### Constructors

| Constructor | Signature | Description |
|---|---|---|
| `FromSeq[T]` | `func FromSeq[T any](seq iter.Seq[T]) Seq[T]` | Wraps native Go `iter.Seq[T]` generator. |
| `EmptySeq[T]` | `func EmptySeq[T any]() Seq[T]` | Constructs an empty lazy `Seq[T]`. |
| `SeqOf[T]` | `func SeqOf[T any](items ...T) Seq[T]` | Views the given items. `SeqOf(xs...)` views slice `xs` without copying. |
| `SeqOfMap[K, V]`| `func SeqOfMap[K comparable, V any](m map[K]V) Seq[Pair[K, V]]` | Lazy iterator over map entries. |
| `FromSeq2[K, V]` | `func FromSeq2[K, V any](seq iter.Seq2[K, V]) Seq[adt.Pair[K, V]]` | Views an `iter.Seq2` as a `Seq` of pairs. |
| `ToSeq2[K, V]` | `func ToSeq2[K, V any](s Seq[adt.Pair[K, V]]) iter.Seq2[K, V]` | Views a `Seq` of pairs as an `iter.Seq2`. |
| `Iterate[T]` | `func Iterate[T any](seed T, next func(T) T) Seq[T]` | Yields `seed`, `next(seed)`, … without end. |
| `Generate[T]` | `func Generate[T any](fn func() T) Seq[T]` | Yields `fn()` without end. |
| `Repeat[T]` | `func Repeat[T any](v T) Seq[T]` | Yields `v` without end. |
| `Range[N]` | `func Range[N Number](start, end N) Seq[N]` | Yields `start` up to, not including, `end`. |
| `Unfold[S, T]` | `func Unfold[S, T any](seed S, step func(S) adt.Option[adt.Pair[T, S]]) Seq[T]` | Yields values from `step` until it returns `None`. |

### Lazy Pipeline Methods

| Method | Signature | Description |
|---|---|---|
| `Filter(fn)` | `func (s Seq[T]) Filter(fn func(T) bool) Seq[T]` | Yields only items satisfying predicate. |
| `Map[U](fn)` | `func (s Seq[T]) Map[U any](fn func(T) U) Seq[U]` | Lazily transforms items from `T` to `U`. |
| `FlatMap[U](fn)`| `func (s Seq[T]) FlatMap[U any](fn func(T) []U) Seq[U]` | Maps each element to a slice of `U` and flattens the results. |
| `FilterMap[U](fn)`| `func (s Seq[T]) FilterMap[U any](fn func(T) (U, bool)) Seq[U]` | Keeps and transforms only the elements for which `fn`'s second return is true. |
| `MapMulti[U](fn)`| `func (s Seq[T]) MapMulti[U any](fn func(item T, emit func(U))) Seq[U]` | `fn` gets each element plus an `emit` callback it can call zero, one, or many times — Java's `mapMulti`. |
| `Take(n)` | `func (s Seq[T]) Take(n int) Seq[T]` | Pulls at most `n` items then stops iterator. |
| `Skip(n)` | `func (s Seq[T]) Skip(n int) Seq[T]` | Skips first `n` items. |
| `TakeWhile(fn)` | `func (s Seq[T]) TakeWhile(fn func(T) bool) Seq[T]` | Pulls items while `fn` is true; short-circuits. |
| `DropWhile(fn)` | `func (s Seq[T]) DropWhile(fn func(T) bool) Seq[T]` | Drops prefix while `fn` is true. |
| `EnumerateSeq[T]` | `func EnumerateSeq[T any](s Seq[T]) Seq[adt.Indexed[T]]` | Pairs each yielded item with its 0-based index. A free function, called as `stream.EnumerateSeq(s)` |
| `DistinctBy[K](fn)` | `func (s Seq[T]) DistinctBy[K comparable](fn func(T) K) Seq[T]` | Keeps only the first element for each key `fn` extracts. |
| `Tap(fn)` | `func (s Seq[T]) Tap(fn func(T)) Seq[T]` | Lazily inspects elements as they pass through without draining. |
| `ForEach(fn)` | `func (s Seq[T]) ForEach(fn func(T))` | Terminal consumer running `fn` on all elements (returns void). |
| `AnyOf(fn)` | `func (s Seq[T]) AnyOf(fn func(T) bool) bool` | Short-circuits on first `true`. |
| `AllOf(fn)` | `func (s Seq[T]) AllOf(fn func(T) bool) bool` | Short-circuits on first `false`. |
| `NoneOf(fn)` | `func (s Seq[T]) NoneOf(fn func(T) bool) bool` | Short-circuits on first `true`. |
| `All()` | `func (s Seq[T]) All() iter.Seq[T]` | Standard range-over-func iterator over sequence elements. |
| `First(fn)` | `func (s Seq[T]) First(fn func(T) bool) adt.Option[T]` | Returns `Some(first element satisfying fn)`, or `None` — short-circuits, never pulling past the match. |
| `Reduce[U](init, fn)`| `func (s Seq[T]) Reduce[U any](initial U, fn func(acc U, item T) U) U` | Consumes sequence into accumulator `U`. |
| `MinBy[K](fn)` | `func (s Seq[T]) MinBy[K cmp.Ordered](fn func(T) K) adt.Option[T]` | Returns item with minimal extracted key, draining the `Seq` fully. |
| `MaxBy[K](fn)` | `func (s Seq[T]) MaxBy[K cmp.Ordered](fn func(T) K) adt.Option[T]` | Returns item with maximal extracted key, draining the `Seq` fully. |
| `MinByCompare(fn)`| `func (s Seq[T]) MinByCompare(fn func(T, T) int) adt.Option[T]` | Finds minimum using a raw comparator. |
| `MapWhile[U](fn)` | `func (s Seq[T]) MapWhile[U any](fn func(T) adt.Option[U]) Seq[U]` | Maps each element to an `Option[U]`, emitting the unwrapped value and stopping at the first `None`. |
| `ScanWhile[A, U](init, fn)` | `func (s Seq[T]) ScanWhile[A, U any](initial A, fn func(A, T) (A, adt.Option[U])) Seq[U]` | A stateful `MapWhile` that accumulates state, stopping on `None`. |
| `Collect()` | `func (s Seq[T]) Collect() []T` | Materializes lazy sequence into slice. |
| `ToMapBy[K, V](fn, merge)`| `func (s Seq[T]) ToMapBy[K comparable, V any](fn func(T) (K, V), merge func(existing, incoming V) V) map[K]V` | Builds a `map[K]V` with explicit collision handling via `merge`. Use `stream.KeepFirst`, `stream.KeepLast`, or a custom combiner. |
| `Count()` | `func (s Seq[T]) Count() int` | Number of elements, draining the `Seq`. |
| `Last(fn)` | `func (s Seq[T]) Last(fn func(T) bool) adt.Option[T]` | Last element satisfying `fn`, draining the `Seq`. |
| `Nth(n)` | `func (s Seq[T]) Nth(n int) adt.Option[T]` | Element at 0-based position `n`, or `None`. |
| `GroupBy[K](fn)` | `func (s Seq[T]) GroupBy[K comparable](fn func(T) K) map[K][]T` | Buckets elements into a map of slices by key `K`. |
| `Partition(fn)` | `func (s Seq[T]) Partition(fn func(T) bool) (matched, unmatched []T)` | Splits elements into `(matches, nonMatches)`. |

### Seq Free Functions

| Function | Signature | Description |
|---|---|---|
| `DistinctSeq[T]` | `func DistinctSeq[T comparable](s Seq[T]) Seq[T]` | Deduplicates items lazily with an internal set. |
| `FlattenSeq[T]` | `func FlattenSeq[T any](s Seq[[]T]) Seq[T]` | Collapses a `Seq[[]T]` into a `Seq[T]`, yielding each inner slice's elements in order. |
| `ZipSeq[A, B]` | `func ZipSeq[A, B any](sa Seq[A], sb Seq[B]) Seq[adt.Pair[A, B]]` | Pairs two lazy sequences positionally, stopping at the shorter one, using `iter.Pull`. |
| `ZipSeq3[A, B, C]` | `func ZipSeq3[A, B, C any](sa Seq[A], sb Seq[B], sc Seq[C]) Seq[adt.Triple[A, B, C]]` | Triples three lazy sequences positionally, stopping at the shortest one. |
| `SeqOf[T]` | `func SeqOf[T any](items ...T) Seq[T]` | Constructs a lazy `Seq` yielding the given items. |
| `SeqOfMap[K, V]` | `func SeqOfMap[K comparable, V any](m map[K]V) Seq[adt.Pair[K, V]]` | Wraps a map's entries as a `Seq` of `adt.Pair`. |
| `EmptySeq[T]` | `func EmptySeq[T any]() Seq[T]` | Returns an empty lazy `Seq`. |

```go
// Short-circuits: only evaluates until a match is found
found := stream.FromSeq(hugeGenerator).
    Filter(isValid).
    First(matchesQuery) // adt.Option[T]: Some(match) or None
```

---

## 2. Gatherers — Custom Stateful Intermediate Operations

A `Gatherer[T, A, U]` is a reusable, named, custom intermediate operation — the sanctioned place for a stateful loop to live when nothing built-in fits (Java 24's `Gatherers` concept). `WindowFixed`, `WindowSliding`, `Scan`, and `Fold` below are pre-built `Gatherer` values; `MapWhile`/`ScanWhile` (the convenience methods documented above) are themselves implemented on top of the equivalent-named `Gatherer` here.

```go
type Gatherer[T, A, U any] struct {
    Init      func() A
    Integrate func(state A, item T, emit func(U)) (next A, cont bool)
    Finish    func(state A, emit func(U)) // optional; nil means nothing to flush
}
```

| Method | Signature | Description |
|---|---|---|
| `s.Gather(g)` | `func (s Seq[T]) Gather[A, U any](g Gatherer[T, A, U]) Seq[U]` | Runs `g` over the sequence. |

| Function | Signature | Description |
|---|---|---|
| `WindowFixed[T]` | `func WindowFixed[T any](n int) Gatherer[T, []T, []T]` | Partitions into non-overlapping chunks of `n` — the last chunk may be shorter. Java 24's `Gatherers.windowFixed`. Panics if `n < 1`. |
| `WindowSliding[T]` | `func WindowSliding[T any](n int) Gatherer[T, []T, []T]` | Produces every overlapping window of size `n` — fewer than `n` elements at the very start are not emitted. Each window is a defensive copy. Panics if `n < 1`. |
| `Scan[T, A]` | `func Scan[T, A any](initial A, fn func(A, T) A) Gatherer[T, A, A]` | Emits every intermediate accumulator value — a running `Reduce`, as opposed to `Reduce`'s single final value. Rust's `Iterator::scan`. |
| `Fold[T, U]` | `func Fold[T, U any](initial U, fn func(U, T) U) Gatherer[T, U, U]` | `Reduce`'s non-terminal twin: the same final accumulator, emitted once at the end as a one-element `Seq` instead of returned as a bare value — so it keeps flowing into `FlatMap`/`Map`. |
| `MapWhile[T, U]` | `func MapWhile[T, U any](fn func(T) adt.Option[U]) Gatherer[T, struct{}, U]` | The `Gatherer` behind `Seq.MapWhile` — use directly with `.Gather()` when composing it with other gatherers in one pass. |
| `ScanWhile[T, A, U]` | `func ScanWhile[T, A, U any](initial A, fn func(A, T) (A, adt.Option[U])) Gatherer[T, A, U]` | The `Gatherer` behind `Seq.ScanWhile`. |

```go
// WindowFixed: batch a Seq into fixed-size chunks
batches := stream.SeqOf(readings...).Gather(stream.WindowFixed[Reading](10)).Collect()

// WindowSliding: every 3-element moving window, e.g. for a running average
windows := stream.SeqOf(prices...).Gather(stream.WindowSliding[float64](3)).Collect()

// Scan: running total emitted at every step, not just the final one
runningTotals := stream.SeqOf(amounts...).Gather(stream.Scan(0.0, func(acc, v float64) float64 {
    return acc + v
})).Collect()
```

---

## 3. Numeric Pipelines

When items are mapped to numeric types, `MapToNumber` exposes dedicated arithmetic aggregators.

```go
type NumberSeq[N Number] struct { /* unexported fields */ }
```

| Method | Return Type | Description |
|---|---|---|
| `s.MapToNumber(fn)` | `NumberSeq[N]` | Converts elements to numbers via projection `fn`. |
| `.Sum()` | `N` | Computes arithmetic sum. |
| `.Product()` | `N` | Computes product of elements. |
| `.Mean()` | `adt.Option[float64]` | Computes average (returns `None` if empty). |
| `.Min()` | `adt.Option[N]` | Finds minimum value. |
| `.Max()` | `adt.Option[N]` | Finds maximum value. |

```go
avgSalary := stream.SeqOf(employees...).
    MapToNumber(Employee.Salary).
    Mean().
    OrElse(0.0)
```

---

## 4. Concurrency Bridge (`async.Pipe`)

`stream` provides seamless bridges to `async.Pipe` for concurrent execution across goroutines.

| Method / Function | Signature | Description |
|---|---|---|
| `s.ToPipe()` | `func (s Seq[T]) ToPipe() async.Pipe[T]` | Lazily feeds sequence items into `async.Pipe[T]`. Back to a `Seq`: `stream.FromSeq(p.All())`. |
| `s.Parallel(n, fn)`| `func (s Seq[T]) Parallel[U any](n int, fn func(T) U) Seq[U]` | Runs `fn` across `n` worker goroutines, lazily. Output order is completion order. Stopping early cancels the workers. |

```go
// Concurrent HTTP processing inside a Seq chain
results := stream.SeqOf(imageURLs...).
    Parallel(8, downloadAndCompress).
    Filter(isValid).
    Collect()
```

---

## 5. Practical Real-World Scenarios

### Scenario A: Word Frequency Histogram & Top-K Ranking (`FlatMap`, `GroupBy`, `Take`)

Tokenizing text documents, filtering stop words, computing frequencies, and extracting the top $K$ most frequent words:

```go
type WordCount struct {
    Word  string
    Count int
}

func (wc WordCount) Freq() int { return wc.Count }

func TopKFrequentWords(documents []string, k int) []WordCount {
    stopWords := []string{"the", "a", "an", "and", "or", "in", "to", "of", "is"}

    // Step 1: Flatten sentences to lowercase words, dropping stop words
    words := stream.SeqOf(documents...).
        FlatMap(strings.Fields).
        Map(strings.ToLower).
        Map(fn.Trim(",.!?\"'")).
        Filter(fn.NotIn(stopWords...)).
        Collect()

    // Step 2: Group words and construct count aggregates
    grouped := stream.SeqOf(words...).GroupBy(fn.Identity[string])
    
    counts := stream.SeqOfMap(grouped).
        Map(func(p adt.Pair[string, []string]) WordCount {
            return WordCount{Word: p.First(), Count: len(p.Second())}
        }).
        Collect()
    slices.SortFunc(counts, func(a, b WordCount) int { return cmp.Compare(b.Freq(), a.Freq()) })

    return stream.SeqOf(counts...).Take(k).Collect()
}
```

### Scenario B: Time-Series Pairwise Price Deltas (`ZipSeq`, `Skip`)

Computing adjacent element rate-of-change by zipping a `Seq` with its own tail:

```go
type PriceTick struct {
    Timestamp time.Time
    Price     float64
}

type PriceChange struct {
    From  float64
    To    float64
    Delta float64
}

func CalculatePriceDeltas(ticks []PriceTick) []PriceChange {
    if len(ticks) < 2 {
        return nil
    }

    currents := stream.SeqOf(ticks...)
    nexts := stream.SeqOf(ticks...).Skip(1)

    // Zip tick[i] with tick[i+1]
    return stream.ZipSeq(currents, nexts).
        Map(func(p adt.Pair[PriceTick, PriceTick]) PriceChange {
            return PriceChange{
                From:  p.First().Price,
                To:    p.Second().Price,
                Delta: p.Second().Price - p.First().Price,
            }
        }).
        Collect()
}
```

### Scenario C: Order Queue Triage & Partitioning (`Partition`)

Splitting an incoming queue of orders into express fulfillment vs standard batching in a single pass:

```go
type Order struct {
    ID        string
    IsExpress bool
    TotalUSD  float64
}

func (o Order) ExpressPriority() bool { return o.IsExpress || o.TotalUSD >= 500.0 }

func TriageOrders(orders []Order) (express []Order, standard []Order) {
    return stream.SeqOf(orders...).Partition(Order.ExpressPriority)
}
```

### Scenario D: Lazy Pull-Based Infinite Stream Processing (`Seq[T]`)

Pulling items from an infinite generator, filtering lazily, and stopping with zero buffer allocation:

```go
// Infinite Fibonacci iterator using standard Go iter.Seq[int64]
func InfiniteFibonacci() iter.Seq[int64] {
    return func(yield func(int64) bool) {
        var a, b int64 = 0, 1
        for {
            if !yield(a) {
                return
            }
            a, b = b, a+b
        }
    }
}

func GetEvenFibonacciBelow(limit int64) []int64 {
    return stream.FromSeq(InfiniteFibonacci()).
        TakeWhile(func(n int64) bool { return n < limit }).
        Filter(func(n int64) bool { return n%2 == 0 }).
        Collect()
}
```

