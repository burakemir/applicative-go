// Copyright 2021 Burak Emir
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package applicative is primarily an exploration of golang generics
// (parametric polymorphism) introduced in the Go 1.18 release.
//
// An applicative functor is a mathematical concept and functional
// programming pattern. In engineering, it often helps to have
// formal, mathematical descriptions of problem domain. Making use
// of these formalism to have an organized/structured approach is
// very different from considering all programming as mathematics.
//
// We are interested here in the particular domain of stream processing,
// or data processing pipelines. This domain is one born from practical
// application and does not have "standard" formal descriptions.
// The purpose of this library is not to capture applicative functors as
// a programming abstraction, but rather to explore its usefulness
// in structuring this problem domain.
package applicative

import (
	"fmt"
	"time"
)

// StreamHandle is a Stream[A] where we don't care about its data type.
type StreamHandle interface {
	// Helpful debug string describing the pipeline.
	Name() string

	// Returns StreamHandles that are (upstream) dependencies.
	Deps() []StreamHandle
}

func StaticAnalyze[T any](StreamHandle StreamHandle, analyzer Analyzer[T]) T {
	return analyzer(StreamHandle, StreamHandle.Deps()...)
}

type Analyzer[T any] func(StreamHandle StreamHandle, deps ...StreamHandle) T

// Stream[T] is a deferred computation that yields a bag (multiset) of T values.
// The type plays a role similar to PCollection[T] from Apache Beam SDK.
//
// The simplest way to run a pipeline is:
// c.Exec(NewSink[T](func (elem T) { ...do something with elem... })
type Stream[T any] interface {
	StreamHandle

	// Register with engine and executes computation.
	// Invokes callback in engine for every element.
	Exec(Engine[T])
}

type Engine[T any] interface {
	// Invoked before the first call to emit(T).
	initialize(StreamHandle StreamHandle)
	// Callback invoked for every element of type T.
	emit(T)
	// Invoked after the last call to emit(T).
	done(StreamHandle)
}

// Sink is the simplest implementation of engine, just a continuation (callback.)
// This can be used for precomposition.
type Sink[T any] struct {
	fn func(T)
}

// NewSink returns a new Sink instance.
func NewSink[T any](fn func(T)) *Sink[T] {
	return &Sink[T]{fn}
}

func (s Sink[T]) initialize(StreamHandle StreamHandle) {}

func (s Sink[T]) emit(t T) { s.fn(t) }

func (s Sink[T]) done(c StreamHandle) {}

// DebugSink is like Sink, but prints initialize and done calls.
type DebugSink[T any] struct {
	fn func(T)
}

func NewDebugSink[T any](fn func(T)) *DebugSink[T] {
	return &DebugSink[T]{fn}
}

func (d DebugSink[T]) initialize(StreamHandle StreamHandle) {
	fmt.Printf("initialize [%s]\n", StreamHandle.Name())
	//	for _, dep := range deps {
	//		fmt.Printf("  dep: %s\n", dep.Name())
	//	}
}

func (d DebugSink[T]) emit(t T) { d.fn(t) }

func (d DebugSink[T]) done(StreamHandle StreamHandle) {
	fmt.Printf("done [%s]\n", StreamHandle.Name())
}

// ConnectedEngine[S, T] takes an Engine[T] and adapts
// it so it can act as an Engine[S]. It forwards initialize()
// and done() methods to another Engine[T]. Pre-composition.
type ConnectedEngine[S, T any] struct {
	fn     func(S)
	engine Engine[T]
}

// connect returns a new ConnectedEngine.
func connect[S, T any](fn func(S), engine Engine[T]) ConnectedEngine[S, T] {
	return ConnectedEngine[S, T]{fn, engine}
}

func (s ConnectedEngine[S, T]) initialize(c StreamHandle) {
	s.engine.initialize(c)
}

func (s ConnectedEngine[S, T]) emit(elem S) {
	s.fn(elem)
}

func (s ConnectedEngine[S, T]) done(c StreamHandle) {
	s.engine.done(c)
}

type Mapped[S, T any] struct {
	// Transformation to apply to each element.
	fn func(S) T
	// Input collection.
	in Stream[S]
}

func (m Mapped[S, T]) Name() string {
	return fmt.Sprintf("map(%s)", m.in.Name())
}

func (m Mapped[S, T]) Exec(engine Engine[T]) {
	engine.initialize(m)
	sink := connect(func(elem S) { engine.emit(m.fn(elem)) }, engine)
	m.in.Exec(sink)
	engine.done(m)
}

func (m Mapped[S, T]) Deps() []StreamHandle {
	return []StreamHandle{m.in}
}

// Fmap applies f a transformation (map, function) to every element of a stream.
func Fmap[S, T any](fn func(S) T) func(Stream[S]) Stream[T] {
	return func(in Stream[S]) Stream[T] {
		return Mapped[S, T]{fn, in}
	}
}

// Fmap applies f a transformation (map, function) to every element of a stream of
// timestamped values, preserving the timestamp.
func FmapWithTime[S, T any](fn func(S) T) func(Stream[Timestamped[S]]) Stream[Timestamped[T]] {
	return func(in Stream[Timestamped[S]]) Stream[Timestamped[T]] {
		return Mapped[Timestamped[S], Timestamped[T]]{func(s Timestamped[S]) Timestamped[T] {
			return Timestamped[T]{fn(s.Value), s.Timestamp}
		}, in}
	}
}

type Lifted[S, T any] struct {
	fncol Stream[func(S) T]
	in    Stream[S]
}

func (l Lifted[S, T]) Name() string {
	return fmt.Sprintf("lifted(%s)(%s)", l.fncol.Name(), l.in.Name())
}

func (l Lifted[S, T]) Exec(engine Engine[T]) {
	engine.initialize(l)
	sink := connect(func(fn func(S) T) {
		Mapped[S, T]{fn, l.in}.Exec(engine)
	}, engine)
	l.fncol.Exec(sink)
	engine.done(l)
}

func (l Lifted[S, T]) Deps() []StreamHandle {
	return []StreamHandle{l.fncol, l.in}
}

// We are indeed dealing with applicative functors (strong monoidal
// functor with tensorial strength).
// In Haskell, this operator is called <*>.
//
// The name "lift" comes from the fact that lift(ret(fn)) will "lift" the
// function fn : S -> T to operate on Stream[S] -> Stream[T].
//
// The same "lifting" story could be told for Fmap, and indeed
// each operations can be implemented in terms of the other.
//
// Note the similarity to axiom K from modal logic: ☐(S->T)->(☐S->☐T)
func Lift[S, T any](fncol Stream[func(S) T]) func(Stream[S]) Stream[T] {
	return func(in Stream[S]) Stream[T] { return Lifted[S, T]{fncol, in} }
}

type ret[T any] struct {
	value T
}

func (r ret[T]) Name() string {
	return fmt.Sprintf("ret(<singleton>)")
}

func (r ret[T]) Exec(engine Engine[T]) {
	engine.initialize(r)
	engine.emit(r.value)
	engine.done(r)
}

func (r ret[T]) Deps() []StreamHandle {
	return nil
}

// Ret turns a value of type T into a singleton stream.
func Ret[T any](value T) Stream[T] {
	return ret[T]{value}
}

func forEach[T any](elems []T, emit func(T)) {
	for _, e := range elems {
		emit(e)
	}
}

type flattened[T any] struct {
	pp Stream[[]T]
}

func (f flattened[T]) Name() string {
	return fmt.Sprintf("flatten(%v)", f.pp.Name())
}

func (f flattened[T]) Exec(engine Engine[T]) {
	engine.initialize(f)
	sink := connect(func(ts []T) {
		for _, t := range ts {
			engine.emit(t)
		}
	}, engine)
	f.pp.Exec(sink)
	engine.done(f)
}

func (f flattened[T]) Deps() []StreamHandle {
	return []StreamHandle{f.pp}
}

// Flatten turns a stream of slices into a flat stream of elements.
func Flatten[T any](pp Stream[[]T]) Stream[T] {
	return flattened[T]{pp}
}

type flattenedWithTime[T any] struct {
	in Stream[Timestamped[[]T]]
}

func (f flattenedWithTime[T]) Name() string {
	return fmt.Sprintf("flattenWithTime(%v)", f.in.Name())
}

func (f flattenedWithTime[T]) Exec(engine Engine[Timestamped[T]]) {
	engine.initialize(f)
	sink := connect(func(ts Timestamped[[]T]) {
		for _, t := range ts.Value {
			engine.emit(Timestamped[T]{t, ts.Timestamp})
		}
	}, engine)
	f.in.Exec(sink)
	engine.done(f)
}

func (f flattenedWithTime[T]) Deps() []StreamHandle {
	return []StreamHandle{f.in}
}

// Flatten of Stream[Stream[T]] is left as exercise to the reader.
func FlattenWithTime[T any](pp Stream[Timestamped[[]T]]) Stream[Timestamped[T]] {
	return flattenedWithTime[T]{pp}
}

type counted[T comparable] struct {
	in Stream[*WindowPane[T]]
}

func (c counted[T]) Name() string {
	return fmt.Sprintf("count(%s)", c.in.Name())
}

func (c counted[T]) Exec(engine Engine[Timestamped[[]Pair[T, int]]]) {
	engine.initialize(c)
	counter := connect(func(w *WindowPane[T]) {
		counts := make(map[T]int)
		for _, t := range w.Values {
			count := counts[t]
			count += 1
			counts[t] = count
		}
		res := make([]Pair[T, int], 0, len(counts))
		for k, v := range counts {
			res = append(res, Pair[T, int]{k, v})
		}
		engine.emit(Timestamped[[]Pair[T, int]]{res, w.EndTime.UnixMicro()})
	}, engine)
	c.in.Exec(counter)
	engine.done(c)
}

func (c counted[T]) Deps() []StreamHandle {
	return []StreamHandle{c.in}
}

func Count[T comparable](p Stream[*WindowPane[T]]) Stream[Timestamped[[]Pair[T, int]]] {
	return counted[T]{p}
}

type filtered[T any] struct {
	in Stream[T]
	fn func(T) bool
}

func (f filtered[T]) Name() string {
	return fmt.Sprintf("filter(%s)", f.in.Name())
}

func (f filtered[T]) Exec(engine Engine[T]) {
	engine.initialize(f)
	sink := connect(func(t T) {
		if f.fn(t) {
			engine.emit(t)
		}
	}, engine)
	f.in.Exec(sink)
	engine.done(f)
}

func (f filtered[T]) Deps() []StreamHandle {
	return []StreamHandle{f.in}
}

// Filter retains only those elements for which fn returns true.
func Filter[T any](fn func(T) bool) func(Stream[T]) Stream[T] {
	return func(p Stream[T]) Stream[T] { return filtered[T]{p, fn} }
}

// FilterWithTime retains only those timestamped elements for which fn returns true.
func FilterWithTime[T any](fn func(T) bool) func(Stream[Timestamped[T]]) Stream[Timestamped[T]] {
	return func(in Stream[Timestamped[T]]) Stream[Timestamped[T]] {
		return filtered[Timestamped[T]]{
			in,
			func(v Timestamped[T]) bool { return fn(v.Value) },
		}
	}
}

type selective[S, T any] struct {
	in Stream[Either[S, T]]
	fn func(S) T
}

// ProcessSelected takes a Stream[Either[S, T]] and applies fn to transform all S values into T.
// This operation makes Stream a "selective" applicative functor and can be used to process
// items conditionally.
func ProcessSelected[S, T any](fn func(S) T) func(Stream[Either[S, T]]) Stream[T] {
	return func(in Stream[Either[S, T]]) Stream[T] { return selective[S, T]{in, fn} }
}

func (s selective[S, T]) Name() string {
	return fmt.Sprintf("processSelected(%v)", s.in.Name())
}

func (s selective[S, T]) Exec(engine Engine[T]) {
	engine.initialize(s)
	sink := connect(func(v Either[S, T]) {
		if v.IsLeft() {
			engine.emit(s.fn(v.Left()))
		} else {
			engine.emit(v.Right())
		}
	}, engine)
	s.in.Exec(sink)
	engine.done(s)
}

func (s selective[S, T]) Deps() []StreamHandle {
	return []StreamHandle{s.in}
}

type selectiveWithTime[S, T any] struct {
	in Stream[Timestamped[Either[S, T]]]
	fn func(S) T
}

// ProcessSelectedWithTime takes a Stream[Timestamped[Either[S, T]]] and applies fn to transform
// all S values into T, preserving timestamp.
func ProcessSelectedWithTime[S, T any](fn func(S) T) func(Stream[Timestamped[Either[S, T]]]) Stream[Timestamped[T]] {
	return func(in Stream[Timestamped[Either[S, T]]]) Stream[Timestamped[T]] {
		return selectiveWithTime[S, T]{in, fn}
	}
}

func (s selectiveWithTime[S, T]) Deps() []StreamHandle {
	return []StreamHandle{s.in}
}

func (s selectiveWithTime[S, T]) Exec(engine Engine[Timestamped[T]]) {
	engine.initialize(s)
	sink := connect(func(v Timestamped[Either[S, T]]) {
		if v.Value.IsLeft() {
			engine.emit(Timestamped[T]{s.fn(v.Value.Left()), v.Timestamp})
		} else {
			engine.emit(Timestamped[T]{v.Value.Right(), v.Timestamp})
		}
	}, engine)
	s.in.Exec(sink)
	engine.done(s)
}

func (s selectiveWithTime[S, T]) Name() string {
	return fmt.Sprintf("processSelectedWithTime(%v)", s.in.Name())
}

// WindowControl is an object that controls when a window pane is emitted.
// It takes care of creating window panes.
type WindowControl interface {
	// If this returns true, the pane will no longer fire.
	Obsolete(paneID int64) bool
	// Returns the accumulation mode.
	AccumulationMode() AccumulationMode
	// Kicks off downstream computation.
	Trigger() <-chan struct{}
}

func (w *windowed[T]) getOrCreate(endTime time.Time) *WindowPane[T] {
	endTimeUsec := endTime.UnixMicro()
	if w.control.Obsolete(endTimeUsec) {
		return nil
	}
	pane, ok := w.windowPane[endTimeUsec]
	if !ok {
		pane = &WindowPane[T]{endTime, []T{}}
		w.windowPane[endTimeUsec] = pane
	}
	return pane
}

type windowed[T any] struct {
	in Stream[T]
	// Assigns Stream element to window panes.
	windowFn func(T) []time.Time
	// Creates window panes and triggers downstream computation.
	control WindowControl
	// Maps window pane ID to active window panes.
	windowPane map[int64]*WindowPane[T]
	dirty      map[int64]bool
}

// Windowed turns to streamm of element into a stream of windows.
func Windowed[T any](windowFn func(T) []time.Time, control WindowControl) func(Stream[T]) Stream[*WindowPane[T]] {
	return func(in Stream[T]) Stream[*WindowPane[T]] {
		return windowed[T]{in, windowFn, control, make(map[int64]*WindowPane[T]), make(map[int64]bool)}
	}
}

func (w windowed[T]) Name() string {
	return fmt.Sprintf("windowed(%v)", w.in.Name())
}

func (w windowed[T]) Deps() []StreamHandle {
	return []StreamHandle{w.in}
}

func (w windowed[T]) Exec(engine Engine[*WindowPane[T]]) {
	engine.initialize(w)
	sink := connect(func(value T) {
		for _, paneID := range w.windowFn(value) {
			if pane := w.getOrCreate(paneID); pane != nil {
				pane.Values = append(pane.Values, value)
				w.dirty[paneID.UnixMicro()] = true
			}
		}
	}, engine)
	trigger := w.control.Trigger()
	done := make(chan struct{})
	// Triggering downstream computation is async.
	go func() {
		for {
			select {
			case <-done:
				return
			case <-trigger:
				for paneID, pane := range w.windowPane {
					if w.control.Obsolete(paneID) {
						delete(w.windowPane, paneID)
						continue
					}
					if w.dirty[paneID] {
						engine.emit(pane)
						w.dirty[paneID] = false
						if w.control.AccumulationMode() == DiscardFiredPane {
							delete(w.windowPane, paneID)
						}
					}
				}
			}
		}
	}()
	w.in.Exec(sink)
	engine.done(w)
	done <- struct{}{}
}

type windowedStripTime[T any] struct {
	in Stream[Timestamped[T]]
	// Assigns Stream element to window panes.
	windowFn func(Timestamped[T]) []time.Time
	// Creates window panes and triggers downstream computation.
	control WindowControl
	// Maps window pane ID to active window panes.
	windowPane map[int64]*WindowPane[T]
	dirty      map[int64]bool
}

func (w *windowedStripTime[T]) getOrCreate(endTime time.Time) *WindowPane[T] {
	endTimeUsec := endTime.UnixMicro()
	if w.control.Obsolete(endTimeUsec) {
		return nil
	}
	pane, ok := w.windowPane[endTimeUsec]
	if !ok {
		pane = &WindowPane[T]{endTime, []T{}}
		w.windowPane[endTimeUsec] = pane
	}
	return pane
}

func (w windowedStripTime[T]) Name() string {
	return fmt.Sprintf("windowedStripTime(%v)", w.in.Name())
}

func (w windowedStripTime[T]) Deps() []StreamHandle {
	return []StreamHandle{w.in}
}

func (w windowedStripTime[T]) Exec(engine Engine[*WindowPane[T]]) {
	engine.initialize(w)
	sink := connect(func(tsvalue Timestamped[T]) {
		for _, endTime := range w.windowFn(tsvalue) {
			if pane := w.getOrCreate(endTime); pane != nil {
				pane.Values = append(pane.Values, tsvalue.Value)
				w.dirty[endTime.UnixMicro()] = true
			}
		}
	}, engine)
	ticker := time.NewTicker(1 * time.Second)
	done := make(chan struct{})
	// Triggering downstream computation is async.
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				for paneID, pane := range w.windowPane {
					if w.control.Obsolete(paneID) {
						delete(w.windowPane, paneID)
						continue
					}
					if w.dirty[paneID] {
						engine.emit(pane)
						w.dirty[paneID] = false

						if w.control.AccumulationMode() == DiscardFiredPane {
							delete(w.windowPane, paneID)
						}
					}
				}
			}
		}
	}()
	w.in.Exec(sink)
	engine.done(w)
	// Need to wait to give ticker a chance to fire window.
	time.Sleep(1 * time.Second)
	ticker.Stop()
	done <- struct{}{}
}

// WindowedStripTime assigns to each timestamped element of a stream a set of windows.
func WindowedStripTime[T any](windowFn func(Timestamped[T]) []time.Time, control WindowControl) func(Stream[Timestamped[T]]) Stream[*WindowPane[T]] {
	return func(in Stream[Timestamped[T]]) Stream[*WindowPane[T]] {
		return windowedStripTime[T]{in, windowFn, control, make(map[int64]*WindowPane[T]), make(map[int64]bool)}
	}
}
