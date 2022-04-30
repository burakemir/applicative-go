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

import "fmt"

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
// The type plays a role similar to Streamlection[T] from Apache Beam SDK.
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

// We choose a principled way to compose our deferred computations.
// This is called fmap in Haskell.
func Fmap[S, T any](fn func(S) T) func(Stream[S]) Stream[T] {
	return func(in Stream[S]) Stream[T] {
		return Mapped[S, T]{fn, in}
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
	return fmt.Sprintf("ret(%v)", r.value)
}

func (r ret[T]) Exec(engine Engine[T]) {
	engine.initialize(r)
	engine.emit(r.value)
	engine.done(r)
}

func (r ret[T]) Deps() []StreamHandle {
	return nil
}

// Ret turns a value of type T into a defered (immediate) computation.
// In Haskell, this is called "pure".
func Ret[T any](value T) Stream[T] {
	return ret[T]{value}
}

func forEach[T any](elems []T, emit func(T)) {
	for _, e := range elems {
		emit(e)
	}
}

type StringCol []string

func (p StringCol) Name() string {
	return fmt.Sprintf("[]string of length %d", len([]string(p)))
}

func (p StringCol) Exec(engine Engine[string]) {
	engine.initialize(p)
	forEach([]string(p), engine.emit)
	engine.done(p)
}

func (p StringCol) Deps() []StreamHandle {
	return nil
}

var _ Stream[string] = StringCol{}

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

// Flatten of Stream[Stream[T]] is left as exercise to the reader.
func Flatten[T any](pp Stream[[]T]) Stream[T] {
	return flattened[T]{pp}
}

type Pair[X, Y any] struct {
	Fst X
	Snd Y
}

type counted[T comparable] struct {
	p Stream[T]
}

func (c counted[T]) Name() string {
	return fmt.Sprintf("count(%s)", c.p.Name())
}

func (c counted[T]) Exec(engine Engine[Pair[T, int]]) {
	engine.initialize(c)
	counts := make(map[T]int)
	counter := connect(func(t T) {
		count := counts[t]
		count += 1
		counts[t] = count
	}, engine)
	c.p.Exec(counter)
	for k, v := range counts {
		engine.emit(Pair[T, int]{k, v})
	}
	engine.done(c)
}

func (c counted[T]) Deps() []StreamHandle {
	return []StreamHandle{c.p}
}

func Count[T comparable](p Stream[T]) Stream[Pair[T, int]] {
	return counted[T]{p}
}

type filtered[T any] struct {
	p  Stream[T]
	fn func(T) bool
}

func (f filtered[T]) Name() string {
	return fmt.Sprintf("filter(%s)", f.p.Name())
}

func (f filtered[T]) Exec(engine Engine[T]) {
	engine.initialize(f)
	sink := connect(func(t T) {
		if f.fn(t) {
			engine.emit(t)
		}
	}, engine)
	f.p.Exec(sink)
	engine.done(f)
}

func (f filtered[T]) Deps() []StreamHandle {
	return []StreamHandle{f.p}
}

func Filter[T any](fn func(T) bool) func(Stream[T]) Stream[T] {
	return func(p Stream[T]) Stream[T] { return filtered[T]{p, fn} }
}
