// Copyright 2022 Burak Emir
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
package applicative

import (
	"fmt"
	"time"
)

type Either[X, Y any] interface {
	IsLeft() bool
	Left() X
	Right() Y
}

type Left[X, Y any] struct {
	Either[X, Y]
	Value X
}

func (l Left[X, Y]) IsLeft() bool {
	return true
}
func (l Left[X, Y]) Left() X {
	return l.Value
}
func (l Left[X, Y]) Right() Y {
	panic("Right() called on Left")
}

type Right[X, Y any] struct {
	Either[X, Y]
	Value Y
}

func (r Right[X, Y]) IsLeft() bool {
	return false
}

func (r Right[X, Y]) Left() X {
	panic("Right() called on Left")
}

func (r Right[X, Y]) Right() Y {
	return r.Value
}

type Pair[X, Y any] struct {
	Fst X
	Snd Y
}

type Timestamped[T any] struct {
	Value     T
	Timestamp int64
}

func AttachProcessingTime[T any](value T) Timestamped[T] {
	return Timestamped[T]{value, time.Now().UTC().UnixMicro()}
}

type AccumulationMode = int

const (
	AccumulateFiredPane AccumulationMode = 0
	DiscardFiredPane                     = 1
)

type WindowPane[T any] struct {
	EndTime time.Time
	Values  []T
}

type ValueWithWindows[T any] struct {
	Value     T
	WindowIDs []int64
}

type StringCol Timestamped[[]string]

func (p StringCol) Name() string {
	return fmt.Sprintf("[]string of length %d", len([]string(p.Value)))
}

func (p StringCol) Exec(engine Engine[Timestamped[string]]) {
	engine.initialize(p)
	for _, v := range p.Value {
		engine.emit(Timestamped[string]{v, p.Timestamp})
	}
	engine.done(p)
}

func (p StringCol) Deps() []StreamHandle {
	return nil
}

var _ Stream[Timestamped[string]] = StringCol{}
