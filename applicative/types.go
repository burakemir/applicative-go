package applicative

import "fmt"

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
	timestamp int64
}

type ValueWithWindows[T any] struct {
	Value     T
	WindowIDs []int64
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
