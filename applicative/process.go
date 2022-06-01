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
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

const kMaxLength = 1 << 20 // 1 MiB.

// Process is an abstraction for a pipeline running
// on a single machine.
type Process struct {
	name       string
	pipelineFn func(Stream[Timestamped[[]byte]]) Stream[string]

	requests *RequestStream
	pipeline Stream[string]
	latest   string
}

func NewProcess(name string, pipelineFn func(Stream[Timestamped[[]byte]]) Stream[string]) *Process {
	return &Process{name, pipelineFn, nil, nil, ""}
}

type Hub struct {
	listenAddr string
	process    map[string]*Process
}

func NewHub(listenAddr string) *Hub {
	return &Hub{listenAddr, make(map[string]*Process)}
}

func (p *Process) ensureInitialized(h *Hub) {
	if p.pipeline == nil {
		p.pipeline = p.pipelineFn(*p.requests)
		go p.pipeline.Exec(NewSink(func(s string) { p.requests.out <- s }))
		// Simple way to see the last value that was output.
		go func() {
			for {
				output, more := <-p.requests.out
				if !more {
					break
				}
				p.latest = output
			}
		}()
		log.Printf("constructed pipeline for process %s\n", p.name)
	}
}

func (p *Process) handle(h *Hub, body []byte) error {
	nowUsec := time.Now().UnixMicro()
	select {
	case p.requests.in <- Timestamped[[]byte]{body, nowUsec}:
		return nil
	default:
		return errors.New("buffer full")
	}
}

// Registers process with hub. This creates an input and output queue.
func (h *Hub) Register(p *Process) error {
	if _, ok := h.process[p.name]; ok {
		return fmt.Errorf("There is already a process registered as %q", p.name)
	}
	h.process[p.name] = p
	// TODO: do better than hardcoded random buffer sizes.
	p.requests = &RequestStream{make(chan Timestamped[[]byte], 1000), make(chan string, 1000)}
	return nil
}

type RequestStream struct {
	in  chan Timestamped[[]byte]
	out chan string
}

func (r RequestStream) Name() string {
	return fmt.Sprintf("<request stream>")
}

func (r RequestStream) Exec(engine Engine[Timestamped[[]byte]]) {
	engine.initialize(r)
	for {
		r, more := <-r.in
		if !more {
			fmt.Println("no more requests.")
			break
		}
		engine.emit(r)
	}
	engine.done(r)
}

func (r RequestStream) Deps() []StreamHandle {
	return nil
}

func (h *Hub) Run() {
	http.HandleFunc("/in", func(w http.ResponseWriter, req *http.Request) {
		requested := req.Header.Get("process")
		if requested == "" {
			http.Error(w, "no process requested", http.StatusBadRequest)
			return
		}
		if req.ContentLength > kMaxLength {
			http.Error(w, "request too large", http.StatusBadRequest)
			return
		}
		bytes, err := io.ReadAll(req.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		process, ok := h.process[requested]
		if !ok {
			http.Error(w, fmt.Sprintf("no process registered as %q", requested), http.StatusBadRequest)
			return
		}
		process.ensureInitialized(h)
		log.Printf("handling request of length %d\n", req.ContentLength)
		if err := process.handle(h, bytes); err != nil {
			http.Error(w, "temporarily unavailable", http.StatusTooManyRequests)
			return
		}
	})
	http.HandleFunc("/out", func(w http.ResponseWriter, req *http.Request) {
		requested := req.Header.Get("process")
		if requested == "" {
			http.Error(w, "no process requested", http.StatusBadRequest)
			return
		}
		process, ok := h.process[requested]
		if !ok {
			http.Error(w, fmt.Sprintf("no process registered as %q", requested), http.StatusBadRequest)
			return
		}
		io.WriteString(w, process.latest)
	})
	log.Fatal(http.ListenAndServe(h.listenAddr, nil))
}
