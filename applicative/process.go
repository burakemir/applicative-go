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
package applicativepipeline

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net/http"
)

// Process is an abstraction for a pipeline running
// on a single machine.
type Process struct {
	Name        string
	PipelineFn  func(PCol[string]) PCol[string]
	SerializeFn func(io.Writer) Sink[string]
}

type Hub struct {
	listenAddr string
	process    []*Process
}

func NewHub(listenAddr string) *Hub {
	return &Hub{listenAddr, nil}
}

// When a process registers with local hub, it creates input and output
// channels.
//   (make(chan In), make(chan Out))
func (h *Hub) Register(p *Process) {
	h.process = append(h.process, p)
}

type RequestCol struct {
	contentLength int64
	data          io.Reader
}

func (r RequestCol) Name() string {
	return fmt.Sprintf("<request of length %d>", r.contentLength)
}

func (r RequestCol) Exec(engine Engine[string]) {
	engine.initialize(r)
	scanner := bufio.NewScanner(r.data)
	for scanner.Scan() {
		engine.emit(scanner.Text())
	}
	engine.done(r)
}

func (r RequestCol) StaticAnalyze(analyzer Analyzer) {
	analyzer.Analyze(r)
}

var _ PCol[string] = StringCol{}

func (h *Hub) Run() {
	dispatch := func(w http.ResponseWriter, req *http.Request) {
		reqProcess := req.Header.Get("process")
		for _, p := range h.process {
			if p.Name == reqProcess {
				task := p.PipelineFn(RequestCol{req.ContentLength, req.Body})
				log.Printf("handling %s\n", task.Name())
				task.Exec(p.SerializeFn(w))
				break
			}
		}
	}
	http.HandleFunc("/in", dispatch)
	log.Fatal(http.ListenAndServe(h.listenAddr, nil))
}
