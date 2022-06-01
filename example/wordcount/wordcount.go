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

// wordcount.go demonstrates how to construct and run a pipeline.
// with an applicative-style pipeline API.
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"strings"
	"time"

	ap "github.com/burakemir/applicative-go/applicative"
)

var mode = flag.String("mode", "demo", "whether to run a demo and exit ('demo'), or run as server ('server')")

type myAnalyzer struct{}

// getStreamHandleName is an analyzer function.
func getStreamHandleNames(n ap.StreamHandle, deps ...ap.StreamHandle) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "StreamHandle %q\n", n.Name())
	for _, d := range deps {
		depName := ap.StaticAnalyze(d, getStreamHandleNames)
		for i, line := range strings.Split(depName, "\n") {
			if len(strings.TrimSpace(line)) == 0 {
				continue
			}
			if i == 0 {
				fmt.Fprintf(&sb, " |_%s\n", line)
			} else {
				fmt.Fprintf(&sb, "   %s\n", line)
			}
		}
	}
	return sb.String()
}

type oneMinuteTumblingWindowsControl struct {
}

func (c oneMinuteTumblingWindowsControl) Obsolete(paneID int64) bool {
	windowTime := time.UnixMicro(paneID)
	return windowTime.Before(time.Now().Add(-2 * time.Minute))
}

func (c oneMinuteTumblingWindowsControl) AccumulationMode() ap.AccumulationMode {
	return ap.AccumulateFiredPane
}

func (c oneMinuteTumblingWindowsControl) Trigger() <-chan struct{} {
	trigger := make(chan struct{})
	ticker := time.NewTicker(100 * time.Millisecond)
	go func() {
		<-ticker.C
		trigger <- struct{}{}
	}()
	return trigger
}

func oneMinuteTumblingWindowsFn(v ap.Timestamped[string]) []time.Time {
	return []time.Time{time.UnixMicro(v.Timestamp).Truncate(time.Minute).Add(time.Minute)}
}

func constructPipeline(source ap.Stream[ap.Timestamped[[]byte]]) ap.Stream[ap.Timestamped[[]ap.Pair[string, int]]] {
	pWords := ap.FlattenWithTime(ap.FmapWithTime(func(body []byte) []string {
		var words []string
		scanner := bufio.NewScanner(bytes.NewReader(body))
		for scanner.Scan() {
			words = append(words, scanner.Text())
		}
		return words
	})(source))
	pSplit := ap.FlattenWithTime(ap.FmapWithTime(func(str string) []string {
		return strings.Split(str, " ")
	})(pWords))
	pCleaned := ap.FmapWithTime(func(str string) string {
		return strings.ToLower(strings.TrimSpace(strings.Replace(str, ",", " ", -1)))
	})(pSplit)
	pFiltered := ap.FilterWithTime(func(str string) bool {
		return str != ""
	})(pCleaned)

	pIdentifySmallWords := ap.FmapWithTime(identifySmallWord)(pFiltered)
	pMarkSmallWords := ap.ProcessSelectedWithTime(func(s string) string { return "*" + s })(pIdentifySmallWords)

	windowed := ap.WindowedStripTime(oneMinuteTumblingWindowsFn, &oneMinuteTumblingWindowsControl{})(pMarkSmallWords)
	return ap.Count(windowed) // TODO
}

func formatTable(table ap.Timestamped[[]ap.Pair[string, int]]) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "[%s]\n", time.UnixMicro(table.Timestamp))
	for _, e := range table.Value {
		fmt.Fprintf(&sb, "%s:%d\n", e.Fst, e.Snd)
	}
	return sb.String()
}

func constructPipelineSerialize(source ap.Stream[ap.Timestamped[[]byte]]) ap.Stream[string] {
	return ap.Fmap(formatTable)(constructPipeline(source))
}

func identifySmallWord(str string) ap.Either[string, string] {
	for _, s := range []string{"a", "in", "of", "on", "the"} {
		if s == str {
			return ap.Left[string, string]{Value: str}
		}
	}
	return ap.Right[string, string]{Value: str}
}

func main() {
	flag.Parse()
	switch *mode {
	case "demo":
		pStrings := ap.Ret(ap.Timestamped[[]byte]{[]byte(`
			The glitter of sunlight on roughened water, the glory of the stars,
			the innocence of morning, the smell of the sea in harbors`),
			time.Now().UnixMicro()})

		pCounts := constructPipeline(pStrings)
		fmt.Printf("Pipeline %s\n", pCounts.Name())

		fmt.Println("-- Let's do some static analysis.")
		fmt.Println(ap.StaticAnalyze(pCounts, getStreamHandleNames))

		fmt.Println("-- Now let's run this pipeline.")
		pCounts.Exec(ap.NewDebugSink(func(table ap.Timestamped[[]ap.Pair[string, int]]) {
			fmt.Printf(formatTable(table))
		}))

		fmt.Println("-- Time for some conditional processing.")
	case "server":
		fmt.Println("-- Starting server at :8080")
		fmt.Println("-- try: curl localhost:8080/in -H \"process: wordcount\" --data \"hello world\"")
		h := ap.NewHub(":8080")
		h.Register(ap.NewProcess("wordcount", constructPipelineSerialize))
		h.Run()
	}
}
