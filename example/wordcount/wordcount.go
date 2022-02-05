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
	"flag"
	"fmt"
	"io"
	"strings"

	ap "github.com/burakemir/applicative-go/applicative"
)

var mode = flag.String("mode", "demo", "whether to run a demo and exit ('demo'), or run as server ('server')")

type myAnalyzer struct{}

func (m myAnalyzer) Analyze(n ap.Node, deps ...ap.Node) {
	fmt.Printf("node %q\n", n.Name())
	for _, d := range deps {
		fmt.Printf("  dep: %q\n", d.Name())
		d.StaticAnalyze(m)
	}
}

func constructPipeline(source ap.PCol[string]) ap.PCol[ap.Pair[string, int]] {
	pSplit := ap.Fmap(func(str string) []string {
		return strings.Split(str, " ")
	})(source)
	pFlattened := ap.Flatten(pSplit)
	pCleaned := ap.Fmap(func(str string) string {
		return strings.ToLower(strings.TrimSpace(strings.Replace(str, ",", " ", -1)))
	})(pFlattened)
	pFiltered := ap.Filter(func(str string) bool {
		return str != ""
	})(pCleaned)
	return ap.Count(pFiltered)
}

func constructPipelineSerialize(source ap.PCol[string]) ap.PCol[string] {
	return ap.Fmap(func(p ap.Pair[string, int]) string {
		return fmt.Sprintf("%s:%d\n", p.Fst, p.Snd)
	})(constructPipeline(source))
}

func main() {
	flag.Parse()
	switch *mode {
	case "demo":
		pStrings := ap.StringCol([]string{
			"The glitter of sunlight on roughened water, the glory of the stars,",
			"the innocence of morning, the smell of the sea in harbors, ",
		})

		pCounts := constructPipeline(pStrings)
		fmt.Printf("Pipeline %s\n", pCounts.Name())

		fmt.Println("-- Let's do some static analysis.")
		pCounts.StaticAnalyze(myAnalyzer{})

		fmt.Println("-- Now let's run this pipeline.")
		pCounts.Exec(ap.NewSink[ap.Pair[string, int]](func(elem ap.Pair[string, int]) {
			fmt.Printf("%s:%d\n", elem.Fst, elem.Snd)
		}))
	case "server":
		fmt.Println("-- Starting server at :8080")
		fmt.Println("-- try: curl localhost:8080/in -H \"process: wordcount\" --data \"hello world\"")
		h := ap.NewHub(":8080")
		h.Register(&ap.Process{"wordcount", constructPipelineSerialize, func(w io.Writer) ap.Sink[string] {
			return *ap.NewSink(func(resultLine string) {
				io.WriteString(w, resultLine)
			})
		}})
		h.Run()
		for {
			// loop
		}
	}

}
