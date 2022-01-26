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
	"fmt"
	"strings"

	ap "github.com/burakemir/applicative-go/applicative"
)

func main() {
	pStrings := ap.StringCol([]string{
		"The glitter of sunlight on roughened water, the glory of the stars,",
		"the innocence of morning, the smell of the sea in harbors, ",
	})
	pSplit := ap.Fmap(func(str string) []string {
		return strings.Split(str, " ")
	})(pStrings)
	pFlattened := ap.Flatten(pSplit)
	pCleaned := ap.Fmap(func(str string) string {
		return strings.ToLower(strings.TrimSpace(strings.Replace(str, ",", " ", -1)))
	})(pFlattened)
	pFiltered := ap.Filter(func(str string) bool {
		return str != ""
	})(pCleaned)
	pCounts := ap.Count(pFiltered)

	fmt.Printf("Pipeline %s\n", pCounts.Name())
	pCounts.Exec(ap.NewDebugSink[ap.Pair[string, int]](func(elem ap.Pair[string, int]) {
		fmt.Printf("%s:%d\n", elem.Fst, elem.Snd)
	}))
}
