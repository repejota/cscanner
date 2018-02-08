// Copyright 2018 The cscanner Authors. All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.  The ASF
// licenses this file to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.

package cscanner_test

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/repejota/cscanner"
)

type delayReader struct {
	chars   string
	lengths []int
	i       int
	wait    time.Duration
}

func (r *delayReader) Read(b []byte) (int, error) {
	if len(r.lengths) == 0 {
		return 0, io.EOF
	}

	length := r.lengths[0]
	r.lengths = r.lengths[1:]

	i := 0
	for ; i < length && i < len(b)-1; i++ {
		b[i] = r.chars[r.i%len(r.chars)]
		r.i++
	}
	b[i] = '\n'
	i++
	time.Sleep(r.wait)
	return i, nil
}

func ExampleConcurrentScanner() {
	// Delay reader writes data of lengths, each delayed by wait
	a := &delayReader{chars: "a", lengths: []int{10, 45, 3}, wait: 1 * time.Second}
	b := &delayReader{chars: "b", lengths: []int{3, 100, 23, 12, 57, 34}, wait: 500 * time.Millisecond}

	fmt.Println("MultiReader:")
	multi := io.MultiReader(a, b)
	io.Copy(os.Stdout, multi)

	fmt.Println()

	// Create new, unread, readers
	a = &delayReader{chars: "a", lengths: []int{10, 45, 3}, wait: 1 * time.Second}
	b = &delayReader{chars: "b", lengths: []int{3, 100, 23, 12, 57, 34}, wait: 500 * time.Millisecond}

	fmt.Println("ConcurrentScanner:")
	readers := []io.Reader{a, b}
	scanner := cscanner.NewConcurrentScanner(readers)
	for scanner.Scan() {
		fmt.Println(scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error: ", err)
	}
}
