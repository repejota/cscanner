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

package cscanner

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

func main() {
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
	scanner := NewConcurrentScanner(a, b)
	for scanner.Scan() {
		fmt.Println(scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error: ", err)
	}
}

// ConcurrentScanner works like io.Scanner, but with multiple io.Readers
type ConcurrentScanner struct {
	scans  chan []byte   // Scanned data from readers
	errors chan error    // Errors from readers
	done   chan struct{} // Signal that all readers have completed
	cancel func()        // Cancel all readers (stop on first error)

	data []byte // Last scanned value
	err  error
}

// NewConcurrentScanner starts scanning each reader in a separate goroutine
// and returns a *ConcurrentScanner.
func NewConcurrentScanner(readers ...io.Reader) *ConcurrentScanner {
	ctx, cancel := context.WithCancel(context.Background())
	s := &ConcurrentScanner{
		scans:  make(chan []byte),
		errors: make(chan error),
		done:   make(chan struct{}),
		cancel: cancel,
	}

	var wg sync.WaitGroup
	wg.Add(len(readers))

	for _, reader := range readers {
		// Start a scanner for each reader in it's own goroutine.
		go func(reader io.Reader) {
			defer wg.Done()
			scanner := bufio.NewScanner(reader)

			for scanner.Scan() {
				select {
				case s.scans <- scanner.Bytes():
					// While there is data, send it to s.scans,
					// this will block until Scan() is called.
				case <-ctx.Done():
					// This fires when context is cancelled,
					// indicating that we should exit now.
					return
				}
			}
			if err := scanner.Err(); err != nil {
				select {
				case s.errors <- err:
					// Reprort we got an error
				case <-ctx.Done():
					// Exit now if context was cancelled, otherwise sending
					// the error and this goroutine will never exit.
					return
				}
			}
		}(reader)
	}

	go func() {
		// Signal that all scanners have completed
		wg.Wait()
		close(s.done)
	}()

	return s
}

func (s *ConcurrentScanner) Scan() bool {
	select {
	case s.data = <-s.scans:
		// Got data from a scanner
		return true
	case <-s.done:
		// All scanners are done, nothing to do.
	case s.err = <-s.errors:
		// One of the scanners error'd, were done.
	}
	s.cancel() // Cancel context regardless of how we exited.
	return false
}

func (s *ConcurrentScanner) Bytes() []byte {
	return s.data
}

func (s *ConcurrentScanner) Text() string {
	return string(s.data)
}

func (s *ConcurrentScanner) Err() error {
	return s.err
}

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
