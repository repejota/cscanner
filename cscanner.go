// Copyright 2018 The concurrentscanner Authors. All rights reserved.
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
	"io"
	"sync"
)

// ConcurrentScanner works like bufio.Scanner, but with multiple io.Readers
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
func NewConcurrentScanner(readers []io.Reader) *ConcurrentScanner {
	ctx, cancel := context.WithCancel(context.Background())

	cscanner := &ConcurrentScanner{
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
				case cscanner.scans <- scanner.Bytes():
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
				case cscanner.errors <- err:
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
		close(cscanner.done)
	}()

	return cscanner
}

// Scan ...
func (cs *ConcurrentScanner) Scan() bool {
	select {
	case cs.data = <-cs.scans:
		// Got data from a scanner
		return true
	case <-cs.done:
		// All scanners are done, nothing to do.
	case cs.err = <-cs.errors:
		// One of the scanners error'd, were done.
	}
	cs.cancel() // Cancel context regardless of how we exited.
	return false
}

// Bytes ...
func (cs *ConcurrentScanner) Bytes() []byte {
	return cs.data
}

// Text ...
func (cs *ConcurrentScanner) Text() string {
	return string(cs.data)
}

// Err ...
func (cs *ConcurrentScanner) Err() error {
	return cs.err
}
