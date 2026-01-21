//
// Copyright (C) 2026 Holger de Carne
//
// This software may be modified and distributed under the terms
// of the MIT license. See the LICENSE file for details.

// Package pool provides functionality for pooling resources which
// require an open-and-close lifecycle (e.g. network connections).
package pool

import "errors"

// ErrPoolClosing indicates this pool instance is currently
// shutting down and about to be closed.
var ErrPoolClosing error = errors.New("pool closing")

// ErrPoolClosed indicates this pool instance has already
// been closed.
var ErrPoolClosed error = errors.New("pool closed")

// ErrPoolExhausted indicates the maximum allowed number
// of resources for this pool instance is in use.
var ErrPoolExhausted error = errors.New("pool exhausted")
