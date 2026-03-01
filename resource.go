//
// Copyright (C) 2026 Holger de Carne
//
// This software may be modified and distributed under the terms
// of the MIT license. See the LICENSE file for details.

package pool

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// ResourceFactory interface is used to create new
// resource instances on demand.
type ResourceFactory[R io.Closer] interface {
	// New creates a new resource instance.
	New(ctx context.Context) (R, error)
}

// Resources provides resource pool functionality for
// the given resource type.
type Resources[R io.Closer] struct {
	factory     ResourceFactory[R]
	maxTotal    int
	maxIdle     int
	maxIdleTime time.Duration
	maxLifetime time.Duration
	activeCount int
	idleQueue   []*Resource[R]
	closing     bool
	closed      atomic.Bool
	logger      *slog.Logger
	triggerCh   chan func()
	activeWG    sync.WaitGroup
	mutex       sync.Mutex
}

// NewResourcePool creates a new resource pool using the given name
// and factory interface.
func NewResourcePool[R io.Closer](name string, factory ResourceFactory[R]) *Resources[R] {
	logger := slog.With("pool", name)
	resources := &Resources[R]{
		factory:   factory,
		idleQueue: make([]*Resource[R], 0),
		logger:    logger,
		triggerCh: make(chan func()),
	}
	go resources.runTrigger()
	return resources
}

func (p *Resources[R]) runTrigger() {
	for {
		select {
		case trigger, ok := <-p.triggerCh:
			if !ok {
				return
			}
			trigger()
		case <-time.After(1 * time.Second):
			p.runHouseKeepingTrigger()
		}
	}
}

func (p *Resources[R]) runHouseKeepingTrigger() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.closing {
		return
	}
	p.runHouseKeepingTriggerLocked()
}

func (p *Resources[R]) runHouseKeepingTriggerLocked() {
	now := time.Now().UTC()
	// Discard/close idle resources exceeding max idle count
	{
		idleCount := len(p.idleQueue)
		deleteCount := 0
		for i := idleCount - 1; i >= p.maxIdle; i-- {
			resource := p.idleQueue[i]
			p.closeResource(resource)
			deleteCount++
		}
		p.idleQueue = slices.Delete(p.idleQueue, idleCount-deleteCount, idleCount)
	}
	// Discard/close idle resources beyond total lifetime or idle timeout
	if p.maxLifetime > 0 || p.maxIdleTime > 0 {
		idleCount := len(p.idleQueue)
		idleQueue := make([]*Resource[R], 0, idleCount)
		for _, resource := range p.idleQueue {
			if p.maxLifetime > 0 && now.Sub(resource.createTime) < 0 {
				p.closeResource(resource)
				continue
			}
			if p.maxIdleTime > 0 && now.Sub(resource.idleStartTime) < 0 {
				p.closeResource(resource)
				continue
			}
			idleQueue = append(idleQueue, resource)
		}
		p.idleQueue = idleQueue
	}
}

// SetMaxTotalResources sets the maximum total number of resource instances
// contained in the pool.
//
// If n <= 0, then there is no limit.
// The default value is 0 (no limit).
func (p *Resources[R]) SetMaxTotalResources(n int) {
	p.triggerCh <- func() {
		p.mutex.Lock()
		defer p.mutex.Unlock()
		if p.closing {
			return
		}
		p.maxTotal = max(n, 0)
		p.maxIdle = min(p.maxIdle, p.maxTotal)
		p.runHouseKeepingTriggerLocked()
	}
}

// SetMaxIdleResources sets the maximum number of idle resource instances
// contained in the pool.
//
// If n <= 0, then there is no limit.
// The default value is 0 (no limit).
func (p *Resources[R]) SetMaxIdleResources(n int) {
	p.triggerCh <- func() {
		p.mutex.Lock()
		defer p.mutex.Unlock()
		if p.closing {
			return
		}
		p.maxIdle = min(max(n, 0), p.maxTotal)
		p.runHouseKeepingTriggerLocked()
	}
}

// SetResourceMaxIdleTime sets the timeout, after which an idle resource instance
// is closed.
//
// If d <= 0, then there is no timeout.
// The default value is 0 (no timeout).
func (p *Resources[R]) SetResourceMaxIdleTime(d time.Duration) {
	p.triggerCh <- func() {
		p.mutex.Lock()
		defer p.mutex.Unlock()
		if p.closing {
			return
		}
		p.maxIdleTime = max(d, 0)
		p.runHouseKeepingTriggerLocked()
	}
}

// SetResourceMaxLifetime sets the timeout, after a resource instance
// is closed.
//
// If d <= 0, then there is no timeout.
// The default value is 0 (no timeout).
func (p *Resources[R]) SetResourceMaxLifetime(d time.Duration) {
	p.triggerCh <- func() {
		p.mutex.Lock()
		defer p.mutex.Unlock()
		if p.closing {
			return
		}
		p.maxLifetime = max(d, 0)
		p.runHouseKeepingTriggerLocked()
	}
}

// Get aquires a resource instance from the pool.
//
// The requested resource is either taken from the existing idle queue
// or a new instance is created by invoking the factory interface given
// during pool creation. If a total resource limit has been set and the
// limit is reached, [ErrPoolExhausted] is returned.
func (p *Resources[R]) Get(ctx context.Context) (*Resource[R], error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.closing {
		return nil, ErrPoolClosing
	}
	// Existing (idle) resource available
	resourceIndex := len(p.idleQueue) - 1
	if resourceIndex >= 0 {
		resource := p.idleQueue[resourceIndex]
		p.idleQueue = slices.Delete(p.idleQueue, resourceIndex, resourceIndex+1)
		resource.active = true
		p.activeCount++
		p.activeWG.Add(1)
		return resource, nil
	}
	// Check if pool is exhausted
	idleCount := len(p.idleQueue)
	if p.maxTotal > 0 && (idleCount+p.activeCount) >= p.maxTotal {
		return nil, ErrPoolExhausted
	}
	// Create new (initially active) resource
	value, err := p.factory.New(ctx)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	resource := &Resource[R]{
		pool:          p,
		value:         value,
		createTime:    now,
		idleStartTime: now,
		active:        true,
	}
	p.activeCount++
	p.activeWG.Add(1)
	return resource, nil
}

func (p *Resources[R]) releaseResource(resource *Resource[R]) {
	if !resource.active {
		p.logger.Warn("superflous release of resource")
		return
	}
	now := time.Now().UTC()
	resource.active = false
	resource.idleStartTime = now
	p.mutex.Lock()
	defer func() {
		p.mutex.Unlock()
		p.activeWG.Done()
	}()
	p.activeCount--
	// Close/discard resource if reset, beyond lifetime or above idle count
	if resource.reset || (p.maxLifetime > 0 && now.Sub(resource.createTime) <= 0) || (p.maxIdle > 0 && len(p.idleQueue) >= p.maxIdle) {
		p.closeResource(resource)
		return
	}
	// Re-insert resource into idle queue
	insertIndex, _ := slices.BinarySearchFunc(p.idleQueue, resource, func(l, r *Resource[R]) int {
		return l.createTime.Compare(r.createTime)
	})
	p.idleQueue = slices.Insert(p.idleQueue, insertIndex, resource)
}

func (p *Resources[R]) closeResource(resource *Resource[R]) {
	err := resource.value.Close()
	if err != nil {
		p.logger.Warn("close resource failure", slog.Any("err", err))
	}
}

// Shutdown shuts down the pool gracefully by waiting for the
// release of any currently active resource.
func (p *Resources[R]) Shutdown(ctx context.Context) error {
	p.markClosing()
	p.activeWG.Wait()
	return nil
}

func (p *Resources[R]) markClosing() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.closing = true
}

// Close closes the pool by closing any idle resource in the pool.
func (p *Resources[R]) Close() error {
	closed := p.closed.Swap(true)
	if closed {
		return ErrPoolClosed
	}
	close(p.triggerCh)
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.closing = true
	closeErrs := make([]error, 0, len(p.idleQueue))
	for _, resource := range p.idleQueue {
		closeErrs = append(closeErrs, resource.value.Close())
	}
	p.idleQueue = p.idleQueue[:0]
	return errors.Join(closeErrs...)
}

// Resource represents a resource pool entry, as returned by
// [Resources.Get].
type Resource[R io.Closer] struct {
	pool          *Resources[R]
	value         R
	createTime    time.Time
	idleStartTime time.Time
	active        bool
	reset         bool
}

// Get retrieves the resource managed by this resource entry instance.
func (r *Resource[R]) Get() R {
	return r.value
}

// Reset marks the resource as to be closed and re-created during
// release.
func (r *Resource[R]) Reset() {
	r.reset = true
}

// Release transfers ownership of the resource back to the pool.
func (r *Resource[R]) Release() {
	r.pool.releaseResource(r)
}
