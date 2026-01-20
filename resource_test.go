//
// Copyright (C) 2026 Holger de Carne
//
// This software may be modified and distributed under the terms
// of the MIT license. See the LICENSE file for details.

package pool_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tdrn-org/go-pool"
)

func TestResourcePool(t *testing.T) {
	// Create pool
	dir := t.TempDir()
	factory := &fileResourceFactory{
		dir: dir,
	}
	resources := pool.NewResourcePool(t.Name(), factory)
	resources.SetMaxIdleResources(10)

	// Stress pool
	tester := &resourcePoolTester{jitter: dynamicJitter}
	tester.run(t, resources, 10, 10)

	// Shutdown pool
	err := resources.Shutdown(t.Context())
	require.NoError(t, err)

	// Close pool
	err = resources.Close()
	require.NoError(t, err)

	// Close pool (2nd time must yield already-closed error)
	err = resources.Close()
	require.ErrorIs(t, pool.ErrPoolClosed, err)

	// No Error expected, as no limits defined
	require.Equal(t, nil, tester.runErr.Load())

	// All resources removed
	require.NoFileExists(t, dir)
}

func TestResourcePoolShutdownAndClose(t *testing.T) {
	// Create pool
	dir := t.TempDir()
	factory := &fileResourceFactory{
		dir: dir,
	}
	resources := pool.NewResourcePool(t.Name(), factory)

	// Stress pool
	tester := &resourcePoolTester{jitter: staticJitter}
	tester.run(t, resources, 2, 10)

	// Shutdown pool
	err := resources.Shutdown(t.Context())
	require.NoError(t, err)

	// Close pool
	err = resources.Close()
	require.NoError(t, err)

	// No Error expected, as no limits defined
	require.Equal(t, nil, tester.runErr.Load())

	// 20 resources created and removed
	require.Equal(t, 20, int(factory.fileCount.Load()))
	require.NoFileExists(t, dir)
}

func TestResourcePoolExhausted(t *testing.T) {
	// Create pool
	dir := t.TempDir()
	factory := &fileResourceFactory{
		dir: dir,
	}
	resources := pool.NewResourcePool(t.Name(), factory)
	resources.SetMaxTotalResources(9)

	// Stress pool
	tester := &resourcePoolTester{jitter: staticJitter}
	tester.run(t, resources, 1, 10)

	// Shutdown pool
	err := resources.Shutdown(t.Context())
	require.NoError(t, err)

	// Close pool
	err = resources.Close()
	require.NoError(t, err)

	// pool-exhausted error expected, as total limit has been reached
	require.Equal(t, pool.ErrPoolExhausted, tester.runErr.Load())

	// 10 resources created and removed
	require.Equal(t, 9, int(factory.fileCount.Load()))
	require.NoFileExists(t, dir)
}

func TestResourcePoolLifetime(t *testing.T) {
	// Create pool
	dir := t.TempDir()
	factory := &fileResourceFactory{
		dir: dir,
	}
	resources := pool.NewResourcePool(t.Name(), factory)
	resources.SetResourceMaxLifetime(1 * time.Second)

	// Get twice, but let lifetime expire inbetween, so 2 resources are created
	resource, err := resources.Get(t.Context())
	require.NoError(t, err)
	resource.Release()
	time.Sleep(2 * time.Second)
	resource, err = resources.Get(t.Context())
	require.NoError(t, err)
	resource.Release()

	// Shutdown pool
	err = resources.Shutdown(t.Context())
	require.NoError(t, err)

	// Close pool
	err = resources.Close()
	require.NoError(t, err)

	// 2 resources created and removed
	require.Equal(t, 2, int(factory.fileCount.Load()))
	require.NoFileExists(t, dir)
}

func TestResourcePoolIdleTimeout(t *testing.T) {
	// Create pool
	dir := t.TempDir()
	factory := &fileResourceFactory{
		dir: dir,
	}
	resources := pool.NewResourcePool(t.Name(), factory)
	resources.SetResourceMaxIdleTime(1 * time.Second)

	// Get twice, but let idle timeout expire inbetween, so 2 resources are created
	resource, err := resources.Get(t.Context())
	require.NoError(t, err)
	resource.Release()
	time.Sleep(2 * time.Second)
	resource, err = resources.Get(t.Context())
	require.NoError(t, err)
	resource.Release()

	// Shutdown pool
	err = resources.Shutdown(t.Context())
	require.NoError(t, err)

	// Close pool
	err = resources.Close()
	require.NoError(t, err)

	// 2 resources created and removed
	require.Equal(t, 2, int(factory.fileCount.Load()))
	require.NoFileExists(t, dir)
}

func staticJitter() time.Duration {
	return 500 * time.Millisecond
}

func dynamicJitter() time.Duration {
	return time.Duration(rand.IntN(11)*100) * time.Millisecond
}

type resourcePoolTester struct {
	startedWG sync.WaitGroup
	jitter    func() time.Duration
	runErr    atomic.Value
}

func (tester *resourcePoolTester) run(t *testing.T, resources *pool.Resources[*fileResource], user, volume int) {
	for range user {
		tester.startedWG.Add(1)
		time.Sleep(tester.jitter())
		go tester.runUser(t, resources, volume)
	}
	tester.startedWG.Wait()
}

func (tester *resourcePoolTester) runUser(t *testing.T, resources *pool.Resources[*fileResource], volume int) {
	rs := make([]*pool.Resource[*fileResource], 0, volume)
	for range volume {
		r, err := resources.Get(t.Context())
		if err == nil {
			rs = append(rs, r)
		} else {
			tester.runErr.CompareAndSwap(nil, err)
		}
	}
	tester.startedWG.Done()
	time.Sleep(tester.jitter())
	for _, r := range rs {
		r.Release()
	}
}

type fileResourceFactory struct {
	dir       string
	fileCount atomic.Int32
}

func (f *fileResourceFactory) New(_ context.Context) (*fileResource, error) {
	fileID := int(f.fileCount.Add(1))
	file, err := os.Create(filepath.Join(f.dir, strconv.Itoa(fileID)))
	if err != nil {
		return nil, err
	}
	fmt.Println("new", file.Name())
	resource := &fileResource{
		f: file,
	}
	return resource, nil
}

type fileResource struct {
	f *os.File
}

func (r *fileResource) Close() error {
	fName := r.f.Name()
	fmt.Println("close", fName)
	closeErr := r.f.Close()
	removeErr := os.Remove(fName)
	return errors.Join(closeErr, removeErr)
}
