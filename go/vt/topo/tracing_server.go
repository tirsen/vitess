/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package topo

import (
  "golang.org/x/net/context"
  "vitess.io/vitess/go/trace"
)

type TracingFactory struct {
  ServiceName string
  Inner       Factory
}

func (tf *TracingFactory) HasGlobalReadOnlyCell(serverAddr, root string) bool {
  return tf.Inner.HasGlobalReadOnlyCell(serverAddr, root)
}

func (tf *TracingFactory) Create(cell, serverAddr, root string) (Conn, error) {
  conn, e := tf.Inner.Create(cell, serverAddr, root)
  if e != nil {
    return nil, e
  }

  return &TracingConn{inner: conn, name: tf.ServiceName}, nil
}

type TracingConn struct {
  name  string
  inner Conn
}

func (tc *TracingConn) ListDir(ctx context.Context, dirPath string, full bool) ([]DirEntry, error) {
  span, ctx := trace.NewClientSpan(ctx, tc.name, "ListDir")
  span.Annotate("dirPath", dirPath)
  defer span.Finish()
  return tc.inner.ListDir(ctx, dirPath, full)
}

func (tc *TracingConn) Create(ctx context.Context, filePath string, contents []byte) (Version, error) {
  span, ctx := trace.NewClientSpan(ctx, tc.name, "Create")
  span.Annotate("filePath", filePath)
  defer span.Finish()
  return tc.inner.Create(ctx, filePath, contents)
}

func (tc *TracingConn) Update(ctx context.Context, filePath string, contents []byte, version Version) (Version, error) {
  span, ctx := trace.NewClientSpan(ctx, tc.name, "Update")
  span.Annotate("filePath", filePath)
  defer span.Finish()
  return tc.inner.Update(ctx, filePath, contents, version)
}

func (tc *TracingConn) Get(ctx context.Context, filePath string) ([]byte, Version, error) {
  span, ctx := trace.NewClientSpan(ctx, tc.name, "Get")
  span.Annotate("filePath", filePath)
  defer span.Finish()
  return tc.inner.Get(ctx, filePath)
}

func (tc *TracingConn) Delete(ctx context.Context, filePath string, version Version) error {
  span, ctx := trace.NewClientSpan(ctx, tc.name, "Delete")
  span.Annotate("filePath", filePath)
  defer span.Finish()
  return tc.inner.Delete(ctx, filePath, version)
}

func (tc *TracingConn) Lock(ctx context.Context, dirPath, contents string) (LockDescriptor, error) {
  span, ctx := trace.NewClientSpan(ctx, tc.name, "Lock")
  span.Annotate("filePath", dirPath)
  defer span.Finish()
  return tc.inner.Lock(ctx, dirPath, contents)
}

func (tc *TracingConn) Watch(ctx context.Context, filePath string) (*WatchData, <-chan *WatchData, CancelFunc) {
  span, ctx := trace.NewClientSpan(ctx, tc.name, "Watch")
  span.Annotate("filePath", filePath)
  current, changes, cancel := tc.inner.Watch(ctx, filePath)
  newCancelFunc := func() {
    cancel()
    span.Finish()
  }
  return current, changes, newCancelFunc
}

func (tc *TracingConn) NewMasterParticipation(ctx context.Context, name, id string) (MasterParticipation, error) {
  span, ctx := trace.NewClientSpan(ctx, tc.name, "NewMasterParticipation")
  span.Annotate("name", name)
  span.Annotate("id", id)
  defer span.Finish()
  return tc.inner.NewMasterParticipation(ctx, name, id)
}

func (tc *TracingConn) Close() {
  span, _ := trace.NewSpan(context.Background(), "Close", trace.Client)
  defer span.Finish()
  tc.inner.Close()
}
