// Copyright 2021 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package clientv3

import (
	"context"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
)

type mockRequireLeaderLeaseClient struct {
	pb.LeaseClient
}

type mockRequireLeaderStream struct {
	ctx context.Context
	grpc.ClientStream
}

func (m *mockRequireLeaderStream) Send(*pb.LeaseKeepAliveRequest) error {
	return nil
}

func (m *mockRequireLeaderStream) Recv() (*pb.LeaseKeepAliveResponse, error) {
	select {
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	}
}

func (m *mockRequireLeaderLeaseClient) LeaseKeepAlive(ctx context.Context, opts ...grpc.CallOption) (pb.Lease_LeaseKeepAliveClient, error) {
	return &mockRequireLeaderStream{ctx: ctx}, nil
}

func TestLeaseKeepAliveRequireLeader(t *testing.T) {
	cli, err := New(Config{
		Endpoints:     []string{"localhost:2379"},
		DialTimeout:   5 * time.Second,
		RequireLeader: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	mock := &mockRequireLeaderLeaseClient{}
	l := NewLeaseFromLeaseClient(mock, cli, 5*time.Second)
	defer l.Close()

	ch, err := l.KeepAlive(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}

	cli.closeRequireLeader()

	select {
	case <-ch:
		// success, channel is closed
	case <-time.After(5 * time.Second):
		t.Fatal("keepalive channel was not closed after closeRequireLeader")
	}
}
