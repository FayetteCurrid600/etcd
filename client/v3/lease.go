// Copyright 2016 The etcd Authors
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
	"sync"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"google.golang.org/grpc"
)

type Lease interface {
	Grant(ctx context.Context, ttl int64) (*LeaseGrantResponse, error)
	Revoke(ctx context.Context, id LeaseID) (*LeaseRevokeResponse, error)
	TimeToLive(ctx context.Context, id LeaseID, opts ...LeaseOption) (*LeaseTimeToLiveResponse, error)
	Leases(ctx context.Context) (*LeaseLeasesResponse, error)
	KeepAlive(ctx context.Context, id LeaseID) (<-chan *LeaseKeepAliveResponse, error)
	KeepAliveOnce(ctx context.Context, id LeaseID) (*LeaseKeepAliveResponse, error)
	Close() error
}

type LeaseID int64

type LeaseGrantResponse pb.LeaseGrantResponse
type LeaseRevokeResponse pb.LeaseRevokeResponse
type LeaseKeepAliveResponse pb.LeaseKeepAliveResponse
type LeaseTimeToLiveResponse pb.LeaseTimeToLiveResponse
type LeaseLeasesResponse pb.LeaseLeasesResponse

type lessor struct {
	mu sync.Mutex // protects all fields

	// signed key index of the lease in the keepAlive map
	keepAlives map[LeaseID]*keepAlive

	hc pb.LeaseClient

	closec chan struct{}

	keepAliveTimeout time.Duration

	// ctx is a context that is cancelled when Close() is called.
	ctx    context.Context
	cancel context.CancelFunc

	firstKeepAliveOnce sync.Once

	// rpcOpts holds the gRPC options for the lease client.
	rpcOpts []grpc.CallOption
}

func NewLease(c *Client) Lease {
	return NewLeaseFromLeaseClient(toV3Client(c).Lease, c, c.cfg.DialTimeout)
}

func NewLeaseFromLeaseClient(lc pb.LeaseClient, c *Client, keepAliveTimeout time.Duration) Lease {
	l := &lessor{
		keepAlives:   make(map[LeaseID]*keepAlive),
		hc:           lc,
		closec:       make(chan struct{}),
		keepAliveTimeout: keepAliveTimeout,
	}
	if c != nil {
		ctx := c.ctx
		if c.cfg.RequireLeader && c.requireLeaderCtx != nil {
			ctx = c.requireLeaderCtx
		}
		l.ctx, l.cancel = context.WithCancel(ctx)
	} else {
		l.ctx, l.cancel = context.WithCancel(context.TODO())
	}

	l.rpcOpts = defaultCallOpts
	if c != nil {
		l.rpcOpts = c.callOpts
	}

	return l
}

func (l *lessor) Grant(ctx context.Context, ttl int64) (*LeaseGrantResponse, error) {
	r := &pb.LeaseGrantRequest{TTL: ttl}
	resp, err := l.hc.LeaseGrant(ctx, r, l.rpcOpts...)
	if err != nil {
		return nil, toErr(ctx, err)
	}
	return (*LeaseGrantResponse)(resp), nil
}

func (l *lessor) Revoke(ctx context.Context, id LeaseID) (*LeaseRevokeResponse, error) {
	r := &pb.LeaseRevokeRequest{ID: int64(id)}
	resp, err := l.hc.LeaseRevoke(ctx, r, l.rpcOpts...)
	if err != nil {
		return nil, toErr(ctx, err)
	}
	return (*LeaseRevokeResponse)(resp), nil
}

func (l *lessor) TimeToLive(ctx context.Context, id LeaseID, opts ...LeaseOption) (*LeaseTimeToLiveResponse, error) {
	r := &pb.LeaseTimeToLiveRequest{ID: int64(id)}
	for _, opt := range opts {
		opt(r)
	}
	resp, err := l.hc.LeaseTimeToLive(ctx, r, l.rpcOpts...)
	if err != nil {
		return nil, toErr(ctx, err)
	}
	return (*LeaseTimeToLiveResponse)(resp), nil
}

func (l *lessor) Leases(ctx context.Context) (*LeaseLeasesResponse, error) {
	r := &pb.LeaseLeasesRequest{}
	resp, err := l.hc.LeaseLeases(ctx, r, l.rpcOpts...)
	if err != nil {
		return nil, toErr(ctx, err)
	}
	return (*LeaseLeasesResponse)(resp), nil
}

func (l *lessor) KeepAlive(ctx context.Context, id LeaseID) (<-chan *LeaseKeepAliveResponse, error) {
	return l.keepAliveCtx(ctx, id)
}

func (l *lessor) KeepAliveOnce(ctx context.Context, id LeaseID) (*LeaseKeepAliveResponse, error) {
	for {
		resp, err := l.keepAliveOnce(ctx, id)
		if err == nil {
			return resp, nil
		}
		if isHaltErr(ctx, err) {
			return nil, toErr(ctx, err)
		}
		select {
		case <-ctx.Done():
			return nil, toErr(ctx, ctx.Err())
		case <-time.After(retryInterval):
		}
	}
}

func (l *lessor) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	select {
	case <-l.closec:
		return nil
	default:
		close(l.closec)
	}
	l.cancel()
	for _, ka := range l.keepAlives {
		close(ka.responses)
	}
	l.keepAlives = make(map[LeaseID]*keepAlive)
	return nil
}

type keepAlive struct {
	id        LeaseID
	donec     chan struct{}
	responses chan *LeaseKeepAliveResponse
}

func (l *lessor) keepAliveCtx(ctx context.Context, id LeaseID) (<-chan *LeaseKeepAliveResponse, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	select {
	case <-l.closec:
		return nil, ErrKeepAliveHalted
	case <-l.ctx.Done():
		return nil, ErrKeepAliveHalted
	default:
	}
	ka, ok := l.keepAlives[id]
	if !ok {
		ka = &keepAlive{
			id:        id,
			donec:     make(chan struct{}),
			responses: make(chan *LeaseKeepAliveResponse, 16),
		}
		l.keepAlives[id] = ka
		go l.keepAliveLoop(ka)
	}
	return ka.responses, nil
}

func (l *lessor) keepAliveOnce(ctx context.Context, id LeaseID) (*LeaseKeepAliveResponse, error) {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := l.hc.LeaseKeepAlive(cctx, l.rpcOpts...)
	if err != nil {
		return nil, err
	}
	req := &pb.LeaseKeepAliveRequest{ID: int64(id)}
	if err := stream.Send(req); err != nil {
		return nil, err
	}
	resp, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	return (*LeaseKeepAliveResponse)(resp), nil
}

func (l *lessor) keepAliveLoop(ka *keepAlive) {
	defer func() {
		l.mu.Lock()
		delete(l.keepAlives, ka.id)
		l.mu.Unlock()
		close(ka.responses)
	}()

	for {
		var (
			stream pb.Lease_LeaseKeepAliveClient
			err    error
		)
		for {
			select {
			case <-l.ctx.Done():
				return
			case <-ka.donec:
				return
			default:
			}
			stream, err = l.hc.LeaseKeepAlive(l.ctx, l.rpcOpts...)
			if err == nil {
				break
			}
			select {
			case <-l.ctx.Done():
				return
			case <-ka.donec:
				return
			case <-time.After(retryInterval):
			}
		}

		req := &pb.LeaseKeepAliveRequest{ID: int64(ka.id)}
		if err := stream.Send(req); err != nil {
			continue
		}

		for {
			resp, err := stream.Recv()
			if err != nil {
				break
			}
			select {
			case ka.responses <- (*LeaseKeepAliveResponse)(resp):
			case <-l.ctx.Done():
				return
			case <-ka.donec:
				return
			}
		}
	}
}
