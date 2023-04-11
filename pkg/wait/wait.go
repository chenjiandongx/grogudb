// Copyright 2023 The grogudb Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wait

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/chenjiandongx/grogudb/pkg/rescue"
)

// Until 持续运行直至 ctx cancel
func Until(ctx context.Context, f func()) {
	UntilPeriod(ctx, f, 0)
}

// UntilPeriod 持续运行直至 ctx cancel
// 每次 quit 等待 period
func UntilPeriod(ctx context.Context, f func(), period time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		func() {
			defer rescue.HandleCrash()
			f()
		}()
		time.Sleep(period)
	}
}

// Waiting 实现类似 sync.WaitGroup 功能 不过允许等待指定 n 个信号量
type Waiting struct {
	n atomic.Int64
}

// Inc 增加信号量
func (w *Waiting) Inc() {
	w.n.Add(1)
}

// Dec 较少信号量
func (w *Waiting) Dec() {
	w.n.Add(-1)
}

// Until 等待 n 个信号量
func (w *Waiting) Until(n int) {
	for {
		if w.n.Load() == int64(n) {
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
}
