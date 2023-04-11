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

package grogudb

import (
	"sync/atomic"
)

type statistics struct {
	put          atomic.Uint64
	del          atomic.Uint64
	gc           atomic.Uint64
	clear        atomic.Uint64
	compact      atomic.Uint64
	rotate       atomic.Uint64
	memHoldBytes atomic.Int64
	memSegment   atomic.Int64
	diskSegment  atomic.Int64
}

// Stats DB 操作统计
type Stats struct {
	Put          uint64
	Del          uint64
	Gc           uint64
	Clear        uint64
	Compact      uint64
	Rotate       uint64
	MemHoldBytes int64
	MemSegment   int64
	DiskSegment  int64
}

func (s *statistics) Load() Stats {
	return Stats{
		Put:          s.put.Load(),
		Del:          s.del.Load(),
		Gc:           s.gc.Load(),
		Clear:        s.clear.Load(),
		Compact:      s.compact.Load(),
		Rotate:       s.rotate.Load(),
		MemHoldBytes: s.memHoldBytes.Load(),
		MemSegment:   s.memSegment.Load(),
		DiskSegment:  s.diskSegment.Load(),
	}
}

type state struct {
	rotating   atomic.Bool
	compacting atomic.Bool
	gc         atomic.Bool
	closed     atomic.Bool
}

// State DB 状态描述
type State struct {
	Rotating   bool
	Compacting bool
	Gc         bool
	Closed     bool
}

func (s *state) Load() State {
	return State{
		Rotating:   s.rotating.Load(),
		Compacting: s.compacting.Load(),
		Gc:         s.gc.Load(),
		Closed:     s.closed.Load(),
	}
}
