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

import "time"

const (
	defaultCompactCheckInterval = time.Minute
	defaultCompactForceInterval = time.Minute * 30
	defaultCompactKeyOpDelta    = 5
	defaultGcInterval           = time.Minute
	defaultCompactFragmentation = 0.5
	defaultMaxMemSegmentBytes   = 1024 * 1024 * 8
	defaultMaxDiskSegmentBytes  = 1024 * 1024 * 50
)

// Options 控制 DB 行为的配置项
type Options struct {
	// MaxMemSegmentBytes memory segment 最大允许字节
	MaxMemSegmentBytes int

	// MaxDiskSegmentBytes disk segment 最大允许字节
	MaxDiskSegmentBytes int

	// CompactFragmentation compact 碎片比例
	// 即超过 HoldBytes 中需要被删除的字节数超过此比例时才需要 compact
	CompactFragmentation float64

	// CompactCheckInterval compact 巡检周期
	CompactCheckInterval time.Duration

	// CompactForceInterval 强制 compact 周期 即 compact 兜底行为 确保该周期内一定会执行一次 compact
	// CompactForceInterval 必须大于 CompactCheckInterval
	// 扫描磁盘是有 IO 开销 force 只是会忽略所有前置判断条件进行扫描 不代表会执行 compact 操作
	CompactForceInterval time.Duration

	// CompactKeyOpDelta compact 时会对两个 CompactCheckInterval 周期的 Put/Del 做差值计算
	// 超过一定差值才会进行 compact 目的是为了尽量减少 compact 操作
	CompactKeyOpDelta int

	// GcInterval gc 巡检周期
	GcInterval time.Duration
}

// DefaultOptions 返回默认 Options 配置
func DefaultOptions() Options {
	opt := &Options{}
	opt.Validate()
	return *opt
}

// Validate 校验 Options
func (o *Options) Validate() {
	if o.MaxMemSegmentBytes <= 0 {
		o.MaxMemSegmentBytes = defaultMaxMemSegmentBytes
	}
	if o.MaxDiskSegmentBytes <= 0 {
		o.MaxDiskSegmentBytes = defaultMaxDiskSegmentBytes
	}
	if o.CompactFragmentation <= 0 {
		o.CompactFragmentation = defaultCompactFragmentation
	}
	if o.CompactCheckInterval <= 0 {
		o.CompactCheckInterval = defaultCompactCheckInterval
	}
	if o.CompactForceInterval <= 0 {
		o.CompactForceInterval = defaultCompactForceInterval
	}
	if o.CompactForceInterval <= o.CompactCheckInterval {
		o.CompactForceInterval = o.CompactCheckInterval * 2
	}
	if o.CompactKeyOpDelta <= 0 {
		o.CompactKeyOpDelta = defaultCompactKeyOpDelta
	}
	if o.GcInterval <= 0 {
		o.GcInterval = defaultGcInterval
	}
}
