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

package rescue

import (
	"runtime"

	"github.com/chenjiandongx/grogudb/pkg/logx"
)

func logPanic(r interface{}) {
	const size = 64 << 10
	stacktrace := make([]byte, size)
	stacktrace = stacktrace[:runtime.Stack(stacktrace, false)]
	if _, ok := r.(string); ok {
		logx.Errorf("Observed a panic: %s\n%s", r, stacktrace)
	} else {
		logx.Errorf("Observed a panic: %#v (%v)\n%s", r, r, stacktrace)
	}
}

// HandleCrash 处理 panic 事件
func HandleCrash() {
	if r := recover(); r != nil {
		logPanic(r)
	}
}
