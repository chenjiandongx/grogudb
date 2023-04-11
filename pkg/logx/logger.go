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

package logx

import (
	"github.com/chenjiandongx/logger"
)

type Logger interface {
	Infof(format string, v ...interface{})
	Errorf(format string, v ...interface{})
}

var logf Logger = logger.New(logger.Options{
	Stdout:      true,
	ConsoleMode: true,
	Skip:        2,
	Level:       logger.ErrorLevel,
})

func SetLogger(logger Logger) {
	logf = logger
}

func Infof(format string, v ...interface{}) {
	if logf == nil {
		return
	}
	logf.Infof(format, v...)
}

func Errorf(format string, v ...interface{}) {
	if logf == nil {
		return
	}
	logf.Errorf(format, v...)
}
