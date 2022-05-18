// Copyright 2022 helloshaohua <wu.shaohua@foxmail.com>;
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package deck

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/coolstina/logger"
)

type deck struct {
	showLogger bool
	store      *sync.Map
	mutex      sync.RWMutex
}

func newDeck(showLogger bool) *deck {
	return &deck{showLogger: showLogger, store: &sync.Map{}}
}

type Effected struct {
	TaskId  int
	Already int
	Newness int
}

func (d *deck) Enqueue(taskId int, values ...interface{}) Effected {
	e := Effected{}
	l := list.New()

	if d.Exists(taskId) {
		l = d.Values(taskId)
		e.Already = l.Len()
	}

	for _, value := range values {
		l.PushBack(value)
	}

	e.Newness = e.Already - l.Len()
	d.store.Store(taskId, l)

	return e
}

func (d *deck) Exists(taskId int) bool {
	_, ok := d.store.Load(taskId)
	return ok
}

func (d *deck) Values(taskId int) *list.List {
	var l *list.List

	load, exists := d.store.Load(taskId)
	if exists {
		return load.(*list.List)
	}

	return l
}

func (d *deck) FetchOrStorage(ctx context.Context, taskId int, ops ...Option) <-chan interface{} {
	var channel = make(chan interface{})
	var options = &option{}

	for _, o := range ops {
		o.apply(options)
	}

	go func(ctx context.Context, taskId int) {
		load, exists := d.store.Load(taskId)
		if exists {
			d.loggerInfo("FetchOrStorage load storage", "exists", true)

			l := load.(*list.List)
			for e := l.Front(); e != nil; e = e.Next() {
				time.Sleep(options.interval)

				// Canceled
				if errors.Is(ctx.Err(), context.Canceled) {
					if options.storage != nil {
						d.loggerInfo("FetchOrStorage storage item")
						options.storage(e.Value)
					}

					continue
				}

				channel <- e.Value
			}

			if options.notification {
				channel <- struct{}{}
				d.loggerInfo("FetchOrStorage done sent notification")
			}

			close(channel)
			d.store.Delete(taskId)
			d.loggerInfo("Delete task message", "task_id", taskId)
		}
	}(ctx, taskId)

	return channel
}

func (d *deck) Len(taskId int) int {
	if d.Exists(taskId) {
		return d.Values(taskId).Len()
	}

	return 0
}

func (d *deck) loggerInfo(msg string, keysAndValues ...interface{}) {
	if d.showLogger {
		logger.Infow(fmt.Sprintf("[Deck]%s", msg), keysAndValues...)
	}
}
