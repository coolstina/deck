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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coolstina/fsfire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestDeckerSuite(t *testing.T) {
	suite.Run(t, &DeckerSuite{})
}

type sms struct {
	TaskId  int    `json:"task_id"`
	UserId  int    `json:"user_id"`
	Message string `json:"message"`
}

func (s sms) String() string {
	data, _ := json.Marshal(&s)
	return string(data)
}

type DeckerSuite struct {
	suite.Suite
	decker Decker
}

func (suite *DeckerSuite) BeforeTest(suiteName, testName string) {
	suite.decker = newDeck(true)
}

func (suite *DeckerSuite) Test_Enqueue_FetchOrStorage_OnlyFetch() {
	taskId := 1
	suite.decker.Enqueue(taskId,
		sms{TaskId: taskId, UserId: 1, Message: "helloworld1"},
		sms{TaskId: taskId, UserId: 2, Message: "helloworld2"},
		sms{TaskId: taskId, UserId: 3, Message: "helloworld3"},
		sms{TaskId: taskId, UserId: 4, Message: "helloworld4"},
		sms{TaskId: taskId, UserId: 5, Message: "helloworld5"},
	)

	l := suite.decker.Len(taskId)
	assert.Equal(suite.T(), 5, l)

	suite.decker.Enqueue(taskId,
		sms{TaskId: taskId, UserId: 1, Message: "helloshaohua1"},
		sms{TaskId: taskId, UserId: 2, Message: "helloshaohua2"},
		sms{TaskId: taskId, UserId: 3, Message: "helloshaohua3"},
		sms{TaskId: taskId, UserId: 4, Message: "helloshaohua4"},
		sms{TaskId: taskId, UserId: 5, Message: "helloshaohua5"},
	)

	l = suite.decker.Len(taskId)
	assert.Equal(suite.T(), 10, l)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(700 * time.Millisecond)
		cancel()
	}()

	var r = make([]sms, 0)
	for i := range suite.decker.FetchOrStorage(ctx, taskId) {
		fmt.Printf("Receiver: %+v\n", i)
		r = append(r, i.(sms))
	}
	assert.Len(suite.T(), r, 10)

	exists := suite.decker.Exists(taskId)
	assert.Equal(suite.T(), false, exists)
}

func (suite *DeckerSuite) Test_Enqueue_FetchOrStorage_WithStorage() {
	taskId := 2
	suite.decker.Enqueue(taskId,
		sms{TaskId: taskId, UserId: 1, Message: "helloworld1"},
		sms{TaskId: taskId, UserId: 2, Message: "helloworld2"},
		sms{TaskId: taskId, UserId: 3, Message: "helloworld3"},
		sms{TaskId: taskId, UserId: 4, Message: "helloworld4"},
		sms{TaskId: taskId, UserId: 5, Message: "helloworld5"},
	)

	l := suite.decker.Len(taskId)
	assert.Equal(suite.T(), 5, l)

	suite.decker.Enqueue(taskId,
		sms{TaskId: taskId, UserId: 1, Message: "helloshaohua1"},
		sms{TaskId: taskId, UserId: 2, Message: "helloshaohua2"},
		sms{TaskId: taskId, UserId: 3, Message: "helloshaohua3"},
		sms{TaskId: taskId, UserId: 4, Message: "helloshaohua4"},
		sms{TaskId: taskId, UserId: 5, Message: "helloshaohua5"},
	)

	l = suite.decker.Len(taskId)
	assert.Equal(suite.T(), 10, l)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(700 * time.Millisecond)
		cancel()
	}()

	filesystem, buffer := StorageToFilesystem()

	var r = make([]sms, 0)
	options := []Option{WithStorage(filesystem), WithInterval(300 * time.Millisecond)}
	for i := range suite.decker.FetchOrStorage(ctx, taskId, options...) {
		fmt.Printf("Receiver: %+v\n", i)
		r = append(r, i.(sms))
	}
	assert.Len(suite.T(), r, 2)

	exists := suite.decker.Exists(taskId)
	assert.Equal(suite.T(), false, exists)
	if buffer != nil {

		path := "test/data"
		err := fsfire.NotExistsMkdirAll(path)
		assert.NoError(suite.T(), err)

		name := filepath.Join(path, "sms.interval.300.txt")
		err = ioutil.WriteFile(name, buffer.Bytes(), os.ModePerm)
		assert.NoError(suite.T(), err)
	}
}

func (suite *DeckerSuite) Test_Enqueue_FetchOrStorage_WithStorageAndInterval() {
	taskId := 3
	suite.decker.Enqueue(taskId,
		sms{TaskId: taskId, UserId: 1, Message: "helloworld1"},
		sms{TaskId: taskId, UserId: 2, Message: "helloworld2"},
		sms{TaskId: taskId, UserId: 3, Message: "helloworld3"},
		sms{TaskId: taskId, UserId: 4, Message: "helloworld4"},
		sms{TaskId: taskId, UserId: 5, Message: "helloworld5"},
	)

	l := suite.decker.Len(taskId)
	assert.Equal(suite.T(), 5, l)

	suite.decker.Enqueue(taskId,
		sms{TaskId: taskId, UserId: 1, Message: "helloshaohua1"},
		sms{TaskId: taskId, UserId: 2, Message: "helloshaohua2"},
		sms{TaskId: taskId, UserId: 3, Message: "helloshaohua3"},
		sms{TaskId: taskId, UserId: 4, Message: "helloshaohua4"},
		sms{TaskId: taskId, UserId: 5, Message: "helloshaohua5"},
	)

	l = suite.decker.Len(taskId)
	assert.Equal(suite.T(), 10, l)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(700 * time.Millisecond)
		cancel()
	}()

	filesystem, buffer := StorageToFilesystem()

	var r = make([]sms, 0)
	options := []Option{WithStorage(filesystem), WithInterval(400 * time.Millisecond)}
	for i := range suite.decker.FetchOrStorage(ctx, taskId, options...) {
		fmt.Printf("Receiver: %+v\n", i)
		r = append(r, i.(sms))
	}
	assert.Len(suite.T(), r, 1)

	exists := suite.decker.Exists(taskId)
	assert.Equal(suite.T(), false, exists)
	if buffer != nil {

		path := "test/data"
		err := fsfire.NotExistsMkdirAll(path)
		assert.NoError(suite.T(), err)

		name := filepath.Join(path, "sms.interval.400.txt")
		err = ioutil.WriteFile(name, buffer.Bytes(), os.ModePerm)
		assert.NoError(suite.T(), err)
	}
}

func StorageToFilesystem() (Storage, *bytes.Buffer) {
	var buffer = &bytes.Buffer{}

	return func(value interface{}) {
		if s, ok := value.(sms); ok {
			buffer.WriteString(s.String())
			buffer.WriteByte('\n')
		}
	}, buffer
}
