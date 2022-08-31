// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package bknet

import (
	"fmt"
	"github.com/protocol-laboratory/bookkeeper-codec-go/codec"
	"github.com/protocol-laboratory/bookkeeper-codec-go/pb"
	"net"
	"sync"
)

type BookkeeperNetClientConfig struct {
	Host             string
	Port             int
	BufferMax        int
	SendQueueSize    int
	PendingQueueSize int
}

func (b BookkeeperNetClientConfig) addr() string {
	return fmt.Sprintf("%s:%d", b.Host, b.Port)
}

type sendRequest struct {
	bytes    []byte
	callback func([]byte, error)
}

type BookkeeperNetClient struct {
	conn         net.Conn
	eventsChan   chan *sendRequest
	pendingQueue chan *sendRequest
	buffer       *buffer
	closeCh      chan struct{}
}

type buffer struct {
	max    int
	bytes  []byte
	cursor int
}

func (b *BookkeeperNetClient) req(request *pb.Request) (*pb.Response, error) {
	reqBytes, err := request.Marshal()
	if err != nil {
		return nil, err
	}
	finalBytes := make([]byte, len(reqBytes)+4)
	codec.PutInt(finalBytes, 0, len(reqBytes))
	copy(finalBytes[4:], reqBytes)
	bytes, err := b.Send(finalBytes)
	if err != nil {
		return nil, err
	}
	resp := &pb.Response{}
	err = resp.Unmarshal(bytes)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (b *BookkeeperNetClient) Add(request *pb.AddRequest) (*pb.AddResponse, error) {
	reqBytes, err := request.Marshal()
	if err != nil {
		return nil, err
	}
	bytes, err := b.Send(reqBytes)
	if err != nil {
		return nil, err
	}
	resp := &pb.AddResponse{}
	err = resp.Unmarshal(bytes)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (b *BookkeeperNetClient) Send(bytes []byte) ([]byte, error) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	var result []byte
	var err error
	b.sendAsync(bytes, func(resp []byte, e error) {
		result = resp
		err = e
		wg.Done()
	})
	wg.Wait()
	if err != nil {
		return nil, err
	}
	return result[4:], nil
}

func (b *BookkeeperNetClient) sendAsync(bytes []byte, callback func([]byte, error)) {
	sr := &sendRequest{
		bytes:    bytes,
		callback: callback,
	}
	b.eventsChan <- sr
}

func (b *BookkeeperNetClient) read() {
	for {
		select {
		case req := <-b.pendingQueue:
			n, err := b.conn.Read(b.buffer.bytes[b.buffer.cursor:])
			if err != nil {
				req.callback(nil, err)
				b.closeCh <- struct{}{}
				break
			}
			b.buffer.cursor += n
			if b.buffer.cursor < 4 {
				continue
			}
			length := int(b.buffer.bytes[3]) | int(b.buffer.bytes[2])<<8 | int(b.buffer.bytes[1])<<16 | int(b.buffer.bytes[0])<<24 + 4
			if b.buffer.cursor < length {
				continue
			}
			if length > b.buffer.max {
				req.callback(nil, fmt.Errorf("response length %d is too large", length))
				b.closeCh <- struct{}{}
				break
			}
			req.callback(b.buffer.bytes[:length], nil)
			b.buffer.cursor -= length
			copy(b.buffer.bytes[:b.buffer.cursor], b.buffer.bytes[length:])
		case <-b.closeCh:
			return
		}
	}
}

func (b *BookkeeperNetClient) write() {
	for {
		select {
		case req := <-b.eventsChan:
			n, err := b.conn.Write(req.bytes)
			if err != nil {
				req.callback(nil, err)
				b.closeCh <- struct{}{}
				break
			}
			if n != len(req.bytes) {
				req.callback(nil, fmt.Errorf("write %d bytes, but expect %d bytes", n, len(req.bytes)))
				b.closeCh <- struct{}{}
				break
			}
			b.pendingQueue <- req
		case <-b.closeCh:
			return
		}
	}
}

func (b *BookkeeperNetClient) Close() {
	_ = b.conn.Close()
	b.closeCh <- struct{}{}
}

func NewBkNetClient(config BookkeeperNetClientConfig) (*BookkeeperNetClient, error) {
	conn, err := net.Dial("tcp", config.addr())
	if err != nil {
		return nil, err
	}
	if config.SendQueueSize == 0 {
		config.SendQueueSize = 1000
	}
	if config.PendingQueueSize == 0 {
		config.PendingQueueSize = 1000
	}
	if config.BufferMax == 0 {
		config.BufferMax = 512 * 1024
	}
	b := &BookkeeperNetClient{}
	b.conn = conn
	b.eventsChan = make(chan *sendRequest, config.SendQueueSize)
	b.pendingQueue = make(chan *sendRequest, config.PendingQueueSize)
	b.buffer = &buffer{
		max:    config.BufferMax,
		bytes:  make([]byte, config.BufferMax),
		cursor: 0,
	}
	b.closeCh = make(chan struct{})
	go func() {
		b.read()
	}()
	go func() {
		b.write()
	}()
	return b, nil
}
