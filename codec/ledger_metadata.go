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

package codec

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/protocol-laboratory/bookkeeper-codec-go/pb"
	"io"
	"strconv"
	"strings"
)

func DecodeLedgerMetadata(data []byte) (*pb.LedgerMetadataFormat, error) {
	r := bufio.NewReader(bytes.NewReader(data))
	metadataVersion, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	split := strings.Split(metadataVersion, "\t")
	if len(split) != 2 {
		return nil, fmt.Errorf("invalid ledger metadata version: %s", metadataVersion)
	}
	version, err := strconv.Atoi(split[1][:len(split[1])-1])
	if err != nil {
		return nil, err
	}
	if version == 3 {
		p := &pb.LedgerMetadataFormat{}
		var remaining []byte
		remaining, err = io.ReadAll(r)
		if err != nil {
			return nil, err
		}
		buffer := proto.NewBuffer(remaining)
		err = buffer.DecodeMessage(p)
		if err != nil {
			return nil, err
		}
		return p, nil
	}
	return nil, fmt.Errorf("invalid ledger metadata version: %d", version)
}
