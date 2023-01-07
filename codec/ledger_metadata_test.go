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
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDecodeLedgerMetadata(t *testing.T) {
	bytes := testBase642Bytes(t, "Qm9va2llTWV0YWRhdGFGb3JtYXRWZXJzaW9uCTMKNwgBEAEYACD///////////8BKAEyEgoOMTI3LjAuMC4xOjMxODEQADgBQgBIAWC8qtmLwpjdrkc=")
	metadata, err := DecodeLedgerMetadata(bytes)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int32(1), *metadata.QuorumSize)
	fmt.Println(metadata)
}
