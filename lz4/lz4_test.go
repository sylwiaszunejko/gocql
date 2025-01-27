//go:build all || unit
// +build all unit

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package lz4

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLZ4Compressor(t *testing.T) {
	var c LZ4Compressor
	require.Equal(t, "lz4", c.Name())

	_, err := c.Decode([]byte{0, 1, 2})
	require.EqualError(t, err, "cassandra lz4 block size should be >4, got=3")

	_, err = c.Decode([]byte{0, 1, 2, 4, 5})
	require.EqualError(t, err, "lz4: invalid source or destination buffer too short")

	// If uncompressed size is zero then nothing is decoded even if present.
	decoded, err := c.Decode([]byte{0, 0, 0, 0, 5, 7, 8})
	require.NoError(t, err)
	require.Nil(t, decoded)

	original := []byte("My Test String")
	encoded, err := c.Encode(original)
	require.NoError(t, err)
	decoded, err = c.Decode(encoded)
	require.NoError(t, err)
	require.Equal(t, original, decoded)
}
