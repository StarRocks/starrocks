// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/iceberg/blob/master/core/src/main/java/org/apache/iceberg/io/ByteBufferInputStream.java

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

package com.starrocks.connector.iceberg.io;

import org.apache.iceberg.io.SeekableInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public abstract class ByteBufferInputStream extends SeekableInputStream {

    public static ByteBufferInputStream wrap(ByteBuffer... buffers) {
        if (buffers.length == 1) {
            return new SingleBufferInputStream(buffers[0]);
        } else {
            return new MultiBufferInputStream(Arrays.asList(buffers));
        }
    }

    public static ByteBufferInputStream wrap(List<ByteBuffer> buffers) {
        if (buffers.size() == 1) {
            return new SingleBufferInputStream(buffers.get(0));
        } else {
            return new MultiBufferInputStream(buffers);
        }
    }

    public void skipFully(long length) throws IOException {
        long skipped = skip(length);
        if (skipped < length) {
            throw new EOFException(
                "Not enough bytes to skip: " + skipped + " < " + length);
        }
    }

    public abstract int read(ByteBuffer out);

    public abstract ByteBuffer slice(int length) throws EOFException;

    public abstract List<ByteBuffer> sliceBuffers(long length) throws EOFException;

    public ByteBufferInputStream sliceStream(long length) throws EOFException {
        return ByteBufferInputStream.wrap(sliceBuffers(length));
    }

    public abstract List<ByteBuffer> remainingBuffers();

    public ByteBufferInputStream remainingStream() {
        return ByteBufferInputStream.wrap(remainingBuffers());
    }
}
