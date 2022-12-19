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
//   https://github.com/apache/iceberg/blob/master/core/src/main/java/org/apache/iceberg/io/SingleBufferInputStream.java

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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

/**
 * This ByteBufferInputStream does not consume the ByteBuffer being passed in,
 * but will create a slice of the current buffer.
 */
class SingleBufferInputStream extends ByteBufferInputStream {

    private final ByteBuffer original;
    private final long startPosition;
    private final int length;
    private ByteBuffer buffer;
    private int mark;

    SingleBufferInputStream(ByteBuffer buffer) {
        // duplicate the buffer because its state will be modified
        this.original = buffer;
        this.startPosition = buffer.position();
        this.length = original.remaining();
        initFromBuffer();
    }

    private void initFromBuffer() {
        this.mark = -1;
        this.buffer = original.duplicate();
    }

    @Override
    public long getPos() {
        // position is relative to the start of the stream, not the buffer
        return buffer.position() - startPosition;
    }

    @Override
    public int read() throws IOException {
        if (!buffer.hasRemaining()) {
            throw new EOFException();
        }
        return buffer.get() & 0xFF; // as unsigned
    }

    @Override
    public int read(byte[] bytes, int offset, int len) throws IOException {
        if (len == 0) {
            return 0;
        }

        int remaining = buffer.remaining();
        if (remaining <= 0) {
            return -1;
        }

        int bytesToRead = Math.min(buffer.remaining(), len);
        buffer.get(bytes, offset, bytesToRead);

        return bytesToRead;
    }

    @Override
    public void seek(long newPosition) throws IOException {
        if (newPosition > length) {
            throw new EOFException(String.format("Cannot seek to position after end of file: %s", newPosition));
        }

        if (getPos() > newPosition) {
            // backwards seek requires returning to the initial state
            initFromBuffer();
        }

        long bytesToSkip = newPosition - getPos();
        skipFully(bytesToSkip);
    }

    @Override
    public long skip(long len) {
        if (len == 0) {
            return 0;
        }

        if (buffer.remaining() <= 0) {
            return -1;
        }

        // buffer.remaining is an int, so this will always fit in an int
        int bytesToSkip = (int) Math.min(buffer.remaining(), len);
        buffer.position(buffer.position() + bytesToSkip);

        return bytesToSkip;
    }

    @Override
    public int read(ByteBuffer out) {
        int bytesToCopy;
        ByteBuffer copyBuffer;
        if (buffer.remaining() <= out.remaining()) {
            // copy all of the buffer
            bytesToCopy = buffer.remaining();
            copyBuffer = buffer;
        } else {
            // copy a slice of the current buffer
            bytesToCopy = out.remaining();
            copyBuffer = buffer.duplicate();
            copyBuffer.limit(buffer.position() + bytesToCopy);
            buffer.position(buffer.position() + bytesToCopy);
        }

        out.put(copyBuffer);
        out.flip();

        return bytesToCopy;
    }

    @Override
    public ByteBuffer slice(int len) throws EOFException {
        if (buffer.remaining() < len) {
            throw new EOFException();
        }

        // length is less than remaining, so it must fit in an int
        ByteBuffer copy = buffer.duplicate();
        copy.limit(copy.position() + len);
        buffer.position(buffer.position() + len);

        return copy;
    }

    @Override
    public List<ByteBuffer> sliceBuffers(long len) throws EOFException {
        if (len == 0) {
            return Collections.emptyList();
        }

        if (len > buffer.remaining()) {
            throw new EOFException();
        }

        // length is less than remaining, so it must fit in an int
        return Collections.singletonList(slice((int) len));
    }

    @Override
    public List<ByteBuffer> remainingBuffers() {
        if (buffer.remaining() <= 0) {
            return Collections.emptyList();
        }

        ByteBuffer remaining = buffer.duplicate();
        buffer.position(buffer.limit());

        return Collections.singletonList(remaining);
    }

    @Override
    public synchronized void mark(int readlimit) {
        this.mark = buffer.position();
    }

    @Override
    public synchronized void reset() throws IOException {
        if (mark >= 0) {
            buffer.position(mark);
            this.mark = -1;
        } else {
            throw new IOException("No mark defined");
        }
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public int available() {
        return buffer.remaining();
    }
}
