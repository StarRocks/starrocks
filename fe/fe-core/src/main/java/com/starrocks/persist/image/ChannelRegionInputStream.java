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

package com.starrocks.persist.image;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Reads {@code [offset, offset + length)} of a file through positional channel reads, without
 * moving the channel's own position.
 */
final class ChannelRegionInputStream extends InputStream {
    private static final int BUFFER_SIZE = 64 << 10;

    private final FileChannel channel;
    private final ByteBuffer buffer;
    private long position;
    private long remaining;

    ChannelRegionInputStream(FileChannel channel, long offset, long length) {
        this.channel = channel;
        this.position = offset;
        this.remaining = length;
        this.buffer = ByteBuffer.allocate((int) Math.max(1, Math.min(BUFFER_SIZE, length)));
        this.buffer.limit(0);
    }

    @Override
    public int read() throws IOException {
        if (!fill()) {
            return -1;
        }
        return buffer.get() & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        if (!fill()) {
            return -1;
        }
        int n = Math.min(len, buffer.remaining());
        buffer.get(b, off, n);
        return n;
    }

    @Override
    public int available() {
        return buffer.remaining();
    }

    /** Ensures the buffer has data; returns false at the end of the region. */
    private boolean fill() throws IOException {
        if (buffer.hasRemaining()) {
            return true;
        }
        if (remaining == 0) {
            return false;
        }
        buffer.clear();
        buffer.limit((int) Math.min(buffer.capacity(), remaining));
        int n = channel.read(buffer, position);
        if (n <= 0) {
            throw new EOFException("unexpected end of file at offset " + position);
        }
        position += n;
        remaining -= n;
        buffer.flip();
        return true;
    }
}
