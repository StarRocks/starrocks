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

package com.staros.stream;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ChunkedInputStream extends FilterInputStream {
    public static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[0]).asReadOnlyBuffer();
    private boolean readEnd;
    private ByteBuffer buffer;

    public ChunkedInputStream(InputStream in) {
        super(in);
        this.readEnd = false;
        this.buffer = EMPTY_BYTE_BUFFER;
    }

    /**
     * Reads the next byte of data from the input stream. The value byte is
     * returned as an <code>int</code> in the range <code>0</code> to
     * <code>255</code>. If no byte is available because the end of the stream
     * has been reached, the value <code>-1</code> is returned. This method
     * blocks until input data is available, the end of the stream is detected,
     * or an exception is thrown.
     *
     * <p> A subclass must provide an implementation of this method.
     *
     * @return the next byte of data, or <code>-1</code> if the end of the
     * stream is reached.
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public int read() throws IOException {
        if (readEnd) {
            return -1;
        }
        if (buffer.remaining() == 0) {
            readNextAvailable();
            if (buffer.remaining() == 0) {
                return -1;
            }
        }
        return (0xFF & buffer.get());
    }

    /**
     * An optimized way to read to byte array, rather read byte one by one
     *
     * @param b   the buffer into which the data is read.
     * @param off the start offset in array <code>b</code>
     *            at which the data is written.
     * @param len the maximum number of bytes to read.
     * @return number of bytes read, or -1 if reaches end of the stream
     * @throws IOException I/O error
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }
        if (readEnd) {
            return -1;
        }
        int done = 0;
        while (done < len) {
            if (buffer.remaining() == 0) {
                readNextAvailable();
                if (buffer.remaining() == 0) {
                    break;
                }
            }
            int todo = Math.min(len - done, buffer.remaining());
            buffer.get(b, off, todo);
            off += todo;
            done += todo;
        }
        return done == 0 ? -1 : done;
    }

    private void readNextAvailable() throws IOException {
        int len = readIntFromParent();
        if (len == 0) {
            readEnd = true;
            buffer = EMPTY_BYTE_BUFFER;
            return;
        }
        buffer = ByteBuffer.allocate(len);
        int done = 0;
        while (done < len) {
            int n = in.read(buffer.array(), buffer.position(), buffer.remaining());
            if (n == -1) {
                throw new EOFException();
            }
            done += n;
            buffer.position(done);
        }
        buffer.flip();
        buffer = buffer.asReadOnlyBuffer();
    }

    @Override
    public void close() throws IOException {
        // do nothing, don't close parent stream
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    /**
     * Leverage Protobuf CodedInputStream to decode varint from input stream
     *
     * @return varint
     * @throws IOException I/O exception
     */
    private int readIntFromParent() throws IOException {
        int firstByte = in.read();
        if (firstByte == -1) {
            throw new EOFException();
        }
        try {
            return CodedInputStream.readRawVarint32(firstByte, in);
        } catch (InvalidProtocolBufferException exception) {
            throw new EOFException();
        }
    }
}
