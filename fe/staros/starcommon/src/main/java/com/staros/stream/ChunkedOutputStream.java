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

import com.google.protobuf.CodedOutputStream;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Must use the symmetric read/write interface between input/output stream.
 * Right now ByteBuffer is used.
 */
public class ChunkedOutputStream extends FilterOutputStream {
    private boolean closed;

    public ChunkedOutputStream(OutputStream out) {
        super(out);
        this.closed = false;
    }

    /**
     * Writes the specified byte to this output stream. The general
     * contract for <code>write</code> is that one byte is written
     * to the output stream. The byte to be written is the eight
     * low-order bits of the argument <code>b</code>. The 24
     * high-order bits of <code>b</code> are ignored.
     * <p>
     * Subclasses of <code>OutputStream</code> must provide an
     * implementation for this method.
     *
     * @param b the <code>byte</code>.
     * @throws IOException if an I/O error occurs. In particular,
     *                     an <code>IOException</code> may be thrown if the
     *                     output stream has been closed.
     */
    @Override
    public void write(int b) throws IOException {
        // write a single byte
        byte[] data = new byte[1];
        data[0] = (byte) b;
        write(data, 0, 1);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if ((off < 0) || (off > b.length) || (len < 0) ||
                ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }
        writeIntInternal(len);
        out.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        writeIntInternal(0);
        out.flush();
        closed = true;
    }

    /**
     * Leverage protobuf varint encoding to minimize the encoding overhead.
     *
     * @param n integer to be written
     * @throws IOException I/O error
     */
    private void writeIntInternal(int n) throws IOException {
        int len = CodedOutputStream.computeUInt32SizeNoTag(n);
        byte[] varintBytes = new byte[len];
        CodedOutputStream os = CodedOutputStream.newInstance(varintBytes);
        os.writeUInt32NoTag(n);
        os.flush();
        out.write(varintBytes, 0, varintBytes.length);
    }
}
