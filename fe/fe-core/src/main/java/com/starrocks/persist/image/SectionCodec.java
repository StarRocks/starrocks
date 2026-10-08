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

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import com.starrocks.persist.proto.CompressionType;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * The compression applied to one independently decodable stream of a section: the manager message,
 * or the entries. Each is its own frame, so the reader can decode the manager message without
 * touching the entries.
 */
abstract class SectionCodec {

    /**
     * Wraps {@code out} for one stream. Closing the returned stream finishes the frame and flushes
     * it into {@code out}, but never closes {@code out} itself.
     */
    abstract OutputStream compress(OutputStream out) throws IOException;

    /** Wraps a stream positioned at the start of one frame. */
    abstract InputStream decompress(InputStream in) throws IOException;

    static SectionCodec forType(CompressionType type, int zstdLevel) throws ImageFormatException {
        switch (type) {
            case COMPRESSION_NONE:
                return NONE;
            case COMPRESSION_ZSTD:
                return new Zstd(zstdLevel);
            default:
                throw new ImageFormatException("unsupported compression type " + type.getNumber());
        }
    }

    private static final SectionCodec NONE = new SectionCodec() {
        @Override
        OutputStream compress(OutputStream out) {
            return new NonClosingOutputStream(out);
        }

        @Override
        InputStream decompress(InputStream in) {
            return in;
        }
    };

    private static final class Zstd extends SectionCodec {
        private final int level;

        Zstd(int level) {
            this.level = level;
        }

        @Override
        OutputStream compress(OutputStream out) throws IOException {
            // The content checksum makes the frame self-verifying after decompression, complementing
            // the container-level crc32 that only covers on-disk bytes.
            return new ZstdOutputStream(new NonClosingOutputStream(out), level).setChecksum(true);
        }

        @Override
        InputStream decompress(InputStream in) throws IOException {
            return new ZstdInputStream(in);
        }
    }

    /** Lets a frame writer "close" without closing the shared file stream underneath. */
    private static final class NonClosingOutputStream extends FilterOutputStream {
        NonClosingOutputStream(OutputStream out) {
            super(out);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
        }

        @Override
        public void close() throws IOException {
            out.flush();
        }
    }
}
