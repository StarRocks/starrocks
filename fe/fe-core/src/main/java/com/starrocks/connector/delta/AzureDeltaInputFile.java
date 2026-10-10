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

package com.starrocks.connector.delta;

import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.DownloadRetryOptions;
import io.delta.kernel.defaults.engine.fileio.InputFile;
import io.delta.kernel.defaults.engine.fileio.SeekableInputStream;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Objects;

final class AzureDeltaInputFile implements InputFile {
    static final int READ_SIZE = 1024 * 1024;
    private final String path;
    private final long size;
    private final BlobClient client;

    AzureDeltaInputFile(String path, long size, BlobClient client) {
        this.path = path;
        this.size = size;
        this.client = client;
    }

    @Override
    public String path() {
        return path;
    }

    @Override
    public long length() throws IOException {
        AzureDeltaFileIO.checkInterrupted();
        try {
            // Kernel uses zero as an unknown length for files such as _last_checkpoint.
            return size > 0 ? size : client.getPropertiesWithResponse(
                    null, AzureDeltaFileIO.REQUEST_TIMEOUT, Context.NONE).getValue().getBlobSize();
        } catch (RuntimeException e) {
            throw AzureDeltaFileIO.ioException("stat", e);
        }
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return new RangeStream(length());
    }

    private final class RangeStream extends SeekableInputStream {
        private final long length;
        private long position;
        private long bufferStart;
        private byte[] buffer = new byte[0];
        private String etag;
        private boolean closed;

        private RangeStream(long length) {
            this.length = length;
        }

        private void checkOpen() throws IOException {
            AzureDeltaFileIO.checkInterrupted();
            if (closed) {
                throw new IOException("Native ADLS stream is closed");
            }
        }

        @Override
        public long getPos() throws IOException {
            checkOpen();
            return position;
        }

        @Override
        public void seek(long newPosition) throws IOException {
            checkOpen();
            if (newPosition < 0) {
                throw new IOException("Negative ADLS seek offset");
            }
            position = newPosition;
        }

        @Override
        public int read() throws IOException {
            byte[] single = new byte[1];
            return read(single, 0, 1) == -1 ? -1 : single[0] & 0xff;
        }

        @Override
        public int read(byte[] bytes, int offset, int count) throws IOException {
            Objects.checkFromIndexSize(offset, count, bytes.length);
            checkOpen();
            if (count == 0) {
                return 0;
            }
            if (position >= length) {
                return -1;
            }
            if (position < bufferStart || position >= bufferStart + buffer.length) {
                loadRange();
            }
            int bufferOffset = (int) (position - bufferStart);
            int copied = Math.min(count, buffer.length - bufferOffset);
            System.arraycopy(buffer, bufferOffset, bytes, offset, copied);
            position += copied;
            return copied;
        }

        private void loadRange() throws IOException {
            int count = (int) Math.min(READ_SIZE, length - position);
            ByteArrayOutputStream output = new ByteArrayOutputStream(count);
            try {
                BlobRequestConditions conditions = new BlobRequestConditions();
                if (etag != null) {
                    conditions.setIfMatch(etag);
                }
                var response = client.downloadStreamWithResponse(output, new BlobRange(position, (long) count),
                        new DownloadRetryOptions().setMaxRetryRequests(0), conditions, false,
                        AzureDeltaFileIO.REQUEST_TIMEOUT, Context.NONE);
                etag = response.getDeserializedHeaders().getETag();
            } catch (RuntimeException e) {
                throw AzureDeltaFileIO.ioException("range read", e);
            }
            AzureDeltaFileIO.checkInterrupted();
            if (output.size() != count) {
                throw new EOFException("Incomplete ADLS range read");
            }
            buffer = output.toByteArray();
            bufferStart = position;
        }

        @Override
        public void readFully(byte[] bytes, int offset, int count) throws IOException {
            Objects.checkFromIndexSize(offset, count, bytes.length);
            checkOpen();
            int read = 0;
            while (read < count) {
                int n = read(bytes, offset + read, count - read);
                if (n < 0) {
                    throw new EOFException("Reached end of ADLS file");
                }
                read += n;
            }
        }

        @Override
        public void close() {
            closed = true;
            buffer = new byte[0];
        }
    }
}
