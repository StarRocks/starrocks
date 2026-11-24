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

package com.starrocks.connector.iceberg.io;

import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;

import java.io.IOException;

/**
 * A remote stream timed for one planning: the time spent inside its calls is summed, and on close it is
 * recorded as one GET. Time the reader spends between calls (decoding) is not counted.
 *
 * <p>It does not expose the wrapped stream as a {@code DelegatingInputStream}: a reader that unwrapped it would
 * read past the timing. A wrapped {@link RangeReadable} stays range readable.
 */
class PlanningIoTimedInputStream extends SeekableInputStream {
    private final SeekableInputStream delegate;
    private final IcebergPlanningIoStats stats;
    private final String location;
    private long nanos;
    private boolean closed;

    static SeekableInputStream wrap(SeekableInputStream delegate, IcebergPlanningIoStats stats, String location,
                                    long openNanos) {
        return delegate instanceof RangeReadable ? new RangeReadableStream(delegate, stats, location, openNanos)
                : new PlanningIoTimedInputStream(delegate, stats, location, openNanos);
    }

    private PlanningIoTimedInputStream(SeekableInputStream delegate, IcebergPlanningIoStats stats, String location,
                                       long openNanos) {
        this.delegate = delegate;
        this.stats = stats;
        this.location = location;
        this.nanos = openNanos;
    }

    protected final void addNanos(long start) {
        nanos += System.nanoTime() - start;
    }

    protected final SeekableInputStream delegate() {
        return delegate;
    }

    @Override
    public long getPos() throws IOException {
        return delegate.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
        long start = System.nanoTime();
        try {
            delegate.seek(newPos);
        } finally {
            addNanos(start);
        }
    }

    @Override
    public int read() throws IOException {
        long start = System.nanoTime();
        try {
            return delegate.read();
        } finally {
            addNanos(start);
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        long start = System.nanoTime();
        try {
            return delegate.read(b, off, len);
        } finally {
            addNanos(start);
        }
    }

    @Override
    public long skip(long n) throws IOException {
        long start = System.nanoTime();
        try {
            return delegate.skip(n);
        } finally {
            addNanos(start);
        }
    }

    @Override
    public int available() throws IOException {
        return delegate.available();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        long start = System.nanoTime();
        try {
            delegate.close();
        } finally {
            addNanos(start);
            stats.recordGet(nanos, location);
        }
    }

    private static final class RangeReadableStream extends PlanningIoTimedInputStream implements RangeReadable {
        private RangeReadableStream(SeekableInputStream delegate, IcebergPlanningIoStats stats, String location,
                                    long openNanos) {
            super(delegate, stats, location, openNanos);
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
            long start = System.nanoTime();
            try {
                ((RangeReadable) delegate()).readFully(position, buffer, offset, length);
            } finally {
                addNanos(start);
            }
        }

        @Override
        public int readTail(byte[] buffer, int offset, int length) throws IOException {
            long start = System.nanoTime();
            try {
                return ((RangeReadable) delegate()).readTail(buffer, offset, length);
            } finally {
                addNanos(start);
            }
        }
    }
}
