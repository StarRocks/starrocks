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

package com.starrocks.common.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * A pool of direct byte buffers whose total size stays within a configurable bound.
 *
 * <p>Buffers come in power-of-two sizes from 64KB up and are kept for reuse once released. {@link #acquire} returns
 * null rather than go over the bound, so callers must have another way to do their work. The bound is read on every
 * call; when it has been lowered, released buffers are dropped instead of kept until the pool is back under it, and
 * {@link #trim} drops idle ones. A bound of 0 or less disables the pool. Dropped buffers give their memory back when
 * the garbage collector reclaims them, not right away.
 */
public final class DirectBufferPool {
    private static final int MIN_SHIFT = 16;
    // allocateDirect takes an int, so 1GB is the largest power of two it can serve.
    private static final int MAX_SHIFT = 30;

    private final LongSupplier capacityBytes;
    private final AtomicLong allocatedBytes = new AtomicLong();
    private final ConcurrentLinkedDeque<ByteBuffer>[] idle;

    @SuppressWarnings("unchecked")
    public DirectBufferPool(LongSupplier capacityBytes) {
        this.capacityBytes = capacityBytes;
        this.idle = new ConcurrentLinkedDeque[MAX_SHIFT - MIN_SHIFT + 1];
        for (int i = 0; i < idle.length; i++) {
            idle[i] = new ConcurrentLinkedDeque<>();
        }
    }

    /**
     * Returns a cleared direct buffer of at least {@code minCapacity} bytes, or null if handing one out would take the
     * pool over its bound. The buffer must go back through {@link #release}.
     */
    public ByteBuffer acquire(int minCapacity) {
        Preconditions.checkArgument(minCapacity >= 0, "negative capacity %s", minCapacity);
        int shift = shiftFor(minCapacity);
        if (shift > MAX_SHIFT) {
            return null;
        }
        ByteBuffer buffer = idle[shift - MIN_SHIFT].pollFirst();
        if (buffer != null) {
            buffer.clear();
            return buffer;
        }
        long size = 1L << shift;
        long capacity = capacityBytes.getAsLong();
        while (true) {
            long current = allocatedBytes.get();
            if (current + size > capacity) {
                return null;
            }
            if (allocatedBytes.compareAndSet(current, current + size)) {
                break;
            }
        }
        try {
            return ByteBuffer.allocateDirect((int) size);
        } catch (OutOfMemoryError e) {
            // -XX:MaxDirectMemorySize is exhausted; the caller falls back the same way as when the pool is full.
            allocatedBytes.addAndGet(-size);
            return null;
        }
    }

    /** Takes back a buffer handed out by {@link #acquire}. */
    public void release(ByteBuffer buffer) {
        int capacity = buffer.capacity();
        if (allocatedBytes.get() > capacityBytes.getAsLong()) {
            allocatedBytes.addAndGet(-capacity);
            return;
        }
        idle[shiftFor(capacity) - MIN_SHIFT].offerFirst(buffer);
    }

    /**
     * Drops idle buffers while the pool owns more than its bound. Lowering the bound alone only drops buffers as they
     * are released, which never happens to idle ones once nothing acquires from the pool any more.
     */
    public void trim() {
        for (ConcurrentLinkedDeque<ByteBuffer> buffers : idle) {
            while (allocatedBytes.get() > capacityBytes.getAsLong()) {
                ByteBuffer buffer = buffers.pollFirst();
                if (buffer == null) {
                    break;
                }
                allocatedBytes.addAndGet(-buffer.capacity());
            }
        }
    }

    /** Whether the bound is currently zero or less, so that {@link #acquire} cannot hand out anything. */
    public boolean isDisabled() {
        return capacityBytes.getAsLong() <= 0;
    }

    /** Total size of the buffers the pool owns, idle or handed out. */
    public long allocatedBytes() {
        return allocatedBytes.get();
    }

    @VisibleForTesting
    static int shiftFor(int capacity) {
        if (capacity <= 1 << MIN_SHIFT) {
            return MIN_SHIFT;
        }
        return 32 - Integer.numberOfLeadingZeros(capacity - 1);
    }
}
