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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class DirectBufferPoolTest {
    private static final int KB = 1024;
    private static final int MB = 1024 * KB;

    @Test
    public void testSizesArePowersOfTwoFrom64KB() {
        Assertions.assertEquals(16, DirectBufferPool.shiftFor(0));
        Assertions.assertEquals(16, DirectBufferPool.shiftFor(64 * KB));
        Assertions.assertEquals(17, DirectBufferPool.shiftFor(64 * KB + 1));
        Assertions.assertEquals(20, DirectBufferPool.shiftFor(MB));
        Assertions.assertEquals(21, DirectBufferPool.shiftFor(MB + 1));

        DirectBufferPool pool = new DirectBufferPool(() -> 16L * MB);
        ByteBuffer buffer = pool.acquire(100 * KB);
        Assertions.assertTrue(buffer.isDirect());
        Assertions.assertEquals(128 * KB, buffer.capacity());
        Assertions.assertEquals(128 * KB, pool.allocatedBytes());
    }

    @Test
    public void testReleasedBufferIsReusedCleared() {
        DirectBufferPool pool = new DirectBufferPool(() -> 16L * MB);
        ByteBuffer first = pool.acquire(MB);
        first.put(new byte[10]);
        pool.release(first);

        ByteBuffer second = pool.acquire(MB - 1);
        Assertions.assertSame(first, second);
        Assertions.assertEquals(0, second.position());
        Assertions.assertEquals(second.capacity(), second.limit());
        Assertions.assertEquals(MB, pool.allocatedBytes());
    }

    @Test
    public void testBoundIsNeverExceeded() {
        DirectBufferPool pool = new DirectBufferPool(() -> 2L * MB);
        ByteBuffer a = pool.acquire(MB);
        ByteBuffer b = pool.acquire(MB);
        Assertions.assertNotNull(a);
        Assertions.assertNotNull(b);
        // Both buffers are handed out, so a third would take the pool over its bound.
        Assertions.assertNull(pool.acquire(64 * KB));
        Assertions.assertEquals(2L * MB, pool.allocatedBytes());

        pool.release(a);
        Assertions.assertSame(a, pool.acquire(MB));
        // A buffer larger than the whole bound is never handed out.
        Assertions.assertNull(new DirectBufferPool(() -> MB).acquire(2 * MB));
        // Nor one larger than allocateDirect can make.
        Assertions.assertNull(new DirectBufferPool(() -> Long.MAX_VALUE).acquire(Integer.MAX_VALUE));
    }

    @Test
    public void testLoweredBoundDropsReleasedBuffers() {
        AtomicLong bound = new AtomicLong(4L * MB);
        DirectBufferPool pool = new DirectBufferPool(bound::get);
        ByteBuffer a = pool.acquire(MB);
        ByteBuffer b = pool.acquire(MB);
        Assertions.assertEquals(2L * MB, pool.allocatedBytes());

        bound.set(MB);
        pool.release(a);
        Assertions.assertEquals(MB, pool.allocatedBytes());
        // Back within the bound, so this one is kept for reuse.
        pool.release(b);
        Assertions.assertEquals(MB, pool.allocatedBytes());
        Assertions.assertSame(b, pool.acquire(MB));
    }

    @Test
    public void testTrimDropsIdleBuffersOverTheBound() {
        AtomicLong bound = new AtomicLong(4L * MB);
        DirectBufferPool pool = new DirectBufferPool(bound::get);
        ByteBuffer a = pool.acquire(MB);
        ByteBuffer b = pool.acquire(MB);
        pool.release(a);
        pool.release(b);
        Assertions.assertFalse(pool.isDisabled());
        // Within the bound, idle buffers are kept.
        pool.trim();
        Assertions.assertEquals(2L * MB, pool.allocatedBytes());

        bound.set(MB);
        pool.trim();
        Assertions.assertEquals(MB, pool.allocatedBytes());

        bound.set(0);
        Assertions.assertTrue(pool.isDisabled());
        pool.trim();
        Assertions.assertEquals(0, pool.allocatedBytes());
        Assertions.assertNull(pool.acquire(64 * KB));
    }

    @Test
    public void testNegativeCapacityIsRejected() {
        DirectBufferPool pool = new DirectBufferPool(() -> MB);
        Assertions.assertThrows(IllegalArgumentException.class, () -> pool.acquire(-1));
    }
}
