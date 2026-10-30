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

package com.starrocks.udf;

import com.starrocks.udf.UDFHelper.DirectByteBufferFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sun.misc.Unsafe;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

public class UDFHelperDirectBufferTest {
    private static final Unsafe UNSAFE;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (Unsafe) field.get(null);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // Verifies batchCreateDirectBuffer wraps native memory correctly end-to-end. This uses
    // whichever DirectByteBuffer constructor matches the running JDK.
    @Test
    public void testBatchCreateDirectBuffer() throws Exception {
        // Three variable-length segments laid out back-to-back in a single native block.
        byte[][] segments = {
                {1, 2, 3},
                {},
                {4, 5, 6, 7, 8}
        };

        int total = 0;
        for (byte[] segment : segments) {
            total += segment.length;
        }

        int[] offsets = new int[segments.length + 1];
        for (int i = 0; i < segments.length; i++) {
            offsets[i + 1] = offsets[i] + segments[i].length;
        }

        long base = UNSAFE.allocateMemory(Math.max(total, 1));
        try {
            for (int i = 0; i < segments.length; i++) {
                for (int j = 0; j < segments[i].length; j++) {
                    UNSAFE.putByte(base + offsets[i] + j, segments[i][j]);
                }
            }

            Object[] buffers = UDFHelper.batchCreateDirectBuffer(base, offsets, segments.length);

            Assertions.assertEquals(segments.length, buffers.length);
            for (int i = 0; i < segments.length; i++) {
                ByteBuffer buffer = (ByteBuffer) buffers[i];
                Assertions.assertTrue(buffer.isDirect());
                Assertions.assertEquals(segments[i].length, buffer.capacity());
                Assertions.assertEquals(segments[i].length, buffer.remaining());

                byte[] actual = new byte[buffer.remaining()];
                buffer.get(actual);
                Assertions.assertArrayEquals(segments[i], actual);
            }
        } finally {
            UNSAFE.freeMemory(base);
        }
    }

    @Test
    public void testBatchCreateDirectBufferEmpty() throws Exception {
        Object[] buffers = UDFHelper.batchCreateDirectBuffer(0L, new int[] {0}, 0);
        Assertions.assertEquals(0, buffers.length);
    }

    // Initializing UDFHelper (which happens for every Java UDF) must not eagerly perform the
    // reflective java.nio.DirectByteBuffer access. Otherwise, on JVMs that do not open
    // java.base/java.nio, class initialization would fail and break UDFs that never build
    // direct buffers.
    @Test
    public void testDirectByteBufferLookupIsLazy() throws Exception {
        ClassLoader loader = UDFHelperDirectBufferTest.class.getClassLoader();
        // initialize = true forces the class's static initializers to run.
        Class<?> loaded = Class.forName("com.starrocks.udf.UDFHelper", true, loader);
        Assertions.assertEquals("com.starrocks.udf.UDFHelper", loaded.getName());
        // A non-buffer helper must work without requiring java.nio to be opened.
        Assertions.assertEquals(
                java.time.LocalDateTime.of(2026, 4, 15, 14, 39, 38),
                UDFHelper.localDateTimeFromPackedTimestamp(
                        UDFHelper.packedTimestampFromLocalDateTime(
                                java.time.LocalDateTime.of(2026, 4, 15, 14, 39, 38))));
    }

    // DirectByteBufferFactory.resolve() picks the constructor for the running JDK: (long, int)
    // on JDK <= 17 and (long, long) on JDK 21+. The resolved factory must build a valid buffer.
    @Test
    public void testFactoryResolveMatchesRuntime() throws Exception {
        DirectByteBufferFactory factory = DirectByteBufferFactory.resolve();
        boolean expectLongCapacity = jdkRemovedIntConstructor();
        Assertions.assertEquals(expectLongCapacity, factory.usesLongCapacity());
        assertFactoryProducesBuffer(factory, new byte[] {10, 20, 30});
    }

    // Explicitly drives the legacy (long, int) path regardless of the running JDK. Skipped on
    // JDK 21+ where the constructor no longer exists.
    @Test
    public void testFactoryLegacyIntCapacityPath() throws Exception {
        if (jdkRemovedIntConstructor()) {
            return;
        }
        Constructor<?> legacy = Class.forName("java.nio.DirectByteBuffer")
                .getDeclaredConstructor(long.class, int.class);
        legacy.setAccessible(true);
        DirectByteBufferFactory factory = new DirectByteBufferFactory(legacy, false);
        Assertions.assertFalse(factory.usesLongCapacity());
        assertFactoryProducesBuffer(factory, new byte[] {42, 43});
    }

    // Explicitly drives the JDK 21+ (long, long) path. The (long, long) constructor only
    // exists on JDK 21+, so this is skipped on older JDKs where it cannot run.
    @Test
    public void testFactoryLongCapacityPath() throws Exception {
        Constructor<?> longCtor;
        try {
            longCtor = Class.forName("java.nio.DirectByteBuffer")
                    .getDeclaredConstructor(long.class, long.class);
        } catch (NoSuchMethodException e) {
            return; // Pre-JDK 21 runtime: the (long, long) constructor does not exist.
        }
        longCtor.setAccessible(true);
        DirectByteBufferFactory factory = new DirectByteBufferFactory(longCtor, true);
        Assertions.assertTrue(factory.usesLongCapacity());
        assertFactoryProducesBuffer(factory, new byte[] {7, 8, 9, 10});
    }

    // getConstructorOrNull returns the constructor when the signature exists and null when it
    // does not, which is how resolve() decides between the (long, int) and (long, long) forms.
    @Test
    public void testGetConstructorOrNull() throws Exception {
        Class<?> dbb = Class.forName("java.nio.DirectByteBuffer");
        // (long, boolean) never exists -> null (covers the NoSuchMethodException path).
        Assertions.assertNull(DirectByteBufferFactory.getConstructorOrNull(dbb, boolean.class));
        // Whichever capacity type this JDK actually provides must resolve to a constructor.
        Class<?> capacityType = jdkRemovedIntConstructor() ? long.class : int.class;
        Assertions.assertNotNull(DirectByteBufferFactory.getConstructorOrNull(dbb, capacityType));
    }

    private static boolean jdkRemovedIntConstructor() {
        try {
            Class.forName("java.nio.DirectByteBuffer").getDeclaredConstructor(long.class, int.class);
            return false;
        } catch (ReflectiveOperationException e) {
            return true;
        }
    }

    private static void assertFactoryProducesBuffer(DirectByteBufferFactory factory, byte[] data) throws Exception {
        long base = UNSAFE.allocateMemory(Math.max(data.length, 1));
        try {
            for (int i = 0; i < data.length; i++) {
                UNSAFE.putByte(base + i, data[i]);
            }
            ByteBuffer buffer = (ByteBuffer) factory.newBuffer(base, data.length);
            Assertions.assertTrue(buffer.isDirect());
            Assertions.assertEquals(data.length, buffer.capacity());
            byte[] actual = new byte[buffer.remaining()];
            buffer.get(actual);
            Assertions.assertArrayEquals(data, actual);
        } finally {
            UNSAFE.freeMemory(base);
        }
    }
}
