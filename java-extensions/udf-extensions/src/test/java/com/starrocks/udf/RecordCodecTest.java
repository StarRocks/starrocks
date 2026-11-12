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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;

public class RecordCodecTest {
    public record Money(String code, Double amount, Double usd) {
    }

    private record Hidden(String name, Integer count) {
    }

    public record Prims(boolean z, byte b, short s, int i, long l, float f, double d) {
    }

    public record Inner(String tag, Long value) {
    }

    public record Outer(String name, Inner inner) {
    }

    public record Wide(Integer f0, Integer f1, Integer f2, Integer f3, Integer f4, Integer f5, Integer f6,
                       String f7) {
    }

    public record Checked(Integer v) {
        public Checked {
            if (v != null && v < 0) {
                throw new IllegalArgumentException("negative: " + v);
            }
        }
    }

    // ---------------------------------------------------------------- helpers

    private static Object[][] newPerField(Class<?> recordClass, int numRows) {
        RecordComponent[] comps = recordClass.getRecordComponents();
        Object[][] perField = new Object[comps.length][];
        for (int f = 0; f < comps.length; f++) {
            Class<?> t = comps[f].getType();
            Class<?> elem = t.isPrimitive() ? java.lang.invoke.MethodType.methodType(t).wrap().returnType() : t;
            perField[f] = (Object[]) Array.newInstance(elem, numRows);
        }
        return perField;
    }

    private static byte[] nullsOf(Object[] records) {
        byte[] nulls = new byte[records.length];
        for (int r = 0; r < records.length; r++) {
            nulls[r] = (byte) (records[r] == null ? 1 : 0);
        }
        return nulls;
    }

    private static void assertRoundTrip(Class<?> recordClass, Object[] records) {
        assertRoundTrip(recordClass, records, nullsOf(records), true);
    }

    /**
     * Drain with the cached codec and the reflection codec and assert they agree, then rebuild from the
     * per-field arrays with both and assert the result equals the input (rows null in {@code nulls} -> null).
     */
    private static void assertRoundTrip(Class<?> recordClass, Object[] records, byte[] nulls,
                                        boolean expectGenerated) {
        RecordCodec gen = RecordCodec.of(recordClass);
        RecordCodec ref = RecordCodec.reflective(recordClass);
        Assertions.assertEquals(expectGenerated, gen.isGenerated(), "codegen use for " + recordClass);
        Assertions.assertFalse(ref.isGenerated());

        int n = records.length;
        Object[][] genFields = newPerField(recordClass, n);
        Object[][] refFields = newPerField(recordClass, n);
        gen.drain(n, records, genFields);
        ref.drain(n, records, refFields);
        for (int f = 0; f < genFields.length; f++) {
            Assertions.assertArrayEquals(refFields[f], genFields[f], "field " + f);
        }

        Object[] genOut = gen.build(n, nulls, genFields);
        Object[] refOut = ref.build(n, nulls, refFields);
        Assertions.assertEquals(recordClass, genOut.getClass().getComponentType());
        Assertions.assertArrayEquals(refOut, genOut);
        for (int r = 0; r < n; r++) {
            Object expected = nulls[r] != 0 ? null : records[r];
            Assertions.assertEquals(expected, genOut[r], "row " + r);
        }
    }

    // ---------------------------------------------------------------- tests

    @Test
    public void testBoxedComponentsWithNullRowsAndNullFields() {
        Object[] records = {
                new Money("USD", 1.5, 1.5),
                null,
                new Money("EUR", null, 2.0),
                new Money(null, 3.0, null),
        };
        assertRoundTrip(Money.class, records);
        // The parent null bitmap on the build side drops a row even if the field arrays have data.
        assertRoundTrip(Money.class, records, new byte[] {0, 1, 1, 0}, true);
        // Without a parent null bitmap (non-nullable column) every row is built.
        Object[] full = {new Money("USD", 1.5, 1.5), new Money(null, null, null)};
        Object[] built = RecordCodec.of(Money.class).build(2, null,
                new Object[] {new String[] {"USD", null}, new Double[] {1.5, null}, new Double[] {1.5, null}});
        Assertions.assertArrayEquals(full, built);
    }

    @Test
    public void testPrivateCanonicalConstructorFallsBackToReflection() {
        // A private record's implicit canonical constructor is private, which a separate generated
        // class cannot invoke; the codec must still work through reflection.
        Object[] records = {new Hidden("a", 1), new Hidden(null, null), null};
        assertRoundTrip(Hidden.class, records, nullsOf(records), false);
    }

    @Test
    public void testPrimitiveComponents() {
        Object[] records = {
                new Prims(true, (byte) 1, (short) 2, 3, 4L, 5.5f, 6.25),
                null,
                new Prims(false, Byte.MIN_VALUE, Short.MAX_VALUE, Integer.MIN_VALUE, Long.MAX_VALUE,
                        Float.NaN, Double.NEGATIVE_INFINITY),
        };
        assertRoundTrip(Prims.class, records);
    }

    @Test
    public void testNullForPrimitiveComponentFails() {
        Object[][] fields = newPerField(Prims.class, 1);
        // all fields null for a non-null row
        for (RecordCodec codec : new RecordCodec[] {RecordCodec.of(Prims.class), RecordCodec.reflective(Prims.class)}) {
            RuntimeException e = Assertions.assertThrows(RuntimeException.class,
                    () -> codec.build(1, null, fields));
            Assertions.assertTrue(e.getMessage().startsWith("Failed to construct record"), e.getMessage());
        }
    }

    @Test
    public void testNestedRecord() {
        Object[] records = {
                new Outer("x", new Inner("t", 7L)),
                new Outer("y", null),
                null,
        };
        assertRoundTrip(Outer.class, records);
        assertRoundTrip(Inner.class, new Object[] {new Inner("t", 7L), null});
    }

    @Test
    public void testMoreThanFiveComponents() {
        Object[] records = {new Wide(0, 1, 2, 3, 4, 5, 6, "seven"), null};
        assertRoundTrip(Wide.class, records);
    }

    @Test
    public void testCompactConstructorExceptionIsWrapped() {
        Object[] fieldArrays = {new Integer[] {1, -1}};
        for (RecordCodec codec : new RecordCodec[] {RecordCodec.of(Checked.class),
                RecordCodec.reflective(Checked.class)}) {
            RuntimeException e = Assertions.assertThrows(RuntimeException.class,
                    () -> codec.build(2, null, fieldArrays));
            Assertions.assertEquals("Failed to construct record " + Checked.class.getName(), e.getMessage());
            Assertions.assertInstanceOf(IllegalArgumentException.class, e.getCause());
            Assertions.assertEquals("negative: -1", e.getCause().getMessage());
        }
    }

    @Test
    public void testFieldCountMismatch() {
        RecordCodec codec = RecordCodec.of(Money.class);
        Assertions.assertThrows(IllegalStateException.class,
                () -> codec.build(1, null, new Object[] {new String[] {"USD"}}));
        Assertions.assertThrows(IllegalStateException.class,
                () -> codec.drain(1, new Object[] {new Money("USD", 1.0, 1.0)}, new Object[2][]));
    }

    @Test
    public void testNotARecord() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> RecordCodec.of(String.class));
        Assertions.assertThrows(IllegalArgumentException.class, () -> RecordCodec.reflective(String.class));
    }

    @Test
    public void testRecordInUnnamedPackage() throws Exception {
        Class<?> recordClass = Class.forName("DefaultPackageRecord");
        Constructor<?> ctor = recordClass.getDeclaredConstructor(Integer.class, String.class);
        Object[] records = (Object[]) Array.newInstance(recordClass, 3);
        records[0] = ctor.newInstance(1, "hello");
        records[1] = ctor.newInstance(null, "world");
        assertRoundTrip(recordClass, records);
    }

    @Test
    public void testCodecIsCachedPerClass() {
        Assertions.assertSame(RecordCodec.of(Money.class), RecordCodec.of(Money.class));
    }

    @Test
    public void testCreateBoxedStructArrayUsesCodec() {
        Object[] fieldArrays = {new String[] {"USD", "EUR", "GBP"}, new Double[] {1.0, 2.0, 3.0},
                new Double[] {1.0, 2.5, 4.5}};
        ByteBuffer nulls = ByteBuffer.wrap(new byte[] {0, 1, 0});
        Object[] out = UDFHelper.createBoxedStructArray(3, nulls, Money.class, fieldArrays);
        Assertions.assertArrayEquals(new Money[] {new Money("USD", 1.0, 1.0), null, new Money("GBP", 3.0, 4.5)},
                out);
        Assertions.assertEquals(Money.class, out.getClass().getComponentType());
    }

    /** Defines one class from the test classpath in its own loader, like UDFClassLoader does for a UDF jar. */
    private static final class IsolatingLoader extends ClassLoader {
        private final String isolated;

        IsolatingLoader(String isolated) {
            super(RecordCodecTest.class.getClassLoader());
            this.isolated = isolated;
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (!name.equals(isolated)) {
                return super.loadClass(name, resolve);
            }
            synchronized (getClassLoadingLock(name)) {
                Class<?> c = findLoadedClass(name);
                if (c == null) {
                    try (InputStream in = getParent().getResourceAsStream(name.replace('.', '/') + ".class")) {
                        byte[] bytes = in.readAllBytes();
                        c = defineClass(name, bytes, 0, bytes.length);
                    } catch (Exception e) {
                        throw new ClassNotFoundException(name, e);
                    }
                }
                return c;
            }
        }
    }

    @Test
    public void testPackagePrivateRecordFromSeparateClassLoader() throws Exception {
        String name = "com.starrocks.udf.isolated.IsolatedRecord";
        Class<?> recordClass = new IsolatingLoader(name).loadClass(name);
        Assertions.assertNotSame(Class.forName(name), recordClass);

        Constructor<?> ctor = recordClass.getDeclaredConstructor(String.class, double.class);
        ctor.setAccessible(true);
        Object[] records = (Object[]) Array.newInstance(recordClass, 3);
        records[0] = ctor.newInstance("USD", 1.25);
        records[2] = ctor.newInstance("EUR", -3.5);
        assertRoundTrip(recordClass, records);

        // The codec class lives next to the record, in the record's loader.
        Class<?> codecClass = Class.forName(name + RecordCodec.CODEC_SUFFIX, false, recordClass.getClassLoader());
        Assertions.assertSame(recordClass.getClassLoader(), codecClass.getClassLoader());
    }
}
