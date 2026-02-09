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

package com.starrocks.memory.estimate;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EstimatorTest {

    @BeforeEach
    void setUp() {
        // Clear registries before each test
        // Note: SHALLOW_SIZE and CLASS_NESTED_FIELDS are ClassValue-based caches and cannot be cleared
        Estimator.CUSTOM_ESTIMATORS.clear();
        Estimator.SHALLOW_MEMORY_CLASS_NAMES.clear();
    }

    @AfterEach
    void tearDown() {
        // Clear registries after each test
        Estimator.CUSTOM_ESTIMATORS.clear();
        Estimator.SHALLOW_MEMORY_CLASS_NAMES.clear();
    }

    // ==================== Null and Basic Tests ====================

    @Test
    void testEstimateNull() {
        assertEquals(0, Estimator.estimate(null));
    }

    @Test
    void testShallowNull() {
        // Explicitly cast to Object to call shallow(Object) instead of shallow(Class<?>)
        assertEquals(0, Estimator.shallow((Object) null));
    }

    // ==================== Primitive Array Tests ====================

    @Test
    void testEstimatePrimitiveIntArray() {
        int[] array = new int[100];
        long size = Estimator.estimate(array);
        // Array header (16) + 100 * 4 bytes = 416
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 100 * 4, size);
    }

    @Test
    void testEstimatePrimitiveLongArray() {
        long[] array = new long[50];
        long size = Estimator.estimate(array);
        // Array header (16) + 50 * 8 bytes = 416
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 50 * 8, size);
    }

    @Test
    void testEstimatePrimitiveByteArray() {
        byte[] array = new byte[200];
        long size = Estimator.estimate(array);
        // Array header (16) + 200 * 1 byte = 216
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 200, size);
    }

    @Test
    void testEstimatePrimitiveBooleanArray() {
        boolean[] array = new boolean[100];
        long size = Estimator.estimate(array);
        // Array header (16) + 100 * 1 byte = 116
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 100, size);
    }

    @Test
    void testEstimatePrimitiveCharArray() {
        char[] array = new char[100];
        long size = Estimator.estimate(array);
        // Array header (16) + 100 * 2 bytes = 216
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 100 * 2, size);
    }

    @Test
    void testEstimatePrimitiveShortArray() {
        short[] array = new short[100];
        long size = Estimator.estimate(array);
        // Array header (16) + 100 * 2 bytes = 216
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 100 * 2, size);
    }

    @Test
    void testEstimatePrimitiveFloatArray() {
        float[] array = new float[100];
        long size = Estimator.estimate(array);
        // Array header (16) + 100 * 4 bytes = 416
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 100 * 4, size);
    }

    @Test
    void testEstimatePrimitiveDoubleArray() {
        double[] array = new double[100];
        long size = Estimator.estimate(array);
        // Array header (16) + 100 * 8 bytes = 816
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 100 * 8, size);
    }

    @Test
    void testEstimateEmptyPrimitiveArray() {
        int[] array = new int[0];
        long size = Estimator.estimate(array);
        assertEquals(Estimator.ARRAY_HEADER_SIZE, size);
    }

    // ==================== Object Array Tests ====================

    @Test
    void testEstimateEmptyObjectArray() {
        Object[] array = new Object[0];
        long size = Estimator.estimate(array);
        assertEquals(Estimator.ARRAY_HEADER_SIZE, size);
    }

    @Test
    void testEstimateObjectArrayWithNulls() {
        Object[] array = new Object[10];
        // All null elements
        long size = Estimator.estimate(array);
        // Array header (16) + 10 * reference size (4) = 56
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 10 * 4, size);
    }

    @Test
    void testEstimateStringArray() {
        String[] array = new String[] {"hello", "world", "test"};
        long size = Estimator.estimate(array);
        // Should be positive and include array overhead + string sizes
        assertTrue(size > Estimator.ARRAY_HEADER_SIZE + 3 * 4);
    }

    // ==================== Collection Tests ====================

    @Test
    void testEstimateEmptyArrayList() {
        List<String> list = new ArrayList<>();
        long size = Estimator.estimate(list);
        assertTrue(size > 0);
    }

    @Test
    void testEstimateArrayListWithElements() {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            list.add("element" + i);
        }
        long size = Estimator.estimate(list);
        // Should be significantly larger than empty list
        assertTrue(size > Estimator.estimate(new ArrayList<>()));
    }

    @Test
    void testEstimateLinkedList() {
        LinkedList<Integer> list = new LinkedList<>();
        for (int i = 0; i < 100; i++) {
            list.add(i);
        }
        long size = Estimator.estimate(list);
        assertTrue(size > 0);
    }

    @Test
    void testEstimateHashSet() {
        Set<String> set = new HashSet<>();
        for (int i = 0; i < 50; i++) {
            set.add("item" + i);
        }
        long size = Estimator.estimate(set);
        assertTrue(size > 0);
    }

    @Test
    void testEstimateConcurrentHashMap() {
        Map<String, Integer> map = new ConcurrentHashMap<>();
        for (int i = 0; i < 50; i++) {
            map.put("key" + i, i);
        }
        long size = Estimator.estimate(map);
        assertTrue(size > 0);
    }

    // ==================== Map Tests ====================

    @Test
    void testEstimateEmptyHashMap() {
        Map<String, String> map = new HashMap<>();
        long size = Estimator.estimate(map);
        assertTrue(size > 0);
    }

    @Test
    void testEstimateHashMapWithElements() {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            map.put("key" + i, "value" + i);
        }
        long size = Estimator.estimate(map);
        assertTrue(size > Estimator.estimate(new HashMap<>()));
    }

    // ==================== Custom Object Tests ====================

    @Test
    void testEstimateSimpleObject() {
        SimpleObject obj = new SimpleObject(42, "test");
        long size = Estimator.estimate(obj);
        assertTrue(size > 0);
    }

    @Test
    void testEstimateNestedObject() {
        NestedObject nested = new NestedObject();
        nested.inner = new SimpleObject(1, "inner");
        long size = Estimator.estimate(nested);
        // Should include both outer and inner object
        assertTrue(size > Estimator.shallow(nested));
    }

    @Test
    void testEstimateObjectWithCollection() {
        ObjectWithCollection obj = new ObjectWithCollection();
        for (int i = 0; i < 50; i++) {
            obj.items.add("item" + i);
        }
        long size = Estimator.estimate(obj);
        assertTrue(size > Estimator.shallow(obj));
    }

    // ==================== Max Depth Tests ====================

    @Test
    void testEstimateWithDepthZero() {
        NestedObject nested = new NestedObject();
        nested.inner = new SimpleObject(1, "inner");

        long shallowSize = Estimator.estimate(nested, 5, 0);
        long deepSize = Estimator.estimate(nested, 5, 8);

        // With depth 0, should only return shallow size
        assertEquals(Estimator.shallow(nested), shallowSize);
        assertTrue(deepSize > shallowSize);
    }

    @Test
    void testEstimateWithCustomDepth() {
        DeeplyNestedObject deep = createDeeplyNestedObject(10);

        long depth2 = Estimator.estimate(deep, 5, 2);
        long depth5 = Estimator.estimate(deep, 5, 5);
        long depth10 = Estimator.estimate(deep, 5, 10);

        // More depth should generally result in larger size estimation
        assertTrue(depth5 >= depth2);
        assertTrue(depth10 >= depth5);
    }

    // ==================== Sample Size Tests ====================

    @Test
    void testEstimateWithCustomSampleSize() {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            list.add("element" + i);
        }

        long sample1 = Estimator.estimate(list, 1, 8);
        long sample5 = Estimator.estimate(list, 5, 8);
        long sample10 = Estimator.estimate(list, 10, 8);

        // All should be positive
        assertTrue(sample1 > 0);
        assertTrue(sample5 > 0);
        assertTrue(sample10 > 0);
    }

    // ==================== Annotation Tests ====================

    @Test
    void testIgnoreMemoryTrackOnClass() {
        IgnoredClass obj = new IgnoredClass();
        obj.data = new int[1000];
        assertEquals(0, Estimator.estimate(obj));
    }

    @Test
    void testIgnoreMemoryTrackOnField() {
        ObjectWithIgnoredField obj = new ObjectWithIgnoredField();
        obj.tracked = "tracked";
        obj.ignored = new int[1000];

        long size = Estimator.estimate(obj);
        // Size should not include the ignored array
        long arraySize = Estimator.ARRAY_HEADER_SIZE + 1000 * 4;
        assertTrue(size < arraySize);
    }

    @Test
    void testShallowMemoryAnnotation() {
        ShallowOnlyClass obj = new ShallowOnlyClass();
        obj.data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            obj.data.add("item" + i);
        }

        long size = Estimator.estimate(obj);
        long shallowSize = Estimator.shallow(obj);
        // Should only return shallow size
        assertEquals(shallowSize, size);
    }

    // ==================== Custom Estimator Tests ====================

    @Test
    void testRegisterAndUseCustomEstimator() {
        Estimator.registerCustomEstimator(CustomEstimatedClass.class, obj -> 12345L);

        CustomEstimatedClass obj = new CustomEstimatedClass();
        assertEquals(12345L, Estimator.estimate(obj));
    }

    @Test
    void testGetCustomEstimator() {
        CustomEstimator estimator = obj -> 100L;
        Estimator.registerCustomEstimator(CustomEstimatedClass.class, estimator);

        assertEquals(estimator, Estimator.getCustomEstimator(CustomEstimatedClass.class));
        assertNull(Estimator.getCustomEstimator(SimpleObject.class));
    }

    @Test
    void testStringEstimator() {
        Estimator.registerCustomEstimator(String.class, new StringEstimator());

        String str = "Hello, World!";
        long size = Estimator.estimate(str);

        // Should be shallow size + array header + string length
        long expected = Estimator.shallow(str) + Estimator.ARRAY_HEADER_SIZE + str.length();
        assertEquals(expected, size);
    }

    // ==================== Shallow Memory Class Registration Tests ====================

    @Test
    void testRegisterShallowMemoryClass() {
        assertFalse(Estimator.isShallowMemoryClass(SimpleObject.class));

        Estimator.registerShallowMemoryClass(SimpleObject.class);

        assertTrue(Estimator.isShallowMemoryClass(SimpleObject.class));
    }

    @Test
    void testIsShallowMemoryClassWithAnnotation() {
        assertTrue(Estimator.isShallowMemoryClass(ShallowOnlyClass.class));
    }

    @Test
    void testCollectionWithShallowMemoryElements() {
        Estimator.registerShallowMemoryClass(SimpleObject.class);

        List<SimpleObject> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            list.add(new SimpleObject(i, "name" + i));
        }

        long size = Estimator.estimate(list);
        // Size should be list shallow size + element shallow size * count
        long listShallow = Estimator.shallow(list);
        long elementShallow = Estimator.shallow(SimpleObject.class);
        long expected = listShallow + elementShallow * 100;
        assertEquals(expected, size);
    }

    // ==================== Shallow Size Tests ====================

    @Test
    void testShallowSizeObject() {
        SimpleObject obj = new SimpleObject(1, "test");
        long shallow = Estimator.shallow(obj);
        assertTrue(shallow > 0);
    }

    @Test
    void testShallowSizeClass() {
        long shallow = Estimator.shallow(SimpleObject.class);
        assertTrue(shallow > 0);
    }

    @Test
    void testShallowSizeArrayThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            Estimator.shallow(int[].class);
        });
    }

    @Test
    void testShallowSizeArrayInstance() {
        int[] array = new int[100];
        long shallow = Estimator.shallow(array);
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 100 * 4, shallow);
    }

    // ==================== Cache Tests ====================

    @Test
    void testShallowSizeIsConsistent() {
        SimpleObject obj = new SimpleObject(1, "test");

        // ClassValue-based cache should return consistent results
        long size1 = Estimator.shallow(obj);
        long size2 = Estimator.shallow(obj);
        assertEquals(size1, size2);
        assertTrue(size1 > 0);
    }

    @Test
    void testPrimitiveArrayShallowSizeReturnsCorrectValues() {
        int[] intArray = new int[100];
        long[] longArray = new long[50];
        byte[] byteArray = new byte[200];

        // Calculate shallow size for primitive arrays
        long intSize = Estimator.shallow(intArray);
        long longSize = Estimator.shallow(longArray);
        long byteSize = Estimator.shallow(byteArray);

        // Verify correct sizes are returned
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 100 * 4, intSize);
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 50 * 8, longSize);
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 200, byteSize);
    }

    @Test
    void testNestedFieldsAreCached() {
        NestedObject obj = new NestedObject();
        obj.inner = new SimpleObject(1, "test");

        Estimator.estimate(obj);

        // ClassValue-based cache returns computed value on get()
        Field[] fields = Estimator.CLASS_NESTED_FIELDS.get(NestedObject.class);
        assertNotNull(fields);
        assertTrue(fields.length > 0);
    }

    @Test
    void testNestedFieldsContainsCorrectFieldNames() {
        // Test SimpleObject: should only contain "name" (id is primitive, skipped)
        Field[] simpleFields = Estimator.CLASS_NESTED_FIELDS.get(SimpleObject.class);
        assertNotNull(simpleFields);
        assertEquals(1, simpleFields.length);
        assertEquals("name", simpleFields[0].getName());
    }

    @Test
    void testNestedFieldsForNestedObject() {
        // Test NestedObject: should contain "inner"
        Field[] nestedFields = Estimator.CLASS_NESTED_FIELDS.get(NestedObject.class);
        assertNotNull(nestedFields);
        assertEquals(1, nestedFields.length);
        assertEquals("inner", nestedFields[0].getName());
    }

    @Test
    void testNestedFieldsForObjectWithCollection() {
        // Test ObjectWithCollection: should contain "items"
        Field[] fields = Estimator.CLASS_NESTED_FIELDS.get(ObjectWithCollection.class);
        assertNotNull(fields);
        assertEquals(1, fields.length);
        assertEquals("items", fields[0].getName());
    }

    @Test
    void testNestedFieldsExcludesIgnoredFields() {
        // Test ObjectWithIgnoredField: should only contain "tracked", not "ignored"
        Field[] fields = Estimator.CLASS_NESTED_FIELDS.get(ObjectWithIgnoredField.class);
        assertNotNull(fields);
        assertEquals(1, fields.length);
        assertEquals("tracked", fields[0].getName());
    }

    @Test
    void testNestedFieldsExcludesEnumFields() {
        // Test ObjectWithEnum: enum field should be excluded
        Field[] fields = Estimator.CLASS_NESTED_FIELDS.get(ObjectWithEnum.class);
        assertNotNull(fields);
        // Enum field should be excluded, so the array should be empty
        assertEquals(0, fields.length);
    }

    @Test
    void testNestedFieldsExcludesStaticFields() {
        // Test ObjectWithStaticField: static field should be excluded
        Field[] fields = Estimator.CLASS_NESTED_FIELDS.get(ObjectWithStaticField.class);
        assertNotNull(fields);
        assertEquals(1, fields.length);
        assertEquals("instanceData", fields[0].getName());
    }

    @Test
    void testNestedFieldsForComplexObject() {
        // Test a complex object with multiple field types
        Field[] fields = Estimator.CLASS_NESTED_FIELDS.get(ComplexObject.class);
        assertNotNull(fields);
        // Should contain: stringField, listField, mapField, nestedField
        // Should NOT contain: intField (primitive), enumField (enum)
        assertEquals(4, fields.length);

        Set<String> fieldNames = java.util.Arrays.stream(fields)
                .map(Field::getName)
                .collect(java.util.stream.Collectors.toSet());
        assertTrue(fieldNames.contains("stringField"));
        assertTrue(fieldNames.contains("listField"));
        assertTrue(fieldNames.contains("mapField"));
        assertTrue(fieldNames.contains("nestedField"));
        assertFalse(fieldNames.contains("intField"));
        assertFalse(fieldNames.contains("enumField"));
    }

    @Test
    void testNestedFieldsForInheritedClass() {
        // Test SubClass: fields are cached per class in hierarchy
        SubClass obj = new SubClass();
        obj.baseField = "base";
        obj.subField = "sub";
        Estimator.estimate(obj);

        // SubClass's own fields
        Field[] subFields = Estimator.CLASS_NESTED_FIELDS.get(SubClass.class);
        assertNotNull(subFields);
        assertEquals(1, subFields.length);
        assertEquals("subField", subFields[0].getName());

        // BaseClass's fields (cached separately)
        Field[] baseFields = Estimator.CLASS_NESTED_FIELDS.get(BaseClass.class);
        assertNotNull(baseFields);
        assertEquals(1, baseFields.length);
        assertEquals("baseField", baseFields[0].getName());
    }

    // ==================== Enum Field Tests ====================

    @Test
    void testEnumFieldsAreSkipped() {
        ObjectWithEnum obj = new ObjectWithEnum();
        obj.status = Status.ACTIVE;

        long size = Estimator.estimate(obj);
        // Enum fields should be skipped (they're singletons)
        // Size should be shallow size of the object only
        long shallow = Estimator.shallow(obj);
        assertEquals(shallow, size);
    }

    // ==================== Static Field Tests ====================

    @Test
    void testStaticFieldsAreSkipped() {
        ObjectWithStaticField.staticData = new int[1000];
        ObjectWithStaticField obj = new ObjectWithStaticField();
        obj.instanceData = "test";

        long size = Estimator.estimate(obj);
        // Static field should not be included
        long staticArraySize = Estimator.ARRAY_HEADER_SIZE + 1000 * 4;
        assertTrue(size < staticArraySize);
    }

    // ==================== Edge Case Tests ====================

    @Test
    void testEstimateCollectionWithAllNulls() {
        List<String> list = new ArrayList<>();
        list.add(null);
        list.add(null);
        list.add(null);

        long size = Estimator.estimate(list);
        assertEquals(Estimator.shallow(list), size);
    }

    @Test
    void testEstimateArrayWithAllNulls() {
        String[] array = new String[10];
        // All elements are null

        long size = Estimator.estimate(array);
        // Should be header + references only
        assertEquals(Estimator.ARRAY_HEADER_SIZE + 10 * 4, size);
    }

    @Test
    void testEstimateSelfReferencingObject() {
        // This tests that depth limit prevents infinite recursion
        SelfReferencingClass obj = new SelfReferencingClass();
        obj.self = obj;

        // Should not hang or throw StackOverflowError
        long size = Estimator.estimate(obj);
        assertTrue(size > 0);
    }

    @Test
    void testEstimateLargeCollection() {
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {
            list.add(i);
        }

        long size = Estimator.estimate(list);
        assertTrue(size > 0);
    }

    // ==================== Inheritance Tests ====================

    @Test
    void testEstimateSubclass() {
        SubClass obj = new SubClass();
        obj.baseField = "base";
        obj.subField = "sub";

        long size = Estimator.estimate(obj);
        // Should include fields from both base and subclass
        assertTrue(size > Estimator.shallow(obj));
    }

    @Test
    void testIgnoreMemoryTrackOnSuperclass() {
        SubClassOfIgnored obj = new SubClassOfIgnored();
        obj.subData = "data";

        // When any class in the hierarchy has @IgnoreMemoryTrack, the entire object returns 0
        // This prevents partial memory tracking which could be misleading
        long size = Estimator.estimate(obj);
        assertEquals(0, size);
    }

    @Test
    void testIgnoreMemoryTrackOnMiddleClass() {
        // Test class hierarchy: SubSubClass -> IgnoredMiddleClass (@IgnoreMemoryTrack) -> BaseClass
        SubSubClassOfIgnoredMiddle obj = new SubSubClassOfIgnoredMiddle();
        obj.baseField = "base";
        obj.subSubField = "subsub";

        // Should return 0 because IgnoredMiddleClass in the hierarchy has @IgnoreMemoryTrack
        long size = Estimator.estimate(obj);
        assertEquals(0, size);
    }

    @Test
    void testNoIgnoreMemoryTrackInHierarchy() {
        // Test class hierarchy without @IgnoreMemoryTrack: SubClass -> BaseClass
        SubClass obj = new SubClass();
        obj.baseField = "base";
        obj.subField = "sub";

        // Should estimate normally since no class in hierarchy has @IgnoreMemoryTrack
        long size = Estimator.estimate(obj);
        assertTrue(size > 0);
        assertTrue(size > Estimator.shallow(obj));
    }

    // ==================== Helper Classes ====================

    static class SimpleObject {
        int id;
        String name;

        SimpleObject(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    static class NestedObject {
        SimpleObject inner;
    }

    static class ObjectWithCollection {
        List<String> items = new ArrayList<>();
    }

    static class DeeplyNestedObject {
        int level;
        DeeplyNestedObject next;
    }

    private DeeplyNestedObject createDeeplyNestedObject(int depth) {
        DeeplyNestedObject root = new DeeplyNestedObject();
        root.level = 0;
        DeeplyNestedObject current = root;
        for (int i = 1; i < depth; i++) {
            current.next = new DeeplyNestedObject();
            current.next.level = i;
            current = current.next;
        }
        return root;
    }

    @IgnoreMemoryTrack
    static class IgnoredClass {
        int[] data;
    }

    static class ObjectWithIgnoredField {
        String tracked;
        @IgnoreMemoryTrack
        int[] ignored;
    }

    @ShallowMemory
    static class ShallowOnlyClass {
        List<String> data;
    }

    static class CustomEstimatedClass {
        int data;
    }

    static class PrimitiveOnlyClass {
        int intValue;
        long longValue;
        double doubleValue;
        boolean boolValue;
    }

    enum Status {
        ACTIVE, INACTIVE
    }

    static class ObjectWithEnum {
        Status status;
    }

    static class ObjectWithStaticField {
        static int[] staticData;
        String instanceData;
    }

    static class SelfReferencingClass {
        SelfReferencingClass self;
    }

    static class BaseClass {
        String baseField;
    }

    static class SubClass extends BaseClass {
        String subField;
    }

    @IgnoreMemoryTrack
    static class IgnoredBaseClass {
        String ignoredData;
    }

    static class SubClassOfIgnored extends IgnoredBaseClass {
        String subData;
    }

    @IgnoreMemoryTrack
    static class IgnoredMiddleClass extends BaseClass {
        String middleData;
    }

    static class SubSubClassOfIgnoredMiddle extends IgnoredMiddleClass {
        String subSubField;
    }

    static class ComplexObject {
        int intField;
        String stringField;
        List<String> listField;
        Map<String, Integer> mapField;
        SimpleObject nestedField;
        Status enumField;
    }

    // ==================== Hidden Class Tests ====================

    @Test
    void testShallowSizeLambdaExpression() {
        // Lambda expressions generate hidden classes (or synthetic classes with $$Lambda$ in name)
        Supplier<String> lambda = () -> "hello";

        long size = Estimator.shallow(lambda);
        // Hidden classes should return HIDDEN_CLASS_SHALLOW_SIZE (16)
        assertEquals(16, size);
    }

    @Test
    void testShallowSizeLambdaWithCapture() {
        // Lambda that captures a variable
        String captured = "captured value";
        Function<String, String> lambda = (s) -> captured + s;

        long size = Estimator.shallow(lambda);
        // Hidden classes should return HIDDEN_CLASS_SHALLOW_SIZE (16)
        assertEquals(16, size);
    }

    @Test
    void testEstimateLambdaExpression() {
        Supplier<Integer> lambda = () -> 42;

        long size = Estimator.estimate(lambda);
        assertEquals(16, size);
    }

    @Test
    void testEstimateObjectContainingLambda() {
        ObjectWithLambda obj = new ObjectWithLambda();
        obj.name = "test";
        obj.callback = () -> "callback result";

        long size = Estimator.estimate(obj);
        // Size should include object shallow size + string size + lambda size (16)
        assertTrue(size > Estimator.shallow(obj));
    }

    @Test
    void testEstimateCollectionOfLambdas() {
        List<Supplier<String>> lambdas = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final int index = i;
            lambdas.add(() -> "value" + index);
        }

        long size = Estimator.estimate(lambdas);
        // Size should be positive and include list overhead + lambda sizes
        assertTrue(size > Estimator.shallow(lambdas));
    }

    @Test
    void testShallowSizeMethodReference() {
        // Method references also generate hidden/synthetic classes
        Function<String, Integer> methodRef = String::length;

        long size = Estimator.shallow(methodRef);
        // Method references should also return HIDDEN_CLASS_SHALLOW_SIZE (16)
        assertEquals(16, size);
    }

    @Test
    void testLambdaClassIsCachedCorrectly() {
        Supplier<String> lambda = () -> "test";

        // ClassValue-based cache should return consistent results
        long size1 = Estimator.shallow(lambda);
        long size2 = Estimator.shallow(lambda);
        assertEquals(size1, size2);
        assertEquals(16, size1);
    }

    @Test
    void testMultipleDifferentLambdasHaveSameShallowSize() {
        Supplier<String> lambda1 = () -> "first";
        Supplier<String> lambda2 = () -> "second";
        Function<Integer, String> lambda3 = (i) -> "number: " + i;

        long size1 = Estimator.shallow(lambda1);
        long size2 = Estimator.shallow(lambda2);
        long size3 = Estimator.shallow(lambda3);

        // All hidden classes should have the same shallow size
        assertEquals(16, size1);
        assertEquals(16, size2);
        assertEquals(16, size3);
    }

    // Helper class for hidden class tests
    static class ObjectWithLambda {
        String name;
        Supplier<String> callback;
    }

    // ==================== Sampled Deduplication Tests ====================

    @Test
    void testSameObjectReferencedMultipleTimesCountedOnce() {
        // Create a shared object that will be referenced multiple times
        SharedObject shared = new SharedObject();
        shared.value = 42;

        // Create a counting estimator to track how many times estimate is called
        AtomicInteger estimateCount = new AtomicInteger(0);
        Estimator.registerCustomEstimator(SharedObject.class, obj -> {
            estimateCount.incrementAndGet();
            return 100L;
        });

        // Create an object that references the same SharedObject multiple times
        MultiReferenceHolder holder = new MultiReferenceHolder();
        holder.ref1 = shared;
        holder.ref2 = shared;
        holder.ref3 = shared;

        Estimator.estimate(holder);

        // The shared object should only be counted once due to sampled deduplication
        assertEquals(1, estimateCount.get());
    }

    @Test
    void testSameObjectInCollectionCountedOnce() {
        SharedObject shared = new SharedObject();
        shared.value = 100;

        AtomicInteger estimateCount = new AtomicInteger(0);
        Estimator.registerCustomEstimator(SharedObject.class, obj -> {
            estimateCount.incrementAndGet();
            return 50L;
        });

        // Add the same object to a list multiple times
        List<SharedObject> list = new ArrayList<>();
        list.add(shared);
        list.add(shared);
        list.add(shared);
        list.add(shared);
        list.add(shared);

        Estimator.estimate(list);

        // The shared object should only be counted once
        assertEquals(1, estimateCount.get());
    }

    @Test
    void testSameObjectInArrayCountedOnce() {
        SharedObject shared = new SharedObject();
        shared.value = 200;

        AtomicInteger estimateCount = new AtomicInteger(0);
        Estimator.registerCustomEstimator(SharedObject.class, obj -> {
            estimateCount.incrementAndGet();
            return 75L;
        });

        // Create an array with the same object multiple times
        SharedObject[] array = new SharedObject[] {shared, shared, shared, shared, shared};

        Estimator.estimate(array);

        // The shared object should only be counted once
        assertEquals(1, estimateCount.get());
    }

    @Test
    void testSameObjectInMapKeyAndValueCountedOnce() {
        SharedObject shared = new SharedObject();
        shared.value = 300;

        AtomicInteger estimateCount = new AtomicInteger(0);
        Estimator.registerCustomEstimator(SharedObject.class, obj -> {
            estimateCount.incrementAndGet();
            return 60L;
        });

        // Use the same object as both key and value
        Map<SharedObject, SharedObject> map = new HashMap<>();
        map.put(shared, shared);

        Estimator.estimate(map);

        // The shared object should only be counted once
        assertEquals(1, estimateCount.get());
    }

    @Test
    void testSameObjectInNestedStructureCountedOnce() {
        SharedObject shared = new SharedObject();
        shared.value = 400;

        AtomicInteger estimateCount = new AtomicInteger(0);
        Estimator.registerCustomEstimator(SharedObject.class, obj -> {
            estimateCount.incrementAndGet();
            return 80L;
        });

        // Create a nested structure where the same object appears at different levels
        NestedReferenceHolder nested = new NestedReferenceHolder();
        nested.direct = shared;
        nested.holder = new MultiReferenceHolder();
        nested.holder.ref1 = shared;
        nested.holder.ref2 = shared;
        nested.list = new ArrayList<>();
        nested.list.add(shared);
        nested.list.add(shared);

        Estimator.estimate(nested);

        // The shared object should only be counted once despite appearing in multiple places
        assertEquals(1, estimateCount.get());
    }

    @Test
    void testDifferentObjectsCountedSeparately() {
        AtomicInteger estimateCount = new AtomicInteger(0);
        Estimator.registerCustomEstimator(SharedObject.class, obj -> {
            estimateCount.incrementAndGet();
            return 100L;
        });

        // Create different objects
        SharedObject obj1 = new SharedObject();
        obj1.value = 1;
        SharedObject obj2 = new SharedObject();
        obj2.value = 2;
        SharedObject obj3 = new SharedObject();
        obj3.value = 3;

        MultiReferenceHolder holder = new MultiReferenceHolder();
        holder.ref1 = obj1;
        holder.ref2 = obj2;
        holder.ref3 = obj3;

        Estimator.estimate(holder);

        // Each different object should be counted once
        assertEquals(3, estimateCount.get());
    }

    @Test
    void testCircularReferenceCountedOnce() {
        AtomicInteger estimateCount = new AtomicInteger(0);
        Estimator.registerCustomEstimator(CircularRefClass.class, obj -> {
            estimateCount.incrementAndGet();
            return 120L;
        });

        // Create circular reference
        CircularRefClass obj = new CircularRefClass();
        obj.self = obj;

        Estimator.estimate(obj);

        // The object should only be counted once even with circular reference
        assertEquals(1, estimateCount.get());
    }

    @Test
    void testMutualReferenceDoesNotCauseInfiniteLoop() {
        // Create mutual reference: A -> B -> A
        MutualRefA a = new MutualRefA();
        MutualRefB b = new MutualRefB();
        a.refB = b;
        b.refA = a;

        // Should not hang or throw StackOverflowError due to sampled deduplication
        long size = Estimator.estimate(a);
        assertTrue(size > 0);
    }

    @Test
    void testSharedObjectInMutualReferenceCountedOnce() {
        // Use a shared object that both A and B reference
        SharedObject shared = new SharedObject();
        shared.value = 999;

        AtomicInteger estimateCount = new AtomicInteger(0);
        Estimator.registerCustomEstimator(SharedObject.class, obj -> {
            estimateCount.incrementAndGet();
            return 100L;
        });

        // Create mutual reference with shared object
        MutualRefWithShared a = new MutualRefWithShared();
        MutualRefWithShared b = new MutualRefWithShared();
        a.other = b;
        a.shared = shared;
        b.other = a;
        b.shared = shared;  // Same shared object

        Estimator.estimate(a);

        // The shared object should only be counted once
        assertEquals(1, estimateCount.get());
    }

    // Helper classes for sampled deduplication tests
    static class SharedObject {
        int value;
    }

    static class MultiReferenceHolder {
        SharedObject ref1;
        SharedObject ref2;
        SharedObject ref3;
    }

    static class NestedReferenceHolder {
        SharedObject direct;
        MultiReferenceHolder holder;
        List<SharedObject> list;
    }

    static class CircularRefClass {
        CircularRefClass self;
    }

    static class MutualRefA {
        MutualRefB refB;
    }

    static class MutualRefB {
        MutualRefA refA;
    }

    static class MutualRefWithShared {
        MutualRefWithShared other;
        SharedObject shared;
    }

    // ==================== Ignore Classes Tests ====================

    @Test
    void testEstimateWithIgnoreClasses() {
        // Create an object that contains a SharedData instance
        ObjectWithSharedData obj = new ObjectWithSharedData();
        obj.name = "test";
        obj.sharedData = new SharedData();
        obj.sharedData.largeArray = new int[1000];

        // Estimate without ignoring - should include SharedData size
        long sizeWithSharedData = Estimator.estimate(obj);

        // Estimate with ignoring SharedData class
        Set<Class<?>> ignoreClasses = Set.of(SharedData.class);
        long sizeWithoutSharedData = Estimator.estimate(obj, ignoreClasses);

        // Size without SharedData should be smaller
        assertTrue(sizeWithoutSharedData < sizeWithSharedData);

        // The difference should be approximately the size of SharedData
        long sharedDataSize = Estimator.estimate(obj.sharedData);
        assertTrue(sharedDataSize > 0);
    }

    @Test
    void testEstimateWithIgnoreClassesInNestedObject() {
        // Create nested structure with shared data at different levels
        NestedWithSharedData nested = new NestedWithSharedData();
        nested.name = "outer";
        nested.sharedData = new SharedData();
        nested.sharedData.largeArray = new int[500];
        nested.inner = new ObjectWithSharedData();
        nested.inner.name = "inner";
        nested.inner.sharedData = new SharedData();
        nested.inner.sharedData.largeArray = new int[500];

        // Estimate without ignoring
        long sizeWithSharedData = Estimator.estimate(nested);

        // Estimate with ignoring SharedData class
        Set<Class<?>> ignoreClasses = Set.of(SharedData.class);
        long sizeWithoutSharedData = Estimator.estimate(nested, ignoreClasses);

        // Size without SharedData should be smaller
        assertTrue(sizeWithoutSharedData < sizeWithSharedData);
    }

    @Test
    void testEstimateWithIgnoreClassesInCollection() {
        // Create a list containing objects with SharedData
        List<ObjectWithSharedData> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ObjectWithSharedData item = new ObjectWithSharedData();
            item.name = "item" + i;
            item.sharedData = new SharedData();
            item.sharedData.largeArray = new int[100];
            list.add(item);
        }

        // Estimate without ignoring
        long sizeWithSharedData = Estimator.estimate(list);

        // Estimate with ignoring SharedData class
        Set<Class<?>> ignoreClasses = Set.of(SharedData.class);
        long sizeWithoutSharedData = Estimator.estimate(list, ignoreClasses);

        // Size without SharedData should be smaller
        assertTrue(sizeWithoutSharedData < sizeWithSharedData);
    }

    @Test
    void testEstimateWithIgnoreClassesInArray() {
        // Create an array containing SharedData objects
        SharedData[] array = new SharedData[5];
        for (int i = 0; i < 5; i++) {
            array[i] = new SharedData();
            array[i].largeArray = new int[200];
        }

        // Estimate without ignoring
        long sizeWithSharedData = Estimator.estimate(array);

        // Estimate with ignoring SharedData class
        Set<Class<?>> ignoreClasses = Set.of(SharedData.class);
        long sizeWithoutSharedData = Estimator.estimate(array, ignoreClasses);

        // Size without SharedData should be only the array shell (header + references)
        long arrayShellSize = Estimator.ARRAY_HEADER_SIZE + 5 * 4; // 5 references
        assertEquals(arrayShellSize, sizeWithoutSharedData);
        assertTrue(sizeWithoutSharedData < sizeWithSharedData);
    }

    @Test
    void testEstimateWithEmptyIgnoreClasses() {
        ObjectWithSharedData obj = new ObjectWithSharedData();
        obj.name = "test";
        obj.sharedData = new SharedData();
        obj.sharedData.largeArray = new int[100];

        // Estimate with empty ignore set should be same as normal estimate
        long normalSize = Estimator.estimate(obj);
        long sizeWithEmptyIgnore = Estimator.estimate(obj, Set.of());

        assertEquals(normalSize, sizeWithEmptyIgnore);
    }

    @Test
    void testEstimateWithNullIgnoreClasses() {
        ObjectWithSharedData obj = new ObjectWithSharedData();
        obj.name = "test";
        obj.sharedData = new SharedData();
        obj.sharedData.largeArray = new int[100];

        // Estimate with null ignore set should be same as normal estimate
        long normalSize = Estimator.estimate(obj);
        long sizeWithNullIgnore = Estimator.estimate(obj, (Set<Class<?>>) null);

        assertEquals(normalSize, sizeWithNullIgnore);
    }

    @Test
    void testEstimateWithIgnoreClassesAndMaxDepth() {
        NestedWithSharedData nested = new NestedWithSharedData();
        nested.name = "outer";
        nested.sharedData = new SharedData();
        nested.sharedData.largeArray = new int[500];
        nested.inner = new ObjectWithSharedData();
        nested.inner.name = "inner";
        nested.inner.sharedData = new SharedData();
        nested.inner.sharedData.largeArray = new int[500];

        Set<Class<?>> ignoreClasses = Set.of(SharedData.class);

        // Test with different max depths
        long depth2 = Estimator.estimate(nested, 5, 2, ignoreClasses);
        long depth8 = Estimator.estimate(nested, 5, 8, ignoreClasses);

        // Both should be smaller than without ignore
        long normalDepth8 = Estimator.estimate(nested, 5, 8);
        assertTrue(depth8 < normalDepth8);

        // Deeper traversal should generally give larger size
        assertTrue(depth8 >= depth2);
    }

    @Test
    void testEstimateWithIgnoreClassesAndSampleSize() {
        // Create a large list with SharedData
        List<ObjectWithSharedData> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            ObjectWithSharedData item = new ObjectWithSharedData();
            item.name = "item" + i;
            item.sharedData = new SharedData();
            item.sharedData.largeArray = new int[50];
            list.add(item);
        }

        Set<Class<?>> ignoreClasses = Set.of(SharedData.class);

        // Test with all parameters
        long size = Estimator.estimate(list, 5, 8, ignoreClasses);

        // Should be smaller than without ignore
        long normalSize = Estimator.estimate(list, 5, 8);
        assertTrue(size < normalSize);
    }

    @Test
    void testEstimateWithMultipleIgnoreClasses() {
        // Create object with multiple types to ignore
        ObjectWithMultipleShared obj = new ObjectWithMultipleShared();
        obj.name = "test";
        obj.sharedData1 = new SharedData();
        obj.sharedData1.largeArray = new int[100];
        obj.sharedData2 = new AnotherSharedData();
        obj.sharedData2.largeList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            obj.sharedData2.largeList.add("item" + i);
        }

        // Estimate without ignoring
        long normalSize = Estimator.estimate(obj);

        // Ignore only SharedData
        long ignoreOne = Estimator.estimate(obj, Set.of(SharedData.class));

        // Ignore both classes
        long ignoreBoth = Estimator.estimate(obj, Set.of(SharedData.class, AnotherSharedData.class));

        // Ignoring more classes should result in smaller size
        assertTrue(ignoreOne < normalSize);
        assertTrue(ignoreBoth < ignoreOne);
    }

    @Test
    void testIgnoreClassesWithSharedReference() {
        // This is the main use case: multiple objects reference the same shared object
        SharedData shared = new SharedData();
        shared.largeArray = new int[1000];

        ObjectWithSharedData obj1 = new ObjectWithSharedData();
        obj1.name = "obj1";
        obj1.sharedData = shared;

        ObjectWithSharedData obj2 = new ObjectWithSharedData();
        obj2.name = "obj2";
        obj2.sharedData = shared; // Same reference

        // Put them in a container
        List<ObjectWithSharedData> container = new ArrayList<>();
        container.add(obj1);
        container.add(obj2);

        // Estimate with ignoring SharedData - useful when we want to calculate
        // the size without the shared data that's counted elsewhere
        Set<Class<?>> ignoreClasses = Set.of(SharedData.class);
        long sizeWithoutShared = Estimator.estimate(container, ignoreClasses);

        // Verify SharedData is not counted
        long normalSize = Estimator.estimate(container);
        assertTrue(sizeWithoutShared < normalSize);
    }

    // Helper classes for ignore classes tests
    static class SharedData {
        int[] largeArray;
    }

    static class AnotherSharedData {
        List<String> largeList;
    }

    static class ObjectWithSharedData {
        String name;
        SharedData sharedData;
    }

    static class NestedWithSharedData {
        String name;
        SharedData sharedData;
        ObjectWithSharedData inner;
    }

    static class ObjectWithMultipleShared {
        String name;
        SharedData sharedData1;
        AnotherSharedData sharedData2;
    }
}
