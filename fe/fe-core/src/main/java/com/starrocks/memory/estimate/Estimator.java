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

import org.jetbrains.annotations.NotNull;
import org.openjdk.jol.info.ClassLayout;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InaccessibleObjectException;
import java.lang.reflect.Modifier;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.RandomAccess;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Utility class for estimating the memory size of Java objects.
 * <p>
 * Features:
 * - Sampling-based estimation for arrays and collections
 * - Recursive traversal of object fields with depth limit
 * - Support for custom estimators for specific types
 * - Caching of class shallow sizes for performance
 * - Dynamic sample size calculation based on max depth to limit total computations
 * <p>
 * Important: This estimator will access all fields of an object,
 * you should make sure the object is immutable or is protected by lock when calling estimate
 */
public class Estimator {

    // Default maximum recursion depth
    private static final int DEFAULT_MAX_DEPTH = 20;

    private static final int DEFAULT_SAMPLE_SIZE = 5;

    // Array header size: object header (12 bytes) + length field (4 bytes) = 16 bytes on 64-bit JVM
    public static final int ARRAY_HEADER_SIZE = 16;

    private static final long HIDDEN_CLASS_SHALLOW_SIZE = 16;

    // Reference size on 64-bit JVM (with compressed oops, typically 4 bytes; without, 8 bytes)
    private static final int REFERENCE_SIZE = 4;

    // Per-entry structural overhead of common container internals. The shallow-size + sampled-element
    // estimate does not otherwise count the node/entry wrappers or the backing table, which on wide
    // maps dominate the footprint. Sizes assume compressed oops (FE heap < 32G):
    //   HashMap.Node / ConcurrentHashMap.Node : header(12)+hash(4)+key+value+next(3*4)  -> 32
    //   LinkedHashMap.Entry                   : Node + before + after(2*4)               -> 40
    //   TreeMap.Entry / TreeSet               : header+key+value+left+right+parent+color -> 40
    //   LinkedList.Node                       : header(12)+item+prev+next(3*4)           -> 24
    private static final int HASH_NODE_SIZE = 32;
    private static final int LINKED_HASH_ENTRY_SIZE = 40;
    private static final int TREE_ENTRY_SIZE = 40;
    private static final int LINKED_LIST_NODE_SIZE = 24;
    //   Guava ImmutableMapEntry                : header(12)+key+value(+nextInBucket)      -> 24
    private static final int GUAVA_ENTRY_SIZE = 24;
    private static final double HASH_LOAD_FACTOR = 0.75;

    // Shallow sizes of classes (bound to Class lifecycle, avoids ClassLoader leaks)
    private static final ClassValue<Long> SHALLOW_SIZE = new ClassValue<>() {
        @Override
        protected Long computeValue(@NotNull Class<?> clazz) {
            if (isHiddenClass(clazz)) {
                return HIDDEN_CLASS_SHALLOW_SIZE;
            }
            try {
                return ClassLayout.parseClass(clazz).instanceSize();
            } catch (RuntimeException e) {
                return HIDDEN_CLASS_SHALLOW_SIZE;
            }
        }
    };

    // Registry for custom estimators
    protected static final Map<String, CustomEstimator> CUSTOM_ESTIMATORS = new HashMap<>();

    // Registry for classes that should only calculate shallow memory
    // Store class names instead of Class<?> to avoid pinning Class/ClassLoader.
    protected static final Set<String> SHALLOW_MEMORY_CLASS_NAMES = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // Cache for nested reference fields of classes (bound to Class lifecycle, avoids ClassLoader leaks)
    // Also moves trySetAccessible into computeValue to avoid doing it in hot path.
    protected static final ClassValue<Field[]> CLASS_NESTED_FIELDS = new ClassValue<>() {
        @Override
        protected Field[] computeValue(Class<?> clazz) {
            ArrayList<Field> nested = new ArrayList<>();
            for (Field field : clazz.getDeclaredFields()) {
                // Skip static fields
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }

                // Skip fields annotated with @IgnoreMemoryTrack
                if (field.isAnnotationPresent(IgnoreMemoryTrack.class)) {
                    continue;
                }

                // Skip primitive fields (already included in shallow size)
                if (field.getType().isPrimitive()) {
                    continue;
                }

                // Skip enum fields (enum instances are singletons, already allocated at class loading)
                if (field.getType().isEnum()) {
                    continue;
                }

                // Do accessibility attempt once during caching, not in hot path.
                try {
                    field.trySetAccessible();
                } catch (Throwable ignore) {
                    // ignore; access may still fail later, handled in estimateNestedField
                }

                nested.add(field);
            }
            return nested.toArray(new Field[0]);
        }
    };

    // Primitive type sizes in bytes
    private static final Map<Class<?>, Integer> PRIMITIVE_SIZES = Map.of(
            boolean.class, 1,
            byte.class, 1,
            char.class, 2,
            short.class, 2,
            int.class, 4,
            float.class, 4,
            long.class, 8,
            double.class, 8
    );

    static {
        registerDefaultEstimators();
    }

    /**
     * Register the estimators every caller needs: JDK value types whose internals are not reflectively
     * reachable (java.lang / java.nio are not opened to the FE), so the field walk would charge only their
     * shell. Registered at class load rather than by the memory tracker's first run, so cache weighers that
     * run before it see the same sizes.
     */
    public static void registerDefaultEstimators() {
        registerCustomEstimator(String.class, new StringEstimator());
        registerCustomEstimator(java.nio.ByteBuffer.class, new ByteBufferEstimator());
        registerCustomEstimator("java.nio.HeapByteBuffer", new ByteBufferEstimator());
        // Autoboxing hands out the JDK's cached instances for small values (Integer/Long/Short/Byte in
        // -128..127, Character up to 127, both Booleans). Those are shared singletons, so a reference to one
        // costs nothing; charging each reference its shell over-counts maps keyed by field ids or holding
        // zero counts. A box allocated another way (deserialized, constructed) is its own object and is charged.
        registerCustomEstimator(Boolean.class, obj -> obj == Boolean.TRUE || obj == Boolean.FALSE ? 0L : shallow(obj));
        registerCustomEstimator(Byte.class, obj -> obj == Byte.valueOf((Byte) obj) ? 0L : shallow(obj));
        registerCustomEstimator(Character.class,
                obj -> (Character) obj <= 127 && obj == Character.valueOf((Character) obj) ? 0L : shallow(obj));
        registerCustomEstimator(Short.class,
                obj -> boxCached((Short) obj) && obj == Short.valueOf((Short) obj) ? 0L : shallow(obj));
        registerCustomEstimator(Integer.class,
                obj -> boxCached((Integer) obj) && obj == Integer.valueOf((Integer) obj) ? 0L : shallow(obj));
        registerCustomEstimator(Long.class,
                obj -> boxCached((Long) obj) && obj == Long.valueOf((Long) obj) ? 0L : shallow(obj));
        registerShallowMemoryClass(Float.class);
        registerShallowMemoryClass(Double.class);
    }

    private static boolean boxCached(Number value) {
        long v = value.longValue();
        return v >= -128 && v <= 127;
    }

    // Set to track visited objects and avoid counting the same object twice
    private final Set<Object> visited;

    // Set of classes to ignore during estimation
    private final Set<Class<?>> ignoreClasses;

    public Estimator() {
        this(Collections.emptySet());
    }

    public Estimator(Set<Class<?>> ignoreClasses) {
        this.visited = Collections.newSetFromMap(new IdentityHashMap<>());
        this.ignoreClasses = ignoreClasses != null ? ignoreClasses : Collections.emptySet();
    }

    /**
     * Register a custom estimator for a specific class.
     *
     * @param clazz     the class to register estimator for
     * @param estimator the custom estimator
     */
    public static void registerCustomEstimator(Class<?> clazz, CustomEstimator estimator) {
        CUSTOM_ESTIMATORS.put(clazz.getName(), estimator);
    }

    /**
     * Register a custom estimator for a specific class.
     *
     * @param clazzName     the class name to register estimator for
     * @param estimator the custom estimator
     */
    public static void registerCustomEstimator(String clazzName, CustomEstimator estimator) {
        CUSTOM_ESTIMATORS.put(clazzName, estimator);
    }

    /**
     * Get the custom estimator for a class, if registered.
     *
     * @param clazz the class to look up
     * @return the custom estimator, or null if not registered
     */
    public static CustomEstimator getCustomEstimator(Class<?> clazz) {
        return CUSTOM_ESTIMATORS.get(clazz.getName());
    }

    /**
     * Register a class as shallow memory class.
     * Classes registered here will only calculate shallow memory size.
     *
     * @param clazz the class to register
     */
    public static void registerShallowMemoryClass(Class<?> clazz) {
        if (clazz != null) {
            SHALLOW_MEMORY_CLASS_NAMES.add(clazz.getName());
        }
    }

    /**
     * Check if a class is a shallow memory class (annotated with @ShallowMemory or registered).
     *
     * @param clazz the class to check
     * @return true if the class should only calculate shallow memory
     */
    public static boolean isShallowMemoryClass(Class<?> clazz) {
        return clazz.isAnnotationPresent(ShallowMemory.class) || SHALLOW_MEMORY_CLASS_NAMES.contains(clazz.getName());
    }

    /**
     * Estimate the memory size of an object with default settings.
     *
     * @param obj the object to estimate
     * @return the estimated memory size in bytes
     */
    public static long estimate(Object obj) {
        return new Estimator().estimateInternal(obj, DEFAULT_MAX_DEPTH, DEFAULT_SAMPLE_SIZE);
    }

    /**
     * Estimate the memory size of an object with specified sample size.
     *
     * @param obj        the object to estimate
     * @param sampleSize the number of elements to sample for collections/arrays
     * @return the estimated memory size in bytes
     */
    public static long estimate(Object obj, int sampleSize) {
        return new Estimator().estimateInternal(obj, DEFAULT_MAX_DEPTH, sampleSize);
    }

    /**
     * Estimate the memory size of an object with specified sample size and max depth.
     *
     * @param obj        the object to estimate
     * @param sampleSize the number of elements to sample for collections/arrays
     * @param maxDepth   the maximum recursion depth
     * @return the estimated memory size in bytes
     */
    public static long estimate(Object obj, int sampleSize, int maxDepth) {
        return new Estimator().estimateInternal(obj, maxDepth, sampleSize);
    }

    /**
     * Estimate the memory size of an object with specified ignore classes.
     * Objects of ignored classes will not be counted in the estimation.
     * This is useful when multiple objects reference the same shared object,
     * and you want to exclude that shared object from the calculation.
     *
     * @param obj           the object to estimate
     * @param ignoreClasses set of classes to ignore during estimation
     * @return the estimated memory size in bytes
     */
    public static long estimate(Object obj, Set<Class<?>> ignoreClasses) {
        return new Estimator(ignoreClasses).estimateInternal(obj, DEFAULT_MAX_DEPTH, DEFAULT_SAMPLE_SIZE);
    }

    /**
     * Estimate the memory size of an object with specified sample size and ignore classes.
     *
     * @param obj           the object to estimate
     * @param sampleSize    the number of elements to sample for collections/arrays
     * @param ignoreClasses set of classes to ignore during estimation
     * @return the estimated memory size in bytes
     */
    public static long estimate(Object obj, int sampleSize, Set<Class<?>> ignoreClasses) {
        return new Estimator(ignoreClasses).estimateInternal(obj, DEFAULT_MAX_DEPTH, sampleSize);
    }

    /**
     * Estimate the memory size of an object with all options specified.
     *
     * @param obj           the object to estimate
     * @param sampleSize    the number of elements to sample for collections/arrays
     * @param maxDepth      the maximum recursion depth
     * @param ignoreClasses set of classes to ignore during estimation
     * @return the estimated memory size in bytes
     */
    public static long estimate(Object obj, int sampleSize, int maxDepth, Set<Class<?>> ignoreClasses) {
        return new Estimator(ignoreClasses).estimateInternal(obj, maxDepth, sampleSize);
    }

    /**
     * Estimate several roots in one pass, in the given order, counting an object reachable from more
     * than one of them once. Put a root first to charge it in full before a later root's sampling would
     * reach it (or miss it).
     *
     * @param ignoreClasses set of classes to ignore during estimation
     * @param roots the objects to estimate
     * @return the estimated memory size in bytes
     */
    public static long estimateAll(Set<Class<?>> ignoreClasses, Object... roots) {
        Estimator estimator = new Estimator(ignoreClasses);
        long size = 0;
        for (Object root : roots) {
            size += estimator.estimateInternal(root, DEFAULT_MAX_DEPTH, DEFAULT_SAMPLE_SIZE);
        }
        return size;
    }

    /**
     * Internal estimation method with sampled set to track visited objects.
     */
    private long estimateInternal(Object obj, int maxDepth, int sampleSize) {
        if (obj == null) {
            return 0;
        }

        // Skip if already visited (avoid counting the same object twice)
        if (!visited.add(obj)) {
            return 0;
        }

        Class<?> clazz = obj.getClass();

        // Skip classes in the ignore set
        if (ignoreClasses.contains(clazz)) {
            return 0;
        }

        // Skip classes annotated with @IgnoreMemoryTrack
        if (clazz.isAnnotationPresent(IgnoreMemoryTrack.class)) {
            return 0;
        }

        // Check for custom estimator
        CustomEstimator customEstimator = getCustomEstimator(clazz);
        if (customEstimator != null) {
            return customEstimator.estimate(obj);
        }

        // Check for shallow memory class (annotated or registered)
        if (isShallowMemoryClass(clazz)) {
            return shallow(clazz);
        }

        // If max depth reached, return shallow size
        if (maxDepth <= 0) {
            return shallow(obj);
        }

        // Handle arrays
        if (clazz.isArray()) {
            return estimateArray(obj, maxDepth, sampleSize);
        }

        // Handle collections. Non-JDK implementations (Iceberg SerializableMap, Guava Immutable*)
        // typically wrap their real storage in a field; the element-sampling path would skip that
        // storage's own structure entirely, so walk their fields instead — the wrapped JDK container
        // is then estimated (and charged) when the recursion reaches it. Cache views are excluded
        // (see isCacheView) and stay on the sampling path.
        if (obj instanceof Collection) {
            Collection<?> collection = (Collection<?>) obj;
            if (!isJdkContainer(clazz) && !isCacheView(clazz) && !isGuavaImmutableMapOrSet(clazz)) {
                return estimateObject(obj, maxDepth, sampleSize);
            }
            return estimateCollection(collection, maxDepth, sampleSize) + collectionBackingOverhead(collection);
        }

        // Handle maps. keySet()/values() are views, so their backing is added once here via
        // mapStructureOverhead rather than inside estimateCollection.
        if (obj instanceof Map<?, ?> map) {
            if (!isJdkContainer(clazz) && !isCacheView(clazz) && !isGuavaImmutableMapOrSet(clazz)) {
                return estimateObject(obj, maxDepth, sampleSize);
            }
            return shallow(obj) +
                    mapStructureOverhead(map) +
                    estimateCollection(map.keySet(), maxDepth, sampleSize) +
                    estimateCollection(map.values(), maxDepth, sampleSize);
        }

        // java.util is not opened to the FE, so the field walk cannot read an Optional's value and would
        // charge only its shell.
        if (obj instanceof Optional<?> optional) {
            return shallow(obj) +
                    (optional.isPresent() ? estimateInternal(optional.get(), maxDepth - 1, sampleSize) : 0);
        }

        // Recursively calculate size for reference fields
        return estimateObject(obj, maxDepth, sampleSize);
    }

    /**
     * Calculate the shallow size of an object (instance size without references).
     *
     * @param obj the object
     * @return the shallow size in bytes
     */
    public static long shallow(Object obj) {
        if (obj == null) {
            return 0;
        }

        Class<?> clazz = obj.getClass();

        // for arrays, calculate size without caching,
        // because the shallow size of arrays depends on their length
        if (clazz.isArray()) {
            int length = Array.getLength(obj);
            Class<?> componentType = clazz.getComponentType();
            if (componentType.isPrimitive()) {
                return ARRAY_HEADER_SIZE + (long) length * PRIMITIVE_SIZES.get(componentType);
            } else {
                return ARRAY_HEADER_SIZE + (long) length * REFERENCE_SIZE;
            }
        }

        // Non-array: shallow size depends only on the class, use ClassValue
        return SHALLOW_SIZE.get(clazz);
    }

    public static long shallow(Class<?> clazz) {
        if (clazz.isArray()) {
            throw new IllegalArgumentException("Use shallow(Object obj) for array instances");
        }
        return SHALLOW_SIZE.get(clazz);
    }

    /**
     * Estimate the size of a collection using sampling.
     *
     * @param collection the collection to estimate
     * @param maxDepth   the maximum recursion depth for elements
     * @param sampleSize the number of elements to sample
     * @return the estimated memory size in bytes
     */
    private long estimateCollection(Collection<?> collection, int maxDepth, int sampleSize) {
        if (collection == null || collection.isEmpty()) {
            return collection == null ? 0 : shallow(collection);
        }

        long shallowSize = shallow(collection);

        // Get first non-null element to determine element type
        Object firstElement = null;
        for (Object item : collection) {
            if (item != null) {
                firstElement = item;
                break;
            }
        }

        if (firstElement == null) {
            return shallowSize;
        }

        // Check if element type is a shallow memory class
        Class<?> elementClass = firstElement.getClass();
        if (isShallowMemoryClass(elementClass)) {
            long elementShallowSize = shallow(elementClass);
            return shallowSize + elementShallowSize * collection.size();
        }

        List<Object> samples = getSamples(collection, sampleSize);
        return shallowSize + extrapolate(samples, collection.size(), maxDepth, sampleSize);
    }

    // Scale the sampled elements up to `count`. Two biases are kept out of it:
    //  - Null slots are sampled positions like any other. The share of non-null samples estimates how many
    //    of the `count` slots hold a value; dropping the nulls would extrapolate a half-null array from its
    //    non-null half.
    //  - Objects the elements share (a schema, a location string, a catalog-wide client) are charged to
    //    the first non-null sample only, since the visited set skips them in the others. Averaging that
    //    sample in with the rest would charge the shared graph once per element, so it stands for its own
    //    slot alone and the other non-null samples are scaled to the other estimated non-null slots. With no
    //    other non-null sample it is the only evidence for those slots and is scaled itself.
    private long extrapolate(List<Object> samples, long count, int maxDepth, int sampleSize) {
        int anchor = 0;
        while (anchor < samples.size() && samples.get(anchor) == null) {
            anchor++;
        }
        if (anchor == samples.size()) {
            return 0;
        }
        long anchorSize = estimateInternal(samples.get(anchor), maxDepth - 1, sampleSize);
        long othersSize = 0;
        int nonNull = 1;
        for (int i = anchor + 1; i < samples.size(); i++) {
            Object sample = samples.get(i);
            if (sample != null) {
                nonNull++;
                othersSize += estimateInternal(sample, maxDepth - 1, sampleSize);
            }
        }
        if (samples.size() >= count) {
            return anchorSize + othersSize;
        }
        double nonNullSlots = (double) count * nonNull / samples.size();
        if (nonNull == 1) {
            return (long) (anchorSize * nonNullSlots);
        }
        return anchorSize + (long) ((double) othersSize / (nonNull - 1) * (nonNullSlots - 1));
    }

    // Structural overhead of a Map's internal entry objects plus the backing table, which shallow
    // size + sampled key/value elements do not otherwise count. Only concrete JDK hash/tree maps are
    // charged; immutable/view maps (Map.of, Guava ImmutableMap, singletonMap, ...) store entries
    // inline or in arrays and would be over-counted, so they get no node overhead here.
    private static long mapStructureOverhead(Map<?, ?> map) {
        int size = map.size();
        if (size == 0) {
            return 0;
        }
        if (map instanceof java.util.LinkedHashMap) {   // subclass of HashMap, check first
            return (long) size * LINKED_HASH_ENTRY_SIZE + hashTableBytes(size);
        }
        if (map instanceof HashMap || map instanceof java.util.concurrent.ConcurrentHashMap) {
            return (long) size * HASH_NODE_SIZE + hashTableBytes(size);
        }
        if (map instanceof java.util.TreeMap) {
            return (long) size * TREE_ENTRY_SIZE;       // red-black tree nodes, no backing table
        }
        if (isGuavaImmutableMapOrSet(map.getClass())) {
            // entry objects, the insertion-order entry array and the hash table array
            return (long) size * GUAVA_ENTRY_SIZE + ARRAY_HEADER_SIZE + (long) size * REFERENCE_SIZE +
                    hashTableBytes(size);
        }
        return 0;
    }

    // Structural overhead of a stand-alone collection's internals. Only concrete JDK collections are
    // charged; immutable/view collections (List.of, Guava Immutable*, Arrays.asList, ...) get none.
    // Not applied to Map key/value views.
    private static long collectionBackingOverhead(Collection<?> collection) {
        int size = collection.size();
        if (size == 0) {
            return 0;
        }
        if (collection instanceof java.util.LinkedHashSet) {   // subclass of HashSet, check first
            return (long) size * LINKED_HASH_ENTRY_SIZE + hashTableBytes(size);
        }
        if (collection instanceof HashSet) {
            return (long) size * HASH_NODE_SIZE + hashTableBytes(size);
        }
        if (collection instanceof java.util.TreeSet) {
            return (long) size * TREE_ENTRY_SIZE;
        }
        if (collection instanceof ArrayList) {
            return ARRAY_HEADER_SIZE + (long) size * REFERENCE_SIZE;   // backing Object[]
        }
        if (collection instanceof java.util.LinkedList) {
            return (long) size * LINKED_LIST_NODE_SIZE;
        }
        if (collection instanceof java.util.concurrent.ConcurrentHashMap.KeySetView) {
            // Backed by a ConcurrentHashMap; charge the same node + table overhead as the map itself.
            return (long) size * HASH_NODE_SIZE + hashTableBytes(size);
        }
        if (isGuavaImmutableMapOrSet(collection.getClass())) {
            // the insertion-order element array and the hash table array
            return ARRAY_HEADER_SIZE + (long) size * REFERENCE_SIZE + hashTableBytes(size);
        }
        return 0;
    }

    // JDK containers get the sampling paths (their internals are modeled by the *Overhead helpers,
    // and their fields are not reflectively accessible anyway); anything else is field-walked.
    private static boolean isJdkContainer(Class<?> clazz) {
        return clazz.getName().startsWith("java.util.");
    }

    // Guava's immutable maps and sets (also the copy relocated into Iceberg) reach the same entries from two
    // arrays, in insertion order and in hash order. Field-walking samples different entries from each, and
    // since the visited set only knows the sampled ones, both extrapolations charge the shared graph: an
    // Iceberg TableMetadata, whose snapshotsById indexes the snapshots list, was weighed at 2.4x. Sample them
    // through the Map/Set interface instead, in insertion order -- which also lands on the same elements as
    // a list they index -- and charge their arrays and entries via the *Overhead helpers.
    private static boolean isGuavaImmutableMapOrSet(Class<?> clazz) {
        String name = clazz.getName();
        return name.contains("com.google.common.collect.") && name.contains("Immutable") &&
                (Map.class.isAssignableFrom(clazz) || Set.class.isAssignableFrom(clazz));
    }

    // A cache's keySet()/values()/asMap() view keeps a back-reference to the owning cache, so
    // field-walking it would re-traverse and double-count the whole cache (and hand a size-0 count
    // down to the array sampler for an empty cache). Sample these views instead of field-walking.
    private static boolean isCacheView(Class<?> clazz) {
        String name = clazz.getName();
        return name.startsWith("com.github.benmanes.caffeine.")
                || name.startsWith("com.google.common.cache.");
    }

    // Bytes of a hash table's backing array: capacity (next power of two that keeps the load factor)
    // times a reference, plus the array header. Sized from the entry count, matching maps built via
    // the sizing/copy constructors (the dominant population); put-grown small maps retain a larger
    // 16-slot minimum table that this under-counts by a few dozen bytes.
    private static long hashTableBytes(int size) {
        int capacity = 2;
        while (capacity * HASH_LOAD_FACTOR < size) {
            capacity <<= 1;
        }
        return ARRAY_HEADER_SIZE + (long) capacity * REFERENCE_SIZE;
    }

    /**
     * Estimate the size of an array using sampling.
     *
     * @param array      the array to estimate
     * @param maxDepth   the maximum recursion depth for elements
     * @param sampleSize the number of elements to sample
     * @return the estimated memory size in bytes
     */
    private long estimateArray(Object array, int maxDepth, int sampleSize) {
        int length = Array.getLength(array);
        Class<?> componentType = array.getClass().getComponentType();

        if (componentType.isPrimitive()) {
            return ARRAY_HEADER_SIZE + (long) length * PRIMITIVE_SIZES.get(componentType);
        }

        // For empty object arrays
        if (length == 0) {
            return ARRAY_HEADER_SIZE;
        }

        // For object arrays: header + references + element sizes
        long shallowSize = ARRAY_HEADER_SIZE + (long) REFERENCE_SIZE * length;

        // Check if component type is a shallow memory class
        if (isShallowMemoryClass(componentType)) {
            long elementShallowSize = shallow(componentType);
            return shallowSize + elementShallowSize * length;
        }

        List<Object> samples = getSamplesFromArray(array, length, sampleSize);
        return shallowSize + extrapolate(samples, length, maxDepth, sampleSize);
    }

    /**
     * Estimate the size of a regular object by traversing its fields.
     *
     * @param obj        the object to estimate
     * @param maxDepth   the maximum recursion depth
     * @param sampleSize the number of elements to sample for collections/arrays
     * @return the estimated memory size in bytes
     */
    private long estimateObject(Object obj, int maxDepth, int sampleSize) {
        if (isHiddenClass(obj.getClass())) {
            return HIDDEN_CLASS_SHALLOW_SIZE;
        }

        long size = shallow(obj);

        for (Class<?> c = obj.getClass(); c != null && c != Object.class; c = c.getSuperclass()) {
            if (c.isAnnotationPresent(IgnoreMemoryTrack.class)) {
                return 0;
            }
            if (c == AbstractMap.SimpleImmutableEntry.class || c == AbstractMap.SimpleEntry.class) {
                // Guava's ImmutableMapEntry (every ImmutableMap entry, also the copy relocated into Iceberg)
                // keeps its key and value in these java.util fields, which the FE cannot read reflectively:
                // java.util is not opened. Read them through the entry instead.
                Map.Entry<?, ?> entry = (Map.Entry<?, ?>) obj;
                size += estimateInternal(entry.getKey(), maxDepth - 1, sampleSize) +
                        estimateInternal(entry.getValue(), maxDepth - 1, sampleSize);
                continue;
            }
            Field[] nestedFields = CLASS_NESTED_FIELDS.get(c);
            for (Field field : nestedFields) {
                size += estimateNestedField(obj, field, maxDepth, sampleSize);
            }
        }

        return size;
    }

    private long estimateNestedField(Object obj, Field field, int maxDepth, int sampleSize) {
        try {
            Object fieldValue = field.get(obj);
            if (fieldValue == null) {
                return 0;
            }
            return estimateInternal(fieldValue, maxDepth - 1, sampleSize);
        } catch (IllegalAccessException | InaccessibleObjectException e) {
            // Skip fields that cannot be accessed
            return 0;
        }
    }

    /**
     * Get sample elements from a collection.
     * Samples are evenly distributed across the collection using a fixed step size. A sampled null is kept
     * (it weighs 0), so the sample stays representative of the null slots.
     * For RandomAccess lists (ArrayList, etc.), uses index access for efficiency.
     * For non-RandomAccess collections (LinkedList, etc.), iterates with step skipping.
     *
     * @param collection the collection to sample
     * @param sampleSize the number of elements to sample
     * @return a list of sample elements
     */
    private static List<Object> getSamples(Collection<?> collection, int sampleSize) {
        if (sampleSize <= 0) {
            return Collections.emptyList();
        }
        int size = collection.size();
        List<Object> samples = new ArrayList<>(Math.min(sampleSize, size));

        if (size <= sampleSize) {
            // If collection is small, use all elements
            for (Object item : collection) {
                samples.add(item);
            }
        } else if (collection instanceof List<?> list && collection instanceof RandomAccess) {
            // For RandomAccess lists, sample evenly distributed elements using index access
            int step = size / sampleSize;
            for (int i = 0; i < size && samples.size() < sampleSize; i += step) {
                Object item = list.get(i);
                samples.add(item);
            }
        } else {
            // For non-RandomAccess collections, iterate with step skipping
            int step = size / sampleSize;
            int index = 0;
            int nextSampleIndex = 0;
            for (Object item : collection) {
                if (index == nextSampleIndex) {
                    samples.add(item);
                    nextSampleIndex += step;
                    if (samples.size() >= sampleSize) {
                        break;
                    }
                }
                index++;
            }
        }

        return samples;
    }

    /**
     * Get sample elements from an array.
     *
     * @param array      the array to sample
     * @param length     the length of the array
     * @param sampleSize the number of elements to sample
     * @return a list of sample elements
     */
    private static List<Object> getSamplesFromArray(Object array, int length, int sampleSize) {
        if (sampleSize <= 0) {
            return Collections.emptyList();
        }
        List<Object> samples = new ArrayList<>(Math.min(sampleSize, length));

        if (length <= sampleSize) {
            // If array is small, use all elements
            for (int i = 0; i < length; i++) {
                Object item = Array.get(array, i);
                samples.add(item);
            }
        } else {
            // Sample evenly distributed elements
            int step = length / sampleSize;
            for (int i = 0; i < length && samples.size() < sampleSize; i += step) {
                Object item = Array.get(array, i);
                samples.add(item);
            }
        }

        return samples;
    }

    private static boolean isHiddenClass(Class<?> c) {
        return c.isHidden()
                || c.isSynthetic() && c.getName().contains("$$Lambda$");
    }
}
