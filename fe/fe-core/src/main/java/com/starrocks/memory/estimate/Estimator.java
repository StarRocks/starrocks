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

import org.openjdk.jol.info.ClassLayout;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InaccessibleObjectException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
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
    private static final int DEFAULT_MAX_DEPTH = 8;

    private static final int DEFAULT_SAMPLE_SIZE = 5;

    // Array header size: object header (12 bytes) + length field (4 bytes) = 16 bytes on 64-bit JVM
    public static final int ARRAY_HEADER_SIZE = 16;

    private static final long HIDDEN_CLASS_SHALLOW_SIZE = 16;

    // Reference size on 64-bit JVM (with compressed oops, typically 4 bytes; without, 8 bytes)
    private static final int REFERENCE_SIZE = 4;

    // Cache for shallow sizes of classes
    protected static final Map<Class<?>, Long> SHALLOW_SIZE_CACHE = new ConcurrentHashMap<>();

    // Registry for custom estimators
    protected static final Map<Class<?>, CustomEstimator> CUSTOM_ESTIMATORS = new HashMap<>();

    // Registry for classes that should only calculate shallow memory
    protected static final Set<Class<?>> SHALLOW_MEMORY_CLASSES = ConcurrentHashMap.newKeySet();

    // Cache for nested reference fields of classes
    protected static final Map<Class<?>, List<Field>> CLASS_NESTED_FIELDS = new ConcurrentHashMap<>();

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

    // Set to track visited objects and avoid counting the same object twice
    private final Set<Object> sampled;

    // Set of classes to ignore during estimation
    private final Set<Class<?>> ignoreClasses;

    public Estimator() {
        this(Collections.emptySet());
    }

    public Estimator(Set<Class<?>> ignoreClasses) {
        this.sampled = Collections.newSetFromMap(new IdentityHashMap<>());
        this.ignoreClasses = ignoreClasses != null ? ignoreClasses : Collections.emptySet();
    }

    /**
     * Register a custom estimator for a specific class.
     *
     * @param clazz     the class to register estimator for
     * @param estimator the custom estimator
     */
    public static void registerCustomEstimator(Class<?> clazz, CustomEstimator estimator) {
        CUSTOM_ESTIMATORS.put(clazz, estimator);
    }

    /**
     * Get the custom estimator for a class, if registered.
     *
     * @param clazz the class to look up
     * @return the custom estimator, or null if not registered
     */
    public static CustomEstimator getCustomEstimator(Class<?> clazz) {
        return CUSTOM_ESTIMATORS.get(clazz);
    }

    /**
     * Register a class as shallow memory class.
     * Classes registered here will only calculate shallow memory size.
     *
     * @param clazz the class to register
     */
    public static void registerShallowMemoryClass(Class<?> clazz) {
        SHALLOW_MEMORY_CLASSES.add(clazz);
    }

    /**
     * Check if a class is a shallow memory class (annotated with @ShallowMemory or registered).
     *
     * @param clazz the class to check
     * @return true if the class should only calculate shallow memory
     */
    public static boolean isShallowMemoryClass(Class<?> clazz) {
        return clazz.isAnnotationPresent(ShallowMemory.class) || SHALLOW_MEMORY_CLASSES.contains(clazz);
    }

    /**
     * Estimate the memory size of an object with default max depth.
     *
     * @param obj the object to estimate
     * @return the estimated memory size in bytes
     */
    public static long estimate(Object obj) {
        return estimate(obj, DEFAULT_MAX_DEPTH);
    }

    /**
     * Estimate the memory size of an object.
     *
     * @param obj      the object to estimate
     * @param maxDepth the maximum recursion depth
     * @return the estimated memory size in bytes
     */
    public static long estimate(Object obj, int maxDepth) {
        return new Estimator().estimateInternal(obj, maxDepth, DEFAULT_SAMPLE_SIZE);
    }

    /**
     * Estimate the memory size of an object with specified sample size.
     *
     * @param obj        the object to estimate
     * @param maxDepth   the maximum recursion depth
     * @param sampleSize the number of elements to sample for collections/arrays
     * @return the estimated memory size in bytes
     */
    public static long estimate(Object obj, int maxDepth, int sampleSize) {
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
     * Estimate the memory size of an object with specified max depth and ignore classes.
     *
     * @param obj           the object to estimate
     * @param maxDepth      the maximum recursion depth
     * @param ignoreClasses set of classes to ignore during estimation
     * @return the estimated memory size in bytes
     */
    public static long estimate(Object obj, int maxDepth, Set<Class<?>> ignoreClasses) {
        return new Estimator(ignoreClasses).estimateInternal(obj, maxDepth, DEFAULT_SAMPLE_SIZE);
    }

    /**
     * Estimate the memory size of an object with all options specified.
     *
     * @param obj           the object to estimate
     * @param maxDepth      the maximum recursion depth
     * @param sampleSize    the number of elements to sample for collections/arrays
     * @param ignoreClasses set of classes to ignore during estimation
     * @return the estimated memory size in bytes
     */
    public static long estimate(Object obj, int maxDepth, int sampleSize, Set<Class<?>> ignoreClasses) {
        return new Estimator(ignoreClasses).estimateInternal(obj, maxDepth, sampleSize);
    }

    /**
     * Internal estimation method with sampled set to track visited objects.
     */
    private long estimateInternal(Object obj, int maxDepth, int sampleSize) {
        if (obj == null) {
            return 0;
        }

        // Skip if already visited (avoid counting the same object twice)
        if (!sampled.add(obj)) {
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

        // Handle collections
        if (obj instanceof Collection) {
            return estimateCollection((Collection<?>) obj, maxDepth, sampleSize);
        }

        // Handle maps
        if (obj instanceof Map<?, ?> map) {
            return shallow(obj) +
                    estimateCollection(map.keySet(), maxDepth, sampleSize) +
                    estimateCollection(map.values(), maxDepth, sampleSize);
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

        return SHALLOW_SIZE_CACHE.computeIfAbsent(clazz, k -> {
            if (isHiddenClass(clazz)) {
                return HIDDEN_CLASS_SHALLOW_SIZE;
            } else {
                try {
                    return ClassLayout.parseInstance(obj).instanceSize();
                } catch (RuntimeException e) {
                    return HIDDEN_CLASS_SHALLOW_SIZE;
                }
            }
        });
    }

    public static long shallow(Class<?> clazz) {
        if (clazz.isArray()) {
            throw new IllegalArgumentException("Use shallow(Object obj) for array instances");
        }
        return SHALLOW_SIZE_CACHE.computeIfAbsent(clazz, k -> {
            if (isHiddenClass(clazz)) {
                return HIDDEN_CLASS_SHALLOW_SIZE;
            } else {
                try {
                    return ClassLayout.parseClass(clazz).instanceSize();
                } catch (RuntimeException e) {
                    return HIDDEN_CLASS_SHALLOW_SIZE;
                }
            }
        });
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

        if (samples.isEmpty()) {
            return shallowSize;
        }

        long sampleTotalSize = 0;
        for (Object sample : samples) {
            sampleTotalSize += estimateInternal(sample, maxDepth - 1, sampleSize);
        }

        double avgSize = (double) sampleTotalSize / samples.size();
        return shallowSize + (long) (avgSize * collection.size());
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

        if (samples.isEmpty()) {
            return shallowSize;
        }

        long sampleTotalSize = 0;
        for (Object sample : samples) {
            sampleTotalSize += estimateInternal(sample, maxDepth - 1, sampleSize);
        }

        double avgSize = (double) sampleTotalSize / samples.size();
        return shallowSize + (long) (avgSize * length);
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
        Class<?> clazzTmp = obj.getClass();
        List<Class<?>> classList = new ArrayList<>();
        while (clazzTmp != null
                && clazzTmp != Object.class) {
            if (clazzTmp.isAnnotationPresent(IgnoreMemoryTrack.class)) {
                return 0;
            }
            classList.add(clazzTmp);
            clazzTmp = clazzTmp.getSuperclass();
        }

        long size = shallow(obj);
        boolean nestedFieldExists = false;
        for (Class<?> clazz : classList) {
            List<Field> nestedFields = CLASS_NESTED_FIELDS.get(clazz);
            if (nestedFields == null) {
                nestedFields = new ArrayList<>();
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

                    nestedFieldExists = true;
                    nestedFields.add(field);
                    size += estimateNestedField(obj, field, maxDepth, sampleSize);
                }

                // cache the nested fields even though it may be empty,
                // if empty we can skip this class quickly next time
                CLASS_NESTED_FIELDS.put(clazz, nestedFields);
            } else {
                for (Field field : nestedFields) {
                    nestedFieldExists = true;
                    size += estimateNestedField(obj, field, maxDepth, sampleSize);
                }
            }
        }

        Class<?> clazz = obj.getClass();
        if (!nestedFieldExists && clazz != Object.class) {
            // Cache as shallow memory class to avoid future recursive traversal
            SHALLOW_MEMORY_CLASSES.add(clazz);
        }

        return size;
    }

    private long estimateNestedField(Object obj, Field field, int maxDepth, int sampleSize) {
        long size = 0;
        try {
            field.setAccessible(true);
            Object fieldValue = field.get(obj);

            if (fieldValue != null) {
                size += estimateInternal(fieldValue, maxDepth - 1, sampleSize);
            }
        } catch (IllegalAccessException | InaccessibleObjectException e) {
            // Skip fields that cannot be accessed
        }
        return size;
    }

    /**
     * Get sample elements from a collection.
     * Samples are evenly distributed across the collection using a fixed step size.
     * For RandomAccess lists (ArrayList, etc.), uses index access for efficiency.
     * For non-RandomAccess collections (LinkedList, etc.), iterates with step skipping.
     *
     * @param collection the collection to sample
     * @param sampleSize the number of elements to sample
     * @return a list of sample elements
     */
    private static List<Object> getSamples(Collection<?> collection, int sampleSize) {
        int size = collection.size();
        List<Object> samples = new ArrayList<>(Math.min(sampleSize, size));

        if (size <= sampleSize) {
            // If collection is small, use all elements
            for (Object item : collection) {
                if (item != null) {
                    samples.add(item);
                }
            }
        } else if (collection instanceof List<?> list && collection instanceof RandomAccess) {
            // For RandomAccess lists, sample evenly distributed elements using index access
            int step = size / sampleSize;
            for (int i = 0; i < size && samples.size() < sampleSize; i += step) {
                Object item = list.get(i);
                if (item != null) {
                    samples.add(item);
                }
            }
        } else {
            // For non-RandomAccess collections, iterate with step skipping
            int step = size / sampleSize;
            int index = 0;
            int nextSampleIndex = 0;
            for (Object item : collection) {
                if (index == nextSampleIndex) {
                    if (item != null) {
                        samples.add(item);
                    }
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
        List<Object> samples = new ArrayList<>(Math.min(sampleSize, length));

        if (length <= sampleSize) {
            // If array is small, use all elements
            for (int i = 0; i < length; i++) {
                Object item = Array.get(array, i);
                if (item != null) {
                    samples.add(item);
                }
            }
        } else {
            // Sample evenly distributed elements
            int step = length / sampleSize;
            for (int i = 0; i < length && samples.size() < sampleSize; i += step) {
                Object item = Array.get(array, i);
                if (item != null) {
                    samples.add(item);
                }
            }
        }

        return samples;
    }

    private static boolean isHiddenClass(Class<?> c) {
        return c.isHidden()
                || c.isSynthetic() && c.getName().contains("$$Lambda$");
    }
}
