# Memory Estimation Module

This module provides utilities for estimating the memory footprint of Java objects in StarRocks FE. It is designed for
memory tracking and monitoring, balancing accuracy and performance by combining shallow sizes, sampling, and bounded
recursion instead of deep object graph traversal.

## Overview

The memory estimation module offers:

- Sampling-based estimation for arrays and collections
- Recursive field traversal with configurable depth limits
- Custom estimator support for specialized object types
- Annotation-based control to exclude or simplify memory tracking for specific classes or fields
- Optional ignore-class sets for excluding shared objects from a single estimate
- ClassValue-based caching of class shallow sizes and reference fields
- Identity-based cycle detection to avoid double counting the same object

## Components

### Estimator

The core utility class (`Estimator.java`) that estimates memory size of Java objects.

Key features:

- Uses JOL (Java Object Layout) for accurate shallow size calculation when possible
- Samples elements from large collections/arrays instead of traversing all elements
- Supports custom estimators and shallow-only classes
- Provides overloads for max depth, sample size, and ignore-class sets
- Caches shallow sizes and reference fields via `ClassValue`

### Annotations

#### @IgnoreMemoryTrack

Marks fields or classes to be excluded from memory tracking.

```java
// Exclude a field from memory estimation
public class MyClass {
    @IgnoreMemoryTrack
    private SomeLargeObject cache;  // Will not be counted
}

// Exclude an entire class - returns 0 for memory estimation
@IgnoreMemoryTrack
public class IgnoredClass {
    // ...
}
```

#### @ShallowMemory

Marks classes that should only calculate shallow memory size (no recursive field traversal).

```java
@ShallowMemory
public class SimpleData {
    private int id;
    private String name;  // name's content won't be recursively calculated
}
```

For collections containing `@ShallowMemory` elements, the estimation uses `shallow_size * element_count` instead of
sampling.

### CustomEstimator

Interface for implementing specialized memory estimators for specific object types.

```java
@FunctionalInterface
public interface CustomEstimator {
    long estimate(Object obj);
}
```

## Usage

### Basic Estimation

```java
// Estimate with default settings (max depth = 20, sample size = 5)
long size = Estimator.estimate(myObject);

// Estimate with custom sample size
long size = Estimator.estimate(myObject, 10);

// Estimate with custom sample size and max depth
long size = Estimator.estimate(myObject, 10, 4);
```

### Ignore Classes for Shared Objects

```java
Set<Class<?>> ignore = Set.of(SharedCache.class);
long size = Estimator.estimate(myObject, ignore);

// With custom sample size and max depth
long size = Estimator.estimate(myObject, 10, 4, ignore);
```

### Register Custom Estimators

```java
// Register a custom estimator for a specific class
Estimator.registerCustomEstimator(String.class, new StringEstimator());

// Register with a lambda
Estimator.registerCustomEstimator(MyClass.class, obj -> {
    MyClass mc = (MyClass) obj;
    return 100 + mc.getData().length * 8;
});
```

### Register Shallow Memory Classes Programmatically

```java
// Register a class to only calculate shallow memory
Estimator.registerShallowMemoryClass(SomeExternalClass.class);
```

### Get Shallow Size Only

```java
// Get shallow size of an object instance
long shallowSize = Estimator.shallow(myObject);

// Get shallow size of a class (non-array types only)
long shallowSize = Estimator.shallow(MyClass.class);
```

## Implementation Details

### Estimation Algorithm

1. Null check: returns 0 for null objects
2. Cycle check: returns 0 if the object was already visited
3. Ignore-class check: returns 0 for classes in the ignore set
4. Annotation check: returns 0 for `@IgnoreMemoryTrack` classes
5. Custom estimator: uses registered custom estimator if available
6. Shallow memory check: returns shallow size for `@ShallowMemory` classes
7. Depth check: returns shallow size if max depth is reached
8. Type-specific handling based on arrays, collections, maps, or regular objects

Hidden or synthetic lambda classes are treated as a fixed shallow size (16 bytes) to avoid JOL failures.

### Type-Specific Handling

- Arrays: header + references + sampled element sizes
- Collections: shallow size + sampled element sizes
- Maps: shallow size + key set + value collection sizes
- Objects: recursively estimates all non-static, non-primitive, non-enum reference fields

### Sampling Strategy

- If size <= sample size: uses all non-null elements
- RandomAccess lists (e.g., ArrayList): evenly distributed sampling via index access
- Non-RandomAccess collections (e.g., LinkedList): iterates and picks evenly spaced elements
- Arrays: evenly distributed sampling

### Caching

- Shallow size cache: `ClassValue<Long>` to avoid repeated JOL reflection work
- Nested reference fields cache: `ClassValue<Field[]>` with `trySetAccessible()` attempted once during caching
- Shallow memory classes: stored by class name to avoid pinning ClassLoaders

### Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `DEFAULT_MAX_DEPTH` | 20 | Default recursion depth limit |
| `DEFAULT_SAMPLE_SIZE` | 5 | Default number of elements to sample |
| `ARRAY_HEADER_SIZE` | 16 | Array header size on a 64-bit JVM |
| `REFERENCE_SIZE` | 4 | Reference size with compressed oops |
| `HIDDEN_CLASS_SHALLOW_SIZE` | 16 | Fallback size for hidden or synthetic lambda classes |

Note: These constants assume a 64-bit JVM with compressed oops. Actual layouts may differ.

## Best Practices

1. Ensure the target object is immutable or protected by a lock during estimation
2. Use annotations for classes with known memory patterns to avoid unnecessary recursion
3. Register custom estimators for complex objects with predictable memory layouts
4. Adjust depth and sample size based on your accuracy vs. performance requirements
5. Use `@IgnoreMemoryTrack` or ignore-class sets for shared objects that shouldn't be counted multiple times
