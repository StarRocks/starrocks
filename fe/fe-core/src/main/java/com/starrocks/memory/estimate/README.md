# Memory Estimation Module

This module provides utilities for estimating the memory footprint of Java objects in StarRocks FE. It is designed for memory tracking and monitoring purposes, enabling accurate memory usage statistics without the overhead of deep object graph traversal.

## Overview

The memory estimation module offers:

- **Sampling-based estimation** for arrays and collections to balance accuracy and performance
- **Recursive field traversal** with configurable depth limits
- **Custom estimator support** for specialized object types
- **Annotation-based control** to exclude or simplify memory tracking for specific classes/fields
- **Caching** of class shallow sizes for optimal performance

## Components

### Estimator

The core utility class (`Estimator.java`) that estimates memory size of Java objects.

**Key Features:**
- Uses JOL (Java Object Layout) for accurate shallow size calculation
- Samples elements from large collections/arrays instead of traversing all elements
- Caches shallow sizes to avoid repeated reflection overhead
- Automatically detects and caches classes with no nested reference fields

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

For collections containing `@ShallowMemory` elements, the estimation uses `shallow_size * element_count` instead of sampling.

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
// Estimate with default settings (max depth = 8, sample size = 5)
long size = Estimator.estimate(myObject);

// Estimate with custom max depth
long size = Estimator.estimate(myObject, 4);

// Estimate with custom max depth and sample size
long size = Estimator.estimate(myObject, 4, 10);
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

// Get shallow size of a class (for non-array types)
long shallowSize = Estimator.shallow(MyClass.class);
```

## Implementation Details

### Estimation Algorithm

1. **Null check**: Returns 0 for null objects
2. **Annotation check**: Returns 0 for `@IgnoreMemoryTrack` classes
3. **Custom estimator**: Uses registered custom estimator if available
4. **Shallow memory check**: Returns shallow size for `@ShallowMemory` classes
5. **Depth check**: Returns shallow size if max depth is reached
6. **Type-specific handling**:
   - **Arrays**: Calculates header + references + sampled element sizes
   - **Collections**: Calculates shallow size + sampled element sizes
   - **Maps**: Calculates shallow size + key set + value collection
   - **Objects**: Recursively estimates all non-static, non-primitive reference fields

### Sampling Strategy

For large collections and arrays:
- **RandomAccess lists** (e.g., ArrayList): Evenly distributed sampling using index access
- **Non-RandomAccess collections** (e.g., LinkedList): Takes first N elements to avoid O(n) traversal
- **Arrays**: Evenly distributed sampling

### Caching

- **Shallow size cache**: `ConcurrentHashMap<Class<?>, Long>` for class instance sizes
- **Nested fields cache**: `ConcurrentHashMap<Class<?>, List<Field>>` for reference fields per class
- **Shallow memory classes**: Auto-detected and cached when a class has no nested reference fields

### Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `DEFAULT_MAX_DEPTH` | 8 | Default recursion depth limit |
| `DEFAULT_SAMPLE_SIZE` | 5 | Default number of elements to sample |
| `ARRAY_HEADER_SIZE` | 16 | Array header size on 64-bit JVM |
| `REFERENCE_SIZE` | 4 | Reference size with compressed oops |

## Best Practices

1. **Use annotations** for classes with known memory patterns to avoid unnecessary recursion
2. **Register custom estimators** for complex objects with predictable memory layouts
3. **Adjust depth and sample size** based on your accuracy vs. performance requirements
4. **Mark external library classes** as shallow memory if deep traversal is not needed
5. **Use `@IgnoreMemoryTrack`** for cached or shared objects that shouldn't be counted multiple times
