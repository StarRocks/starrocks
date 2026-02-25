# AGENTS.md - StarRocks Java Extensions

> Guidelines for AI coding agents working with Java Extensions.
> This module provides JNI connectors for external data sources.

## Overview

Java Extensions provide connectors to external data sources that are accessed through JNI from the C++ Backend. These connectors enable StarRocks to read data from various external systems.

## Module Structure

```
java-extensions/
├── jni-connector/          # Base JNI connector infrastructure
├── common-runtime/         # Shared runtime utilities
├── java-utils/             # General Java utilities
├── hive-reader/            # Hive data source reader
├── hudi-reader/            # Hudi data source reader
├── iceberg-metadata-reader/# Iceberg metadata reader
├── paimon-reader/          # Paimon data source reader
├── odps-reader/            # ODPS (MaxCompute) reader
├── kudu-reader/            # Kudu data source reader
├── jdbc-bridge/            # JDBC connector bridge
├── udf-extensions/         # User-defined function extensions
├── hadoop-ext/             # Hadoop extensions
├── hadoop-lib/             # Hadoop library dependencies
└── udf-examples/           # UDF example implementations
```

## Build Commands

```bash
# Build from project root
./build.sh --fe  # Java extensions are built with FE

# Build directly with Maven
cd java-extensions
mvn package -DskipTests

# Build specific module
mvn package -pl hive-reader -am -DskipTests

# Run tests
mvn test

# Check code style
mvn checkstyle:check
```

## Code Style

Uses the same checkstyle configuration as FE: `java-extensions/checkstyle.xml`

```bash
# Check style
mvn checkstyle:check
```

### Key Rules
- **Indent**: 4 spaces
- **Line limit**: 130 characters (same as FE)
- **Import order**: Third-party, Java standard, static

## JNI Development Guidelines

### Memory Management

JNI code requires careful memory management:

```java
public class MyReader implements Closeable {
    private long nativeHandle;  // C++ object pointer

    // Called from C++ to initialize
    public void open() {
        // Initialize resources
    }

    // Called from C++ to read data
    public int getNext() {
        // Return data to C++
    }

    // Must release all resources
    @Override
    public void close() {
        // Release native resources
        // Close file handles
        // Clear collections
    }
}
```

### Exception Handling

Always handle exceptions properly - unhandled exceptions crash the BE:

```java
public int getNext() {
    try {
        return doGetNext();
    } catch (Exception e) {
        LOG.error("Error reading data", e);
        throw new RuntimeException("Read failed: " + e.getMessage(), e);
    }
}
```

### Thread Safety

JNI methods may be called from multiple threads:

```java
// Use thread-safe collections
private final ConcurrentHashMap<String, Object> cache;

// Or synchronize access
private synchronized void updateState() { }
```

## Adding a New Connector

1. **Create new Maven module**:
   ```
   java-extensions/
   └── my-reader/
       ├── pom.xml
       └── src/main/java/com/starrocks/jni/connector/MyReader.java
   ```

2. **Add to parent pom.xml**:
   ```xml
   <modules>
       ...
       <module>my-reader</module>
   </modules>
   ```

3. **Implement reader interface**:
   ```java
   public class MyReader implements Closeable {
       public void open(String params) { }
       public int getNext() { }
       public void close() { }
   }
   ```

4. **Add corresponding C++ code in BE** to load and call the Java class.

5. **Add tests**:
   ```
   my-reader/src/test/java/com/starrocks/jni/connector/MyReaderTest.java
   ```

## Key Classes

| Class | Purpose |
|-------|---------|
| `jni-connector/ScannerHelper` | Base JNI scanner utilities |
| `common-runtime/ColumnValue` | Column value abstraction |
| `hive-reader/HiveScanner` | Hive data scanner |
| `jdbc-bridge/JDBCBridge` | JDBC connection bridge |

## Testing

```bash
# Run all tests
cd java-extensions
mvn test

# Run specific test
mvn test -Dtest=HiveScannerTest

# Run with debug logging
mvn test -Dlog4j.configuration=file:src/test/resources/log4j.properties
```

## Dependencies

When adding dependencies:
- Prefer libraries already used by FE
- Avoid version conflicts with Hadoop/Hive ecosystem
- Use `<scope>provided</scope>` for libraries provided by BE at runtime

## Troubleshooting

### ClassNotFoundException at Runtime

Check that:
1. JAR is included in `CLASSPATH`
2. All dependencies are packaged
3. No version conflicts exist

### JVM Crashes

1. Check for null pointers passed from C++
2. Verify memory is properly released
3. Check for exceptions not caught in Java

### Slow Performance

1. Minimize JNI boundary crossings
2. Batch data transfers
3. Use primitive arrays instead of object arrays

## License Header

```java
// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// ...
```
