# AGENTS.md - StarRocks Backend (BE)

> Backend-specific guidelines for AI coding agents.
> The BE is written in C++ and handles query execution, storage management, and data processing.

## Architecture Overview

The Backend is responsible for:
- **Query Execution**: Pipeline-based vectorized execution engine
- **Storage Engine**: Column-oriented storage with LSM-tree structure
- **Data Processing**: SIMD-optimized operations on columnar data

### Key Components

```
be/src/
├── base/             # Base primitives (Status, StatusOr, logging, macros)
├── storage/          # Storage engine (tablets, segments, compaction)
├── exec/             # Query execution (operators, pipeline, scan)
├── column/           # Column types and operations
├── exprs/            # Expression evaluation
├── runtime/          # Runtime state, memory management
├── formats/          # File formats (Parquet, ORC, etc.)
├── connector/        # External data source connectors
├── util/             # Utilities and helpers
├── common/           # Common config, tracing, process utilities
├── http/             # HTTP service endpoints
├── service/          # Thrift services
└── simd/             # SIMD utilities
```

## Build Commands

```bash
# Standard release build
./build.sh --be

# Debug build
BUILD_TYPE=Debug ./build.sh --be

# ASAN (Address Sanitizer) build for debugging
BUILD_TYPE=ASAN ./build.sh --be

# Clean and rebuild
./build.sh --be --clean

# Build with specific options
./build.sh --be --enable-shared-data  # For shared-data mode
```

### Fast UT Build Tip

Use `run-be-ut.sh` with `--build-target` to compile only a single test binary and speed up iteration, e.g.:

```bash
CMAKE_BUILD_PREFIX=/path/to/build \
STARROCKS_THIRDPARTY=/path/to/thirdparty \
./run-be-ut.sh --build-target base_test --module base_test --without-java-ext --gtest_filter 'StatusTest.*'
```

### Fast UT Build Tip

Use `run-be-ut.sh` with `--build-target` to compile only a single test binary and speed up iteration, e.g.:

```bash
CMAKE_BUILD_PREFIX=/path/to/build \
STARROCKS_THIRDPARTY=/path/to/thirdparty \
./run-be-ut.sh --build-target base_test --module base_test --without-java-ext --gtest_filter 'StatusTest.*'
```

### CMake Options

Key CMake options (see `be/CMakeLists.txt`):
- `CMAKE_BUILD_TYPE`: Release, Debug, ASAN
- `USE_AVX2`, `USE_AVX512`: SIMD instruction sets
- `USE_STAROS`: Enable shared-data mode
- `WITH_GCOV`: Enable code coverage
- `MAKE_TEST`: Build unit tests

### Unit Test Linking Note

- Minimal UT binaries like `base_test` avoid `WRAP_LINKER_FLAGS` because they don’t link `Util`,
  which provides `__wrap___cxa_throw`. Other UTs still use the default wrap via `TEST_LINK_LIBS`.
- When `ENABLE_MULTI_DYNAMIC_LIBS=ON`, shared BE libs are built without `WRAP_LINKER_FLAGS` to avoid
  undefined `__wrap___cxa_throw` in libs that don’t link `Util`.
- When `ENABLE_MULTI_DYNAMIC_LIBS=ON`, shared BE libs are built without `WRAP_LINKER_FLAGS` to avoid
  undefined `__wrap___cxa_throw` in libs that don’t link `Util`.

### Unit Test Linking Note

- Minimal UT binaries like `base_test` avoid `WRAP_LINKER_FLAGS` because they don’t link `Util`,
  which provides `__wrap___cxa_throw`. Other UTs still use the default wrap via `TEST_LINK_LIBS`.

## Code Style

### Formatting
- **Style base**: Google C++ Style Guide
- **Config file**: `.clang-format` at project root
- **Indent**: 4 spaces
- **Line limit**: 120 characters
- **Pointer alignment**: Left (`int* ptr`, not `int *ptr`)

```bash
# Format a file
clang-format -i src/storage/tablet.cpp

# Check formatting
clang-format --dry-run -Werror src/storage/tablet.cpp
```

### Naming Conventions

```cpp
// Classes: PascalCase
class TabletManager { };

// Functions: snake_case
Status create_tablet(const TCreateTabletReq& req);

// Member variables: underscore prefix
class Tablet {
private:
    int64_t _tablet_id;
    std::string _path;
};

// Constants: ALL_CAPS with underscores
constexpr int64_t MAX_SEGMENT_SIZE = 1024 * 1024 * 1024;

// Namespaces: lowercase
namespace starrocks { }
```

### Header Guards

Use `#pragma once` instead of traditional include guards:

```cpp
// Good
#pragma once

// Avoid
#ifndef STARROCKS_BE_STORAGE_TABLET_H
#define STARROCKS_BE_STORAGE_TABLET_H
...
#endif
```

### Include Order

1. Corresponding header file (for .cpp files)
2. C system headers
3. C++ standard library headers
4. Third-party library headers
5. StarRocks headers

```cpp
#include "storage/tablet.h"  // Corresponding header

#include <sys/types.h>       // C system

#include <memory>            // C++ standard
#include <vector>

#include <glog/logging.h>    // Third-party

#include "base/status.h"     // StarRocks
#include "storage/rowset.h"
```

## Module Boundaries

### base
- **Minimal deps only**: `be/src/base` may only depend on `gen_cpp/*`, `gutil/*`, system headers, and third-party libraries.
- **No other BE modules**: do not include headers from `common/*`, `util/*`, `storage/*`, etc.

### gutil
- **No StarRocks module deps**: `be/src/gutil` must not include headers from other BE modules (e.g. `common/*`, `util/*`, `storage/*`).
- **Allowed deps**: `gutil/*`, system headers, and third-party base/butil headers.
- **Logging**: use `gutil/logging.h` (glog wrapper) instead of `common/logging.h`.
- **Compiler macros**: use `gutil/compiler_util.h` (do not include `common/compiler_util.h` from gutil).

### common
- **Allowed deps only**: `be/src/common` may depend on `base/*`, `gutil/*`, `gen_cpp/*`, system headers, and third-party libraries.
- **No other BE modules**: do not include headers from `util/*`, `runtime/*`, `storage/*`, `exec/*`, `service/*`, `http/*`, etc.

## Common Patterns

### Status and StatusOr

Always use `Status` for error handling:

```cpp
Status do_something() {
    RETURN_IF_ERROR(validate_input());

    if (condition_failed) {
        return Status::InvalidArgument("reason");
    }

    return Status::OK();
}

// Use StatusOr for returning values with potential errors
StatusOr<TabletSharedPtr> get_tablet(int64_t tablet_id) {
    auto tablet = _find_tablet(tablet_id);
    if (tablet == nullptr) {
        return Status::NotFound("tablet not found");
    }
    return tablet;
}

// Usage
auto result = get_tablet(id);
if (!result.ok()) {
    return result.status();
}
auto tablet = std::move(result).value();
```

### Column and Chunk

Data is processed in columnar batches:

```cpp
// Column: A single column of data
ColumnPtr column = Int32Column::create();
column->append(1);
column->append(2);

// Chunk: A collection of columns (like a row batch)
ChunkPtr chunk = std::make_unique<Chunk>();
chunk->append_column(column, slot_id);

// Processing
for (size_t i = 0; i < chunk->num_rows(); i++) {
    // Process row i
}
```

### Memory Management

Use smart pointers and RAII:

```cpp
// Prefer unique_ptr for ownership
std::unique_ptr<Segment> segment = std::make_unique<Segment>();

// Use shared_ptr when shared ownership is needed
using TabletSharedPtr = std::shared_ptr<Tablet>;

// Use raw pointers only for non-owning references
void process(const Tablet* tablet);  // Does not take ownership
```

### Logging

Use Google Logging (glog):

```cpp
LOG(INFO) << "Starting tablet " << tablet_id;
LOG(WARNING) << "Slow query detected: " << query_id;
LOG(ERROR) << "Failed to open file: " << path;

// Conditional logging
VLOG(1) << "Debug info";  // Verbose logging, level 1

// Check and log
CHECK(ptr != nullptr) << "Pointer must not be null";
DCHECK(condition) << "Debug-only check";
```

## Testing

### Test Framework

Uses Google Test (gtest). Tests are in `be/test/`, mirroring source structure.

```bash
# Run all BE tests
./run-be-ut.sh

# Run specific test suite
./run-be-ut.sh --test CompactionUtilsTest

# Run with gtest filter
./run-be-ut.sh --gtest_filter "TabletUpdatesTest*:CompactionTest*"

# Run with ASAN
BUILD_TYPE=ASAN ./run-be-ut.sh
```

### Writing Tests

```cpp
// File: be/test/storage/tablet_test.cpp
#include "storage/tablet.h"

#include <gtest/gtest.h>

namespace starrocks {

class TabletTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Setup code
    }

    void TearDown() override {
        // Cleanup code
    }
};

TEST_F(TabletTest, CreateTablet) {
    auto tablet = create_test_tablet();
    ASSERT_NE(tablet, nullptr);
    EXPECT_EQ(tablet->tablet_id(), 12345);
}

TEST_F(TabletTest, InsertData) {
    auto tablet = create_test_tablet();
    auto status = tablet->insert(test_data);
    ASSERT_TRUE(status.ok()) << status.message();
}

}  // namespace starrocks
```

### Test Utilities

Common test helpers in `be/src/testutil/`:
- `column_test_helper.h`: Create test columns
- `assert.h`: Custom assertions
- `init_test_env.h`: Test environment setup

## Performance Guidelines

### SIMD Usage

Leverage SIMD for performance-critical code:

```cpp
#include "simd/simd.h"

// Check SIMD support at runtime
if (SIMD::support_avx2()) {
    // Use AVX2 implementation
}

// Use SIMD utilities for common operations
SIMD::filter(column, filter_data);
```

### Vectorization Tips

1. Process data in batches, not row-by-row
2. Avoid branches in inner loops
3. Use `Column` operations instead of element-wise access
4. Preallocate memory when possible

```cpp
// Good: Batch processing
void process_batch(const Column* input, Column* output) {
    auto* in_data = down_cast<const Int32Column*>(input)->get_data().data();
    auto* out_data = down_cast<Int32Column*>(output)->get_data().data();

    for (size_t i = 0; i < input->size(); i++) {
        out_data[i] = in_data[i] * 2;
    }
}

// Avoid: Row-by-row with virtual calls
void process_slow(const Column* input, Column* output) {
    for (size_t i = 0; i < input->size(); i++) {
        output->append_datum(input->get(i).get_int32() * 2);
    }
}
```

### Memory Allocation

1. Avoid allocations in hot paths
2. Use memory pools for frequent allocations
3. Reserve capacity for vectors when size is known

```cpp
// Good: Reserve capacity
std::vector<int> data;
data.reserve(expected_size);

// Use ColumnAllocator for column data
auto* allocator = ColumnAllocatorFactory::instance();
```

## Key Files to Know

| File | Purpose |
|------|---------|
| `src/common/status.h` | Error handling with Status |
| `src/column/column.h` | Base Column class |
| `src/column/chunk.h` | Chunk (batch of columns) |
| `src/exec/pipeline/pipeline_driver.h` | Pipeline execution driver |
| `src/storage/tablet.h` | Tablet abstraction |
| `src/storage/rowset/segment.h` | Storage segment |
| `src/exprs/expr.h` | Expression base class |

## Debugging Tips

### ASAN Build

For memory error detection:
```bash
BUILD_TYPE=ASAN ./run-be-ut.sh
```

### Core Dumps

Enable core dumps for crash analysis:
```bash
ulimit -c unlimited
# Cores will be in the working directory
```

### Logging

Increase verbosity for debugging:
```cpp
// In code
VLOG(1) << "Detailed debug info";

// At runtime
export GLOG_v=1  # Enable VLOG level 1
```

## Configuration and Metrics Guidelines

### Adding/Modifying BE Configuration

BE configuration is defined in `be/src/common/config.h`.

```cpp
// Example: Adding a new config parameter
CONF_mInt64(my_new_config, "100");  // mutable, default 100
CONF_Int64(my_static_config, "50"); // immutable, default 50
CONF_Bool(enable_feature, "false"); // boolean config
```

**When adding/modifying BE config, you MUST:**

1. Add the config in `be/src/common/config.h`
2. **Update documentation**: `docs/en/administration/management/BE_configuration.md`
3. Document in the markdown:
   - Parameter name
   - Default value
   - Type and valid range
   - Description
   - Whether it's mutable (can be changed at runtime)
   - Whether restart is required

### Adding/Modifying BE Metrics

BE metrics are defined in `be/src/util/starrocks_metrics.h`.

```cpp
// Example: Adding a new metric
METRIC_DEFINE_INT_COUNTER(my_counter, MetricUnit::REQUESTS);
METRIC_DEFINE_INT_GAUGE(my_gauge, MetricUnit::BYTES);
```

**When adding/modifying BE metrics, you MUST:**

1. Define metric in appropriate file (usually `starrocks_metrics.h`)
2. **Update documentation**: `docs/en/administration/management/monitoring/metrics.md`
3. Document in the markdown:
   - Metric name
   - Type (Counter/Gauge/Histogram)
   - Unit
   - Description of what it measures
   - Labels (if any)

## PR Guidelines for BE Changes

### PR Title
```
[Type] Brief description of BE change
```
Example: `[BugFix] Fix memory leak in hash join operator`

### PR Checklist for BE Changes

- [ ] Code follows `.clang-format` style
- [ ] Added/updated unit tests in `be/test/`
- [ ] No memory leaks (test with ASAN build)
- [ ] Performance impact considered for hot paths
- [ ] **If config changed**: Updated `docs/en/administration/management/BE_configuration.md`
- [ ] **If metrics changed**: Updated `docs/en/administration/management/monitoring/metrics.md`

### Before Submitting

```bash
# Format code
clang-format -i src/path/to/file.cpp

# Run relevant tests
./run-be-ut.sh --test YourTest

# Build with ASAN to check for memory issues
BUILD_TYPE=ASAN ./run-be-ut.sh --test YourTest
```

For full PR guidelines, see the root [AGENTS.md](../AGENTS.md#commit--pr-guidelines).

## License Header

All C++ files must have the Apache 2.0 license header:

```cpp
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
```
