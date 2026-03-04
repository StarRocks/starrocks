# Testing Patterns

**Analysis Date:** 2026-03-04

## Overview

StarRocks has three distinct test layers:
1. **BE Unit Tests** — C++ Google Test in `be/test/`
2. **FE Unit Tests** — Java JUnit 5 + JMockit in `fe/fe-core/src/test/`
3. **SQL Integration Tests** — Python SQL-tester framework in `test/`

---

## BE Unit Tests (C++)

### Test Framework

**Runner:** Google Test (gtest)
**Config:** `be/CMakeLists.txt` (built with `MAKE_TEST=ON`)
**Test utilities:** `be/src/testutil/` and `be/src/base/testutil/`

**Run Commands:**
```bash
./run-be-ut.sh                                      # Run all BE tests
./run-be-ut.sh --test CompactionUtilsTest           # Run specific test suite
./run-be-ut.sh --gtest_filter "TabletUpdatesTest*"  # Filter with pattern
BUILD_TYPE=ASAN ./run-be-ut.sh                      # Run with address sanitizer

# Fast iteration — build only one test binary
CMAKE_BUILD_PREFIX=/path/to/build \
STARROCKS_THIRDPARTY=/path/to/thirdparty \
./run-be-ut.sh --build-target column_test --module column_test --without-java-ext

# Build column_test quickly (minimal deps)
STARROCKS_LINKER=lld STARROCKS_THIRDPARTY=/var/local/thirdparty \
./run-be-ut.sh --clean --with-dynamic --build-target column_test
```

### Test File Organization

**Location:** `be/test/` mirrors `be/src/` structure exactly

```
be/test/
├── base/           # Tests for be/src/base/
├── column/         # Tests for be/src/column/
├── storage/        # Tests for be/src/storage/
├── exec/           # Tests for be/src/exec/
├── exprs/          # Tests for be/src/exprs/
├── runtime/        # Tests for be/src/runtime/
├── connector/      # Tests for be/src/connector/
└── util/           # Tests for be/src/util/
```

**Naming:** `<module>_test.cpp` (e.g., `map_column_test.cpp`, `memtable_test.cpp`, `int256_test.cpp`)

**Test binary grouping:** Related tests compile into named binaries (`column_test`, `base_test`, `storage_test`, etc.) to enable fast targeted builds.

### Test Structure

**Fixture-based tests (shared state or setup/teardown):**
```cpp
// File: be/test/storage/tablet_test.cpp
#include "storage/tablet.h"  // corresponding header first
#include <gtest/gtest.h>

namespace starrocks {

class TabletTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Per-test setup
    }
    void TearDown() override {
        // Per-test cleanup
    }
};

TEST_F(TabletTest, CreateTablet) {
    auto tablet = create_test_tablet();
    ASSERT_NE(tablet, nullptr);
    EXPECT_EQ(tablet->tablet_id(), 12345);
}

TEST_F(TabletTest, InsertDataStatus) {
    auto tablet = create_test_tablet();
    auto status = tablet->insert(test_data);
    ASSERT_TRUE(status.ok()) << status.message();
}

}  // namespace starrocks
```

**Standalone parallel tests (no fixture):**
```cpp
// Use PARALLEL_TEST macro (from be/src/base/testutil/parallel_test.h)
// Gives each test a unique test case name for gtest-parallel compatibility
#include "base/testutil/parallel_test.h"

// NOLINTNEXTLINE
PARALLEL_TEST(MapColumnTest, test_create) {
    auto column = MapColumn::create(...);
    ASSERT_TRUE(column->is_map());
    ASSERT_EQ(0, column->size());
}
```

**Grouped tests for parallel runners:**
```cpp
GROUP_PARALLEL_TEST(group_name, TestSuite, test_name)
GROUP_TEST_F(group_name, TestSuite, test_name)
GROUP_SLOW_PARALLEL_TEST(TestSuite, test_name)  // skipped in Debug builds
```

### Assertions

**Standard gtest assertions:**
```cpp
ASSERT_EQ(expected, actual);      // Fatal on failure
ASSERT_NE(unexpected, actual);
ASSERT_TRUE(condition);
ASSERT_FALSE(condition);
ASSERT_LT(a, b);
EXPECT_EQ(expected, actual);      // Non-fatal on failure
EXPECT_TRUE(condition);
```

**StarRocks Status assertions** (from `be/src/base/testutil/assert.h`):
```cpp
ASSERT_OK(status_expr);           // ASSERT_TRUE(st.ok()) << st
EXPECT_OK(status_expr);           // EXPECT_TRUE(st.ok()) << st
ASSERT_ERROR(status_expr);        // ASSERT_FALSE(st.ok())
EXPECT_ERROR(status_expr);        // EXPECT_TRUE(!st.ok())
EXPECT_STATUS(expected, actual);  // Compare status codes
ASSIGN_OR_ABORT(lhs, statusor);   // Unwrap StatusOr or abort
CHECK_OK(stmt);                    // Runtime CHECK (not test assertion)
```

### Mocking (BE)

Google Mock (gmock) is bundled with gtest. The BE codebase generally uses test doubles and helper factories rather than extensive gmocking. Sync points are used for concurrency testing:
- `be/src/base/testutil/sync_point.h` — inject synchronization points into production code paths for deterministic testing

### Test Utilities (BE)

| File | Purpose |
|------|---------|
| `be/src/testutil/column_test_helper.h` | Create typed test columns |
| `be/src/testutil/schema_test_helper.h/cpp` | Build tablet schemas |
| `be/src/testutil/tablet_test_helper.h/cpp` | Create test tablets |
| `be/src/testutil/exprs_test_helper.h/cpp` | Build expression trees for tests |
| `be/src/testutil/chunk_assert.h/cpp` | Assert chunk contents |
| `be/src/testutil/desc_tbl_builder.h` | Build descriptor tables |
| `be/src/testutil/init_test_env.h` | Initialize test environment |
| `be/src/base/testutil/assert.h` | Status assertion macros |
| `be/src/base/testutil/parallel_test.h` | `PARALLEL_TEST` macro |
| `be/src/base/testutil/sync_point.h` | Inject sync points for concurrency |
| `be/src/base/testutil/scoped_updater.h` | RAII value restoration in tests |

### Test Initialization (BE)

Many BE tests require environment initialization:
```cpp
// In test_main.cpp or SetUpTestSuite
#include "testutil/init_test_env.h"
// or
#include "testutil/init_config.h"
```

---

## FE Unit Tests (Java)

### Test Framework

**Runner:** JUnit 5 (Jupiter) 5.8.2
**Mocking:** JMockit 1.49.4 (via `com.github.hazendaz.jmockit:jmockit`)
**Utilities:** `fe/fe-testing/src/main/java/` and `fe/fe-core/src/test/java/com/starrocks/utframe/`

**Run Commands:**
```bash
./run-fe-ut.sh                                                         # Run all FE tests
./run-fe-ut.sh --test com.starrocks.sql.plan.TPCHPlanTest              # Run specific class
./run-fe-ut.sh --filter "com.starrocks.sql.dump.QueryDumpRegressionTest"  # Skip specific
./run-fe-ut.sh -j 8                                                    # Run with 8 threads (default: 4)
cd fe && mvn test -Dtest=MyTestClass                                   # Run via Maven
cd fe && mvn checkstyle:check                                          # Check style before test
```

### Test File Organization

**Location:** `fe/fe-core/src/test/java/` (mirrors `fe/fe-core/src/main/java/`)

```
fe/fe-core/src/test/java/
├── com/starrocks/sql/plan/     # SQL plan tests (largest suite)
├── com/starrocks/scheduler/    # Scheduler tests
├── com/starrocks/catalog/      # Catalog/metadata tests
├── com/starrocks/qe/           # Query execution tests
├── com/starrocks/utframe/      # Test framework utilities
└── com/starrocks/common/       # Common utilities (ExceptionChecker)
```

**Naming:** `<ClassName>Test.java` (e.g., `JoinTest.java`, `TaskRunSchedulerTest.java`)

### Test Structure

**Base class pattern (SQL plan tests):**
```java
// File: fe/fe-core/src/test/java/com/starrocks/sql/plan/MyPlanTest.java
package com.starrocks.sql.plan;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class MyPlanTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();  // sets up in-memory cluster with tables
        // Additional setup
        starRocksAssert.withTable("CREATE TABLE ...");
    }

    @Test
    public void testSimpleSelect() throws Exception {
        String sql = "SELECT * FROM t0 WHERE c1 > 10";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "OlapScanNode");
    }

    @Test
    public void testFromFile() {
        runFileUnitTest("optimized-plan/join");  // Runs SQL from resource file
    }
}
```

**Standalone test pattern:**
```java
public class TaskRunSchedulerTest {

    private static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @BeforeEach
    public void setUp() {
        // per-test setup
    }

    @Test
    public void testSchedulePriority() {
        // ...
    }
}
```

### Mocking (FE)

JMockit is used for mocking — uses `Expectations` for strict mocking and `MockUp` for partial mocking:

```java
import mockit.Expectations;
import mockit.MockUp;
import mockit.Mock;

// Expectation-based mocking (JMockit)
new Expectations() {
    {
        GlobalStateMgr.getCurrentState();
        minTimes = 0;
        result = globalStateMgr;

        globalStateMgr.getNextId();
        minTimes = 0;
        returns(100L, 101L, 102L);
    }
};

// Partial mock with MockUp (used in join/plan tests)
new MockUp<SomeClass>() {
    @Mock
    public ReturnType someMethod(ArgType arg) {
        return mockValue;
    }
};
```

### Assertions (FE)

**JUnit 5 assertions:**
```java
import org.junit.jupiter.api.Assertions;

Assertions.assertEquals(expected, actual);
Assertions.assertTrue(condition);
Assertions.assertNotNull(value);
Assertions.assertThrows(ExceptionType.class, () -> {...});
```

**StarRocksAssert plan assertions** (inherited from `PlanTestBase`):
```java
assertContains(plan, "OlapScanNode");
assertContains(plan, "join op: LEFT OUTER JOIN");
assertNotContains(plan, "HASH JOIN");
```

**Exception checking utility** (`fe/fe-core/src/test/java/com/starrocks/common/ExceptionChecker.java`):
```java
ExceptionChecker.expectThrowsNoException(() -> analyze(goodSql));
ExceptionChecker.expectThrows(SemanticException.class, () -> analyze(badSql));
ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Column not found", () -> ddl());
```

### Test Utilities (FE)

| Class | Location | Purpose |
|-------|----------|---------|
| `PlanTestBase` | `fe-core/src/test/java/.../sql/plan/PlanTestBase.java` | Base for plan tests; sets up in-memory cluster with standard tables |
| `PlanTestNoneDBBase` | `fe-core/src/test/java/.../sql/plan/` | Lighter base without default database |
| `PlanWithCostTestBase` | `fe-core/src/test/java/.../sql/plan/` | Base for cost-based optimizer tests |
| `StarRocksAssert` | `fe-core/src/test/java/.../utframe/StarRocksAssert.java` | Fluent API for creating databases, tables, views |
| `UtFrameUtils` | `fe-core/src/test/java/.../utframe/UtFrameUtils.java` | Cluster setup, context creation |
| `ExceptionChecker` | `fe-core/src/test/java/.../common/ExceptionChecker.java` | Assert exception types and messages |
| `fe-testing` module | `fe/fe-testing/src/main/java/` | Shared utilities and mock helpers |

### Setting Up In-Memory Cluster

FE tests run against an in-memory simulated StarRocks cluster:
```java
@BeforeAll
public static void beforeClass() throws Exception {
    FeConstants.runningUnitTest = true;          // disable tablet checks
    UtFrameUtils.createMinStarRocksCluster();    // minimal cluster setup

    ConnectContext ctx = UtFrameUtils.createDefaultCtx();

    // Use StarRocksAssert for DDL
    StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
    starRocksAssert.withDatabase("test").useDatabase("test");
    starRocksAssert.withTable("CREATE TABLE t0 (...) DISTRIBUTED BY HASH(v1) BUCKETS 3 ...");
}
```

---

## SQL Integration Tests

### Test Framework

**Runner:** Python `test/run.py` (SQL-tester framework)
**Requires:** Running StarRocks cluster; Python 3.8+; `pip3 install -r test/requirements.txt`
**Config:** `test/conf/sr.conf`

**Run Commands:**
```bash
cd test

python3 run.py -v                              # Validate all tests
python3 run.py -d sql/test_select -v          # Test specific directory
python3 run.py -d sql/test_select/R/test_basic -v  # Test specific file
python3 run.py -d sql/test_select -r          # Record mode (generate R files)
python3 run.py -l                             # List tests without running
python3 run.py -c 16 -v                       # Run with 16 parallel workers (default: 8)
python3 run.py -t 300 -v                      # Custom timeout (default: 600s)
python3 run.py --case_filter "test_join*" -v  # Filter by case name
```

### Test File Organization

```
test/sql/
├── test_join/
│   ├── T/test_basic_join        # T file: SQL statements
│   └── R/test_basic_join        # R file: SQL + expected results
├── test_agg/
│   ├── T/test_agg_basic
│   └── R/test_agg_basic
└── ...
```

Tests are grouped by feature in `test_<feature>/` directories. Each feature has `T/` (test input) and `R/` (expected results) subdirectories.

### Test File Format

**T files** contain SQL statements with directives:
```sql
-- name: test_basic_select
create database test_db_${uuid0};
use test_db_${uuid0};
create table t1 (c1 int, c2 string) distributed by hash(c1);
insert into t1 values (1, 'a'), (2, 'b');
select * from t1 order by c1;
drop database test_db_${uuid0};
```

**R files** add `-- result:` / `-- !result` blocks:
```sql
-- name: test_basic_select
create database test_db_${uuid0};
use test_db_${uuid0};
create table t1 (c1 int, c2 string) distributed by hash(c1);
insert into t1 values (1, 'a'), (2, 'b');
select * from t1 order by c1;
-- result:
1	a
2	b
-- !result
drop database test_db_${uuid0};
```

### Result Validation Directives

```sql
-- Exact match (default)
select 1;
-- result:
1
-- !result

-- Regex match
select version();
-- result:
[REGEX].*StarRocks.*
-- !result

-- Order-sensitive comparison
[ORDER]select * from t1;
-- result:
1
2
-- !result

-- Skip result check
[UC]show backends;

-- Shell command
shell: curl ${url}/api/health
-- result:
0
-- !result

-- Python helper function
function: wait_load_finish("my_label")

-- Cluster mode tags
-- name: test_native @native   # shared-nothing only
-- name: test_cloud @cloud     # shared-data only
```

### Writing New SQL Tests

1. Create T file: `test/sql/test_<feature>/T/test_<case>`
2. Write test SQL using `${uuid0}` for unique names, clean up at end
3. Generate R file: `python3 run.py -d sql/test_feature/T/test_my_feature -r`
4. Verify: `python3 run.py -d sql/test_feature/R/test_my_feature -v`

**Rules:**
- Always use `${uuid0}` (or `${uuid1}`, etc.) for database/table names to avoid conflicts
- Always drop created objects at test end
- Use `[ORDER]` tag when result order must match exactly
- One thing per test case; descriptive case names
- Prefer minimal schemas — only columns needed for the test

---

## Coverage

**BE:** Coverage build with `WITH_GCOV=ON` CMake option
**FE:** Coverage via Maven Surefire plugin
**No enforced coverage threshold** is configured; focus is on correctness tests

---

## Test Types Summary

| Type | Framework | Location | Scope |
|------|-----------|----------|-------|
| BE Unit | Google Test (gtest) | `be/test/` | Single module/class |
| FE Unit | JUnit 5 + JMockit | `fe/fe-core/src/test/` | SQL planning, optimizer, catalog |
| SQL Integration | Python SQL-tester | `test/sql/` | End-to-end query execution (requires cluster) |

---

## Common Patterns

### BE: Testing Status-returning functions

```cpp
TEST_F(MyTest, OperationSucceeds) {
    auto status = do_something();
    ASSERT_OK(status);
}

TEST_F(MyTest, OperationFails) {
    auto status = do_something_invalid();
    ASSERT_ERROR(status);
}

TEST_F(MyTest, OperationReturnsValue) {
    ASSIGN_OR_ABORT(auto result, get_value());
    ASSERT_EQ(42, result);
}
```

### BE: Testing async/concurrent operations

```cpp
#include "base/testutil/sync_point.h"

// Inject sync points in production code, then control them in tests
SyncPoint::GetInstance()->EnableProcessing();
SyncPoint::GetInstance()->SetCallBack("point_name", [](void* arg) { /* ... */ });
// ... trigger concurrent operation ...
SyncPoint::GetInstance()->DisableProcessing();
SyncPoint::GetInstance()->ClearAllCallBacks();
```

### FE: Testing SQL query plans

```java
@Test
public void testJoinOrder() throws Exception {
    // Assert plan shape
    String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4";
    String plan = getFragmentPlan(sql);
    assertContains(plan, "HASH JOIN");
    assertContains(plan, "join op: INNER JOIN");
}

@Test
public void testExplainOutput() throws Exception {
    // Verbose plan
    String plan = getVerboseExplain(sql);
    assertContains(plan, "pred: [1: v1, BIGINT, true] > 10");
}
```

### FE: Testing DDL exceptions

```java
@Test
public void testDuplicateColumn() {
    ExceptionChecker.expectThrowsWithMsg(
        AnalysisException.class,
        "Duplicate column name",
        () -> starRocksAssert.withTable("CREATE TABLE bad (c1 int, c1 int) ...")
    );
}
```

---

*Testing analysis: 2026-03-04*
