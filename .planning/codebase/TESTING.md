# Testing

## Test Structure Overview

| Test Type | Location | Framework | Command |
|-----------|----------|-----------|---------|
| BE Unit Tests | `be/test/` | Google Test | `./run-be-ut.sh` |
| FE Unit Tests | `fe/fe-core/src/test/` | JUnit 5 | `./run-fe-ut.sh` |
| Java Extensions Tests | `java-extensions/*/src/test/` | JUnit | `./run-java-exts-ut.sh` |
| SQL Integration Tests | `test/sql/` | SQL-tester | `cd test && python3 run.py` |

## Backend (C++) Testing

### Framework
- **Google Test** (`gtest`)
- Test files mirror source structure under `be/test/`
- Naming: `*_test.cpp`

### Directory Structure
```
be/test/
├── types/              # Data type tests
├── column/             # Column structure tests
├── exec/               # Execution operator tests
├── storage/            # Storage engine tests
├── formats/            # File format tests
├── exprs/              # Expression tests
├── runtime/            # Runtime tests
├── util/               # Utility tests
└── testutil/           # Test utilities
```

### Running Tests
```bash
# All BE tests
./run-be-ut.sh

# Specific test
./run-be-ut.sh --test CompactionUtilsTest

# With filter
./run-be-ut.sh --gtest_filter "TabletUpdatesTest*"
```

### Test Example
```cpp
// be/test/storage/tablet_test.cpp
#include <gtest/gtest.h>
#include "storage/tablet.h"

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

TEST_F(TabletTest, test_create_tablet) {
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    Status st = Tablet::create_tablet(tablet_meta, &tablet);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(tablet->tablet_id(), expected_id);
}

} // namespace starrocks
```

## Frontend (Java) Testing

### Framework
- **JUnit 5** (Jupiter)
- **Mockito** for mocking
- Tests in `fe/fe-core/src/test/java/` mirror main source

### Directory Structure
```
fe/fe-core/src/test/java/com/starrocks/
├── sql/               # SQL tests
│   ├── plan/          # Plan tests
│   └── optimizer/     # Optimizer tests
├── catalog/           # Catalog tests
├── connector/         # Connector tests
├── analysis/          # Analysis tests
├── server/            # Server tests
└── utframe/           # Test framework utilities
```

### Running Tests
```bash
# All FE tests
./run-fe-ut.sh

# Specific test
./run-fe-ut.sh --test com.starrocks.sql.plan.TPCHPlanTest

# With filter
./run-fe-ut.sh --filter com.starrocks.sql.plan.Demo#Test0

# Coverage
./run-fe-ut.sh --coverage
```

### Test Example
```java
// fe/fe-core/src/test/java/com/starrocks/sql/plan/PlanTest.java
package com.starrocks.sql.plan;

import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class PlanTest extends PlanTestBase {
    
    @Test
    public void testSelect() throws Exception {
        String sql = "SELECT * FROM t0";
        String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);
        assertContains(plan, "OlapScanNode");
    }
    
    @Test
    public void testJoin() throws Exception {
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "HASH JOIN");
    }
}
```

### Test Utilities

| Utility | Purpose | Location |
|---------|---------|----------|
| `UtFrameUtils` | Test framework setup | `utframe/` |
| `PlanTestBase` | Base class for plan tests | `sql/plan/` |
| `MockedFrontend` | Mock FE for tests | `utframe/` |

## Java Extensions Testing

### Running Tests
```bash
./run-java-exts-ut.sh
```

### Framework
- JUnit 4/5
- Tests in each extension's `src/test/java/`

## SQL Integration Testing

### Framework
- Python-based SQL-tester
- Test cases in `test/sql/`

### Running Tests
```bash
cd test
python3 run.py -v                    # Verbose
python3 run.py -g                    # Generate results
python3 run.py -d sql/test_sql/      # Specific directory
```

### Test File Format
```sql
-- name: test_case_name
SELECT * FROM table;
-- result:
1 | a
2 | b
```

## Test Data

### BE Test Data
- Located in `be/test/*/test_data/`
- Binary and text formats for testing

### FE Test Data
- Mock data created in test setup
- Mini clusters for integration

## Best Practices

### Unit Test Guidelines
1. **One concept per test**: Test one behavior at a time
2. **Fast**: Tests should run quickly (< 1s each)
3. **Isolated**: No dependencies between tests
4. **Repeatable**: Same results every run
5. **Self-validating**: Boolean pass/fail

### Test Naming
- BE: `test_<behavior>_<condition>`
- FE: `test<Behavior><Condition>`

### Assertions
```cpp
// C++
EXPECT_EQ(expected, actual);
EXPECT_TRUE(condition);
ASSERT_STATUS_OK(status);  // Fatal assertion
```

```java
// Java
assertEquals(expected, actual);
assertTrue(condition);
assertThrows(Exception.class, () -> code());
```

### Coverage
- Aim for > 80% code coverage for new code
- Critical paths should have > 90% coverage
- Use coverage reports to identify gaps

## CI Pipeline

Tests run automatically on PR:
- `FE UT`: Frontend unit tests
- `BE UT`: Backend unit tests
- `SQL Test`: Integration tests
- `Build`: Full build verification

---
*Mapped: 2026-03-18*
