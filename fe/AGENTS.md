# AGENTS.md - StarRocks Frontend (FE)

> Frontend-specific guidelines for AI coding agents.
> The FE is written in Java and handles SQL parsing, query optimization, and metadata management.

## Architecture Overview

The Frontend is responsible for:
- **SQL Parsing**: ANTLR-based parser for StarRocks SQL dialect
- **Query Optimization**: Cost-based optimizer with rule-based transformations
- **Metadata Management**: Catalog, database, table, and statistics management
- **Cluster Coordination**: FE leader election, BE management, load balancing

### Module Structure

```
fe/
├── fe-core/           # Core FE logic (optimizer, catalog, execution coordination)
├── fe-parser/         # SQL parser and AST definitions
├── fe-grammar/        # ANTLR grammar files (.g4)
├── fe-type/           # Type system definitions
├── fe-spi/            # Service Provider Interfaces
├── fe-utils/          # General-purpose utilities (no business logic)
├── fe-testing/        # Shared test utilities and mocks
├── connector/         # Data source connector implementations
├── plugin/            # Non-connector plugins (auth, audit, etc.)
└── spark-dpp/         # Spark DPP application
```

### Key Packages (fe-core)

```
com.starrocks/
├── sql/
│   ├── parser/       # SQL parsing utilities
│   ├── ast/          # Abstract Syntax Tree nodes
│   ├── analyzer/     # Semantic analysis
│   └── optimizer/    # Query optimization
├── planner/          # Physical plan generation
├── catalog/          # Metadata (databases, tables, partitions)
├── qe/               # Query execution coordination
├── privilege/        # Authentication and authorization
├── scheduler/        # Task scheduling
├── load/             # Data loading
└── backup/           # Backup and restore
```

## Build Commands

```bash
# Build FE only
./build.sh --fe

# Clean and build
./build.sh --fe --clean

# Build FE using Maven directly
cd fe && mvn package -DskipTests

# Build specific module
cd fe && mvn package -pl fe-core -am -DskipTests
```

### Maven Commands

```bash
cd fe

# Compile
mvn compile

# Run tests
mvn test

# Package without tests
mvn package -DskipTests

# Check code style
mvn checkstyle:check

# Generate compile commands
mvn compile -Dmaven.compiler.showCompilationChanges=true
```

## Code Style

### Checkstyle

FE uses Checkstyle based on Google Java Style. Config: `fe/checkstyle.xml`

```bash
# Check style
cd fe && mvn checkstyle:check

# Check specific module
cd fe && mvn checkstyle:check -pl fe-core
```

### Formatting Rules

- **Indent**: 4 spaces
- **Line limit**: 130 characters
- **Braces**: Same line for control structures
- **Blank lines**: One blank line between methods

### Import Order

Checkstyle enforces this import order:
1. Third-party packages (com.google, org.apache, etc.)
2. Java standard packages (java.*, javax.*)
3. Static imports

```java
// Third-party
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;

// Java standard
import java.util.ArrayList;
import java.util.List;

// Static imports
import static com.starrocks.common.util.Util.checkNotNull;
```

### Naming Conventions

```java
// Classes: PascalCase
public class QueryPlanner { }

// Interfaces: PascalCase (no "I" prefix)
public interface Catalog { }

// Methods: camelCase
public void analyzeStatement(StatementBase stmt) { }

// Variables: camelCase
private final String tableName;

// Constants: UPPER_SNAKE_CASE
public static final int DEFAULT_TIMEOUT = 300;

// Package names: lowercase
package com.starrocks.sql.optimizer;
```

## Testing

### Test Framework

Uses JUnit 5 (Jupiter) with Mockito for mocking.

```bash
# Run all FE tests
./run-fe-ut.sh

# Run specific test class
./run-fe-ut.sh --test com.starrocks.sql.plan.TPCHPlanTest

# Run with filter (skip certain tests)
./run-fe-ut.sh --filter "com.starrocks.sql.dump.QueryDumpRegressionTest"

# Parallel execution (default: 4)
./run-fe-ut.sh -j 8
```

### Writing Tests

```java
// File: fe/fe-core/src/test/java/com/starrocks/sql/plan/MyPlanTest.java
package com.starrocks.sql.plan;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class MyPlanTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        // Additional setup
    }

    @Test
    public void testSimpleSelect() throws Exception {
        String sql = "SELECT * FROM t0 WHERE c1 > 10";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "OlapScanNode");
    }

    @Test
    public void testJoinOrder() {
        runFileUnitTest("optimized-plan/join");  // Runs test from file
    }
}
```

### Test Utilities

- `PlanTestBase`: Base class for plan tests
- `StarRocksAssert`: Utility for creating test objects
- `UtFrameUtils`: Test environment setup

Location: `fe/fe-testing/src/main/java/`

## Key Design Principles

### AST Immutability

**Critical**: AST nodes must be immutable after parsing.

```java
// DO NOT add setters to AST nodes
public class SelectStmt extends QueryStatement {
    private final List<SelectListItem> selectList;  // Final

    // NO: public void setSelectList(...) { }

    // YES: Create new node if modification needed
    public SelectStmt withSelectList(List<SelectListItem> newList) {
        return new SelectStmt(newList, this.fromClause, ...);
    }
}
```

**Reasons:**
- AST should not be used as temporary storage between Parser and Analyzer
- Immutability ensures thread safety
- Facilitates caching and sharing

### SPI Boundaries

Connectors and plugins should only depend on `fe-spi`, not FE internals.

```java
// Good: Implement SPI interface
public class MyConnector implements Connector {
    // Only uses types from fe-spi
}

// Bad: Direct dependency on internal classes
import com.starrocks.catalog.OlapTable;  // Internal!
```

### Utilities (fe-utils)

- Contains only general-purpose helpers
- No business logic or FE internals
- Can be used by any module

## Common Patterns

### Status and Error Handling

Use `StarRocksException` hierarchy for errors:

```java
// Throwing errors
throw new SemanticException("Column not found: " + columnName);
throw new AnalysisException("Invalid syntax");

// Catching and wrapping
try {
    analyze(stmt);
} catch (Exception e) {
    throw new DdlException("Failed to analyze: " + e.getMessage(), e);
}
```

### Visitor Pattern

AST traversal uses the Visitor pattern:

```java
public class MyVisitor extends AstVisitor<Void, Void> {
    @Override
    public Void visitSelectStatement(SelectStmt stmt, Void context) {
        // Process select
        visit(stmt.getFromClause(), context);
        return null;
    }

    @Override
    public Void visitJoinRelation(JoinRelation join, Void context) {
        // Process join
        return null;
    }
}

// Usage
new MyVisitor().visit(ast, null);
```

### Configuration Properties

```java
// Access session variables
ConnectContext ctx = ConnectContext.get();
long timeout = ctx.getSessionVariable().getQueryTimeoutS();

// Access global config
int maxConnections = Config.max_connections;
```

## Extending FE

### Adding New SQL Syntax

1. Update ANTLR grammar in `fe/fe-grammar/`
2. Add AST node in `fe/fe-parser/src/main/java/com/starrocks/sql/ast/`
3. Update parser visitor to create AST node
4. Add semantic analysis in analyzer
5. Add execution logic
6. Add tests

### Adding New Connector

1. Implement interfaces in `fe/fe-spi/`
2. Create connector module in `fe/connector/`
3. Register via ServiceLoader or plugin manifest
4. Add integration tests

### Adding New Function

1. Define function signature in `FunctionSet`
2. Implement evaluation in BE (for scalar functions)
3. Add tests

## Key Files to Know

| File | Purpose |
|------|---------|
| `fe-core/.../sql/StatementPlanner.java` | Main entry for query planning |
| `fe-core/.../sql/optimizer/Optimizer.java` | Query optimizer |
| `fe-core/.../catalog/GlobalStateMgr.java` | Global metadata manager |
| `fe-core/.../qe/StmtExecutor.java` | Statement execution |
| `fe-parser/.../sql/ast/QueryStatement.java` | Query AST base |
| `fe-grammar/StarRocks.g4` | Main SQL grammar |

## RESTful API Guidelines

When adding HTTP endpoints, follow the standard in `docs/en/developers/code-style-guides/restful-api-standard.md`:

```
Format: /api/{version}/{target-object-path}/{action}

GET  /api/v2/catalogs/hive/databases           # List databases
GET  /api/v2/catalogs/hive/databases/tpch      # Get database details
POST /api/v2/catalogs/hive/databases           # Create database
DELETE /api/v2/catalogs/hive/databases/tpch    # Delete database
```

## Performance Tips

- Avoid creating objects in hot paths
- Use `Preconditions` for early validation
- Prefer `ImmutableList/Map` for readonly collections
- Use appropriate collection types (ArrayList vs LinkedList)
- Be careful with synchronized blocks

## Debugging

### Logging

Use Log4j2:

```java
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MyClass {
    private static final Logger LOG = LogManager.getLogger(MyClass.class);

    public void doSomething() {
        LOG.info("Processing started");
        LOG.debug("Details: {}", details);  // Lazy evaluation
        LOG.warn("Slow query: {}", queryId);
    }
}
```

### Remote Debugging

```bash
# Start FE with debug port
export JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
./fe/bin/start_fe.sh
```

## Configuration and Metrics Guidelines

### Adding/Modifying FE Configuration

FE configuration is defined in `fe/fe-core/src/main/java/com/starrocks/common/Config.java`.

```java
// Example: Adding a new config parameter
@ConfField(mutable = true, description = "Description of my config")
public static int my_new_config = 100;  // mutable, default 100

@ConfField(description = "Description of static config")
public static long my_static_config = 50;  // immutable

@ConfField(mutable = true, varType = VariableAnnotation.EXPERIMENTAL)
public static boolean enable_experimental_feature = false;
```

**When adding/modifying FE config, you MUST:**

1. Add the config in `Config.java` with `@ConfField` annotation
2. **Update documentation**: `docs/en/administration/management/FE_configuration.md`
3. Document in the markdown:
   - Parameter name
   - Default value
   - Type and valid range
   - Description
   - Whether it's mutable (`mutable = true`)
   - Whether restart is required

### Adding/Modifying FE Metrics

FE metrics are typically registered via metric registry classes.

```java
// Example: Adding a new metric
public static final LongCounterMetric MY_COUNTER =
    new LongCounterMetric("my_counter", Metric.MetricUnit.REQUESTS, "description");

public static final GaugeMetric<Long> MY_GAUGE =
    new GaugeMetric<>("my_gauge", Metric.MetricUnit.BYTES, "description") {
        @Override
        public Long getValue() {
            return someValue;
        }
    };
```

**When adding/modifying FE metrics, you MUST:**

1. Register metric in appropriate class
2. **Update documentation**: `docs/en/administration/management/monitoring/metrics.md`
3. Document in the markdown:
   - Metric name
   - Type (Counter/Gauge/Histogram)
   - Unit
   - Description of what it measures
   - Labels (if any)

## PR Guidelines for FE Changes

### PR Title
```
[Type] Brief description of FE change
```
Example: `[Feature] Add support for PIVOT clause in SQL`

### PR Checklist for FE Changes

- [ ] Code passes `mvn checkstyle:check`
- [ ] Added/updated unit tests in `fe/fe-core/src/test/`
- [ ] No new checkstyle violations
- [ ] AST nodes remain immutable (no setters added)
- [ ] **If config changed**: Updated `docs/en/administration/management/FE_configuration.md`
- [ ] **If metrics changed**: Updated `docs/en/administration/management/monitoring/metrics.md`

### Before Submitting

```bash
cd fe

# Check code style
mvn checkstyle:check

# Run relevant tests
mvn test -Dtest=YourTestClass

# Run all tests (takes longer)
cd .. && ./run-fe-ut.sh
```

### SQL Syntax Changes

If your PR adds/modifies SQL syntax:
1. Update grammar in `fe/fe-grammar/`
2. Add AST nodes in `fe/fe-parser/`
3. Add analyzer/optimizer support
4. Add plan tests in `fe/fe-core/src/test/java/com/starrocks/sql/plan/`
5. Add SQL integration tests in `test/sql/`

For full PR guidelines, see the root [AGENTS.md](../AGENTS.md#commit--pr-guidelines).

## License Header

All Java files must have the Apache 2.0 license header:

```java
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

package com.starrocks.xxx;
```
