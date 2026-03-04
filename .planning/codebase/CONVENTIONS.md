# Coding Conventions

**Analysis Date:** 2026-03-04

## Overview

StarRocks has two major language environments with distinct conventions:
- **Backend (BE)**: C++ with Google C++ Style, config at `.clang-format`
- **Frontend (FE)**: Java with Google Java Style, config at `fe/checkstyle.xml`

---

## C++ Backend Conventions (`be/src/`)

### Naming Patterns

**Classes:**
- PascalCase: `TabletManager`, `ChunkSink`, `AsyncFlushStreamPoller`

**Functions and methods:**
- snake_case: `create_tablet()`, `get_data()`, `upgrade_if_overflow()`

**Member variables:**
- Underscore prefix + snake_case: `_tablet_id`, `_path`, `_chunk_sink`

**Constants:**
- ALL_CAPS_UNDERSCORES: `constexpr int64_t MAX_SEGMENT_SIZE = 1024 * 1024 * 1024;`

**Namespaces:**
- Lowercase: `namespace starrocks { }`, `namespace starrocks::connector { }`

**Template type parameters:**
- Single uppercase letter or PascalCase: `T`, `KeyT`, `ValueT`

### Code Style

**Formatting:**
- Tool: `clang-format` (run: `clang-format -i <file.cpp>`)
- Config: `.clang-format` at project root
- Base style: Google C++ Style
- Indent: 4 spaces
- Line limit: 120 characters
- Pointer alignment: Left (`int* ptr`, not `int *ptr`)
- Short inline functions allowed on single line
- Constructor initializer indent: 8 spaces (double of indent)

**Linting:**
- `// NOLINTNEXTLINE` suppresses clang-tidy on the following line (used before `PARALLEL_TEST` macros)

### Header Guards

Use `#pragma once` exclusively:
```cpp
#pragma once
// Never use traditional include guards (#ifndef ... #define ... #endif)
```

### Include Order

For `.cpp` files, strictly follow this order:
1. Corresponding header file (e.g., `"storage/tablet.h"` in `tablet.cpp`)
2. C system headers (`<sys/types.h>`)
3. C++ standard library headers (`<memory>`, `<vector>`)
4. Third-party library headers (`<glog/logging.h>`, `<fmt/format.h>`)
5. StarRocks internal headers (`"base/status.h"`, `"storage/rowset.h"`)

### License Header

Every C++ file begins with:
```cpp
// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// ...
```

### Error Handling

**Primary mechanism:** `Status` and `StatusOr<T>` from `be/src/base/status.h`

```cpp
// Functions that can fail return Status
Status do_something() {
    RETURN_IF_ERROR(validate_input());
    if (condition_failed) {
        return Status::InvalidArgument("reason");
    }
    return Status::OK();
}

// Use StatusOr<T> to return value or error
StatusOr<TabletSharedPtr> get_tablet(int64_t tablet_id) {
    auto tablet = _find_tablet(tablet_id);
    if (tablet == nullptr) {
        return Status::NotFound("tablet not found");
    }
    return tablet;
}

// Caller pattern
auto result = get_tablet(id);
if (!result.ok()) {
    return result.status();
}
auto tablet = std::move(result).value();
```

**Propagation macros** (defined in `be/src/base/`):
- `RETURN_IF_ERROR(stmt)` — early return on non-OK status
- `ASSIGN_OR_RETURN(lhs, rhs)` — assign StatusOr value or return status

### Memory Management

Prefer RAII and smart pointers; raw pointers are non-owning references only:
```cpp
std::unique_ptr<Segment> segment = std::make_unique<Segment>();  // ownership
using TabletSharedPtr = std::shared_ptr<Tablet>;                 // shared ownership
void process(const Tablet* tablet);                              // non-owning ref
```

### Logging

Use Google Logging (glog) via `be/src/common/logging.h` (or `be/src/gutil/logging.h` inside gutil):
```cpp
LOG(INFO) << "Starting tablet " << tablet_id;
LOG(WARNING) << "Slow query detected: " << query_id;
LOG(ERROR) << "Failed to open file: " << path;
VLOG(1) << "Verbose debug info";
CHECK(ptr != nullptr) << "Pointer must not be null";  // aborts on failure
DCHECK(condition) << "Debug-only assertion";           // only in debug builds
```

### Comments

- Block comments use `//` (not `/* */`) except for doxygen-style class/function docs
- `// NOLINTNEXTLINE` suppresses the next clang-tidy warning

### Module Boundaries (strictly enforced)

| Module | May depend on |
|--------|--------------|
| `be/src/base` | `gen_cpp/*`, `gutil/*`, system, third-party only |
| `be/src/gutil` | `gutil/*`, system, third-party only |
| `be/src/common` | `base/*`, `gutil/*`, `gen_cpp/*`, system, third-party |
| `be/src/column` (ColumnCore) | TypesCore, Common, Base, Gutil, gen_cpp only |
| `be/src/runtime` (RuntimeCore) | ChunkCore, ColumnCore, TypesCore, Common, Base, Gutil |
| `be/src/exprs` (ExprCore) | RuntimeCore and its transitive deps |

Circular dependencies are strictly forbidden. No higher-level module headers from lower-level modules.

### Data Processing Patterns

Always process columnar data in batches via `Column`/`Chunk` abstractions:
```cpp
// Column: a typed array of values
ColumnPtr column = Int32Column::create();
column->append(1);

// Chunk: a batch of columns
ChunkPtr chunk = std::make_unique<Chunk>();
chunk->append_column(column, slot_id);
```

Use `down_cast<>` (not `dynamic_cast`) when type is known statically:
```cpp
auto* data = down_cast<const Int32Column*>(input)->get_data().data();
```

---

## Java Frontend Conventions (`fe/`)

### Naming Patterns

**Classes and interfaces:**
- PascalCase: `QueryPlanner`, `TaskRunScheduler`, `GlobalStateMgr`
- No "I" prefix for interfaces: `Catalog`, `Connector` (not `ICatalog`)

**Methods:**
- camelCase (at least 2 chars): `analyzeStatement()`, `getFragmentPlan()`
- Regex enforced: `^[a-z][a-z0-9][a-zA-Z0-9_]*$`

**Variables and parameters:**
- camelCase: `tableName`, `connectContext`, `queryTimeoutS`

**Constants:**
- UPPER_SNAKE_CASE: `public static final int DEFAULT_TIMEOUT = 300;`

**Package names:**
- All lowercase: `com.starrocks.sql.optimizer`

**Type parameters:**
- Single letter or PascalCase ending in T: `T`, `KeyT`, `ValueT`

### Code Style

**Formatting:**
- Tool: `mvn checkstyle:check` (enforced in CI)
- Config: `fe/checkstyle.xml` (based on Google Java Style)
- Indent: 4 spaces; line wrap indent: 8 spaces
- Line limit: 130 characters (imports excluded)
- No tab characters allowed

**Required braces:**
- All control structures (`if`, `for`, `while`, `do`, `else`) require braces — enforced by Checkstyle

**Whitespace:**
- Space after commas, semicolons, keywords
- Space around operators and assignments

**Annotations:**
- Each annotation on its own line (except multiple annotations on variable declarations)
- Modifier order enforced (e.g., `public static final` not `static public final`)

### Import Order

Checkstyle enforces this exact order with blank lines between groups:
1. Third-party packages (com.google, org.apache, etc.) — alphabetical
2. Java standard packages (`java.*`, `javax.*`) — alphabetical
3. Static imports — alphabetical

Star imports (`import com.example.*`) are forbidden.

```java
// Correct order:
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;

import java.util.ArrayList;
import java.util.List;

import static com.starrocks.common.util.Util.checkNotNull;
```

### License Header

Every Java file begins with:
```java
// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// ...
```
(Checkstyle enforces this via `RegexpHeader`; ASF license header also accepted for contributed files.)

### Error Handling

Use the `StarRocksException` hierarchy — never catch and swallow:
```java
throw new SemanticException("Column not found: " + columnName);
throw new AnalysisException("Invalid syntax");

try {
    analyze(stmt);
} catch (Exception e) {
    throw new DdlException("Failed to analyze: " + e.getMessage(), e);
}
```

Use `ExceptionChecker` in tests for asserting exception types and messages:
```java
ExceptionChecker.expectThrows(SemanticException.class, () -> analyze(badSql));
ExceptionChecker.expectThrowsWithMsg(DdlException.class, "expected msg", () -> ddl());
```

### Logging

Use Log4j2 with lazy evaluation:
```java
private static final Logger LOG = LogManager.getLogger(MyClass.class);

LOG.info("Processing started");
LOG.debug("Details: {}", details);   // {} placeholders for lazy evaluation
LOG.warn("Slow query: {}", queryId);
LOG.error("Failed with error", exception);
```

### AST Immutability (Critical FE Rule)

AST nodes must be immutable after parsing. Never add setters:
```java
// Correct: create new node for modification
public SelectStmt withSelectList(List<SelectListItem> newList) {
    return new SelectStmt(newList, this.fromClause, ...);
}
// Wrong: never do this
// public void setSelectList(List<SelectListItem> list) { ... }
```

### Visitor Pattern

AST traversal uses `AstVisitor<R, C>`:
```java
public class MyVisitor extends AstVisitor<Void, Void> {
    @Override
    public Void visitSelectStatement(SelectStmt stmt, Void context) {
        visit(stmt.getFromClause(), context);
        return null;
    }
}
new MyVisitor().visit(ast, null);
```

### Configuration Access

```java
// Session variables
ConnectContext ctx = ConnectContext.get();
long timeout = ctx.getSessionVariable().getQueryTimeoutS();

// Global config (defined in Config.java with @ConfField)
int maxConnections = Config.max_connections;
```

### SPI Boundaries

Connectors and plugins must only depend on `fe-spi` module, never on FE internals:
```java
// Good
import com.starrocks.connector.ConnectorContext;  // from fe-spi

// Bad
import com.starrocks.catalog.OlapTable;  // internal FE class
```

### Collections

- Prefer `ImmutableList`/`ImmutableMap` for readonly collections
- Use `Preconditions.checkNotNull()` for null guards in hot paths
- `ArrayList` over `LinkedList` unless insertion/deletion at head is frequent

### Comments

- Javadoc (`/** */`) on public classes and methods when documentation adds value
- Inline comments for non-obvious logic
- `// TODO` and `// FIXME` for deferred work (tracked by Grep in CI reviews)

---

## Generated Code Conventions (`gensrc/`)

### Protobuf (`.proto` files)
- Message names: `PascalCasePB` (e.g., `TabletSchemaPB`, `OlapTableIndexPB`)
- Field names: `snake_case`
- **Never** use `required` fields (breaks forward compatibility)
- **Never** change field ordinals (breaks serialization)

### Thrift (`.thrift` files)
- Struct names: `TPascalCase` (e.g., `TTabletInfo`, `TCreateTabletReq`)
- Field names: `snake_case`
- **Never** use `required` fields
- **Never** change field ordinals

---

## REST API Conventions

Format: `/api/{version}/{target-object-path}/{action}`

```
GET    /api/v2/catalogs/hive/databases           # List
GET    /api/v2/catalogs/hive/databases/tpch      # Get single
POST   /api/v2/catalogs/hive/databases           # Create
DELETE /api/v2/catalogs/hive/databases/tpch      # Delete
```

---

## Configuration and Metrics Documentation

When adding/modifying configs or metrics, documentation updates are mandatory:

| What changed | Update |
|-------------|--------|
| FE config (`Config.java` `@ConfField`) | `docs/en/administration/management/FE_configuration.md` |
| BE config (`be/src/common/config.h` `CONF_*`) | `docs/en/administration/management/BE_configuration.md` |
| Any metric | `docs/en/administration/management/monitoring/metrics.md` |

---

*Convention analysis: 2026-03-04*
