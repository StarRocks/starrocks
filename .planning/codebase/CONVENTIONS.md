# Coding Conventions

## C++ (Backend)

### Style Guide
- **Base**: Google C++ Style with modifications
- **Config**: `.clang-format` at project root
- **Indent**: 4 spaces
- **Line limit**: 120 characters
- **Pointer alignment**: Left (`int* ptr`)

### Format Configuration
```yaml
Language: Cpp
BasedOnStyle: Google
AccessModifierOffset: -4
AllowShortFunctionsOnASingleLine: Inline
ColumnLimit: 120
IndentWidth: 4
PointerAlignment: Left
```

### Naming Conventions
| Type | Pattern | Example |
|------|---------|---------|
| Files | snake_case | `tablet_manager.cpp` |
| Classes | PascalCase | `TabletManager` |
| Functions | snake_case | `get_tablet()` |
| Variables | snake_case | `tablet_id` |
| Constants | kPascalCase | `kMaxTabletCount` |
| Macros | UPPER_CASE | `STARROCKS_ENABLE_AVX2` |
| Enums | EnumType + ENUM_NAME | `StorageType::LOCAL` |

### Code Patterns

#### Header Guards
```cpp
#pragma once
// OR
#ifndef STARROCKS_BE_STORAGE_TABLET_H
#define STARROCKS_BE_STORAGE_TABLET_H
// ...
#endif
```

#### Class Structure
```cpp
class Tablet {
public:
    // Public types
    using TabletSharedPtr = std::shared_ptr<Tablet>;
    
    // Public methods
    Status load();
    
private:
    // Private members with _ prefix
    int64_t _tablet_id;
    std::string _tablet_path;
};
```

#### Error Handling
```cpp
Status do_something() {
    Status st = check_condition();
    if (!st.ok()) {
        LOG(WARNING) << "Failed to check: " << st;
        return st;
    }
    return Status::OK();
}
```

#### Memory Management
- Prefer `std::unique_ptr` for ownership
- Use `std::shared_ptr` for shared ownership
- Raw pointers for non-owning references

### Protobuf Conventions
- Message names: `PascalCasePB` (e.g., `TabletPB`)
- Field names: `snake_case`
- **Never** use `required` fields
- **Never** change field numbers

### Thrift Conventions
- Struct names: `TPascalCase` (e.g., `TTabletInfo`)
- Field names: `snake_case`
- **Never** use `required` fields

## Java (Frontend)

### Style Guide
- **Base**: Google Java Style with modifications
- **Config**: `fe/checkstyle.xml`
- **Indent**: 4 spaces
- **Line limit**: 130 characters
- **Import order**: Third-party, Java standard, static

### Format Configuration
```xml
<module name="LineLength">
    <property name="max" value="130"/>
</module>
```

### License Header
All Java files must include:
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
```

### Naming Conventions
| Type | Pattern | Example |
|------|---------|---------|
| Classes | PascalCase | `QueryOptimizer` |
| Methods | camelCase | `optimizeQuery()` |
| Variables | camelCase | `tableName` |
| Constants | UPPER_SNAKE | `MAX_TABLET_NUM` |
| Packages | lowercase | `com.starrocks.sql.optimizer` |

### Code Patterns

#### Null Safety
```java
// Prefer Optional
Optional<Table> findTable(long id);

// Or explicit null checks
if (table == null) {
    throw new SemanticException("Table not found");
}
```

#### Exception Handling
```java
try {
    processQuery();
} catch (AnalysisException e) {
    LOG.warn("Failed to analyze query", e);
    throw new SemanticException(e.getMessage());
}
```

#### Collections
```java
// Prefer immutable
List<String> names = ImmutableList.of("a", "b");
Map<Long, Tablet> tabletMap = Maps.newHashMap();

// Stream operations
List<Long> ids = tablets.stream()
    .map(Tablet::getId)
    .collect(Collectors.toList());
```

### Module Organization

```
fe/fe-core/src/main/java/com/starrocks/
├── server/          # Main server, global state
├── sql/             # SQL processing
│   ├── analyzer/    # Semantic analysis
│   ├── optimizer/   # CBO optimizer
│   └── planner/     # Physical planning
├── catalog/         # Metadata management
├── connector/       # External connectors
├── load/            # Data loading
├── transaction/     # Transaction management
└── common/          # Utilities
```

## SQL Conventions

### Keywords
- Use UPPERCASE for SQL keywords: `SELECT`, `FROM`, `WHERE`
- Lowercase for identifiers: `table_name`, `column_name`

### Formatting
```sql
SELECT 
    column1,
    column2,
    column3
FROM table_name
WHERE condition1
  AND condition2
GROUP BY column1
HAVING count(*) > 10;
```

## Git Conventions

### Commit Messages
- Write in English
- Start with verb in imperative mood
- Format: `[Type] Description`

### PR Title Format
| Type | Usage |
|------|-------|
| `[BugFix]` | Bug fixes |
| `[Feature]` | New features |
| `[Enhancement]` | Improvements |
| `[Refactor]` | Code refactoring |
| `[UT]` | Unit tests |
| `[Doc]` | Documentation |
| `[Tool]` | Tooling changes |

Example: `[BugFix] Fix memory leak in hash join operator`

### Commit Example
```
[Feature] Add ARRAY_AGG function

Support ARRAY_AGG aggregate function with:
- DISTINCT option
- ORDER BY clause
- NULL handling

Fixes #12345
```

## Documentation

### Configuration Changes
Update docs when adding/modifying configs:
| Component | Documentation File |
|-----------|-------------------|
| FE Config | `docs/en/administration/management/FE_configuration.md` |
| BE Config | `docs/en/administration/management/BE_configuration.md` |

### Metrics Changes
| Metrics Type | Documentation File |
|--------------|-------------------|
| General | `docs/en/administration/management/monitoring/metrics.md` |
| Shared-Data | `docs/en/administration/management/monitoring/metrics-shared-data.md` |
| MV Metrics | `docs/en/administration/management/monitoring/metrics-materialized_view.md` |

---
*Mapped: 2026-03-18*
