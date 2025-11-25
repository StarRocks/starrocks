# View Feature Testing Summary

## Overall Status

**Test Coverage**: 112/112 tests (100%)
**Status**: Production-Ready (1 known issue in autogenerate downgrade)

**Test Distribution**:

- Schema & Compiler: 16 tests
- Unit Tests (Compare): 24 tests
- Unit Tests (Render): 24 tests (+5 special character tests)
- Integration (Reflection): 14 tests
- Integration (Autogenerate): 23 tests
- System (Lifecycle): 11 tests (+1 special character test)

**Feature Coverage**:

- View Definition (String & Selectable)
- Column Specification (Column objects, strings, dicts)
- Attributes (Comment, Security, Columns, Schema)
- DDL Compilation (CREATE/ALTER/DROP)
- Database Reflection (Full metadata extraction)
- Alembic Integration (Autogenerate, upgrade, downgrade)

---

## Test Files Overview

| Test Type            | File Path                                     | Module              |
| -------------------- | --------------------------------------------- | ------------------- |
| Schema & Compiler    | `test/sql/test_compiler_view.py`              | SQL Compilation     |
| Unit - Ops & Compare | `test/unit/test_compare_views.py`             | Autogenerate Logic  |
| Unit - Render        | `test/unit/test_render_views.py`              | Script Rendering    |
| Reflection           | `test/integration/test_reflection_view.py`    | Database Reflection |
| Integration          | `test/integration/test_autogenerate_views.py` | End-to-End          |
| System               | `test/system/test_view_lifecycle.py`          | Lifecycle Testing   |

---

## 1. Compiler Tests

**File**: `test/sql/test_compiler_view.py`

### CREATE VIEW

**Simple Cases**:

- Basic view creation

**Coverage Cases**:

- Options: OR REPLACE, IF NOT EXISTS, schema qualification
- Attributes: comment, security (DEFINER/INVOKER)
- Columns: basic definition, with comment, with security, without comment

**Complex Cases**:

- Definition from Selectable object
- Combined attributes (columns + comment + security + schema)

### ALTER VIEW

**Simple Cases**:

- Basic ALTER VIEW

**Coverage Cases**:

- Schema qualification
- Complex queries: JOIN, aggregation, GROUP BY, HAVING, ORDER BY
- Subqueries
- Special characters (backticks, quotes)

**Complex Cases**:

- CTE (WITH clause)
- Window functions (ROW_NUMBER, RANK, OVER)
- Multiple JOINs (INNER, LEFT, RIGHT)
- Aggregation functions (COUNT, AVG, MIN, MAX, SUM)

### DROP VIEW

**Simple Cases**:

- Basic DROP VIEW

**Coverage Cases**:

- IF EXISTS clause

---

## 2. Compare Tests

**File**: `test/unit/test_compare_views.py`

### CREATE VIEW

**Simple Cases**:

- Basic view creation

**Coverage Cases**:

- With comment attribute
- With security attribute
- With columns attribute

**Complex Cases**:

- (Covered in autogenerate integration tests)

### DROP VIEW

**Simple Cases**:

- Basic drop operation

### ALTER VIEW

**Simple Cases**:

- Definition change only

**Coverage Cases**:

- Comment changes: none→value, value→different, no-change (warning logged)
- Security changes: none→INVOKER, INVOKER→none, INVOKER→NONE, no-change (warning logged)
- Columns changes: with definition (allowed), only columns (raises ValueError), no-change

**Complex Cases**:

- Forward/Reverse parameter validation for all change types

### No Change (1 test)

**Coverage Cases**:

- SQL normalization (whitespace differences ignored)

### View Exceptions

**Coverage Cases**:

- Definition validation: missing, invalid type, None
- Selectable definition support
- Columns parameter compatibility

---

## 3. Render Tests

**File**: `test/unit/test_render_views.py`

### CREATE VIEW Rendering

**Simple Cases**:

- Basic rendering

**Coverage Cases** (Each attribute tested independently):

- With schema attribute
- With comment attribute
- With security attribute (DEFINER/INVOKER)
- With columns attribute

**Complex Cases**:

- All attributes combined (schema + comment + security + columns)
- Special character escaping (definition and schema)
- Special characters in definition: Single quotes, double quotes, Backslashes in paths, Multi-line SQL, and Mixed special characters (quotes + backslashes + newlines)

### DROP VIEW Rendering

**Simple Cases**:

- Basic rendering

**Coverage Cases** (Each attribute tested independently):

- With schema attribute
- With if_exists attribute

**Complex Cases**:

- All attributes combined (schema + if_exists)

### ALTER VIEW Rendering

**Simple Cases**:

- Basic rendering (definition only)

**Coverage Cases** (Each attribute tested independently):

- With comment attribute only
- With security attribute only
- With multiple but not all attributes (partial changes - key scenario)

**Complex Cases**:

- All attributes combined (definition + schema + comment + security)

### Reverse Operations

**Coverage Cases**:

- CreateViewOp → DropViewOp
- AlterViewOp → AlterViewOp (with swapped attributes)
- DropViewOp → CreateViewOp (with and without columns)

---

## 4. Reflection Tests

**File**: `test/integration/test_reflection_view.py`

### Reflection API

**Simple Cases**:

- Basic view reflection via `Table.autoload_with`

**Coverage Cases** (Each attribute tested independently):

- Comment attribute reflection
- Security attribute reflection
- Columns reflection (names, comments)
- Column types: INTEGER, VARCHAR, BOOLEAN, DECIMAL, DATE
- Using `inspector.reflect_table()`
- Using `inspector.get_view()` (low-level API)

**Complex Cases**:

- Comprehensive view with all attributes (comment + security + columns + complex definition)
- Complex definition (CTE, window functions, joins, aggregations)
- Case sensitivity handling
- Error handling (non-existent view)

---

## 5. Autogenerate Integration Tests

**File**: `test/integration/test_autogenerate_views.py`

### Test Goals

These integration tests verify the complete autogenerate workflow:

1. **Reflection**: Reading view metadata from database
2. **Comparison**: Detecting differences between metadata and database
3. **Operations**: Generating correct CREATE/ALTER/DROP operations
4. **Execution**: Applying and reverting migrations (upgrade/downgrade)
5. **Filters**: Testing include_object and name_filters configuration

### CREATE VIEW

**Simple Cases**:

- Basic CREATE VIEW (metadata has, db not)

**Coverage Cases**:

- With comment attribute
- With security attribute
- With columns attribute

**Complex Cases**:

- Comprehensive (all attributes: comment + security + columns)

### DROP VIEW

**Simple Cases**:

- Basic DROP VIEW (db has, metadata not)

**Coverage Cases**:

- With attributes (verify reflection for downgrade)

### ALTER VIEW

**Simple Cases**:

- Definition change only

**Coverage Cases**:

- Definition and columns changed together
- Comment change (with warning)
- Security change (with warning)

**Known Issue**:

- Downgrade doesn't restore original definition in one test case

### Idempotency

**Coverage Cases**:

- No-ops when metadata matches database
- SQL normalization (whitespace differences ignored)

### Multiple Views

**Simple Cases**:

- Create multiple views simultaneously
- Drop multiple views simultaneously

**Complex Cases**:

- Mixed CREATE/ALTER/DROP operations

### Filters

**Coverage Cases**:

- `include_object`:
  - Basic functionality for views
  - Excludes regular tables
  - Custom filters handling both tables and views
  - Pattern exclusion (e.g., `tmp_*`)
- `include_name`:
  - Include pattern filtering (e.g., `public_*`)
  - Exclude pattern filtering
  - Filter combination and priority

---

## 6. System Tests

**File**: `test/system/test_view_lifecycle.py`

### CREATE VIEW

**Simple Cases**:

- Basic view creation lifecycle

**Coverage Cases**:

- With columns attribute
- With schema attribute

### ALTER VIEW

**Coverage Cases**:

- Definition change
- Unsupported attribute changes (comment/security) with warning logs
- Columns change detection

### DROP VIEW

**Simple Cases**:

- Basic view deletion lifecycle

### Idempotency

**Coverage Cases**:

- No-ops when metadata matches database

### Multiple Views

**Complex Cases**:

- Mixed CREATE/ALTER/DROP operations in single migration

### Filter Configuration

**Coverage Cases**:

- Custom include*object filter (exclude specific patterns like `tmp*\*`)

### Special Characters

**Complex Cases**:

- View definitions with special characters (single quotes, double quotes, backslashes, newlines)
- Full lifecycle test: render → execute → reflect → idempotency
- Verify proper escaping in migration scripts
- Verify correct execution to database
- Verify accurate reflection back from database

---

## Test Design Principles

### Test Philosophy: Unit vs. Integration vs. System

- **Unit Tests**: Fast, isolated, no database. Verify individual function logic (e.g., `compare.py`, `render.py`).
- **Integration Tests**: Medium speed, require database. Verify interactions between components (e.g., reflection + compare + ops generation). Focus on comprehensive attribute and filter coverage via direct API calls.
- **System Tests**: Slower, require database. Verify the complete, end-to-end user workflow, including migration script generation, content validation, and `alembic upgrade/downgrade` commands.

### Relationship and Focus

System tests and integration tests are complementary, not redundant. System tests validate the user-facing workflow and what gets written to migration scripts, while integration tests provide faster, more granular coverage of all possible attribute combinations at the API level.

```
System Tests (e.g., test_view_lifecycle.py)
├── Goal: Verify full Alembic workflow & script files
└── Focus: User scenarios, script content, versioning

Integration Tests (e.g., test_autogenerate_views.py)
├── Goal: Verify Ops generation & execution via API
└── Focus: Attribute coverage, filter logic, edge cases

Unit Tests (e.g., test_compare_views.py)
├── Goal: Verify individual functions in isolation
└── Focus: Logic, inputs, outputs (no DB)
```

---

## Key Design Decisions

### AlterViewOp: Only Set Changed Attributes

**Design Principle**: `AlterViewOp` only sets attributes that have actually changed, leaving others as `None`.

**Example**:

```python
# Scenario: Only comment changed (definition unchanged)

# Generated AlterViewOp:
AlterViewOp(
    'my_view',
    definition=None,           # Not changed → None
    comment='New comment',     # Changed → set value
    security=None,             # Not changed → None
    existing_definition=None,
    existing_comment='Old comment',
    existing_security=None,
)
```

**Benefits**:

1. **Future-Ready**: When StarRocks supports independent modification of comment/security (without requiring full definition), our implementation is already prepared.
2. **Clear Migration Scripts**: Users can immediately see which attributes changed by looking at the migration file.
3. **Better Testing**: Tests explicitly validate which parameters should be set vs. `None`.

**Current Limitation**: StarRocks currently requires full definition for ALTER VIEW, so comment-only or security-only changes will log warnings. But the design is ready for future StarRocks enhancements.

---

## Related Documentation

### View Feature Design

- `docs/design/view_and_mv.md` - View/MV Design Document
- `docs/usage_guide/views.md` - View Usage Guide
- `docs/usage_guide/alembic.md` - Alembic Integration Guide
