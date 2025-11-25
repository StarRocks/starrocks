# Materialized View Feature Testing Summary

## Overall Status

**Status**: In-Development

**Test Distribution**:

- Schema & Compiler
- Unit Tests (Compare)
- Unit Tests (Render)
- Integration (Reflection)
- Integration (Autogenerate)
- System (Lifecycle)

**Feature Coverage**:

- MV Definition (String & Selectable)
- Attributes (Comment, Refresh Strategy, Partition, Distribution, Order By, Properties)
- DDL Compilation (CREATE/ALTER/DROP)
- Database Reflection (Full metadata extraction)
- Alembic Integration (Autogenerate, upgrade, downgrade)

---

## Test Files Overview

| Test Type            | File Path                                   | Module              |
| -------------------- | ------------------------------------------- | ------------------- |
| Schema & Compiler    | `test/sql/test_compiler_mv.py`              | SQL Compilation     |
| Unit - Ops & Compare | `test/unit/test_compare_mvs.py`             | Autogenerate Logic  |
| Unit - Render        | `test/unit/test_render_mvs.py`              | Script Rendering    |
| Reflection           | `test/integration/test_reflection_mv.py`    | Database Reflection |
| Integration          | `test/integration/test_autogenerate_mvs.py` | End-to-End          |
| System               | `test/system/test_mv_lifecycle.py`          | Lifecycle Testing   |

---

## 1. Compiler Tests

**File**: `test/sql/test_compiler_mv.py`

### CREATE MATERIALIZED VIEW

**Simple Cases**:

- Basic MV creation with a simple SELECT statement

**Coverage Cases** (Each attribute tested independently):

- Options: `OR REPLACE`, `IF NOT EXISTS`, schema qualification
- Definition from Selectable object
- Comment attribute
- Refresh strategy: `ASYNC`, `MANUAL`, `DEFERRED`, `IMMEDIATE` with `START WITH` / `EVERY` clauses
- Partition by: simple column, expression-based (`date_trunc`)
- Distributed by: `HASH`, `RANDOM`, multiple columns, bucket number
- Order by: single and multiple columns
- Properties: `replication_num`, `storage_medium`, etc.

**Complex Cases**:

- Combined attributes (e.g., partition + distribution + refresh + properties)
- Complex queries: JOIN, aggregation, GROUP BY, HAVING, ORDER BY
- Special characters in definition and properties

### ALTER MATERIALIZED VIEW

**Coverage Cases**:

- Rename MV
- Change refresh strategy
- Set/change properties
- **Note**: Test for unsupported changes (e.g., altering definition) should ensure proper exceptions or warnings are raised.

### DROP MATERIALIZED VIEW

**Simple Cases**:

- Basic DROP MV

**Coverage Cases**:

- `IF EXISTS` clause
- Schema qualification

---

## 2. Compare Tests

**File**: `test/unit/test_compare_mvs.py`

### CREATE MATERIALIZED VIEW

**Simple Cases**:

- Basic MV creation (metadata has, db not)

**Coverage Cases** (Detecting new MVs with specific attributes):

- With comment
- With refresh strategy
- With partitioning
- With distribution
- With order by
- With properties

**Complex Cases**:

- Comprehensive MV: Detect a new MV that combines all attributes (comment, refresh strategy, partitioning, distribution, order by, and properties).

### DROP MATERIALIZED VIEW

**Simple Cases**:

- Basic drop operation (db has, metadata not)

**Coverage Cases**:

- Drop an MV with all attributes set, ensuring reflection for downgrade operation is complete.

### ALTER MATERIALIZED VIEW

**Simple Cases**:

- Definition change (NotImplementedError)

**Coverage Cases** (Detecting attribute changes):

- Refresh strategy changes: none→value, value→different, value→none
- Properties changes: none→value, value→different, value→none, partial changes
- **Immutable attribute changes** (should trigger DROP/CREATE, not ALTER):
  - Partition by change
  - Distributed by change
  - Order by change
- Comment change (NotImplementedError, as StarRocks does not support `ALTER MV ... COMMENT`)

### No Change

**Coverage Cases**:

- Identical MVs (metadata matches database)
- SQL normalization (whitespace/casing differences in definition ignored)

---

## 3. Render Tests

**File**: `test/unit/test_render_mvs.py`

### CREATE MATERIALIZED VIEW Rendering

**Simple Cases**:

- Basic rendering

**Coverage Cases** (Each attribute rendered independently):

- With schema
- With comment
- With full refresh strategy
- With partition by
- With distributed by
- With order by
- With properties

**Complex Cases**:

- All attributes combined
- Special character escaping (in definition, comments, properties)

### DROP MATERIALIZED VIEW Rendering

**Simple Cases**:

- Basic rendering

**Coverage Cases**:

- With schema
- With `if_exists`

### ALTER MATERIALIZED VIEW Rendering

**Simple Cases**:

- Render rename operation
- Render refresh strategy change
- Render properties change

**Complex Cases**:

- Render multiple changes in one operation (e.g., refresh and properties)

### Reverse Operations

**Coverage Cases**:

- CreateMaterializedViewOp → DropMaterializedViewOp
- AlterMaterializedViewOp → AlterMaterializedViewOp (with swapped attributes)
- DropMaterializedViewOp → CreateMaterializedViewOp (ensuring all original attributes are restored)

---

## 4. Reflection Tests

**File**: `test/integration/test_reflection_mvs.py`

### Reflection API

**Simple Cases**:

- Basic MV reflection via `Table.autoload_with`

**Coverage Cases** (Each attribute reflected independently):

- Comment
- Refresh strategy (parsing all parts correctly)
- Partition by (simple and expression-based)
- Distributed by (hash/random, buckets)
- Order by
- Properties
- Column types and comments

**Complex Cases**:

- Comprehensive MV with all attributes
- Complex definition (CTE, window functions, joins)
- Case sensitivity handling
- Error handling (non-existent MV)

---

## 5. Autogenerate Integration Tests

**File**: `test/integration/test_autogenerate_mvs.py`

### CREATE MATERIALIZED VIEW

**Simple Cases**:

- Basic CREATE MV

**Coverage Cases**:

- Create MV with all attributes (comment, refresh, partition, distribution, etc.) to verify full reflection and op generation.

### DROP MATERIALIZED VIEW

**Simple Cases**:

- Basic DROP MV

### ALTER MATERIALIZED VIEW

**Coverage Cases**:

- Refresh strategy change (generates `AlterMaterializedViewOp`)
- Properties change (generates `AlterMaterializedViewOp`)
- Change of immutable attributes (e.g., partition, distribution) results in `DropMaterializedViewOp` + `CreateMaterializedViewOp`.

**Known Issues**:

- Test for potential issues in downgrade logic for complex changes.

### Idempotency

**Coverage Cases**:

- No-ops when metadata matches database
- SQL normalization for definition

### Multiple MVs

**Complex Cases**:

- Mixed CREATE/ALTER/DROP operations for multiple MVs in a single migration.

### Filters

**Coverage Cases**:

- `include_object` and `include_name` filters work correctly for MVs.

---

## 6. System Tests

**File**: `test/system/test_mv_lifecycle.py`

### Full Lifecycle Tests

**Goal**: Verify the end-to-end user workflow by generating migration scripts and running `alembic upgrade` and `alembic downgrade`.

**Simple Cases**:

- Create a simple MV, then drop it.

**Complex Cases**:

- **CREATE**: Create a comprehensive MV with partitioning, distribution, refresh strategy, and properties. Verify script content and database state.
- **ALTER**:
  - Change refresh strategy. Verify `alter_materialized_view` is in the script and correctly applied.
  - Change properties.
- **DROP/CREATE**: Change an immutable attribute (e.g., `partition_by`). Verify the migration script contains `drop_materialized_view` and `create_materialized_view` and that the downgrade path works.
- **Idempotency**: Run `alembic revision --autogenerate` again after an upgrade; no new migration should be generated.
- **Mixed Schema Objects and Operations**: Test a single migration that includes:
  - **Tables**: a new table, a dropped table, an altered table (e.g., add column).
  - **Views**: a new view, a dropped view, an altered view.
  - **MVs**: a new comprehensive MV, a dropped MV, an altered MV (refresh/properties).
  - All with an unchanged object (table/view/MV).
- **Special Characters**: Full lifecycle test for an MV with special characters in its definition or properties.
