# UUID v7 Implementation

This document describes the UUID v7 implementation in StarRocks.

## Overview

UUID v7 is a time-ordered UUID format defined in RFC 9562. It provides better database performance compared to random UUIDs (v4) because it maintains temporal ordering, which improves index locality and reduces fragmentation.

## Functions

### uuid_v7()

Generates a UUID v7 value in standard string format (8-4-4-4-12).

**Returns:** VARCHAR(36)

**Example:**
```sql
SELECT uuid_v7();
-- Returns: 019ba290-15ce-7f13-86b4-ebc97d2f72c0
```

### uuid_v7_numeric()

Generates a UUID v7 value as a 128-bit integer.

**Returns:** LARGEINT

**Example:**
```sql
SELECT uuid_v7_numeric();
-- Returns: 2088748395792837468371928374619283746
```

## UUID v7 Format

UUID v7 follows RFC 9562 specification:

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       unix_ts_ms (48 bits)                    |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|    unix_ts_ms (cont'd)   |  ver  |      rand_a (12 bits)      |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|var|                     rand_b (62 bits)                      |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                      rand_b (cont'd)                          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

- **unix_ts_ms (48 bits)**: Unix timestamp in milliseconds
- **ver (4 bits)**: Version field, always 7 (0111 in binary)
- **rand_a (12 bits)**: Random data
- **var (2 bits)**: Variant field, always 10 (RFC 4122)
- **rand_b (62 bits)**: Random data

## Usage Examples

### As Default Value

You can use `uuid_v7()` as a default value for columns:

```sql
CREATE TABLE users (
    id VARCHAR(36) DEFAULT uuid_v7(),
    name VARCHAR(100),
    created_at DATETIME
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 4;

INSERT INTO users(name, created_at) VALUES 
    ('Alice', NOW()),
    ('Bob', NOW());

SELECT id, name FROM users;
```

### As Numeric Default Value

```sql
CREATE TABLE events (
    event_id LARGEINT DEFAULT uuid_v7_numeric(),
    event_type VARCHAR(50),
    event_time DATETIME
) DUPLICATE KEY(event_id)
DISTRIBUTED BY HASH(event_id) BUCKETS 4;
```

## Benefits

1. **Time-ordered**: UUIDs generated later will sort after earlier ones
2. **Improved Index Performance**: Better locality in B-tree indexes
3. **Reduced Fragmentation**: Sequential inserts cause less page splits
4. **Unique**: Random bits ensure uniqueness even within the same millisecond
5. **Standard Compliant**: Follows RFC 9562

## Implementation Details

### Backend (C++)

- Location: `be/src/util/uuid_generator.h`
- Generator: `ThreadLocalUUIDGenerator::next_uuid_v7()`
- Thread-safe random number generation using `std::mt19937`

### Function Registration

- Function definitions: `gensrc/script/functions.py`
- Function IDs: 100025 (uuid_v7), 100026 (uuid_v7_numeric)

### Frontend (Java)

- Constants: `fe/fe-core/src/main/java/com/starrocks/catalog/FunctionSet.java`
- Non-deterministic function marking ensures proper query optimization

## Testing

Comprehensive tests ensure UUID v7 compliance:

1. **Unit Tests** (`be/test/util/uuid_generator_test.cpp`):
   - Version and variant field validation
   - Uniqueness verification
   - Time ordering validation
   - Parallel generation testing

2. **Function Tests** (`be/test/exprs/utility_functions_test.cpp`):
   - Format compliance
   - Uniqueness across batches
   - Time ordering

3. **SQL Integration Tests** (`test/sql/test_function/T/test_uuid_v7`):
   - Basic generation
   - Format validation with regex
   - Default value usage in tables
   - Uniqueness in real-world scenarios

## Comparison with UUID v4

| Feature | UUID v4 (uuid()) | UUID v7 (uuid_v7()) |
|---------|------------------|---------------------|
| Ordering | Random | Time-ordered |
| Index Performance | Poor (random inserts) | Good (sequential inserts) |
| Fragmentation | High | Low |
| Timestamp | No | Yes (48-bit ms) |
| Version | 4 | 7 |

## References

- RFC 9562: Universally Unique IDentifiers (UUIDs)
- MariaDB UUID v7 implementation
- DuckDB UUID v7 implementation
