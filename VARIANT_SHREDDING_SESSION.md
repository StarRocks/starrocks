# Variant Shredding Implementation - Session Notes

**Date**: 2026-01-20  
**Branch**: `shredding-reading`  
**Status**: ✅ Implementation complete, ready for testing

---

## Overview

Successfully implemented proper handling of **partially shredded objects** in Apache Parquet Variant Shredding format, fixing a critical data loss issue where non-shredded fields (like `name` and `email`) would be lost during reading.

---

## The Problem

According to the [Apache Parquet Variant Shredding spec](https://github.com/apache/parquet-format/blob/master/VariantShredding.md), for **partially shredded objects**, both `value` and `typed_value` can be non-null simultaneously:

- **`value`**: Contains non-shredded fields as raw variant-encoded binary
- **`typed_value`**: Contains shredded fields as typed struct columns
- **No overlap**: The spec guarantees writers won't duplicate fields

### Example from test data:
```
Root object:
├── name: "name_0"     ← NOT shredded (stays in value)
├── email: "user0@..." ← NOT shredded (stays in value)
├── age: 20            ← Shredded (goes to typed_value.age)
├── city: "city_0"     ← Shredded (goes to typed_value.city)
├── score: 80          ← Shredded (goes to typed_value.score)
└── status: "active"   ← Shredded (goes to typed_value.status)
```

**In Parquet file structure:**
```
variant_column {
  metadata: <field dictionary>
  value: <binary variant encoding of {"name": "name_0", "email": "user0@..."}> 
  typed_value: struct {
    age: struct { typed_value: 20 }
    city: struct { typed_value: "city_0" }
    score: struct { typed_value: 80 }
    status: struct { typed_value: "active" }
  }
}
```

**The original implementation only encoded from `typed_value` when both were present**, losing the `name` and `email` fields entirely.

---

## The Solution

### Commit History

**Commit 1**: `567afd8c296` - Add validation and test coverage
- Added spec compliance warnings for non-object types with both fields non-null
- Added comprehensive documentation explaining the 4 valid combinations
- Added SQL test coverage to explicitly query `name` and `email` fields
- Added C++ unit test to validate all variant rows are non-null and well-formed

**Commit 2**: `d4475e72083` - Implement merge logic for partially shredded objects
- Implemented merge logic in `variant_encoder.cpp`
- Updated reader in `complex_column_reader.cpp` to construct wrapper structs
- Properly handles the case where both value and typed_value are non-null

### Implementation Details

#### 1. Merge Logic in `variant_encoder.cpp` (lines 592-680)

```cpp
case TYPE_STRUCT: {
    // Detect shredded wrapper struct (has exactly "value" and "typed_value" fields)
    if (is_shredded_wrapper(struct_col, &typed_idx, &value_idx)) {
        const bool has_typed = /* check if typed_value is non-null */;
        const bool has_value = /* check if value is non-null */;
        
        // Special case: Partially shredded object
        if (has_typed && has_value && typed_value_type.type == TYPE_STRUCT) {
            // 1. Decode value binary to extract non-shredded fields
            VariantValue value_variant(value_slice);
            auto value_obj_info = value_variant.get_object_info();
            
            // 2. Build merged field map
            std::map<uint32_t, std::string> merged_fields;
            
            // 3. Add non-shredded fields from value
            for (uint32_t i = 0; i < value_obj_info.num_elements; ++i) {
                uint32_t field_id = /* read from value binary */;
                std::string field_data = /* extract from value binary */;
                merged_fields[field_id] = field_data;
            }
            
            // 4. Add shredded fields from typed_value (override if conflict)
            for (size_t i = 0; i < typed_struct->fields_size(); ++i) {
                auto encoded_value = encode_value_from_column_row(...);
                merged_fields[field_id] = std::move(encoded_value);
            }
            
            // 5. Encode merged object
            return encode_variant_object_from_map(merged_fields);
        }
        
        // Normal cases: only typed_value OR only value
        if (has_typed) {
            return encode_value_from_column_row(typed_col, ...);
        }
        if (has_value) {
            return std::string(value_slice.data, value_slice.size);
        }
        return encode_variant_null();
    }
    // ... normal struct handling ...
}
```

#### 2. Reader Changes in `complex_column_reader.cpp` (lines 750-807, 831-863)

```cpp
if (has_typed) {
    // Validation: Warn if both non-null for non-objects
    if (!value_slice.empty() && !_typed_value_type.is_struct_type()) {
        VLOG_FILE << "Warning: Both value and typed_value non-null for non-object";
    }

    VariantMetadata meta(metadata_slice);
    RETURN_IF_ERROR(_ctx.use_metadata(meta));
    
    // For partially shredded objects, construct wrapper struct
    if (!value_slice.empty() && _typed_value_type.is_struct_type()) {
        // Create wrapper: struct<value: binary, typed_value: struct<...>>
        Columns fields{value_col, typed_value_col};
        auto wrapper_col = StructColumn::create(fields, {"value", "typed_value"});
        
        TypeDescriptor wrapper_type;
        wrapper_type.type = TYPE_STRUCT;
        wrapper_type.field_names = {"value", "typed_value"};
        wrapper_type.children.push_back(TypeDescriptor(TYPE_VARBINARY));
        wrapper_type.children.push_back(_typed_value_type);
        
        // Pass wrapper to encoder for merging
        auto variant = VariantEncoder::encode_shredded_column_row(
            wrapper_col, wrapper_type, i, &_ctx);
        variant_column->append(variant.value());
    } else {
        // Only typed_value present, encode directly
        auto variant = VariantEncoder::encode_shredded_column_row(
            typed_value_col, _typed_value_type, i, &_ctx);
        variant_column->append(variant.value());
    }
    continue;
}
```

---

## Key Technical Insights

### 1. Mutual Exclusivity Rules (Per Spec)

For a **single row** in the Parquet file:

| Type | value | typed_value | Meaning |
|------|-------|-------------|---------|
| Primitive/Array | null | null | Missing value (invalid) |
| Primitive/Array | non-null | null | Use raw variant encoding |
| Primitive/Array | null | non-null | Use shredded type |
| Primitive/Array | non-null | non-null | **SPEC VIOLATION** (warning logged) |
| Object | null | null | Missing value (valid for object fields) |
| Object | non-null | null | Use raw variant encoding |
| Object | null | non-null | Fully shredded object |
| Object | non-null | non-null | **Partially shredded object** (MERGE REQUIRED) |

### 2. Why Merge is Necessary

Writers may choose to not shred certain fields for various reasons:
- Fields with rare/variable schema (not worth columnar storage)
- Fields that don't compress well in columnar format
- Fields added later (backward compatibility)

These non-shredded fields go into `value` as raw variant encoding, while common/typed fields go into `typed_value` as columnar format. **The reader MUST merge both to reconstruct the complete object.**

### 3. Implementation Strategy

The merge happens in the encoder, not the reader, because:
1. **Encoder already handles variant encoding** - reuse existing logic
2. **Reader provides raw columns** - separating concerns
3. **Wrapper pattern** - encoder detects `struct<value, typed_value>` pattern
4. **Recursive encoding** - typed_value fields may themselves contain nested merges

---

## Test Coverage

### SQL Integration Tests

**Location**: `test/sql/test_iceberg/T/test_iceberg_shredding_variant_query`

**Query 2** (Lines 54-62): Explicitly tests non-shredded fields
```sql
SELECT 
    get_variant_int(data, '$.id') as id,
    get_variant_string(data, '$.name') as name,     -- NOT shredded
    get_variant_string(data, '$.email') as email    -- NOT shredded
FROM test_shredding_variant
ORDER BY id;
```

**Expected Results** (Lines 69-75 in R file):
```
1000	name_0	user0@example.com
1001	name_1	user1@example.com
1002	name_2	user2@example.com
1003	name_3	user3@example.com
1004	name_4	user4@example.com
```

**Query 7** (Lines 133-137): Full variant comparison
- Compares shredded vs non-shredded files field-by-field
- Verifies complete object reconstruction
- Expected: Identical data except field ordering

### C++ Unit Tests

**Location**: `be/test/formats/parquet/file_reader_test.cpp`

**Test**: `TEST_F(FileReaderTest, test_read_variant_shredding_all_fields)`
- Reads full variant column from shredded Parquet file
- Validates all rows are non-null
- Validates all variants are well-formed
- Validates variant data is non-empty

---

## Files Modified

```
M  be/src/formats/parquet/complex_column_reader.cpp   (+67 lines)
M  be/src/util/variant_encoder.cpp                    (+89 lines)
M  be/test/formats/parquet/file_reader_test.cpp       (+40 lines)
M  test/sql/test_iceberg/T/test_iceberg_shredding_variant_query (+20 lines)
M  test/sql/test_iceberg/R/test_iceberg_shredding_variant_query (+13 lines)
```

**Total**: 5 files changed, 229 insertions(+)

---

## How to Test

### 1. Build the Project

```bash
cd /Users/dirlt/repo/StarRocks
# Build command (adjust based on your build system)
# Example:
# ./build.sh --be
```

### 2. Run C++ Unit Tests

```bash
# Run specific test
./output/be/test/file_reader_test --gtest_filter="FileReaderTest.test_read_variant_shredding*"

# Or run all variant-related tests
./output/be/test/file_reader_test --gtest_filter="*variant*"
```

**Expected**: All tests pass, no segfaults or errors.

### 3. Run SQL Integration Tests

**Prerequisites**:
- Iceberg catalog configured
- OSS (Object Storage Service) credentials set up
- Test data files available in `be/test/formats/parquet/test_data/`

```bash
# Run SQL test (command varies by test framework)
./run-sql-test.sh test_iceberg_shredding_variant_query

# Or manually:
cd test/sql/test_iceberg
# Execute T/test_iceberg_shredding_variant_query
# Compare output with R/test_iceberg_shredding_variant_query
```

**Expected Results**:
- Query 2: Returns `name_0` through `name_4` and corresponding emails
- Query 7: Shredded and non-shredded data match (except field order)
- No NULL values for name/email fields
- All variant objects have complete field sets

### 4. Verify Test Data

```bash
# Inspect test Parquet file structure
cd be/test/formats/parquet/test_data

# Check shredded file
parquet-tools schema variant_shredding.parquet
parquet-tools meta variant_shredding.parquet

# Check non-shredded file (for comparison)
parquet-tools schema variant_noshredding.parquet
```

**Expected Schema**:
```
optional group data (VARIANT) {
  required binary metadata;
  optional binary value;
  optional group typed_value {
    required group id { optional binary value; optional int32 typed_value; }
    required group age { optional binary value; optional int32 typed_value; }
    required group city { optional binary value; optional binary typed_value (STRING); }
    required group score { optional binary value; optional int32 typed_value (INT_32); optional binary typed_value (STRING); }
    required group status { optional binary value; optional binary typed_value (STRING); }
    // Note: 'name' and 'email' are NOT in typed_value (they're in value)
    required group profile { ... }
    required group events { ... }
    required group deep_items { ... }
  }
}
```

---

## Debugging Tips

### If name/email fields are NULL:

1. **Check merge logic is triggered**:
   ```cpp
   // Add debug logging in variant_encoder.cpp:598
   LOG(INFO) << "Partially shredded object detected at row " << row 
             << ", has_typed=" << has_typed << ", has_value=" << has_value;
   ```

2. **Verify value binary is not empty**:
   ```cpp
   // Add logging in complex_column_reader.cpp:764
   VLOG_FILE << "Row " << i << ": value_slice.size=" << value_slice.size 
             << ", typed_value_type=" << type_to_string(_typed_value_type.type);
   ```

3. **Check field IDs in metadata**:
   ```cpp
   // In variant_encoder.cpp, log merged fields
   for (const auto& [field_id, field_data] : merged_fields) {
       ASSIGN_OR_RETURN(auto field_name, ctx->key_to_id.find(field_id));
       LOG(INFO) << "Merged field: " << field_name << " (id=" << field_id << ")";
   }
   ```

### If tests crash:

1. **Check VariantValue decoding**:
   - Ensure `value_slice` is valid before creating `VariantValue`
   - Verify `get_object_info()` returns success

2. **Check column construction**:
   - Ensure `value_col` and `typed_value_col` have same size
   - Verify `StructColumn::create()` succeeds

3. **Check field ID lookups**:
   - Ensure `ctx->key_to_id` contains all necessary field names
   - Verify metadata is correctly passed to encoder

---

## References

- **Apache Parquet Variant Shredding Spec**: https://github.com/apache/parquet-format/blob/master/VariantShredding.md
- **Initial Implementation**: Commit `5689a4454b0` by yan zhang (7 commits ago)
- **Current Branch**: `shredding-reading` (2 commits ahead of origin)
- **Test Data Generator**: `be/test/formats/parquet/test_data/IcebergVariantShreddingTest.java`
- **Related Files**:
  - `be/src/formats/parquet/complex_column_reader.cpp` - Reader logic
  - `be/src/util/variant_encoder.cpp` - Encoder logic
  - `be/src/util/variant.h` - Variant data structures
  - `be/test/formats/parquet/test_data/variant_shredding.parquet` - Test data (12KB)

---

## Next Steps

1. ✅ **Implementation Complete**: Merge logic implemented
2. ⏳ **Run Tests**: Execute C++ and SQL tests to verify correctness
3. ⏳ **Code Review**: Request review from team members
4. ⏳ **Performance Testing**: Measure impact of merge logic on query performance
5. ⏳ **Documentation**: Update user-facing documentation about variant shredding

---

## Performance Considerations

### Merge Path Performance

- **Trigger Condition**: Only when both `value` and `typed_value` are non-null for objects
- **Frequency**: Depends on writer behavior (likely rare)
- **Overhead**:
  - Decode `value` binary: O(num_non_shredded_fields)
  - Encode `typed_value` struct: O(num_shredded_fields)
  - Merge: O(total_fields * log(total_fields)) - map insertion
  - Re-encode: O(total_fields)

### Fast Paths (Unchanged)

- **Fully shredded objects**: Direct columnar read (no merge)
- **Non-shredded objects**: Direct binary copy (no merge)
- **Primitives/Arrays**: Always use single source (no merge check)

### Optimization Opportunities (Future)

1. **Pre-allocate merged_fields map**: Reserve space based on estimated field count
2. **Avoid re-encoding**: If possible, directly construct variant binary without intermediate map
3. **Cache field ID lookups**: Reuse `ctx->key_to_id` across rows
4. **Batch processing**: Process multiple rows together to amortize overhead

---

## Known Limitations

1. **Field ordering**: Merged objects may have different field order than original
   - Spec allows this (field order is not guaranteed)
   - Tests account for this by comparing field values, not full JSON strings

2. **Nested partial shredding**: Current implementation handles one level
   - Nested objects can also be partially shredded
   - Recursive call to `encode_value_from_column_row` handles this automatically

3. **Error handling**: Malformed data falls back to NULL
   - Could be enhanced with more detailed error reporting
   - Current behavior matches existing variant reader semantics

---

## Session Status

**Branch**: `shredding-reading`  
**Commits ahead of origin**: 2

```bash
git log --oneline -3
```
```
d4475e72083 [Enhancement] Implement merge logic for partially shredded Variant objects
567afd8c296 [Enhancement] Add validation and test coverage for Variant Shredding
a5fe0e23e3c fix use-after-free bug
```

**Ready for**:
- Testing (C++ unit tests + SQL integration tests)
- Code review
- Performance benchmarking
- Push to remote (after tests pass)

---

**End of Session Notes**
