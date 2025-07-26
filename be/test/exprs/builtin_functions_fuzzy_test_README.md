# Built-in Functions Fuzzy Test

## Overview

`builtin_functions_fuzzy_test.cpp` is a comprehensive fuzzy test designed to test the robustness of all built-in functions in StarRocks BE. The main goal of this test is to ensure that functions do not crash when receiving various mismatched input types, thereby improving system stability.

## Design Goals

1. **Automatic Function Discovery**: Automatically retrieve all registered built-in functions from `BuiltinFunctions::get_all_functions()`
2. **Comprehensive Type Coverage**: Test all Column types, including basic types and composite types
3. **Type Combination Testing**: Test various combinations of wrappers like Const and Nullable
4. **Parameter Count Variations**: Test function behavior when receiving incorrect parameter counts
5. **Extensibility**: When new built-in functions are added, tests will automatically cover them without manual modification

## Column Types Covered

### Basic Types
- `TYPE_BOOLEAN`
- `TYPE_TINYINT`, `TYPE_SMALLINT`, `TYPE_INT`, `TYPE_BIGINT`, `TYPE_LARGEINT`
- `TYPE_FLOAT`, `TYPE_DOUBLE`
- `TYPE_VARCHAR`, `TYPE_VARBINARY`
- `TYPE_DATE`, `TYPE_DATETIME`
- `TYPE_DECIMAL32`, `TYPE_DECIMAL64`, `TYPE_DECIMAL128`
- `TYPE_JSON`

### Composite Types
- **Array Types**: `ARRAY_*` arrays with various element types
- **Map Types**: `MAP_*` key-value mappings (future extension)
- **Struct Types**: Structure types (future extension)

### Wrapper Types
- **Nullable**: Nullable wrapper that can be combined with any basic type
- **Const**: Constant wrapper that can be combined with any basic type
- **Nullable + Const**: Combination of both wrappers

## Testing Strategy

### 1. Random Data Generation
- Generate random data within reasonable ranges for each type
- Generate random length strings for string types
- Generate random values within valid ranges for numeric types
- Generate valid timestamps for date/time types

### 2. Type Mismatch Testing
- Randomly select different input type combinations
- Test function signature mismatches
- Test parameter count mismatches

### 3. Boundary Condition Testing
- Empty Column testing
- All-NULL Column testing
- Mixed NULL and non-NULL data testing

### 4. Exception Handling Verification
- Functions should gracefully handle type mismatches, returning errors instead of crashing
- Capture and log exceptions, but don't treat them as test failures
- Only unexpected exception types (like segfault) are considered real problems

## Test Case Organization

### TestAllBuiltinFunctions
Iterate through all built-in functions and perform basic random input testing on each function.

### TestSpecificFunctionTypes
Test functions by functional category:
- Array functions
- Map functions  
- JSON functions
- Time functions
- String functions
- Other functions

Perform more in-depth testing on some functions in each category.

### TestNullAndEmptyInputs
Specifically test boundary conditions:
- Empty Column input
- All-NULL Column input

## Expected Behavior

### Normal Cases
- Functions should execute normally when receiving correct input types
- Returned Columns should be non-null and have correct size

### Type Mismatch Cases
- Functions should return error status instead of crashing
- May throw `std::exception` type exceptions, which is expected
- Should not throw unhandled exceptions or cause segfaults

### Parameter Count Mismatch Cases
- Functions should gracefully handle incorrect parameter counts
- Should return errors instead of accessing invalid memory

## Usage

### Compilation
```bash
cd /workspace/be/build
make builtin_functions_fuzzy_test
```

### Running
```bash
# Run all fuzzy tests
./test/builtin_functions_fuzzy_test

# Run specific test cases
./test/builtin_functions_fuzzy_test --gtest_filter="*TestAllBuiltinFunctions*"
```

### CI Integration
This test is designed as a standalone executable that can run in CI as part of regression testing.

## Extensibility

### Adding New Column Types
Add new types to the `get_all_logical_types()` function and add corresponding creation logic in `create_random_column()`.

### Adding New Testing Strategies
New test functions can be added, such as:
- Performance stress testing
- Memory leak detection
- Concurrency safety testing

### Custom Test Data
The random data generator can be extended to add more targeted test data patterns.

## Notes

1. **Fixed Random Seed**: Tests use a fixed random seed (42) to ensure reproducible test results
2. **Exception Handling**: Tests distinguish between expected exceptions and real problems; only the latter cause test failures
3. **Memory Management**: All Columns use smart pointers to avoid memory leaks
4. **Test Duration**: Due to testing many functions and type combinations, tests may take considerable time

## Troubleshooting

### If Tests Fail
1. Check if functions threw unexpected exception types
2. Review log output to identify which function and input combination caused the problem
3. Check the input type validation logic of the relevant function

### If Tests Take Too Long
1. Reduce the number of test combinations
2. Selectively skip certain function categories
3. Adjust random data sizes

## Contribution Guidelines

When adding new built-in functions:
1. Ensure the function is properly registered in `BuiltinFunctions::_fn_tables`
2. Ensure the function has appropriate input type checking
3. Run fuzzy tests to verify function robustness
4. If crashes are discovered, fix the function's input validation logic