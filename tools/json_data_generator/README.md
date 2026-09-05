# JSON Data Generator

A Python tool for generating JSONL (JSON Lines) data files with configurable characteristics.

## Performance Optimization

This tool includes a high-performance C++ implementation that significantly improves data generation speed. The tool automatically uses the C++ executable if available, otherwise falls back to the pure Python implementation.

### Building the C++ Program

To build the C++ program for better performance:

```bash
cd tools/json_data_generator
./build.sh
```

Or manually:

```bash
cd tools/json_data_generator
g++ -std=c++17 -O3 -Wall -o json_generator json_generator.cpp
```

**Requirements:**
- C++17 compatible compiler (g++ or clang++)

The C++ executable will be automatically detected and used when available. If the executable is not built, the tool will use the pure Python implementation (slower but fully functional).

## Parameters

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--num-records` | int | 1000 | Number of JSON records to generate |
| `--num-fields` | int | 10 | Number of top-level fields per record |
| `--sparsity` | float | 0.0 | Sparsity ratio (0.0 to 1.0). Fields are randomly omitted based on this ratio |
| `--max-depth` | int | 4 | Maximum nesting depth for nested objects |
| `--nest-probability` | float | 0.2 | Probability of creating nested objects (0.0 to 1.0) |
| `--field-types` | string | `string,int,bool` | Comma-separated list of field types: string, int, bool, datetime, array, object |
| `--high-cardinality-fields` | int | 0 | Number of fields with high cardinality (unique values) |
| `--low-cardinality-fields` | int | 0 | Number of fields with low cardinality (limited value sets) |
| `--seed` | int | None | Random seed for reproducible data generation |
| `--output` | string | None | Output file path (default: stdout) |
| `--pretty` | flag | False | Pretty-print JSON |
| `--gen-query-type` | string | None | Generate queries: filter, aggregation, or select |
| `--gen-query-num` | int | 10 | Number of queries to generate (default: 10) |
| `--gen-query-output` | string | None | Output file for queries (default: stdout) |
| `--gen-query-table` | string | json_test_table | Table name in generated queries |
| `--gen-query-column` | string | json_data | JSON column name in generated queries |

## Example

```bash
./json_generator \
  --num-records 1000 \
  --num-fields 50 \
  --sparsity 0.3 \
  --max-depth 5 \
  --nest-probability 0.2 \
  --field-types string,int,bool,datetime,array,object \
  --high-cardinality-fields 10 \
  --low-cardinality-fields 15 \
  --seed 42 \
  --output data.jsonl
```

This generates 1000 JSONL records with 50 fields per record, where the first 10 fields have high cardinality, the next 15 fields have low cardinality, 30% of fields are randomly omitted, and nested objects can be up to 5 levels deep.

## Query Generation

Generate SQL queries for testing JSON data:

```bash
# Generate filter queries (with WHERE conditions based on actual data values)
./json_generator \
  --num-records 1000 \
  --num-fields 20 \
  --high-cardinality-fields 5 \
  --low-cardinality-fields 10 \
  --gen-query-type filter \
  --gen-query-num 20 \
  --gen-query-output queries_filter.sql

# Generate aggregation queries (GROUP BY, COUNT, AVG, etc.)
./json_generator \
  --num-records 1000 \
  --gen-query-type aggregation \
  --gen-query-num 10 \
  --gen-query-output queries_agg.sql

# Generate select queries (field projections)
./json_generator \
  --num-records 1000 \
  --gen-query-type select \
  --gen-query-num 15 \
  --gen-query-output queries_select.sql
```

**Query Types:**
- **filter**: Generates WHERE conditions using actual data values to ensure queries return results
- **aggregation**: Generates GROUP BY queries with COUNT, AVG, SUM, MAX, MIN aggregations
- **select**: Generates SELECT queries with field projections

Note: Filter queries analyze sample data to use actual values, ensuring query effectiveness.

## Testing with StarRocks

Use the automated test script to perform a complete test workflow:

```bash
# Basic usage with default settings (127.0.0.1:9030, user: root, no password)
./test_starrocks_json.sh

# Custom StarRocks connection
SR_HOST=192.168.1.100 SR_PASSWORD=mypass ./test_starrocks_json.sh

# Custom data generation parameters
NUM_RECORDS=50000 NUM_FIELDS=50 SPARSITY=0.3 ./test_starrocks_json.sh

# Keep generated data file after test
./test_starrocks_json.sh --keep-data

# Show help
./test_starrocks_json.sh --help
```

The test script will:
1. Create database and table in StarRocks
2. Generate JSON test data using the generator
3. Import data via Stream Load
4. Run query tests to verify functionality

### Configuration

Configure StarRocks connection via environment variables:

- `SR_HOST` - StarRocks FE host (default: 127.0.0.1)
- `SR_QUERY_PORT` - Query port (default: 9030)
- `SR_HTTP_PORT` - HTTP port (default: 8030)
- `SR_USER` - Username (default: root)
- `SR_PASSWORD` - Password (default: empty)
- `SR_DATABASE` - Database name (default: json_test_db)
- `SR_TABLE` - Table name (default: json_test_table)

Data generation parameters can also be customized via environment variables (see `--help` for details).
