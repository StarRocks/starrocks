# AGENTS.md - StarRocks SQL Integration Tests

> Guidelines for AI coding agents working with SQL integration tests.
> For detailed documentation, see `test/README.md`.

## Overview

The `test/` directory contains SQL integration tests using the SQL-tester framework. These tests verify end-to-end functionality by executing SQL statements against a running StarRocks cluster.

## Quick Start

### Prerequisites

```bash
# Python 3.8+
python3 --version

# Install dependencies
pip3 install -r test/requirements.txt
```

### Running Tests

```bash
cd test

# Run all tests (validate mode)
python3 run.py -v

# Run tests in specific directory
python3 run.py -d sql/test_select -v

# Run specific test file
python3 run.py -d sql/test_select/R/test_basic -v

# List tests without running
python3 run.py -l

# Record mode (generate expected results)
python3 run.py -d sql/test_select -r
```

### Configuration

Edit `test/conf/sr.conf` with your cluster info:

```ini
[mysql-client]
host = 127.0.0.1
port = 9030
user = root
password =
http_port = 8030

[replace]
url = http://${mysql-client:host}:${mysql-client:http_port}
```

## Test File Format

### T Files (Test Statements)

Location: `test/sql/*/T/`

```sql
-- name: test_basic_select
create database test_db_${uuid0};
use test_db_${uuid0};
create table t1 (c1 int, c2 string) distributed by hash(c1);
insert into t1 values (1, 'a'), (2, 'b');
select * from t1 order by c1;
drop database test_db_${uuid0};
```

### R Files (Results)

Location: `test/sql/*/R/`

```sql
-- name: test_basic_select
create database test_db_${uuid0};
use test_db_${uuid0};
create table t1 (c1 int, c2 string) distributed by hash(c1);
insert into t1 values (1, 'a'), (2, 'b');
select * from t1 order by c1;
-- result:
1	a
2	b
-- !result
drop database test_db_${uuid0};
```

## Key Features

### Variables

```sql
-- UUID for unique names
create database db_${uuid0};

-- Config variables from sr.conf
shell: curl ${url}/api/health
```

### Result Validation

```sql
-- Exact match
select 1;
-- result:
1
-- !result

-- Regex match
select version();
-- result:
[REGEX].*StarRocks.*
-- !result

-- Order-sensitive
[ORDER]select * from t1 order by c1;
-- result:
1
2
3
-- !result
```

### Skip Validation

```sql
-- Skip result check for this statement
[UC]show backends;
```

### Shell Commands

```sql
shell: echo "hello"
-- result:
0
hello
-- !result
```

### Functions

```sql
-- Call Python helper functions
function: wait_load_finish("label")
```

### Cluster Mode Tags

```sql
-- Run only in shared-nothing mode
-- name: test_native @native

-- Run only in shared-data mode
-- name: test_cloud @cloud
```

## Writing New Tests

1. **Create T file** in appropriate directory:
   ```
   test/sql/test_feature/T/test_my_feature
   ```

2. **Write test cases**:
   ```sql
   -- name: test_my_feature_basic
   create database test_db_${uuid0};
   use test_db_${uuid0};
   -- Your test SQL here
   drop database test_db_${uuid0};
   ```

3. **Generate R file**:
   ```bash
   python3 run.py -d sql/test_feature/T/test_my_feature -r
   ```

4. **Verify**:
   ```bash
   python3 run.py -d sql/test_feature/R/test_my_feature -v
   ```

## Common Parameters

| Parameter | Description |
|-----------|-------------|
| `-v` | Validate mode (check results) |
| `-r` | Record mode (generate results) |
| `-d PATH` | Specify test path |
| `-c N` | Concurrency (default: 8) |
| `-t N` | Timeout in seconds (default: 600) |
| `-l` | List tests only |
| `--file_filter REGEX` | Filter files by name |
| `--case_filter REGEX` | Filter cases by name |

## Best Practices

1. **Use unique names**: Always use `${uuid0}` for database/table names
2. **Clean up**: Drop created objects at the end
3. **Order results**: Use `[ORDER]` tag when order matters
4. **Minimal tests**: Test one thing per case
5. **Descriptive names**: Name cases clearly

## Troubleshooting

### Test Fails with Diff

Check the actual vs expected output in the test log.

### Connection Failed

Verify `test/conf/sr.conf` has correct cluster info.

### Timeout

Increase timeout with `-t` parameter.

## Reference

Full documentation: `test/README.md`
