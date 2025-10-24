---
displayed_sidebar: docs
---

# Skew Join V2

Skew Join V2 is an advanced optimization feature in StarRocks that addresses data skew issues in JOIN operations by broadcasting skew values. This feature significantly improves query performance when dealing with heavily skewed data distributions.

## Overview

Data skew occurs when certain values in join columns appear much more frequently than others, leading to uneven data distribution across nodes and causing performance bottlenecks. Skew Join V2 solves this problem by:

1. **Identifying skew values**: Manually specifying values that cause data skew
2. **Broadcasting skew values**: Broadcasting these specific values to all nodes to ensure even data distribution
3. **Hybrid execution**: Using a combination of shuffle and broadcast joins for optimal performance

## Syntax

```sql
SELECT select_list FROM
table1 JOIN [skew|table1.column(skew_value1, skew_value2, ...)] table2
ON join_condition
[WHERE where_clause]
```

### Syntax Components

- `[skew|table1.column(skew_value1, skew_value2, ...)]`: The skew hint that specifies:
  - `table1.column`: The column from the left table that contains skew values
  - `skew_value1, skew_value2, ...`: A comma-separated list of values that cause data skew

## Configuration

### Session Variables

Configure Skew Join V2 using the following session variables:

```sql
-- Enable Skew Join V2 optimization
SET enable_optimize_skew_join_v2 = true;
```

### Configuration Parameters

| Parameter | Description | Default Value |
|-----------|-------------|---------------|
| `enable_optimize_skew_join_v2` | Enable Skew Join V2 optimization | `false` |
| `enable_optimize_skew_join_v1` | Enable the old query rewrite method | `true` |

## Examples

### Basic Usage

```sql
-- Create test tables
CREATE TABLE orders (
    order_id INT,
    customer_id INT,
    order_date DATE,
    amount DECIMAL(10,2)
) DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 8;

CREATE TABLE customers (
    customer_id INT,
    customer_name VARCHAR(100),
    city VARCHAR(50)
) DUPLICATE KEY(customer_id)
DISTRIBUTED BY HASH(customer_id) BUCKETS 8;

-- Insert sample data with skew
INSERT INTO orders VALUES 
(1, 1001, '2024-01-01', 100.00),
(2, 1001, '2024-01-02', 200.00),
(3, 1001, '2024-01-03', 150.00),
(4, 1002, '2024-01-01', 300.00),
(5, 1003, '2024-01-01', 250.00);

INSERT INTO customers VALUES 
(1001, 'John Doe', 'New York'),
(1002, 'Jane Smith', 'Los Angeles'),
(1003, 'Bob Johnson', 'Chicago');

-- Query with Skew Join V2
SET enable_optimize_skew_join_v2 = true;

SELECT o.order_id, c.customer_name, o.amount
FROM orders o 
JOIN [skew|o.customer_id(1001)] c 
ON o.customer_id = c.customer_id;
```

### Multiple Skew Values

```sql
-- Handle multiple skew values
SELECT o.order_id, c.customer_name, o.amount
FROM orders o 
JOIN [skew|o.customer_id(1001, 1002, 1003)] c 
ON o.customer_id = c.customer_id;
```

### Different Data Types

Skew Join V2 supports various data types for skew values:

```sql
-- String values
SELECT t1.id, t2.name
FROM table1 t1 
JOIN [skew|t1.category('electronics', 'clothing', 'books')] t2 
ON t1.category = t2.category;

-- Date values
SELECT t1.id, t2.event_name
FROM events t1 
JOIN [skew|t1.event_date('2024-01-01', '2024-01-02')] t2 
ON t1.event_date = t2.event_date;

-- Numeric values
SELECT t1.id, t2.region
FROM sales t1 
JOIN [skew|t1.region_id(1, 2, 3)] t2 
ON t1.region_id = t2.region_id;
```

### Complex Join Conditions

```sql
-- With complex expressions in join conditions
SELECT t1.id, t2.value
FROM table1 t1 
JOIN [skew|t1.key(abs(t1.key))] t2 
ON abs(t1.key) = abs(t2.key);

-- Multiple join conditions
SELECT t1.id, t2.name
FROM table1 t1 
JOIN [skew|t1.category('electronics')] t2 
ON t1.category = t2.category AND t1.region = t2.region;
```

### Different Join Types

```sql
-- Left Join
SELECT t1.id, t2.name
FROM table1 t1 
LEFT JOIN [skew|t1.category('electronics')] t2 
ON t1.category = t2.category;

-- Left Semi Join
SELECT t1.id
FROM table1 t1 
LEFT SEMI JOIN [skew|t1.category('electronics')] t2 
ON t1.category = t2.category;

-- Left Anti Join
SELECT t1.id
FROM table1 t1 
LEFT ANTI JOIN [skew|t1.category('electronics')] t2 
ON t1.category = t2.category;
```

## How It Works

### Execution Plan

Skew Join V2 creates a hybrid execution plan that combines:

1. **Shuffle Join**: For non-skew data, uses the standard shuffle-based join
2. **Broadcast Join**: For skew values, broadcasts the right table data to all nodes



## Best Practices

### 1. Identify Skew Values

Before using Skew Join V2, identify the values that cause data skew:

```sql
-- Analyze data distribution
SELECT customer_id, COUNT(*) as cnt
FROM orders 
GROUP BY customer_id 
ORDER BY cnt DESC 
LIMIT 10;
```

### 2. Choose Appropriate Skew Values

- Include only the most frequent values that cause significant skew
- Avoid including too many values as it may increase broadcast overhead
- Consider the trade-off between skew optimization and broadcast cost



## Limitations

1. **Supported Join Types**: Currently supports INNER JOIN, LEFT JOIN, LEFT SEMI JOIN, and LEFT ANTI JOIN
2. **Data Types**: Supports all basic data types (INT, BIGINT, STRING, DATE, DATETIME, etc.)
3. **Complex Expressions**: Limited support for complex expressions in join conditions
4. **Skew Table**: Can only handle scenarios where the large table in the JOIN operation is skewed, and the large table must be used as the left table

## Troubleshooting
### Debug Information

Enable verbose explain to see the execution plan:

```sql
EXPLAIN VERBOSE 
SELECT ... FROM ... JOIN [skew|...] ...;
```

Look for:
- `SplitCastDataSink` nodes indicating data splitting
- `BROADCAST` and `SHUFFLE` distribution types
- `UNION` nodes combining results from both join types

## Migration from Skew Join V1

If you're currently using the old skew join optimization and experiencing poor performance, consider enabling `enable_optimize_skew_join_v2` by default:

```sql
-- Disable old method
SET enable_optimize_skew_join_v1 = false;

-- Enable V2
SET enable_optimize_skew_join_v2 = true;

-- Update queries to use the new syntax
-- Old: Query rewrite method
-- New: Explicit skew value specification with broadcast
```

### Limitations
Currently, Skew Join V2 does not support automatic Plan rewriting based on statistics and can only use hint-based manual SQL rewriting.

## Related Topics

- [Query Planning](../query_planning.md)
- [Query Profile Tuning](../query_profile_tuning_recipes.md)
- [System Variables](../../sql-reference/System_variable.md)
- [JOIN Operations](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md#join)