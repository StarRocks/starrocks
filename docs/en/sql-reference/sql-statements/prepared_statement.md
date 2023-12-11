---
displayed_sidebar: "English"
---

# Prepared statements

From v3.2 onwards, StarRocks provides prepared statements for executing SQL statements multiple times with the same structure but different variables. This feature significantly improves execution efficiency and prevents SQL injection.

## Description

The prepared statements basically work as follows:

1. **Preparation**: Users prepare a SQL statement where variables are represented by placeholders `?`. The FE parses the SQL statement and generates an execution plan.
2. **Execution**: After declaring variables, users pass these variables to the statement and execute the statement. Users can execute the same statement multiple times with different variables.

**Benefits**

- **Saves overhead of parsing**: In real-world business scenarios, an application often executes a statement multiple times with the same structure but different variables. With prepared statements supported, StarRocks needs to parse the statement only once during the preparation phase. Subsequent executions of the same statement with different variables can directly use the pre-generated parsing result. As such, statement execution performance is significantly improved, especially for complex queries.
- **Prevents SQL injection attacks**: By separating the statement from the variables and passing user-input data as parameters rather than directly concatenating the variables into the statement, StarRocks can prevent malicious users from executing malicious SQL codes.

**Usages**

Prepared statements are effective only in the current session and cannot be used in other sessions. After the current session exits, the prepared statements created in that session are automatically dropped.

## Syntax

The execution of a prepared statement consists of the following phases:

- PREPARE: Prepare the statement where variables are represented by placeholders `?`.
- SET: Declare variables within the statement.
- EXECUTE: Pass the declared variables to the statement and execute it.
- DROP PREPARE or DEALLOCATE PREPARE: Delete the prepared statement.

### PREPARE

**Syntax:**

```SQL
PREPARE <stmt_name> FROM <preparable_stmt>
```

**Parameters:**

- `stmt_name`: the name given to the prepared statement, which is subsequently used to execute or deallocate that prepared statement. The name must be unique within a single session.
- `preparable_stmt`: the SQL statement to be prepared, where the placeholder for variables is a question mark (`?`).

**Example:**

Prepare an `INSERT INTO VALUES()` statement with specific values represented by placeholders `?`.

```SQL
PREPARE insert_stmt FROM 'INSERT INTO users (id, country, city, revenue) VALUES (?, ?, ?, ?)';
```

### SET

**Syntax:**

```SQL
SET @var_name = expr [, ...];
```

**Parameters:**

- `var_name`: the name of a user-defined variable.
- `expr`: a user-defined variable.

**Example:** Declare four variables.

```SQL
SET @id1 = 1, @country1 = 'USA', @city1 = 'New York', @revenue1 = 1000000;
```

For more information, see [user-defined variables](../../reference/user_defined_variables.md).

### EXECUTE

**Syntax:**

```SQL
EXECUTE <stmt_name> [USING @var_name [, @var_name] ...]
```

**Parameters:**

- `var_name`: the name of a variable declared in the `SET` statement.
- `stmt_name`: the name of the prepared statement.

**Example:**

Pass variables to an `INSERT` statement and execute that statement.

```SQL
EXECUTE insert_stmt USING @id1, @country1, @city1, @revenue1;
```

### DROP PREPARE or DEALLOCATE PREPARE

**Syntax:**

```SQL
{DEALLOCATE | DROP} PREPARE <stmt_name>
```

**Parameters:**

- `stmt_name`: The name of the prepared statement.

**Example:**

Delete a prepared statement.

```SQL
DROP PREPARE insert_stmt;
```

## Examples

### Use prepared statements

The following example demonstrates how to use prepared statements to insert, delete, update, and query data from a StarRocks table:

Assuming the following database named `demo` and table named `users` are alrealy created:

```SQL
CREATE DATABASE IF NOT EXISTS demo;
USE demo;
CREATE TABLE users (
  id BIGINT NOT NULL,
  country STRING,
  city STRING,
  revenue BIGINT
)
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id);
```

1. Prepare statements for execution.

    ```SQL
    PREPARE insert_stmt FROM 'INSERT INTO users (id, country, city, revenue) VALUES (?, ?, ?, ?)';
    PREPARE select_all_stmt FROM 'SELECT * FROM users';
    PREPARE select_by_id_stmt FROM 'SELECT * FROM users WHERE id = ?';
    PREPARE update_stmt FROM 'UPDATE users SET revenue = ? WHERE id = ?';
    PREPARE delete_stmt FROM 'DELETE FROM users WHERE id = ?';
    ```

2. Declare variables in these statements.

    ```SQL
    SET @id1 = 1, @id2 = 2;
    SET @country1 = 'USA', @country2 = 'Canada';
    SET @city1 = 'New York', @city2 = 'Toronto';
    SET @revenue1 = 1000000, @revenue2 = 1500000, @revenue3 = (SELECT (revenue) * 1.1 FROM users);
    ```

3. Use the declared variables to execute the statements.

    ```SQL
    -- Insert two rows of data.
    EXECUTE insert_stmt USING @id1, @country1, @city1, @revenue1;
    EXECUTE insert_stmt USING @id2, @country2, @city2, @revenue2;

    -- Query all data from the table.
    EXECUTE select_all_stmt;

    -- Query data with ID 1 or 2 separately.
    EXECUTE select_by_id_stmt USING @id1;
    EXECUTE select_by_id_stmt USING @id2;

    -- Partially update the row with ID 1. Only update the revenue column.
    EXECUTE update_stmt USING @revenue3, @id1;

    -- Delete data with ID 1.
    EXECUTE delete_stmt USING @id1;

    -- Delete prepared statements.
    DROP PREPARE insert_stmt;
    DROP PREPARE select_all_stmt;
    DROP PREPARE select_by_id_stmt;
    DROP PREPARE update_stmt;
    DROP PREPARE delete_stmt;
    ```

### Using Prepared Statements in Java application

The following example demonstrates how a Java application can use a JDBC driver to insert, delete, update, and query data from a StarRocks table:

1. When specifying StarRocks' connection URL in JDBC, you need to enable server-side prepared statements:

    ```Plaintext
    jdbc:mysql://<fe_ip>:<fe_query_port>/useServerPrepStmts=true
    ```

2. The StarRocks GitHub project provides a [Java code example](https://github.com/StarRocks/starrocks/blob/main/fe/fe-core/src/test/java/com/starrocks/analysis/PreparedStmtTest.java) that explains how to insert, delete, update, and query data from a StarRocks table through a JDBC driver.
