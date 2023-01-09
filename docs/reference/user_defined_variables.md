# User-defined variables

This topic describes how to declare and use user-defined variables.

StarRocks 2.5 and later versions support user-defined variables. These variables are used to store specific values that are referenced in later SQL statements, thereby simplifying the writing of SQL statements and avoiding duplicate computation.

## Usage notes

- User-defined variables are variables, which can be created by the user and exist in the session. This means that no one can access user-defined variables that have been declared by another user, and when the session is closed these variables expire.
- StarRocks does not support using the SHOW statement to display existing user-defined variables.
- The following types of values cannot be declared as user-defined variables: BITMAP, HLL, PERCENTILE, and ARRAY. User-defined variables of the JSON type are converted to the STRING type for storage.

## Declare user-defined variables

### Syntax

```Plain
SET @var_name = expr [, ...];
```

> **NOTE**
>
> - All variables must be preceded by a single at sign (@).
> - Multiple variables can be declared in the same SET statement and need to be separated with commas (,).
> - You can declare the same variable multiple times. The newly declared value overwrites the original value.
> - If an undeclared variable is used, the value of the variable is `NULL` by default, and the NULL type is STRING.

### Parameters

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| var_name      | Yes          | The name of the user-defined variable. The naming conventions are as follows:<ul><li>The name can contain letters, digits (0-9), and underscores (\_). </li><li>The name cannot exceed 64 characters in length.</li></ul>You can declare a string as a user-defined variable, such as @'my-var', @"my-var", and @\`my-var\`. User-defined variables of the STRING type can contain characters other than letters, digits, and underscores (_), such as periods (.). |
| expr          | Yes          | The value of the user-defined variable. You can specify a number (such as 43) or a complex expression (such as the value returned by a SELECT statement) for this parameter. The data type of the variable is the same as the data type of the result returned by the expression. |

### Examples

Example 1: Declare a number as a user-defined variable.

```SQL
SET @var = 43;
```

Example 2: Declare the value returned by a SELECT query as a user-defined variable.

```SQL
SET @var = (SELECT SUM(v1) FROM test);
```

Example 3: Declare multiple user-defined variables in the same SET statement.

```SQL
SET @v1=1, @v2=2;
```

## Use user-defined variables in SQL

- Simplifies the writing of SQL statements. For example, when you execute the following SELECT statement, StarRocks parses `@var` as `1`.

  ```Plain
  SET @var = 1;
  SELECT @var, v1 from test;
  ```

- Avoids duplicate computation. For example, when you execute the following SELECT statement, StarRocks parses `@var` as the result returned by the `select sum(c1) from tbl` command.

  ```Plain
  SET @var = (select sum(c1) from tbl);
  SELECT @var, v1 from test;
  ```
