---
displayed_sidebar: docs
---

# 预准备语句

自 3.2 版本起，StarRocks 提供预准备语句（prepared statement），用于多次执行结构相同、仅部分变量不同的 SQL 语句，能显著提升执行效率，并且还能防止 SQL 注入。

## 功能

**工作流程**

1. 预准备：准备 SQL 语句。语句中的变量用占位符 `?` 表示。FE 解析语句并生成执行计划。
2. 执行：声明变量后，将变量传递至语句中并执行语句。并且可以使用不同的变量多次执行该语句。

**优势**

- 节省解析 SQL 语句的开销。业务中应用程序通常会使用不同变量多次执行结构相同的语句。使用预准备语句后，StarRocks 只需要在预准备阶段解析一次语句，后续多次执行相同语句（变量不同）时直接使用解析结果，因此能显著提高语句（特别是复杂语句）执行性能。
- 防止 SQL 注入攻击。分离了语句和变量，将用户输入的数据作为参数进行传递，而不是将其直接拼接到语句中。这样可以防止恶意用户利用输入来执行恶意的 SQL 代码。

**使用说明**

预准备语句仅在当前会话中生效，不适用于其他会话。当前会话退出后，所创建预准备语句会自动删除。

## 语法

预准备语句如下：

- PREPARE：准备 SQL 语句，语句中变量用占位符 `?` 表示。
- SET：声明语句中的变量。
- EXECUTE：传递已声明的变量并执行语句。
- DROP PREPARE 或 DEALLOCATE PREPARE：删除预准备语句。

### PREPARE

**语法：**

```SQL
PREPARE <stmt_name> FROM <preparable_stmt>
```

**参数说明：**

- `stmt_name`：为预准备语句赋予一个名称，以便后续执行和删除语句时引用。该名称在单个会话内必须是唯一的。
- `preparable_stmt`：需要进行预准备的 SQL 语句。注意，**目前仅支持 `SELECT` 语句**。其中的变量占位符为英文半角问号 (`?`)。

**示例：**

准备一个 `SELECT` 语句，其中变量用 `?` 占位。并为该语句赋予一个名称 `select_by_id_stmt`。

```SQL
PREPARE select_by_id_stmt FROM 'SELECT * FROM users WHERE id = ?';
```

### SET

**语法：**

```Plain
SET @var_name = expr [, ...];
```

**参数说明：**

- `var_name`：自定义变量名称。
- `expr`：自定义变量。

**示例：**

声明变量。

```SQL
SET @id1 = 1, @id2 = 2;
```

详细信息，请参见[用户自定义变量](../user_defined_variables.md)。

### EXECUTE

**语法：**

```SQL
EXECUTE <stmt_name> [USING @var_name [, @var_name] ...]
```

**参数说明：**

- `var_name`：在 `SET` 语句中已声明的变量名称。
- `stmt_name`：预准备语句名称。

**示例：**

传递变量并执行 `SELECT` 语句。

```SQL
EXECUTE select_by_id_stmt USING @id1;
```

### DEALLOCATE PREPARE 或 DROP PREPARE

```SQL
{DEALLOCATE | DROP} PREPARE <stmt_name>
```

**参数说明：**

`stmt_name`：预准备语句名称。

**示例：**

删除一个预准备语句。

```SQL
DROP PREPARE select_by_id_stmt;
```

## 示例

### 使用预准备语句

本示例介绍如何使用预准备语句增删改查 StarRocks 表的数据。

假设已经创建如下数据库 `demo` 和表 `users`。

```SQL
CREATE DATABASE IF NOT EXISTS demo;
USE demo;
create table users (
  id bigint not null,
  country string,
  city string,
  revenue bigint
)
primary key (id)
distributed by hash(id);
```

1. 准备语句以供执行。

   ```SQL
   PREPARE select_all_stmt FROM 'SELECT * FROM users';
   PREPARE select_by_id_stmt FROM 'SELECT * FROM users WHERE id = ?';
   ```

2. 声明语句中的变量。

   ```SQL
   SET @id1 = 1, @id2 = 2;
   ```

3. 使用已声明的变量来执行语句。

   ```SQL
   -- 查询表中所有数据
   EXECUTE select_all_stmt;
   
   -- 分别查询 ID 为 1 和 2 的数据
   EXECUTE select_by_id_stmt USING @id1;
   EXECUTE select_by_id_stmt USING @id2;
   ```

4. 删除预准备语句。

   ```SQL
   DROP PREPARE select_all_stmt;
   DROP PREPARE select_by_id_stmt;
   ```

### JAVA 应用程序使用预准备语句

本示例介绍 JAVA 应用程序如何通过 JDBC 驱动使用预准备语句增删改查 StarRocks 表的数据。

1. 在 JDBC URL 中配置 StarRocks 的连接地址时，需要启用服务端预准备语句。

    ```Plaintext
    jdbc:mysql://<fe_ip>:<fe_query_port>/useServerPrepStmts=true
    ```

2. StarRocks Github 项目中提供 [JAVA 代码示例](https://github.com/StarRocks/starrocks/blob/main/fe/fe-core/src/test/java/com/starrocks/analysis/PreparedStmtTest.java)，说明如何增删改查 StarRocks 表的数据。
