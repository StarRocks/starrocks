---
displayed_sidebar: docs
---

# trim



从字符串的左侧和右侧移除连续出现的空格或指定的字符。从 2.5.0 版本开始，支持从字符串中移除指定字符。

## 语法

```Haskell
trim(str[,characters]);
```

## 参数说明

`str`: 必选，要裁剪的字符串，支持的数据类型为 VARCHAR。

`characters`: 可选，要移除的字符，支持的数据类型为 VARCHAR。如果不指定该参数，默认移除空格。如果该参数为空字符，返回报错。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

示例一：移除字符串左侧和右侧共 5 个空格。

```Plain Text
MySQL > SELECT trim("   ab c  ");
+-------------------+
| trim('   ab c  ') |
+-------------------+
| ab c              |
+-------------------+
```

示例二：移除字符串中左侧和右侧的指定字符。

```Plain Text
MySQL > SELECT trim("abcd", "ad");
+--------------------+
| trim('abcd', 'ad') |
+--------------------+
| bc                 |
+--------------------+

MySQL > SELECT trim("xxabcdxx", "x");
+-----------------------+
| trim('xxabcdxx', 'x') |
+-----------------------+
| abcd                  |
+-----------------------+
```

## 兼容 MySQL 的 `TRIM(... FROM ...)` 语法

除了 `trim(str)` 和 `trim(str, characters)`，StarRocks 还支持 MySQL/SQL 标准的写法：

```SQL
TRIM([{BOTH | LEADING | TRAILING}] [remstr] FROM str)
```

- `BOTH`（省略说明符时的默认值）裁剪两端；`LEADING` 仅裁剪开头；`TRAILING` 仅裁剪结尾。
- `remstr` 为可选的**字符串字面量**。若省略，则裁剪空格。
- **`remstr` 作为一个完整的子串被反复移除**，而不是被当作字符集合。这一点与 `trim(str, characters)` 不同，后者的第二个参数是字符集合。

```Plain Text
MySQL > SELECT TRIM(LEADING 'x' FROM 'xxxbarxxx');
+------------------------------------+
| TRIM(LEADING 'x' FROM 'xxxbarxxx') |
+------------------------------------+
| barxxx                             |
+------------------------------------+

MySQL > SELECT TRIM(TRAILING 'xyz' FROM 'barxxyzxyz');
+----------------------------------------+
| TRIM(TRAILING 'xyz' FROM 'barxxyzxyz') |
+----------------------------------------+
| barx                                   |
+----------------------------------------+

MySQL > SELECT TRIM(BOTH 'ab' FROM 'abfooab');
+--------------------------------+
| TRIM(BOTH 'ab' FROM 'abfooab') |
+--------------------------------+
| foo                            |
+--------------------------------+
```

注意子串（`FROM`）写法与字符集合（逗号）写法的区别：

```Plain Text
-- 子串：将完整的 'xyz' 移除一次，得到 'barx'
MySQL > SELECT TRIM(TRAILING 'xyz' FROM 'barxxyz');  -- 'barx'

-- 字符集合：移除结尾处任意的 x/y/z，得到 'bar'
MySQL > SELECT rtrim('barxxyz', 'xyz');              -- 'bar'
```

`FROM` 写法由内置函数 `trim_string(str, remstr)`、`ltrim_string(str, remstr)`、`rtrim_string(str, remstr)` 实现，这些函数也可直接调用。`remstr` 必须为常量字符串字面量。

> **注意**
>
> `remstr` 不能为 NULL。向这些内置函数传入 NULL 的 `remstr`（例如 `trim_string('abc', CAST(NULL AS STRING))`）会返回错误而非 NULL。这与逗号写法 `trim(str, characters)` 的行为一致，但与 MySQL 不同（MySQL 返回 NULL）。`str` 为 NULL 时仍会传播为 NULL（例如 `TRIM('x' FROM NULL)` 返回 NULL）。

## 相关文档

- [ltrim](ltrim.md)
- [rtrim](rtrim.md)
