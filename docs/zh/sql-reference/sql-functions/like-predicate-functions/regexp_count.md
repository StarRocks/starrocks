---
displayed_sidebar: docs
---

# regexp_count

对字符串进行正则匹配，计算正则表达式 Pattern 在目标字符串中出现的次数。

## 语法

```Haskell
INT regexp_count(VARCHAR str, VARCHAR pattern)
```

## 参数说明

`str`: 要搜索的字符串。支持的数据类型为 VARCHAR。如果输入为 NULL，则返回 NULL。

`pattern`: 要搜索的正则表达式模式。支持的数据类型为 VARCHAR。如果模式为 NULL，则返回 NULL。

## 返回值说明

返回一个表示出现次数的 INT 类型值。如果没有找到匹配项或输入字符串为空，则返回 0。

## 示例

```Plain Text
-- 计算数字出现的次数
MySQL > SELECT regexp_count('abc123def456', '[0-9]');
+---------------------------------------+
| regexp_count('abc123def456', '[0-9]') |
+---------------------------------------+
|                                     6 |
+---------------------------------------+

-- 计算点号出现的次数
MySQL > SELECT regexp_count('test.com test.net test.org', '\\.');
+---------------------------------------------------+
| regexp_count('test.com test.net test.org', '\\.') |
+---------------------------------------------------+
|                                                 3 |
+---------------------------------------------------+

-- 计算空白序列出现的次数
MySQL > SELECT regexp_count('a b  c   d', '\\s+');
+----------------------------------------+
| regexp_count('a b  c   d', '\\s+')     |
+----------------------------------------+
|                                      3 |
+----------------------------------------+

-- 计算重复模式出现的次数
MySQL > SELECT regexp_count('ababababab', 'ab');
+------------------------------------+
| regexp_count('ababababab', 'ab')   |
+------------------------------------+
|                                  5 |
+------------------------------------+

-- 使用 NULL 和空值
MySQL > SELECT 
    regexp_count('', '.') AS empty_str,
    regexp_count(NULL, '.') AS null_str,
    regexp_count('abc', NULL) AS null_pattern;
+------------+----------+--------------+
| empty_str  | null_str | null_pattern |
+------------+----------+--------------+
|          0 |     NULL |         NULL |
+------------+----------+--------------+

-- 计算 Unicode/中文字符出现的次数
MySQL > SELECT regexp_count('abc中文def', '[\\p{Han}]+');
+-----------------------------------------------+
| regexp_count('abc中文def', '[\\p{Han}]+')     |
+-----------------------------------------------+
|                                             1 |
+-----------------------------------------------+
```

### 使用表数据

```Plain Text
MySQL > CREATE TABLE sample_text (
    str VARCHAR(65533) NULL,
    regex VARCHAR(65533) NULL
);

MySQL > INSERT INTO sample_text VALUES 
    ('abc123def456', '[0-9]'), 
    ('test.com test.net test.org', '\\.'), 
    ('a b  c   d', '\\s+'), 
    ('ababababab', 'ab');

MySQL > SELECT str, regex, regexp_count(str, regex) AS count 
    FROM sample_text 
    ORDER BY str;
+-----------------------------+------+-------+
| str                         | regex| count |
+-----------------------------+------+-------+
| a b  c   d                  | \s+  |     3 |
| abc123def456                | [0-9]|     6 |
| ababababab                  | ab   |     5 |
| test.com test.net test.org  | \.   |     3 |
+-----------------------------+------+-------+
```

## 关键字

REGEXP_COUNT,REGEXP,COUNT