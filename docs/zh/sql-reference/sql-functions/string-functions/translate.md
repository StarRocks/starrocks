---
displayed_sidebar: docs
---

# translate

## 功能

用于替换给定字符串中的字符。该函数会将给定字符串 `source` 中出现在 `from_string` 中的字符替换为对应位置的 `to_string` 中的字符。

该函数从 3.2 版本开始支持。

## 语法

```SQL
TRANSLATE(source, from_string, to_string)
```

## 参数说明

- `source`: 要进行字符替换的字符串，支持的数据类型为 VARCHAR。对于不在 `from_string` 中的字符，直接在结果字符串中原样输出。

- `from_string`: 支持的数据类型为 VARCHAR。如果一个字符在 `from_string` 中出现了多次，那么只有出现的第一次是有效的，见第六个示例。`from_string` 中的字符遵守如下规则：
  - 如果在 `to_string` 中有对应位置的字符，那么替换为该字符。
  - 如果在 `to_string` 中没有对应位置的字符（即 `to_string` 的字符长度小于 `from_string`），那么在结果字符串删除这个字符，见示例二、三、四。

- `to_string`：用于替换 `from_string` 中对应位置的字符。支持的数据类型为 VARCHAR。如果 `to_string` 的字符长度大于 `from_string`，那么这部分多余的字符会被忽略，不会有任何影响，见示例五。

## 返回值说明

返回的数值类型为 VARCHAR。

返回结果为 `NULL` 的场景：

- 当任意一个输入参数为 `NULL` 时。

- 当替换后结果字符串的长度超过了 VARCHAR 类型的最大长度（1048576）时。

## 示例

```plaintext
-- 将原字符串中的字符 'ab' 按顺序替换为 '12'。
MySQL > select translate('abcabc', 'ab', '12') as test;
+--------+
| test   |
+--------+
| 12c12c |
+--------+

-- 将原字符串中的字符 'mf1' 按顺序替换为 'to'。由于 'to' 长度小于 'mf1'，在结果字符串中删除字符 '1'。
MySQL > select translate('s1m1a1rrfcks','mf1','to') as test;
+-----------+
| test      |
+-----------+
| starrocks |
+-----------+

-- 将原字符串中的字符 '测试a忽略' 按顺序替换为 'CS1'。由于 'CS1' 长度小于 '测试a忽略'，在结果字符串中删除字符 '忽略'。
MySQL > select translate('测abc试忽略', '测试a忽略', 'CS1') as test;
+-------+
| test  |
+-------+
| C1bcS |
+-------+

-- 将原字符串中的字符 'ab' 按顺序替换为 '1'。由于 '1' 长度小于 'ab'，在结果字符串中删除字符 'b'。
MySQL > select translate('abcabc', 'ab', '1') as test;
+------+
| test |
+------+
| 1c1c |
+------+

-- 将原字符串中的字符 'ab' 按顺序替换为 '123'。由于 '123' 长度大于 'ab'，忽略多余的字符 '3'。
MySQL > select translate('abcabc', 'ab', '123') as test;
+--------+
| test   |
+--------+
| 12c12c |
+--------+

-- 将原字符串中的字符 'aba' 按顺序替换为 '123'。由于 'a' 重复出现，仅出现第一次时是有效的，即仅能将 'a' 替换成 '1'。
MySQL > select translate('abcabc', 'aba', '123') as test;
+--------+
| test   |
+--------+
| 12c12c |
+--------+

-- 该函数与 repeat() 和 concat() 搭配使用。替换后结果字符串长度超过了 VARCHAR 类型的最大长度，返回 NULL。
MySQL > select translate(concat('b', repeat('a', 1024*1024-3)), 'a', '膨') as test;
+--------+
| test   |
+--------+
| NULL   |
+--------+

-- 该函数与 length()，repeat() 和 concat() 搭配使用，计算替换后的字符串的长度。
MySQL > select length(translate(concat('b', repeat('a', 1024*1024-3)), 'b', '膨')) as test
+---------+
| test    |
+---------+
| 1048576 |
+---------+
```

## 相关函数

- [concat](./concat.md)
- [length](./length.md)
- [repeat](./repeat.md)
