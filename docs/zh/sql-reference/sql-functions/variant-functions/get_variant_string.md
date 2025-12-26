---
displayed_sidebar: docs
---

# get_variant_string

从指定路径的 VARIANT 对象中提取字符串值。

此函数导航到 VARIANT 值中的指定路径,并将该值作为 VARCHAR 返回。如果路径处的值不是字符串或无法转换为字符串,函数将返回 NULL。

## 语法

```Haskell
VARCHAR get_variant_string(variant_expr, path)
```

## 参数

- `variant_expr`:表示 VARIANT 对象的表达式。通常是来自 Iceberg 表的 VARIANT 列。

- `path`:表示 VARIANT 对象中元素路径的表达式。此参数的值是一个字符串。路径语法类似于 JSON 路径:
  - `$` 表示根元素
  - `.` 用于访问对象字段
  - `[index]` 用于访问数组元素(基于 0 的索引)
  - 包含特殊字符的字段名可以用引号括起来:`$."field.name"`

## 返回值

返回 VARCHAR 值。

如果元素不存在、路径无效或值无法转换为字符串,函数将返回 NULL。

## 示例

示例 1:从根级别提取字符串字段。

```SQL
SELECT
    get_variant_string(data, '$.kind') AS kind,
    get_variant_string(data, '$.did') AS did
FROM bluesky
LIMIT 3;
```

```plaintext
+--------+--------------------------------------+
| kind   | did                                  |
+--------+--------------------------------------+
| commit | did:plc:gw3yid42zz6qx6gxukvkgxgq     |
| commit | did:plc:jbaujn4dd466ebt6pdf5vxod     |
| commit | did:plc:ytgql26s6zoifhlnvf7qheea     |
+--------+--------------------------------------+
```

示例 2:从嵌套字段提取字符串。

```SQL
SELECT
    get_variant_string(data, '$.commit.collection') AS collection,
    get_variant_string(data, '$.commit.operation') AS operation
FROM bluesky
WHERE get_variant_string(data, '$.commit.operation') = 'create'
LIMIT 5;
```

```plaintext
+------------------------+-----------+
| collection             | operation |
+------------------------+-----------+
| app.bsky.feed.post     | create    |
| app.bsky.graph.follow  | create    |
| app.bsky.feed.repost   | create    |
| app.bsky.feed.repost   | create    |
| app.bsky.feed.like     | create    |
+------------------------+-----------+
```

示例 3:提取深层嵌套的字符串值。

```SQL
SELECT get_variant_string(data, '$.commit.record.text') AS post_text
FROM bluesky
WHERE get_variant_string(data, '$.commit.collection') = 'app.bsky.feed.post'
  AND get_variant_string(data, '$.commit.record.text') IS NOT NULL
LIMIT 3;
```

```plaintext
+---------------------------------------------------------------------------------+
| post_text                                                                       |
+---------------------------------------------------------------------------------+
| I went on a walk today                                                          |
| Are you interested in developing your own graph sequence model based on a sp... |
| Tam im tiež celkom slušne práší...                                              |
+---------------------------------------------------------------------------------+
```

示例 4:在聚合查询中使用。

```SQL
SELECT
    get_variant_string(data, '$.commit.collection') AS collection,
    COUNT(*) AS count
FROM bluesky
GROUP BY collection
ORDER BY count DESC
LIMIT 5;
```

```plaintext
+------------------------+---------+
| collection             | count   |
+------------------------+---------+
| app.bsky.feed.like     | 5067917 |
| app.bsky.graph.follow  | 2931757 |
| app.bsky.feed.post     | 960723  |
| app.bsky.feed.repost   | 666364  |
| app.bsky.graph.block   | 196053  |
+------------------------+---------+
```

示例 5:在 WHERE 子句中用于过滤。

```SQL
SELECT
    get_variant_string(data, '$.commit.record.subject') AS follow_subject
FROM bluesky
WHERE get_variant_string(data, '$.commit.collection') = 'app.bsky.graph.follow'
  AND get_variant_string(data, '$.commit.operation') = 'create'
LIMIT 5;
```

```plaintext
+--------------------------------------+
| follow_subject                       |
+--------------------------------------+
| did:plc:rhqpfbdmabrwrd3o552tlsy7     |
| did:plc:vlq5rxxanqrzrqdcjfoirgfz     |
| did:plc:iefkhz4tg3vldzmtgyqkj3xb     |
| did:plc:2phsrz5r5kynlq6pnrrzvrno     |
| did:plc:hxz45fftjaa62vnwiiwh6fhj     |
+--------------------------------------+
```

## keyword

GET_VARIANT_STRING,VARIANT
