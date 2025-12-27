---
displayed_sidebar: docs
---

# get_variant

一系列从 VARIANT 对象指定路径中提取类型化值的函数。

这些函数导航到 VARIANT 值中的指定路径,并将该值作为特定数据类型返回。如果路径处的值不存在、路径无效或值无法转换为所请求的类型,函数将返回 NULL。

## 语法

```Haskell
BIGINT get_variant_int(variant_expr, path)
DOUBLE get_variant_double(variant_expr, path)
VARCHAR get_variant_string(variant_expr, path)
BOOLEAN get_variant_bool(variant_expr, path)
```

## 参数

- `variant_expr`:表示 VARIANT 对象的表达式。通常是来自 Iceberg 表的 VARIANT 列。

- `path`:表示 VARIANT 对象中元素路径的表达式。此参数的值是一个字符串。路径语法类似于 JSON 路径:
  - `$` 表示根元素
  - `.` 用于访问对象字段
  - `[index]` 用于访问数组元素(基于 0 的索引)
  - 包含特殊字符的字段名可以用引号括起来:`$."field.name"`

## 返回值

- `get_variant_int`:返回 BIGINT 值,如果值无法转换为整数则返回 NULL。
- `get_variant_double`:返回 DOUBLE 值,如果值无法转换为双精度浮点数则返回 NULL。
- `get_variant_string`:返回 VARCHAR 值,如果值无法转换为字符串则返回 NULL。
- `get_variant_bool`:返回 BOOLEAN 值(1 或 0),如果值无法转换为布尔值则返回 NULL。

## 示例

### 示例 1:从根级别提取基本字段

从 Bluesky firehose 数据中提取字符串和整数值:

```SQL
SELECT
    get_variant_string(data, '$.kind') AS kind,
    get_variant_string(data, '$.did') AS did,
    get_variant_int(data, '$.time_us') AS timestamp_us
FROM bluesky
LIMIT 3;
```

```plaintext
+--------+--------------------------------------+------------------+
| kind   | did                                  | timestamp_us     |
+--------+--------------------------------------+------------------+
| commit | did:plc:gw3yid42zz6qx6gxukvkgxgq     | 1733267476040329 |
| commit | did:plc:jbaujn4dd466ebt6pdf5vxod     | 1733267476040803 |
| commit | did:plc:ytgql26s6zoifhlnvf7qheea     | 1733267476041472 |
+--------+--------------------------------------+------------------+
```

### 示例 2:提取嵌套字段

从嵌套的 commit 结构中提取操作详细信息:

```SQL
SELECT
    get_variant_string(data, '$.commit.collection') AS collection,
    get_variant_string(data, '$.commit.operation') AS operation,
    get_variant_string(data, '$.commit.rkey') AS record_key
FROM bluesky
WHERE get_variant_string(data, '$.commit.operation') = 'create'
LIMIT 5;
```

```plaintext
+------------------------+-----------+---------------+
| collection             | operation | record_key    |
+------------------------+-----------+---------------+
| app.bsky.feed.post     | create    | 3lckw3k2abc2d |
| app.bsky.graph.follow  | create    | 3lckw3k2def3e |
| app.bsky.feed.repost   | create    | 3lckw3k2ghi4f |
| app.bsky.feed.repost   | create    | 3lckw3k2jkl5g |
| app.bsky.feed.like     | create    | 3lckw3k2mno6h |
+------------------------+-----------+---------------+
```

### 示例 3:提取深层嵌套的内容

从深层嵌套结构中提取帖子文本和元数据:

```SQL
SELECT
    get_variant_string(data, '$.commit.record.text') AS post_text,
    get_variant_string(data, '$.commit.record.langs[0]') AS language,
    get_variant_int(data, '$.commit.record.createdAt') AS created_timestamp
FROM bluesky
WHERE get_variant_string(data, '$.commit.collection') = 'app.bsky.feed.post'
  AND get_variant_string(data, '$.commit.record.text') IS NOT NULL
LIMIT 3;
```

```plaintext
+---------------------------------------------------------------------------------+----------+-------------------+
| post_text                                                                       | language | created_timestamp |
+---------------------------------------------------------------------------------+----------+-------------------+
| I went on a walk today                                                          | en       | 1733267476        |
| Are you interested in developing your own graph sequence model based on a sp... | en       | 1733267476        |
| Tam im tiež celkom slušne práší...                                              | sk       | 1733267476        |
+---------------------------------------------------------------------------------+----------+-------------------+
```

### 示例 4:提取布尔值

从帖子记录中提取布尔标志:

```SQL
SELECT
    get_variant_string(data, '$.commit.record.text') AS post_text,
    get_variant_bool(data, '$.commit.record.bridgyOriginalUrl') AS is_bridged,
    get_variant_bool(data, '$.commit.record.selfLabel.settings.adult') AS has_adult_label
FROM bluesky
WHERE get_variant_string(data, '$.commit.collection') = 'app.bsky.feed.post'
LIMIT 5;
```

```plaintext
+------------------------------------+-------------+------------------+
| post_text                          | is_bridged  | has_adult_label  |
+------------------------------------+-------------+------------------+
| Great day for coding!              | NULL        | 0                |
| Check out this amazing view        | 1           | 0                |
| Working on my new project          | NULL        | NULL             |
| Another beautiful sunset           | NULL        | 0                |
| Weekend plans anyone?              | 0           | NULL             |
+------------------------------------+-------------+------------------+
```

### 示例 5:提取数值并进行计算

将微秒转换为秒并执行时间戳计算:

```SQL
SELECT
    get_variant_int(data, '$.time_us') AS timestamp_us,
    get_variant_int(data, '$.time_us') / 1000000 AS timestamp_seconds,
    FROM_UNIXTIME(get_variant_int(data, '$.time_us') / 1000000) AS readable_time
FROM bluesky
LIMIT 3;
```

```plaintext
+------------------+-------------------+---------------------+
| timestamp_us     | timestamp_seconds | readable_time       |
+------------------+-------------------+---------------------+
| 1733267476040329 | 1733267476        | 2024-12-03 20:17:56 |
| 1733267476040803 | 1733267476        | 2024-12-03 20:17:56 |
| 1733267476041472 | 1733267476        | 2024-12-03 20:17:56 |
+------------------+-------------------+---------------------+
```

### 示例 6:在聚合查询中使用

按 collection 类型统计记录:

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

### 示例 7:按操作类型统计记录

```SQL
SELECT
    get_variant_string(data, '$.commit.operation') AS operation,
    COUNT(*) AS count
FROM bluesky
GROUP BY operation;
```

```plaintext
+-----------+---------+
| operation | count   |
+-----------+---------+
| delete    | 420223  |
| update    | 40283   |
| NULL      | 39361   |
| create    | 9500118 |
+-----------+---------+
```

### 示例 8:提取和分析互动指标

从帖子中提取数值互动指标:

```SQL
SELECT
    get_variant_string(data, '$.commit.record.text') AS post_text,
    get_variant_int(data, '$.commit.record.likeCount') AS likes,
    get_variant_int(data, '$.commit.record.replyCount') AS replies,
    get_variant_int(data, '$.commit.record.repostCount') AS reposts,
    get_variant_double(data, '$.commit.record.engagementRate') AS engagement_rate
FROM bluesky
WHERE get_variant_string(data, '$.commit.collection') = 'app.bsky.feed.post'
  AND get_variant_int(data, '$.commit.record.likeCount') > 100
LIMIT 5;
```

```plaintext
+----------------------------------------+-------+---------+---------+-----------------+
| post_text                              | likes | replies | reposts | engagement_rate |
+----------------------------------------+-------+---------+---------+-----------------+
| This is amazing!                       | 245   | 23      | 45      | 0.12            |
| Can't believe this happened            | 189   | 12      | 34      | 0.08            |
| Just finished my project               | 156   | 8       | 19      | 0.05            |
| Beautiful day at the park              | 134   | 15      | 28      | 0.07            |
| Check out this cool thing I made       | 112   | 6       | 11      | 0.04            |
+----------------------------------------+-------+---------+---------+-----------------+
```

### 示例 9:在过滤中组合多个 variant 函数

查找活跃发帖的已验证账户:

```SQL
SELECT
    get_variant_string(data, '$.did') AS user_did,
    get_variant_string(data, '$.commit.record.text') AS post_text,
    get_variant_bool(data, '$.commit.record.verified') AS is_verified
FROM bluesky
WHERE get_variant_string(data, '$.commit.collection') = 'app.bsky.feed.post'
  AND get_variant_bool(data, '$.commit.record.verified') = TRUE
  AND get_variant_int(data, '$.time_us') > 1733267476000000
LIMIT 5;
```

```plaintext
+--------------------------------------+----------------------------------+-------------+
| user_did                             | post_text                        | is_verified |
+--------------------------------------+----------------------------------+-------------+
| did:plc:rhqpfbdmabrwrd3o552tlsy7     | Official announcement coming up  | 1           |
| did:plc:vlq5rxxanqrzrqdcjfoirgfz     | New feature release today        | 1           |
| did:plc:iefkhz4tg3vldzmtgyqkj3xb     | Thank you all for your support   | 1           |
| did:plc:2phsrz5r5kynlq6pnrrzvrno     | Excited to share this news       | 1           |
| did:plc:hxz45fftjaa62vnwiiwh6fhj     | Join us for the live stream      | 1           |
+--------------------------------------+----------------------------------+-------------+
```

### 示例 10:NULL 处理

当路径不存在或值无法转换时返回 NULL:

```SQL
SELECT
    get_variant_int(data, '$.nonexistent.field') AS missing_int,
    get_variant_double(data, '$.kind') AS string_as_double,
    get_variant_bool(data, '$.time_us') AS int_as_bool,
    get_variant_string(data, '$.commit.record.nonexistent') AS missing_string
FROM bluesky
LIMIT 1;
```

```plaintext
+-------------+------------------+--------------+----------------+
| missing_int | string_as_double | int_as_bool  | missing_string |
+-------------+------------------+--------------+----------------+
| NULL        | NULL             | NULL         | NULL           |
+-------------+------------------+--------------+----------------+
```

## keyword

GET_VARIANT_INT,GET_VARIANT_DOUBLE,GET_VARIANT_STRING,GET_VARIANT_BOOL,VARIANT
