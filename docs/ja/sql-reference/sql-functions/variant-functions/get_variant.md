---
displayed_sidebar: docs
---

# get_variant

指定したパスから VARIANT オブジェクト内の値を抽出し、指定のデータ型に変換して返す関数群です。

これらの関数は、VARIANT 値内の指定パスに移動し、その値を特定のデータ型として返します。指定したパスに値が存在しない場合、パスが不正な場合、または要求されたデータ型に変換できない場合は、NULL を返します。

## 構文

```Haskell
BIGINT get_variant_int(variant_expr, path)
DOUBLE get_variant_double(variant_expr, path)
VARCHAR get_variant_string(variant_expr, path)
BOOLEAN get_variant_bool(variant_expr, path)
```

## パラメータ

- `variant_expr`: VARIANT オブジェクトを表す式です。通常は、Iceberg テーブルの VARIANT 型カラムを指定します。

- `path`: VARIANT オブジェクト内の要素を指定するパスを表す式です。文字列型で指定します。パスの構文は JSON Path に類似しています。
  - `$`：ルート要素を表します。
  - `.`：オブジェクトのフィールドにアクセスします。
  - `[index]`：配列要素にアクセスします（0 始まり）。
  - 特殊文字を含むフィールド名は引用符で囲みます（例：`$."field.name"`）。

## 戻り値

- `get_variant_int`: BIGINT 型の値を返します。整数に変換できない場合は NULL を返します。
- `get_variant_double`: DOUBLE 型の値を返します。DOUBLE に変換できない場合は NULL を返します。
- `get_variant_string`: VARCHAR 型の値を返します。文字列に変換できない場合は NULL を返します。
- `get_variant_bool`: BOOLEAN 型の値（1 または 0）を返します。BOOLEAN に変換できない場合は NULL を返します。

## 例

### 例 1：ルートレベルの基本フィールドを抽出する

Bluesky firehose データから、文字列および整数の値を抽出します。

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

### 例 2：ネストされたフィールドを抽出する

ネストされた commit 構造から、操作に関する詳細情報を抽出します。

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

### 例 3：深くネストされたコンテンツを抽出する

多重にネストされた構造から、投稿テキストおよびメタデータを抽出します。

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
| Tam im tiež celkom slušne práši...                                              | sk       | 1733267476        |
+---------------------------------------------------------------------------------+----------+-------------------+
```

### 例 4：BOOLEAN 値を抽出する

投稿レコードから、BOOLEAN フラグを抽出します。

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

### 例 5：数値を抽出し計算に利用する

マイクロ秒を秒に変換し、タイムスタンプ計算を行います。

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

### 例 6：集計クエリで使用する

コレクションタイプごとにレコード数を集計します。

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

### 例 7：操作タイプごとにレコードを集計する

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

### 例 8：エンゲージメント指標を抽出・分析する

投稿から数値型のエンゲージメント指標を抽出します。

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

### 例 9：複数の variant 関数を組み合わせてフィルタリングする

アクティブに投稿している認証済みアカウントを抽出します。

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

### 例 10：NULL の扱い

パスが存在しない場合、または値を指定型に変換できない場合は NULL を返します。

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

## Keywords

GET_VARIANT_INT,GET_VARIANT_DOUBLE,GET_VARIANT_STRING,GET_VARIANT_BOOL,VARIANT
