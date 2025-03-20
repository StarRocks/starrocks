---
displayed_sidebar: docs
---

# JSON

バージョン 2.2.0 以降、StarRocks は JSON をサポートしています。この記事では、JSON の基本概念、StarRocks が JSON 型のカラムを作成し、JSON データをロードおよびクエリし、JSON 関数と演算子を通じて JSON データを構築および処理する方法を紹介します。

## JSON とは

JSON は軽量なデータ交換フォーマットです。JSON 型データは半構造化されており、ツリー構造をサポートしています。JSON データは階層的で柔軟性があり、読みやすく処理しやすいため、データストレージや分析のシナリオで広く使用されています。JSON は、NUMBER、STRING、BOOLEAN、ARRAY、OBJECT、および NULL 値などのデータ型をサポートしています。

JSON の詳細については、[JSON 公式ウェブサイト](https://www.json.org/json-en.html) を参照してください。JSON データの入力および出力の構文については、JSON 仕様 [RFC 7159](https://tools.ietf.org/html/rfc7159?spm=a2c63.p38356.0.0.14d26b9fcp7fcf#page-4) を参照してください。

StarRocks は JSON データの保存と効率的なクエリおよび分析をサポートしています。StarRocks は、入力テキストを直接保存するのではなく、バイナリ形式のエンコーディングを使用して JSON データを保存し、データ計算およびクエリ中の解析コストを削減し、クエリ効率を向上させます。

## JSON データの使用

### JSON 型カラムの作成

テーブルを作成する際、キーワード `JSON` を使用してカラム `j` を JSON 型として指定します。

```SQL
CREATE TABLE `tj` (
    `id` INT(11) NOT NULL COMMENT "",
    `j`  JSON NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
    "replication_num" = "3",
    "storage_format" = "DEFAULT"
);
```

### データのロードと JSON 型としての保存

StarRocks は、データをロードして JSON 型として保存するための以下の方法をサポートしています。

- 方法 1: `INSERT INTO` を使用して JSON 型カラム（例: カラム `j`）にデータを書き込みます。

```SQL
INSERT INTO tj (id, j) VALUES (1, parse_json('{"a": 1, "b": true}'));
INSERT INTO tj (id, j) VALUES (2, parse_json('{"a": 2, "b": false}'));
INSERT INTO tj (id, j) VALUES (3, parse_json('{"a": 3, "b": true}'));
INSERT INTO tj (id, j) VALUES (4, json_object('a', 4, 'b', false)); 
```

> PARSE_JSON 関数は、文字列型データに基づいて JSON 型データを構築できます。JSON_OBJECT 関数は JSON オブジェクト型データを構築でき、既存のテーブルを JSON 型に変換することができます。詳細については、[PARSE_JSON](../../sql-functions/json-functions/json-constructor-functions/parse_json.md) および [JSON_OBJECT](../../sql-functions/json-functions/json-constructor-functions/json_object.md) を参照してください。

- 方法 2: Stream Load を使用して JSON ファイルをインポートし、JSON 型として保存します。インポート方法については、[Import JSON Data](../../../loading/StreamLoad.md) を参照してください。
  - JSON ファイルのルートノードにある JSON オブジェクトを JSON 型としてインポートして保存するには、`jsonpaths` を `$` に設定します。
  - JSON ファイル内の JSON オブジェクトの値を JSON 型としてインポートして保存するには、`jsonpaths` を `$.a` に設定します（ここで `a` はキーを表します）。その他の JSON パス式については、[JSON path](../../sql-functions/json-functions/overview-of-json-functions-and-operators.md#json-path) を参照してください。

- 方法 3: Broker Load を使用して Parquet ファイルをインポートし、JSON 型として保存します。インポート方法については、[Broker Load](../../sql-statements/loading_unloading/BROKER_LOAD.md) を参照してください。

インポート中に以下のようにデータ型の変換がサポートされています。

| Parquet ファイルのデータ型                                     | 変換後の JSON データ型 |
| ------------------------------------------------------------- | ------------------------ |
| 整数型 (INT8, INT16, INT32, INT64, UINT8, UINT16, UINT32, UINT64) | JSON Number              |
| 浮動小数点型 (FLOAT, DOUBLE)                                  | JSON Number              |
| BOOLEAN                                                       | JSON Boolean             |
| STRING                                                        | JSON String              |
| MAP                                                           | JSON Object              |
| STRUCT                                                        | JSON Object              |
| LIST                                                          | JSON Array               |
| UNION, TIMESTAMP, その他の型                                   | サポートされていません   |

- 方法 4: [Routine Load](../../../loading/RoutineLoad.md#導入-json-データ) を使用して、Kafka から JSON 形式のデータを継続的に消費し、StarRocks にインポートします。

### JSON 型データのクエリと処理

StarRocks は JSON 型データのクエリと処理をサポートし、JSON 関数と演算子を使用することができます。

この例では、テーブル `tj` を使用して説明します。

```SQL
mysql> select * from tj;
+------+----------------------+
| id   |          j           |
+------+----------------------+
| 1    | {"a": 1, "b": true}  |
| 2    | {"a": 2, "b": false} |
| 3    | {"a": 3, "b": true}  |
| 4    | {"a": 4, "b": false} |
+------+----------------------+
```

例 1: 条件 `id=1` を満たす JSON 型カラムのデータをフィルタリングします。

```SQL
mysql> select * from tj where id = 1;
+------+---------------------+
| id   |           j         |
+------+---------------------+
| 1    | {"a": 1, "b": true} |
+------+---------------------+
```

例 2: JSON 型カラムに基づいてテーブル内のデータをフィルタリングします。

> 以下の例では、`j->'a'` は JSON 型データを返します。最初の例と比較して、データに対して暗黙の変換を行います。または、CAST 関数を使用して JSON 型データを INT に構築して比較します。

```SQL
mysql> select * from tj where j->'a' = 1;
+------+---------------------+
| id   | j                   |
+------+---------------------+
|    1 | {"a": 1, "b": true} |
+------+---------------------+

mysql> select * from tj where cast(j->'a' as INT) = 1; 
+------+---------------------+
|   id |         j           |
+------+---------------------+
|   1  | {"a": 1, "b": true} |
+------+---------------------+
```

例 3: JSON 型カラムに基づいてテーブル内のデータをフィルタリングします（CAST 関数を使用して JSON 型カラムを BOOLEAN 型として構築できます）。

```SQL
mysql> select * from tj where cast(j->'b' as boolean);
+------+---------------------+
|  id  |          j          |
+------+---------------------+
| 1    | {"a": 1, "b": true} |
| 3    | {"a": 3, "b": true} |
+------+---------------------+
```

例 4: 条件を満たす JSON 型カラムのデータをフィルタリングし、数値演算を行います。

```SQL
mysql> select cast(j->'a' as int) from tj where cast(j->'b' as boolean);
+-----------------------+
|  CAST(j->'a' AS INT)  |
+-----------------------+
|          3            |
|          1            |
+-----------------------+

mysql> select sum(cast(j->'a' as int)) from tj where cast(j->'b' as boolean);
+----------------------------+
| sum(CAST(j->'a' AS INT))  |
+----------------------------+
|              4             |
+----------------------------+
```

例 5: JSON 型カラムに基づいてソートします。

```SQL
mysql> select * from tj
       where j->'a' <= 3
       order by cast(j->'a' as int);
+------+----------------------+
| id   | j                    |
+------+----------------------+
|    1 | {"a": 1, "b": true}  |
|    2 | {"a": 2, "b": false} |
|    3 | {"a": 3, "b": true}  |
|    4 | {"a": 4, "b": false} |
+------+----------------------+
```

## JSON 関数と演算子

JSON 関数と演算子を使用して JSON データを構築および処理することができます。詳細については、[JSON Functions and Operators](../../sql-functions/json-functions/overview-of-json-functions-and-operators.md) を参照してください。

## JSON 配列

JSON は、オブジェクト、配列、または他の JSON データ型を配列内にネストすることができます。StarRocks は、これらの複雑なネストされた JSON データ構造を処理するための豊富な関数と演算子を提供しています。以下では、配列を含む JSON データを処理する方法を紹介します。

`events` テーブルに `event_data` という JSON フィールドがあり、以下の内容を持っているとします。
```
{
  "user": "Alice",
  "actions": [
    {"type": "click", "timestamp": "2024-03-17T10:00:00Z", "quantity": 1},
    {"type": "view", "timestamp": "2024-03-17T10:05:00Z", "quantity": 2},
    {"type": "purchase", "timestamp": "2024-03-17T10:10:00Z", "quantity": 3}
  ]
}
```

以下の例では、いくつかの一般的な JSON 配列分析シナリオを示します。

1. 配列要素の抽出: actions 配列から type、timestamp などの特定のフィールドを抽出し、投影操作を行います。
2. 配列の展開: `json_each` 関数を使用して、ネストされた JSON 配列を多行多列のテーブル構造に展開し、後続の分析を行います。
3. 配列計算: 配列関数を使用して、配列要素をフィルタリング、変換、および集計し、特定の操作タイプの数をカウントします。

### 1. JSON 配列から要素を抽出

JSON 配列からネストされた要素を抽出するには、次の構文を使用します。
- 戻り値の型は依然として JSON 配列であり、CAST 式を使用して型変換を行うことができます。
```
MySQL > SELECT json_query(event_data, '$.actions[*].type') as json_array FROM events;
+-------------------------------+
| json_array                    |
+-------------------------------+
| ["click", "view", "purchase"] |
+-------------------------------+

MySQL > SELECT cast(json_query(event_data, '$.actions[*].type') as array<string>) array_string FROM events;
+-----------------------------+
| array_string                |
+-----------------------------+
| ["click","view","purchase"] |
+-----------------------------+
```

### 2. json_each を使用した展開
StarRocks は `json_each` 関数を提供しており、JSON 配列を展開し、複数行のデータに変換します。例えば:
```
MySQL > select value from events, json_each(event_data->'actions');
+--------------------------------------------------------------------------+
| value                                                                    |
+--------------------------------------------------------------------------+
| {"quantity": 1, "timestamp": "2024-03-17T10:00:00Z", "type": "click"}    |
| {"quantity": 2, "timestamp": "2024-03-17T10:05:00Z", "type": "view"}     |
| {"quantity": 3, "timestamp": "2024-03-17T10:10:00Z", "type": "purchase"} |
+--------------------------------------------------------------------------+
```

type と timestamp フィールドを個別に抽出するには:
```
MySQL > select value->'timestamp', value->'type' from events, json_each(event_data->'actions');
+------------------------+---------------+
| value->'timestamp'     | value->'type' |
+------------------------+---------------+
| "2024-03-17T10:00:00Z" | "click"       |
| "2024-03-17T10:05:00Z" | "view"        |
| "2024-03-17T10:10:00Z" | "purchase"    |
+------------------------+---------------+
```

これにより、JSON 配列データはおなじみのリレーショナルモデルになり、一般的な関数を使用して分析を行うことができます。

### 3. 配列関数を使用したフィルタリングと計算
StarRocks は ARRAY 関連の関数もサポートしており、JSON 関数と組み合わせてより効率的なクエリを行うことができます。これらの関数を組み合わせることで、JSON 配列データをフィルタリング、変換、および集計することができます。以下の例では、これらの関数を使用する方法を示します。
```
MySQL > 
WITH step1 AS (
 SELECT cast(event_data->'actions' as ARRAY<JSON>) as docs
   FROM events
)
SELECT array_filter(doc -> get_json_string(doc, 'type') = 'click', docs) as clicks
FROM step1
+---------------------------------------------------------------------------+
| clicks                                                                    |
+---------------------------------------------------------------------------+
| ['{"quantity": 1, "timestamp": "2024-03-17T10:00:00Z", "type": "click"}'] |
+---------------------------------------------------------------------------+
```

さらに、他の ARRAY 関数を組み合わせて、配列要素に対する集計計算を行うことができます。
```
MySQL > 
WITH step1 AS (
 SELECT cast(event_data->'actions' as ARRAY<JSON>) as docs
   FROM events
), step2 AS (
    SELECT array_filter(doc -> get_json_string(doc, 'type') = 'click', docs) as clicks
    FROM step1
)
SELECT array_sum(
            array_map(doc -> get_json_double(doc, 'quantity'), clicks)
            ) as click_amount
FROM step2
+--------------+
| click_amount |
+--------------+
| 1.0          |
+--------------+
```

## 制限事項と考慮事項

- JSON 型データの最大サポート長は現在 16 MB です。
- ORDER BY、GROUP BY、および JOIN 句は JSON 型カラムを参照することをサポートしていません。参照する必要がある場合は、事前に CAST 関数を使用して JSON 型カラムを他の SQL 型に変換することができます。具体的な変換方法については、[JSON Type Conversion](../../sql-functions/json-functions/json-query-and-processing-functions/cast.md) を参照してください。
- JSON 型カラムは、重複キーテーブル、主キーテーブル、およびユニークキーテーブルに存在できますが、集計テーブルには存在できません。
- JSON 型カラムは、重複キーテーブル、主キーテーブル、およびユニークキーテーブルのパーティションキー、バケッティングキー、またはディメンションカラムとしてサポートされておらず、JOIN、GROUP BY、または ORDER BY 句で使用することはできません。
- StarRocks は `<`, `<=`, `>`, `>=`, `=`, `!=` 演算子を使用して JSON データをクエリすることをサポートしていますが、IN 演算子はサポートしていません。
