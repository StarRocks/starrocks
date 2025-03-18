---
displayed_sidebar: docs
keywords: ['CTAS']
---

# CREATE TABLE AS SELECT

## 説明

CREATE TABLE AS SELECT (CTAS) 文を使用して、同期または非同期でテーブルをクエリし、クエリ結果に基づいて新しいテーブルを作成し、その後クエリ結果を新しいテーブルに挿入できます。

非同期 CTAS タスクを [SUBMIT TASK](../loading_unloading/ETL/SUBMIT_TASK.md) を使用して送信できます。

## 構文

- テーブルを同期的にクエリし、クエリ結果に基づいて新しいテーブルを作成し、その後クエリ結果を新しいテーブルに挿入します。

  ```SQL
  CREATE [TEMPORARY] TABLE [IF NOT EXISTS] [database.]table_name
  [column_name1 [, column_name2, ...]]
  [index_definition1 [, index_definition2, ...]]
  [key_desc]
  [COMMENT "table comment"]
  [partition_desc]
  [distribution_desc]
  [ORDER BY (column_name1 [, column_name2, ...])]
  [PROPERTIES ("key"="value", ...)]
  AS SELECT query
  [ ... ]
  ```

- テーブルを非同期でクエリし、クエリ結果に基づいて新しいテーブルを作成し、その後クエリ結果を新しいテーブルに挿入します。

  ```SQL
  SUBMIT [/*+ SET_VAR(key=value) */] TASK [[database.]<task_name>]AS
  CREATE TABLE [IF NOT EXISTS] [database.]table_name
  [column_name1 [, column_name2, ...]]
  [index_definition1 [, index_definition2, ...]]
  [key_desc]
  [COMMENT "table comment"]
  [partition_desc]
  [distribution_desc]
  [ORDER BY (column_name1 [, column_name2, ...])]
  [PROPERTIES ("key"="value", ...)] AS SELECT query
  [ ... ]
  ```

## パラメータ

| **パラメータ**   | **必須** | **説明**                                                                 |
| ----------------- | ------------ | ------------------------------------------------------------------------ |
| TEMPORARY         | いいえ       | 一時テーブルを作成します。v3.3.1 から、StarRocks は Default Catalog での一時テーブルの作成をサポートしています。詳細は [Temporary Table](../../../table_design/StarRocks_table_design.md#temporary-table) を参照してください。現在、StarRocks は SUBMIT TASK を使用した非同期タスクでの一時テーブルの作成をサポートしていません。 |
| column_name       | いいえ       | 新しいテーブルの列名です。列のデータ型を指定する必要はありません。StarRocks が自動的に適切なデータ型を指定します。StarRocks は FLOAT と DOUBLE データを DECIMAL(38,9) データに変換します。また、CHAR、VARCHAR、STRING データを VARCHAR(65533) データに変換します。 |
| index_definition  | いいえ       | v3.1.8 以降、新しいテーブルにビットマップインデックスを作成できます。構文は `INDEX index_name (col_name[, col_name, ...]) [USING BITMAP] COMMENT 'xxxxxx'` です。パラメータの説明と使用上の注意については、[Bitmap indexes](../../../table_design/indexes/Bitmap_index.md) を参照してください。 |
| key_desc          | いいえ       | 構文は `key_type ( <col_name1> [, <col_name2> , ...])` です。<br />**パラメータ**:<ul><li>`key_type`: [新しいテーブルのキータイプ](../../../table_design/table_types/table_types.md)。有効な値: `DUPLICATE KEY` と `PRIMARY KEY`。デフォルト値: `DUPLICATE KEY`。</li><li> `col_name`: キーを形成する列。</li></ul> |
| COMMENT           | いいえ       | 新しいテーブルのコメントです。                                           |
| partition_desc    | いいえ       | 新しいテーブルのパーティション化の手法です。デフォルトでは、このパラメータを指定しない場合、新しいテーブルにはパーティションがありません。パーティション化の詳細については、[CREATE TABLE](./CREATE_TABLE.md#partition_desc) を参照してください。 |
| distribution_desc | いいえ       | 新しいテーブルのバケット化の手法です。このパラメータを指定しない場合、バケット列はコストベースオプティマイザ (CBO) によって収集された最も高いカーディナリティを持つ列にデフォルト設定されます。バケット数はデフォルトで 10 です。CBO がカーディナリティに関する情報を収集しない場合、バケット列は新しいテーブルの最初の列にデフォルト設定されます。バケット化の詳細については、[CREATE TABLE](./CREATE_TABLE.md#distribution_desc) を参照してください。 |
| ORDER BY          | いいえ       | v3.1.8 以降、新しいテーブルが主キーテーブルである場合、ソートキーを指定できます。ソートキーは任意の列の組み合わせにできます。主キーテーブルは、テーブル作成時に `PRIMARY KEY (xxx)` が指定されたテーブルです。 |
| Properties        | いいえ       | 新しいテーブルのプロパティです。                                         |
| AS SELECT query   | はい         | クエリ結果です。`... AS SELECT query` で列を指定できます。例えば、`... AS SELECT a, b, c FROM table_a;` のようにします。この例では、`a`、`b`、`c` はクエリされたテーブルの列名を示します。新しいテーブルの列名を指定しない場合、新しいテーブルの列名も `a`、`b`、`c` になります。`... AS SELECT query` で式を指定できます。例えば、`... AS SELECT a+1 AS x, b+2 AS y, c*c AS z FROM table_a;` のようにします。この例では、`a+1`、`b+2`、`c*c` はクエリされたテーブルの列名を示し、`x`、`y`、`z` は新しいテーブルの列名を示します。注意: 新しいテーブルの列数は SELECT 文で指定された列数と同じである必要があります。識別しやすい列名を使用することをお勧めします。 |

## 使用上の注意

- CTAS 文は、次の要件を満たす新しいテーブルのみを作成できます:
  - `ENGINE` は `OLAP` です。

  - テーブルはデフォルトで重複キーテーブルです。`key_desc` で主キーテーブルとして指定することもできます。

  - ソートキーは最初の 3 列であり、これらの 3 列のデータ型のストレージスペースは 36 バイトを超えません。

- CTAS 文は、新しく作成されたテーブルにインデックスを設定することをサポートしていません。

- CTAS 文が FE の再起動などの理由で実行に失敗した場合、次のいずれかの問題が発生する可能性があります:
  - 新しいテーブルが正常に作成されますが、データが含まれていません。

  - 新しいテーブルの作成に失敗します。

- 新しいテーブルが作成された後、複数の方法（例えば INSERT INTO）を使用して新しいテーブルにデータを挿入する場合、最初に INSERT 操作を完了した方法がそのデータを新しいテーブルに挿入します。

- 新しいテーブルが作成された後、このテーブルに対する権限をユーザーに手動で付与する必要があります。

- テーブルを非同期でクエリし、クエリ結果に基づいて新しいテーブルを作成する際にタスクの名前を指定しない場合、StarRocks は自動的にタスクの名前を生成します。

## 例

例 1: テーブル `order` を同期的にクエリし、クエリ結果に基づいて新しいテーブル `order_new` を作成し、その後クエリ結果を新しいテーブルに挿入します。

```SQL
CREATE TABLE order_new
AS SELECT * FROM order;
```

例 2: テーブル `order` の `k1`、`k2`、`k3` 列を同期的にクエリし、クエリ結果に基づいて新しいテーブル `order_new` を作成し、その後クエリ結果を新しいテーブルに挿入します。さらに、新しいテーブルの列名を `a`、`b`、`c` に設定します。

```SQL
CREATE TABLE order_new (a, b, c)
AS SELECT k1, k2, k3 FROM order;
```

または

```SQL
CREATE TABLE order_new
AS SELECT k1 AS a, k2 AS b, k3 AS c FROM order;
```

例 3: テーブル `employee` の `salary` 列の最大値を同期的にクエリし、クエリ結果に基づいて新しいテーブル `employee_new` を作成し、その後クエリ結果を新しいテーブルに挿入します。さらに、新しいテーブルの列名を `salary_max` に設定します。

```SQL
CREATE TABLE employee_new
AS SELECT MAX(salary) AS salary_max FROM employee;
```

データが挿入された後、新しいテーブルをクエリします。

```SQL
SELECT * FROM employee_new;

+------------+
| salary_max |
+------------+
|   10000    |
+------------+
```

例 4: テーブル `customers` の `customer_id` と `first_name` 列を同期的にクエリし、クエリ結果に基づいて新しいテーブル `customers_new` を作成し、その後クエリ結果を新しいテーブルに挿入します。さらに、新しいテーブルの列名を `customer_id_new` と `first_name_new` に設定します。また、新しいテーブルの `customer_id_new` 列にビットマップインデックスを構築します。

```SQL
CREATE TABLE customers_new 
(   customer_id_new,
    first_name_new,
    INDEX idx_bitmap_customer_id (customer_id_new) USING BITMAP
) 
AS SELECT customer_id,first_name FROM customers;
```

例 5: テーブル `customers` を同期的にクエリし、クエリ結果に基づいて新しいテーブル `customers_new` を作成し、その後クエリ結果を新しいテーブルに挿入します。さらに、新しいテーブルを主キーテーブルとして指定し、そのソートキーを `first_name` と `last_name` に指定します。

```SQL
CREATE TABLE customers_pk
PRIMARY KEY (customer_id)
ORDER BY (first_name,last_name)
AS SELECT  * FROM customers;
```

例 6: CTAS を使用して主キーテーブルを作成します。主キーテーブルのデータ行数はクエリ結果の行数より少ない場合があります。これは、[Primary Key](../../../table_design/table_types/primary_key_table.md) テーブルが同じ主キーを持つ行の中で最新のデータ行のみを保存するためです。

```SQL
CREATE TABLE employee_new
PRIMARY KEY(order_id)
AS SELECT
    order_list.order_id,
    sum(goods.price) as total
FROM order_list INNER JOIN goods ON goods.item_id1 = order_list.item_id2
GROUP BY order_id;
```

例 7: `lineorder`、`customer`、`supplier`、`part` の 4 つのテーブルを同期的にクエリし、クエリ結果に基づいて新しいテーブル `lineorder_flat` を作成し、その後クエリ結果を新しいテーブルに挿入します。さらに、新しいテーブルのパーティション化の手法とバケット化の手法を指定します。

```SQL
CREATE TABLE lineorder_flat
PARTITION BY RANGE(`LO_ORDERDATE`)
(
    START ("1993-01-01") END ("1999-01-01") EVERY (INTERVAL 1 YEAR)
)
DISTRIBUTED BY HASH(`LO_ORDERKEY`) AS SELECT
    l.LO_ORDERKEY AS LO_ORDERKEY,
    l.LO_LINENUMBER AS LO_LINENUMBER,
    l.LO_CUSTKEY AS LO_CUSTKEY,
    l.LO_PARTKEY AS LO_PARTKEY,
    l.LO_SUPPKEY AS LO_SUPPKEY,
    l.LO_ORDERDATE AS LO_ORDERDATE,
    l.LO_ORDERPRIORITY AS LO_ORDERPRIORITY,
    l.LO_SHIPPRIORITY AS LO_SHIPPRIORITY,
    l.LO_QUANTITY AS LO_QUANTITY,
    l.LO_EXTENDEDPRICE AS LO_EXTENDEDPRICE,
    l.LO_ORDTOTALPRICE AS LO_ORDTOTALPRICE,
    l.LO_DISCOUNT AS LO_DISCOUNT,
    l.LO_REVENUE AS LO_REVENUE,
    l.LO_SUPPLYCOST AS LO_SUPPLYCOST,
    l.LO_TAX AS LO_TAX,
    l.LO_COMMITDATE AS LO_COMMITDATE,
    l.LO_SHIPMODE AS LO_SHIPMODE,
    c.C_NAME AS C_NAME,
    c.C_ADDRESS AS C_ADDRESS,
    c.C_CITY AS C_CITY,
    c.C_NATION AS C_NATION,
    c.C_REGION AS C_REGION,
    c.C_PHONE AS C_PHONE,
    c.C_MKTSEGMENT AS C_MKTSEGMENT,
    s.S_NAME AS S_NAME,
    s.S_ADDRESS AS S_ADDRESS,
    s.S_CITY AS S_CITY,
    s.S_NATION AS S_NATION,
    s.S_REGION AS S_REGION,
    s.S_PHONE AS S_PHONE,
    p.P_NAME AS P_NAME,
    p.P_MFGR AS P_MFGR,
    p.P_CATEGORY AS P_CATEGORY,
    p.P_BRAND AS P_BRAND,
    p.P_COLOR AS P_COLOR,
    p.P_TYPE AS P_TYPE,
    p.P_SIZE AS P_SIZE,
    p.P_CONTAINER AS P_CONTAINER FROM lineorder AS l
INNER JOIN customer AS c ON c.C_CUSTKEY = l.LO_CUSTKEY
INNER JOIN supplier AS s ON s.S_SUPPKEY = l.LO_SUPPKEY
INNER JOIN part AS p ON p.P_PARTKEY = l.LO_PARTKEY;
```

例 8: テーブル `order_detail` を非同期でクエリし、クエリ結果に基づいて新しいテーブル `order_statistics` を作成し、その後クエリ結果を新しいテーブルに挿入します。

```plaintext
SUBMIT TASK AS CREATE TABLE order_statistics AS SELECT COUNT(*) as count FROM order_detail;

+-------------------------------------------+-----------+
| TaskName                                  | Status    |
+-------------------------------------------+-----------+
| ctas-df6f7930-e7c9-11ec-abac-bab8ee315bf2 | SUBMITTED |
+-------------------------------------------+-----------+
```

タスクの情報を確認します。

```SQL
SELECT * FROM INFORMATION_SCHEMA.tasks;

-- タスクの情報

TASK_NAME: ctas-df6f7930-e7c9-11ec-abac-bab8ee315bf2
CREATE_TIME: 2022-06-14 14:07:06
SCHEDULE: MANUAL
DATABASE: default_cluster:test
DEFINITION: CREATE TABLE order_statistics AS SELECT COUNT(*) as cnt FROM order_detail
EXPIRE_TIME: 2022-06-17 14:07:06
```

TaskRun の状態を確認します。

```SQL
SELECT * FROM INFORMATION_SCHEMA.task_runs;

-- TaskRun の状態

QUERY_ID: 37bd2b63-eba8-11ec-8d41-bab8ee315bf2
TASK_NAME: ctas-df6f7930-e7c9-11ec-abac-bab8ee315bf2
CREATE_TIME: 2022-06-14 14:07:06
FINISH_TIME: 2022-06-14 14:07:07
STATE: SUCCESS
DATABASE: 
DEFINITION: CREATE TABLE order_statistics AS SELECT COUNT(*) as cnt FROM order_detail
EXPIRE_TIME: 2022-06-17 14:07:06
ERROR_CODE: 0
ERROR_MESSAGE: NULL
```

TaskRun の状態が `SUCCESS` のときに新しいテーブルをクエリします。

```SQL
SELECT * FROM order_statistics;
```