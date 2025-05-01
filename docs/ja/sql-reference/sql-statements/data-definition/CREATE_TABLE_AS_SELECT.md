---
displayed_sidebar: docs
keywords: ['CTAS']
---

# CREATE TABLE AS SELECT

## 説明

CREATE TABLE AS SELECT (CTAS) ステートメントを使用して、同期または非同期でテーブルをクエリし、クエリ結果に基づいて新しいテーブルを作成し、その後クエリ結果を新しいテーブルに挿入できます。

## 構文

- テーブルを同期的にクエリし、クエリ結果に基づいて新しいテーブルを作成し、その後クエリ結果を新しいテーブルに挿入します。

  ```SQL
  CREATE TABLE [IF NOT EXISTS] [database.]table_name
  [(column_name [, column_name2, ...]]
  [key_desc]
  [COMMENT "table comment"]
  [partition_desc]
  [distribution_desc]
  [PROPERTIES ("key"="value", ...)]
  AS SELECT query
  [ ... ]
  ```

- テーブルを非同期でクエリし、クエリ結果に基づいて新しいテーブルを作成し、その後クエリ結果を新しいテーブルに挿入します。

  ```SQL
  SUBMIT [/*+ SET_VAR(key=value) */] TASK [[database.]<task_name>]AS
  CREATE TABLE [IF NOT EXISTS] [database.]table_name
  [(column_name [, column_name2, ...]]
  [key_desc]
  [COMMENT "table comment"]
  [partition_desc]
  [distribution_desc]
  [PROPERTIES ("key"="value", ...)]AS SELECT query
  [ ... ]
  ```

## パラメータ

| **パラメータ**     | **必須** | **説明**                                              |
| ----------------- | ------------ | ------------------------------------------------------------ |
| column_name       | はい          | 新しいテーブルのカラム名です。カラムのデータ型を指定する必要はありません。StarRocks はカラムに適切なデータ型を自動的に指定します。StarRocks は FLOAT および DOUBLE データを DECIMAL(38,9) データに変換します。また、CHAR、VARCHAR、および STRING データを VARCHAR(65533) データに変換します。 |
| key_desc          | いいえ           | 構文は `key_type ( <col_name1> [, <col_name2> , ...])` です。<br />**パラメータ**:<ul><li>`key_type`: [新しいテーブルのキータイプ](../../../table_design/table_types/table_types.md)。有効な値: `DUPLICATE KEY` と `PRIMARY KEY`。デフォルト値: `DUPLICATE KEY`。</li><li> `col_name`: キーを形成するカラム。</li></ul> |
| COMMENT           | いいえ           | 新しいテーブルのコメントです。                                |
| partition_desc    | いいえ           | 新しいテーブルのパーティション化の手法です。このパラメータを指定しない場合、デフォルトで新しいテーブルにはパーティションがありません。パーティション化の詳細については、CREATE TABLE を参照してください。 |
| distribution_desc | いいえ           | 新しいテーブルのバケッティング手法です。このパラメータを指定しない場合、バケットカラムはコストベースオプティマイザ (CBO) によって収集された最も高いカーディナリティを持つカラムにデフォルト設定されます。バケット数はデフォルトで10です。CBO がカーディナリティに関する情報を収集しない場合、バケットカラムは新しいテーブルの最初のカラムにデフォルト設定されます。バケッティングの詳細については、CREATE TABLE を参照してください。 |
| Properties        | いいえ           | 新しいテーブルのプロパティです。                             |
| AS SELECT query   | はい          | クエリ結果です。`... AS SELECT query` でカラムを指定できます。例えば、`... AS SELECT a, b, c FROM table_a;` のように指定します。この例では、`a`、`b`、`c` はクエリされたテーブルのカラム名を示します。新しいテーブルのカラム名を指定しない場合、新しいテーブルのカラム名も `a`、`b`、`c` になります。`... AS SELECT query` で式を指定することもできます。例えば、`... AS SELECT a+1 AS x, b+2 AS y, c*c AS z FROM table_a;` のように指定します。この例では、`a+1`、`b+2`、`c*c` はクエリされたテーブルのカラム名を示し、`x`、`y`、`z` は新しいテーブルのカラム名を示します。注意: 新しいテーブルのカラム数は、SELECT ステートメントで指定されたカラム数と同じである必要があります。識別しやすいカラム名を使用することをお勧めします。 |

## 使用上の注意

- CTAS ステートメントは、以下の要件を満たす新しいテーブルのみを作成できます:
  - `ENGINE` は `OLAP` です。

  - テーブルはデフォルトで重複キーテーブルです。`key_desc` で主キーテーブルとして指定することもできます。

  - ソートキーは最初の3つのカラムであり、これらの3つのカラムのデータ型のストレージスペースは36バイトを超えません。

- CTAS ステートメントは、新しく作成されたテーブルにインデックスを設定することをサポートしていません。

- CTAS ステートメントが FE の再起動などの理由で実行に失敗した場合、以下のいずれかの問題が発生する可能性があります:
  - 新しいテーブルは正常に作成されますが、データが含まれていません。

  - 新しいテーブルの作成に失敗します。

- 新しいテーブルが作成された後、複数の方法（INSERT INTO など）を使用して新しいテーブルにデータを挿入する場合、INSERT 操作を最初に完了した方法がそのデータを新しいテーブルに挿入します。

- 新しいテーブルが作成された後、このテーブルに対する権限を手動でユーザーに付与する必要があります。

- クエリ結果に基づいてテーブルを非同期でクエリし、新しいテーブルを作成する際にタスクの名前を指定しない場合、StarRocks はタスクの名前を自動的に生成します。

## 例

例 1: テーブル `order` を同期的にクエリし、クエリ結果に基づいて新しいテーブル `order_new` を作成し、その後クエリ結果を新しいテーブルに挿入します。

```SQL
CREATE TABLE order_new
AS SELECT * FROM order;
```

例 2: テーブル `order` の `k1`、`k2`、`k3` カラムを同期的にクエリし、クエリ結果に基づいて新しいテーブル `order_new` を作成し、その後クエリ結果を新しいテーブルに挿入します。さらに、新しいテーブルのカラム名を `a`、`b`、`c` に設定します。

```SQL
CREATE TABLE order_new (a, b, c)
AS SELECT k1, k2, k3 FROM order;
```

または

```SQL
CREATE TABLE order_new
AS SELECT k1 AS a, k2 AS b, k3 AS c FROM order;
```

例 3: テーブル `employee` の `salary` カラムの最大値を同期的にクエリし、クエリ結果に基づいて新しいテーブル `employee_new` を作成し、その後クエリ結果を新しいテーブルに挿入します。さらに、新しいテーブルのカラム名を `salary_max` に設定します。

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

例 4: CTAS を使用して主キーテーブルを作成します。主キーテーブルのデータ行数は、クエリ結果の行数より少ない場合があります。これは、[主キーテーブル](../../../table_design/table_types/primary_key_table.md) が同じ主キーを持つ行の中で最新のデータ行のみを保存するためです。

```SQL
CREATE TABLE employee_new
PRIMARY KEY(order_id)
AS SELECT
    order_list.order_id,
    sum(goods.price) as total
FROM order_list INNER JOIN goods ON goods.item_id1 = order_list.item_id2
GROUP BY order_id;
```

例 5: `lineorder`、`customer`、`supplier`、`part` の4つのテーブルを同期的にクエリし、クエリ結果に基づいて新しいテーブル `lineorder_flat` を作成し、その後クエリ結果を新しいテーブルに挿入します。さらに、新しいテーブルのパーティション化の手法とバケッティング手法を指定します。

```SQL
CREATE TABLE lineorder_flat
PARTITION BY RANGE(`LO_ORDERDATE`)
(
    START ("1993-01-01") END ("1999-01-01") EVERY (INTERVAL 1 YEAR)
)
DISTRIBUTED BY HASH(`LO_ORDERKEY`) BUCKETS 120 AS SELECT
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

例 6: テーブル `order_detail` を非同期でクエリし、クエリ結果に基づいて新しいテーブル `order_statistics` を作成し、その後クエリ結果を新しいテーブルに挿入します。

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