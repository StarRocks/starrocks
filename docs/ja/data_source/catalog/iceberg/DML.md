---
displayed_sidebar: docs
toc_max_heading_level: 5
keywords: ['iceberg', 'dml', 'insert', 'sink data', 'overwrite']
---

# Iceberg DML 操作

このドキュメントでは、StarRocksにおけるIcebergカタログのデータ操作言語（DML）操作について説明します。これには、Icebergテーブルへのデータ挿入が含まれます。

DML操作を実行するには、適切な権限が必要です。権限の詳細については、[権限](../../../administration/user_privs/authorization/privilege_item.md)を参照してください。

---

## INSERT

Icebergテーブルにデータを挿入します。この機能はv3.1以降でサポートされています。

StarRocksの内部テーブルと同様に、Icebergテーブルに対して [INSERT](../../../administration/user_privs/authorization/privilege_item.md#table) 権限を持っている場合、 [INSERT](../../../sql-reference/sql-statements/loading_unloading/INSERT.md) ステートメントを使用して、StarRocksテーブルのデータをそのIcebergテーブルにシンクできます（現在、Parquet形式のIcebergテーブルのみがサポートされています）。

### 構文

```SQL
INSERT {INTO | OVERWRITE} <table_name>
[ (column_name [, ...]) ]
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }

-- 指定されたパーティションにデータをシンクする場合、次の構文を使用します。
INSERT {INTO | OVERWRITE} <table_name>
PARTITION (par_col1=<value> [, par_col2=<value>...])
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }
```

:::note

パーティション列は `NULL` 値を許可しません。したがって、Icebergテーブルのパーティション列に空の値がロードされないようにする必要があります。

:::

### パラメーター

#### INTO

StarRocks テーブルのデータを Iceberg テーブルに追加します。

#### OVERWRITE

StarRocks テーブルのデータで Iceberg テーブルの既存のデータを上書きします。

#### column_name

データをロードしたい宛先列の名前。1 つ以上の列を指定できます。複数の列を指定する場合、カンマ (`,`) で区切ります。Iceberg テーブルに実際に存在する列のみを指定できます。また、指定した宛先列には Iceberg テーブルのパーティション列を含める必要があります。指定した宛先列は、StarRocks テーブルの列と順番に 1 対 1 でマッピングされます。宛先列名が何であっても関係ありません。宛先列が指定されていない場合、データは Iceberg テーブルのすべての列にロードされます。StarRocks テーブルの非パーティション列が Iceberg テーブルの任意の列にマッピングできない場合、StarRocks は Iceberg テーブル列にデフォルト値 `NULL` を書き込みます。INSERT ステートメントに含まれるクエリステートメントの戻り列タイプが宛先列のデータタイプと異なる場合、StarRocks は不一致の列に対して暗黙の変換を行います。変換が失敗した場合、構文解析エラーが返されます。

#### expression

宛先列に値を割り当てる式。

#### DEFAULT

宛先列にデフォルト値を割り当てます。

#### query

Iceberg テーブルにロードされるクエリストートメントの結果。StarRocks がサポートする任意の SQL ステートメントである可能性があります。

#### PARTITION

データをロードしたいパーティション。Iceberg テーブルのすべてのパーティション列をこのプロパティで指定する必要があります。このプロパティで指定するパーティション列は、テーブル作成ステートメントで定義したパーティション列と異なる順序であってもかまいません。このプロパティを指定する場合、`column_name` プロパティを指定することはできません。

### 例

1. `partition_tbl_1` テーブルに 3 行のデータを挿入します。

   ```SQL
   INSERT INTO partition_tbl_1
   VALUES
       ("buy", 1, "2023-09-01"),
       ("sell", 2, "2023-09-02"),
       ("buy", 3, "2023-09-03");
   ```

2. 簡単な計算を含む SELECT クエリの結果を `partition_tbl_1` テーブルに挿入します。

   ```SQL
   INSERT INTO partition_tbl_1 (id, action, dt) SELECT 1+1, 'buy', '2023-09-03';
   ```

3. `partition_tbl_1` テーブルからデータを読み取る SELECT クエリの結果を同じテーブルに挿入します。

   ```SQL
   INSERT INTO partition_tbl_1 SELECT 'buy', 1, date_add(dt, INTERVAL 2 DAY)
   FROM partition_tbl_1
   WHERE id=1;
   ```

4. `partition_tbl_2` テーブルの `dt='2023-09-01'` と `id=1` の 2 つの条件を満たすパーティションに SELECT クエリの結果を挿入します。

   ```SQL
   INSERT INTO partition_tbl_2 SELECT 'order', 1, '2023-09-01';
   ```

   または

   ```SQL
   INSERT INTO partition_tbl_2 partition(dt='2023-09-01',id=1) SELECT 'order';
   ```

5. `dt='2023-09-01'` と `id=1` の 2 つの条件を満たす `partition_tbl_1` テーブルのパーティション内のすべての `action` 列の値を `close` で上書きします。

   ```SQL
   INSERT OVERWRITE partition_tbl_1 SELECT 'close', 1, '2023-09-01';
   ```

   または

   ```SQL
   INSERT OVERWRITE partition_tbl_1 partition(dt='2023-09-01',id=1) SELECT 'close';
   ```

---
