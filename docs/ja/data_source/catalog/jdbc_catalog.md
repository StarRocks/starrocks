---
sidebar_position: 70
displayed_sidebar: docs
description: "StarRocks v3.0 以降の JDBC catalog で、JDBC データソースからデータをインジェストせずにクエリおよび変換ロード。"
toc_max_heading_level: 4
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'
import JoinPushdown from '../../_assets/commonMarkdown/join_pushdown.mdx'

# JDBC catalog

<Beta />

StarRocks は v3.0 以降で JDBC catalog をサポートしています。

JDBC catalog は、データを取り込むことなく、JDBC を通じてアクセスされるデータソースからデータをクエリすることを可能にする一種の external catalog です。

また、JDBC catalog を使用して、JDBC データソースからデータを直接変換してロードすることもできます。[INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) を使用します。

JDBC catalog は v3.0 から MySQL と PostgreSQL を、v3.2.9 と v3.3.1 から Oracle と SQLServer を、v3.3.0 から ClickHouse (実験的) をサポートしています。

## PostgreSQL の精度未指定 numeric

精度とスケールを指定していない PostgreSQL の `numeric` および `decimal` 列は、`VARCHAR` ではなく StarRocks の `DECIMAL(38,18)`（Decimal128）にマッピングされます。整数部は最大 20 桁、小数部は最大 18 桁です。値は正確に表現できる必要があります。オーバーフロー、小数点以下 18 桁を超えるゼロ以外の数字、`NaN`、無限大は読み取りエラーになります。小数部の余分な末尾ゼロは許可され、NULL は NULL のままです。

この変更により、これらの列の並べ替え、比較、グループ化、関数解決には数値のセマンティクスが使用されます。精度とスケールを明示した numeric 列のマッピングは変更されません。精度未指定 numeric 列を読み取るスキャンでは、変換後に StarRocks で評価するため、述語、式、集約、結合のプッシュダウンを保守的に無効にします。PostgreSQL ビューや `native_query` の戻り値も、メタデータが精度未指定 numeric を示す場合は同じマッピングを使用します。

このマッピングを使用する前に、FE、BE/CN、JDBC bridge を一緒にアップグレードしてください。範囲外の値には、業務に適した精度とスケールへ明示的に変換する PostgreSQL ビューを使用できます。テキスト表現の保持が必要な場合は、ビューからテキストを返してください。

## 前提条件

- StarRocks クラスター内の FEs と BEs または CNs が、`driver_url` パラメーターで指定されたダウンロード URL から JDBC ドライバーをダウンロードできること。
- 各 BE または CN ノードの **$BE_HOME/bin/start_be.sh** ファイル内の `JAVA_HOME` が、JRE 環境のパスではなく JDK 環境のパスとして適切に設定されていること。例えば、`export JAVA_HOME = <JDK_absolute_path>` と設定できます。この設定をスクリプトの先頭に追加し、BE または CN を再起動して設定を有効にする必要があります。

## JDBC catalog の作成

### 構文

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES ("key"="value", ...)
```

### パラメーター

#### `catalog_name`

JDBC catalog の名前。命名規則は以下の通りです：

- 名前には文字、数字 (0-9)、アンダースコア (_) を含めることができます。文字で始める必要があります。
- 名前は大文字と小文字を区別し、長さは 1023 文字を超えることはできません。

#### `comment`

JDBC catalog の説明。このパラメーターはオプションです。

#### `PROPERTIES`

JDBC catalog のプロパティ。`PROPERTIES` には以下のパラメーターを含める必要があります：

| **Parameter**     | **Description**                                                     |
| ----------------- | ------------------------------------------------------------ |
| type              | リソースのタイプ。値を `jdbc` に設定します。           |
| user              | ターゲットデータベースに接続するために使用されるユーザー名。 |
| password          | ターゲットデータベースに接続するために使用されるパスワード。 |
| jdbc_uri          | JDBC ドライバーがターゲットデータベースに接続するために使用する URI。MySQL の場合、URI は `"jdbc:mysql://ip:port"` 形式です。PostgreSQL の場合、URI は `"jdbc:postgresql://ip:port/db_name"` 形式です。詳細は [PostgreSQL](https://jdbc.postgresql.org/documentation/head/connect.html) を参照してください。 |
| driver_url        | JDBC ドライバー JAR パッケージのダウンロード URL。HTTP URL またはファイル URL がサポートされます。例えば、`https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar` や `file:///home/disk1/postgresql-42.3.3.jar` です。<br />**注意**<br />JDBC ドライバーを FE と BE または CN ノードの同じパスに配置し、`driver_url` をそのパスに設定することもできます。この場合、`file:///<path>/to/the/driver` 形式でなければなりません。 |
| driver_class      | JDBC ドライバーのクラス名。一般的なデータベースエンジンの JDBC ドライバークラス名は以下の通りです：<ul><li>MySQL: `com.mysql.jdbc.Driver` (MySQL v5.x およびそれ以前) および `com.mysql.cj.jdbc.Driver` (MySQL v6.x およびそれ以降)</li><li>PostgreSQL: `org.postgresql.Driver`</li><li>Oracle: `oracle.jdbc.driver.OracleDriver`</li></ul> |
| schema_resolver   | (オプション) 使用するスキーマリゾルバーを明示的に指定します。有効な値：`postgresql`、`mysql`、`oracle`、`sqlserver`、`clickhouse`。ドライバークラス名から自動検出できない非標準の JDBC ドライバーを使用する場合にこのパラメーターを使用します。指定しない場合、StarRocks は `driver_class` パラメーターに基づいて適切なリゾルバーを自動検出します。 |

#### オプションの Oracle プロパティ

`driver_class` が Oracle に設定されている場合、以下のオプションのプロパティを設定できます：

| **パラメータ**                  | **デフォルト** | **説明**                                                                                                      |
| ------------------------------ | ----------- | ------------------------------------------------------------------------------------------------------------- |
| oracle.number.default-scale    | 6           | Oracle の `NUMBER` メタデータに明示的な精度とスケールが指定されていない場合に設定します。有効な範囲：`0`～`38`。             |
| oracle.temporal.to-datetime    | false       | Oracle の `DATE`、`TIMESTAMP`、および `TIMESTAMP WITH LOCAL TIME ZONE` のマッピングを制御します。この設定が `true` に設定されている場合、これらのデータ型は StarRocks の `DATETIME` 型にマッピングされます。そうでない場合、`DATE` は `DATE` のままとなり、`TIMESTAMP` および `TIMESTAMP WITH LOCAL TIME ZONE` は `VARCHAR(64)` にマッピングされます。 |
| oracle.timestamptz.to-datetime | false       | Oracle の `TIMESTAMP WITH TIME ZONE` のマッピングを制御します。`true` に設定されている場合、StarRocksの `DATETIME` 型にマッピングされます。それ以外の場合は、 `VARCHAR(64)` にマッピングされます。 |

#### オプションの統計情報キャッシュプロパティ

StarRocks は JDBC データソースから読み取ったテーブルごとの統計情報をキャッシュし、クエリプランニング時のブロッキングを回避します。1 つのキャッシュエントリはテーブルの行数とデータソースが報告する列統計情報の両方を保持し、両者は一体として読み込みおよび削除されます。これらのプロパティを使用して、カタログごとにキャッシュの動作を調整できます。設定しない場合は、グローバル FE 設定値が使用されます。パラメータ名は列統計情報の導入以前のもので、変更されていません。

| **パラメータ**                       | **デフォルト** | **説明**                                                                                                              |
| ----------------------------------- | ------------ | --------------------------------------------------------------------------------------------------------------------- |
| jdbc_row_count_cache_refresh_sec    | 600          | バックグラウンド更新間隔（秒）。この間隔を過ぎると、古いキャッシュエントリを即座に返しながらバックグラウンドで非同期に再読み込みします。 |
| jdbc_row_count_cache_expire_sec     | 1200         | 強制削除 TTL（秒）。この期間内にアクセスされなかったキャッシュエントリは削除されます。`jdbc_row_count_cache_refresh_sec` より大きい値を設定してください。 |
| jdbc_row_count_cache_max_size       | 10000        | このカタログの統計情報キャッシュの最大テーブルエントリ数。                                                                |

> **注意**
>
> FEs は JDBC catalog 作成時に JDBC ドライバー JAR パッケージをダウンロードし、BEs または CNs は最初のクエリ時に JDBC ドライバー JAR パッケージをダウンロードします。ダウンロードにかかる時間はネットワークの状況によって異なります。

### PostgreSQL の配列

`org.postgresql.Driver` を使用すると、次の PostgreSQL 配列列を読み取れます。

| PostgreSQL の列型 | StarRocks の型 |
| --- | --- |
| `boolean[]` | `ARRAY<BOOLEAN>` |
| `smallint[]` | `ARRAY<SMALLINT>` |
| `integer[]` | `ARRAY<INT>` |
| `bigint[]` | `ARRAY<BIGINT>` |
| `real[]` | `ARRAY<FLOAT>` |
| `double precision[]` | `ARRAY<DOUBLE>` |
| `date[]` | `ARRAY<DATE>` |
| `timestamp[]`（タイムゾーンなし） | `ARRAY<DATETIME>` |
| `text[]`、`varchar[]`、`char(n)[]` | `ARRAY<VARCHAR>` |

一次元配列の要素順序、UTF-8 文字列、NULL 配列、空配列、NULL 要素は保持されます。`char(n)[]` は `ARRAY<CHAR>` ではなく `ARRAY<VARCHAR>` にマッピングされ、要素は PostgreSQL が格納する空白埋めをそのまま保持します。`date[]` と `timestamp[]` の要素はセッションのタイムゾーンに影響されない壁時計の値として読み取られ、`0001-01-01` から `9999-12-31` の範囲外の要素（紀元前の日付や `infinity`）は別の値として読まれるのではなくクエリが失敗します。これらの列をクエリする前に、FE、BE/CN、JDBC Bridge、およびパッケージ内の JDBC 型マッピングを一緒にアップグレードしてください。

その他の配列要素型はすべて未対応の型にマッピングされ、その列はクエリできません。`numeric[]`、`uuid[]`、`time[]`、`bytea[]`、`json[]`、`jsonb[]` に加えて `timestamptz[]` も含まれます。`timestamp with time zone` を `ARRAY<DATETIME>` として読み込むと、夏時間の繰り下げにあたる 2 つの時刻が同じ壁時計に潰れ、後段では区別できなくなるためです。

StarRocks 配列の位置は 1 から始まり、PostgreSQL の配列の下限は保持されません。たとえば PostgreSQL の `[0:1]={a,b}` は `["a","b"]` として読み込まれ、StarRocks の `items[1]` は `a` を返します。このような列に対する定数の添字（`items[1]`、および等価な `element_at(items, 1)`）は PostgreSQL で評価され、その要素だけが返されます。配列全体を読み戻してからローカルで添字を取ることはありません。添字はそのままプッシュダウンされ、下限は 1 であると仮定されます。PostgreSQL 自身が構築する配列の下限は 1 です。そのため、別の下限で格納された値では、プッシュダウンされた添字は StarRocks でローカルに評価した同じ添字とは異なる要素を、警告なく返します。`[0:2]={zero,one,two}` では、リモートの `items[1]` は `one`、ローカルでは `zero` です。プッシュダウンの結果をローカルの評価と完全に一致させるには、[`enable_jdbc_array_lower_bound_correction`](../../sql-reference/System_variable.md#enable_jdbc_array_lower_bound_correction) を `true` に設定してください。この場合、プッシュダウンされる添字は各値自身の下限に合わせて補正されます。[`enable_jdbc_array_subscript_push_down`](../../sql-reference/System_variable.md#enable_jdbc_array_subscript_push_down) を `false` に設定すると、このプッシュダウン自体を完全に無効化できます。フィルターと射影の両方の経路が閉じられ、配列カラム全体を読み戻して StarRocks で添字を取る動作に戻ります。2 つの変数が組み合わさる方向は 1 つだけです。プッシュダウンを無効にすると、PostgreSQL に添字が届かないため、`enable_jdbc_array_lower_bound_correction` は何も制御しなくなります。変数の添字（`items[id]` など）、配列全体の比較、結合、ソート、およびグループ化や重複排除集約は StarRocks で実行されます。多次元配列の読み込みは明示的な未対応エラーとなり、一次元には展開されません。ただし多次元の値を保持する列に対する定数の添字は、他と同様にプッシュダウンされます。PostgreSQL は全次元を指定しない添字に対して NULL を返すため、列を直接読めばエラーになるところでクエリは NULL を返します。`enable_jdbc_array_lower_bound_correction` のどちらの設定でもこの挙動は変わりません。PostgreSQL は列に次元を記録しないためです（`attndims` は強制されず、同一の列が行ごとに異なる次元の値を保持できます）。ただし `enable_jdbc_array_subscript_push_down` を無効にすると変わります。その場合はカラムが再び直接読み取られるため、クエリは NULL ではなく未対応エラーになります。

### 例

以下の例では、5 つの異なる JDBC catalog を作成します。

```SQL
-- PostgresSQL
CREATE EXTERNAL CATALOG jdbc0
PROPERTIES
(
    "type"="jdbc", 
    "user"="postgres",
    "password"="changeme",
    "jdbc_uri"="jdbc:postgresql://127.0.0.1:5432/jdbc_test",
    "driver_url"="https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar",
    "driver_class"="org.postgresql.Driver"
);
-- MySQL
CREATE EXTERNAL CATALOG jdbc1
PROPERTIES
(
    "type"="jdbc",
    "user"="root",
    "password"="changeme",
    "jdbc_uri"="jdbc:mysql://127.0.0.1:3306",
    "driver_url"="https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar",
    "driver_class"="com.mysql.cj.jdbc.Driver"
);
-- Oracle
CREATE EXTERNAL CATALOG jdbc2
PROPERTIES
(
    "type"="jdbc",
    "user"="root",
    "password"="changeme",
    "jdbc_uri"="jdbc:oracle:thin:@127.0.0.1:1521:ORCL",
    "driver_url"="https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc10/19.18.0.0/ojdbc10-19.18.0.0.jar",
    "driver_class"="oracle.jdbc.driver.OracleDriver"
);
-- Oracle (with Oracle-specific optional properties)
CREATE EXTERNAL CATALOG jdbc2_ext
PROPERTIES
(
    "type"="jdbc",
    "user"="root",
    "password"="changeme",
    "jdbc_uri"="jdbc:oracle:thin:@127.0.0.1:1521/ORCLPDB1",
    "driver_url"="https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc10/19.18.0.0/ojdbc10-19.18.0.0.jar",
    "driver_class"="oracle.jdbc.driver.OracleDriver",
    "oracle.number.default-scale"="6",
    "oracle.temporal.to-datetime"="true",
    "oracle.timestamptz.to-datetime"="true"
);
-- SQL Server
CREATE EXTERNAL CATALOG jdbc3
PROPERTIES
(
    "type"="jdbc",
    "user"="root",
    "password"="changeme",
    "jdbc_uri"="jdbc:sqlserver://127.0.0.1:1433;databaseName=MyDatabase;",
    "driver_url"="https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.4.2.jre11/mssql-jdbc-12.4.2.jre11.jar",
    "driver_class"="com.microsoft.sqlserver.jdbc.SQLServerDriver"
);
-- ClickHouse
CREATE EXTERNAL CATALOG jdbc4
PROPERTIES
(
    "type"="jdbc",
    "user"="default",
    "jdbc_uri"="jdbc:clickhouse://127.0.0.1:8443",
    "driver_url"="https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6.jar",
    "driver_class"="com.clickhouse.jdbc.ClickHouseDriver"
);
-- 非標準ドライバーに schema_resolver を使用
CREATE EXTERNAL CATALOG jdbc5
PROPERTIES
(
    "type"="jdbc",
    "user"="postgres",
    "password"="changeme",
    "jdbc_uri"="jdbc:postgresql://127.0.0.1:5432/mydb",
    "driver_url"="file:///path/to/custom-postgresql-driver.jar",
    "driver_class"="com.custom.PostgresDriver",
    "schema_resolver"="postgresql"
);
```

## JDBC catalog の表示

現在の StarRocks クラスター内のすべての catalog をクエリするには、[SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) を使用します。

```SQL
SHOW CATALOGS;
```

また、external catalog の作成ステートメントをクエリするには、[SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) を使用します。以下の例では、`jdbc0` という名前の JDBC catalog の作成ステートメントをクエリします。

```SQL
SHOW CREATE CATALOG jdbc0;
```

## JDBC catalog の削除

JDBC catalog を削除するには、[DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) を使用します。

以下の例では、`jdbc0` という名前の JDBC catalog を削除します。

```SQL
DROP Catalog jdbc0;
```

## JDBC catalog のテーブルをクエリする

1. JDBC 互換クラスター内のデータベースを表示するには、[SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) を使用します。

   ```SQL
   SHOW DATABASES FROM <catalog_name>;
   ```

2. 現在のセッションで目的の catalog に切り替えるには、[SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) を使用します。

    ```SQL
    SET CATALOG <catalog_name>;
    ```

    次に、現在のセッションでアクティブなデータベースを指定するには、[USE](../../sql-reference/sql-statements/Database/USE.md) を使用します。

    ```SQL
    USE <db_name>;
    ```

    または、目的の catalog 内でアクティブなデータベースを直接指定するには、[USE](../../sql-reference/sql-statements/Database/USE.md) を使用します。

    ```SQL
    USE <catalog_name>.<db_name>;
    ```

3. 指定されたデータベース内の目的のテーブルをクエリするには、[SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT/SELECT.md) を使用します。

   ```SQL
   SELECT * FROM <table_name>;
   ```

<JoinPushdown />

### PostgreSQL の日付とタイムスタンプ

JDBC catalog を通じて PostgreSQL の `date` と `timestamp without time zone` を読み取る場合、元の年月日と時刻が保持されます。JVM のデフォルトタイムゾーンによる時刻の変更はなく、タイムスタンプはマイクロ秒精度を保持します。サポートされる年の範囲は西暦 0001 年から 9999 年です。紀元前の日付、`infinity`、`-infinity`、または範囲外の値を読み取ると、年代や値を暗黙に変更せずエラーを返します。

## ネイティブ SQL で JDBC データをクエリする

v4.1 以降、StarRocks は [`native_query`](../../sql-reference/sql-functions/table-functions/native_query.md) テーブル関数を使用して、データベースネイティブの `SELECT` 文で JDBC データをクエリできます。

ソースデータベース側の Join、事前にフィルタリングしたサブクエリ、またはベンダー固有の SQL 構文など、単一の external table クエリでは表現できない SQL をソースデータベースで実行する必要がある場合に `native_query` を使用できます。StarRocks はパススルークエリの結果を通常のリレーションとして公開するため、StarRocks 側でさらにフィルター、Join、集計、射影を適用できます。

構文、制限事項、例については、[`native_query`](../../sql-reference/sql-functions/table-functions/native_query.md) を参照してください。

## FAQ

「Malformed database URL, failed to parse the main URL sections」というエラーが発生した場合はどうすればよいですか？

このようなエラーが発生した場合、`jdbc_uri` に渡した URI が無効です。渡した URI を確認し、有効であることを確認してください。詳細については、このトピックの「[PROPERTIES](#properties)」セクションのパラメーター説明を参照してください。
