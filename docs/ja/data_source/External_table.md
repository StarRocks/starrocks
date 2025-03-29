---
displayed_sidebar: docs
---

# 外部テーブル

:::note

- v3.0以降、Hive、Iceberg、Hudiからデータをクエリするには、catalogを使用することを推奨します。詳細は [Hive catalog](../data_source/catalog/hive_catalog.md)、[Iceberg catalog](./catalog/iceberg/iceberg_catalog.md)、[Hudi catalog](../data_source/catalog/hudi_catalog.md) を参照してください。

- v3.1以降、MySQLとPostgreSQLからデータをクエリするには [JDBC catalog](../data_source/catalog/jdbc_catalog.md) を、Elasticsearchからデータをクエリするには [Elasticsearch catalog](../data_source/catalog/elasticsearch_catalog.md) を使用することを推奨します。

- 外部テーブル機能は、StarRocksにデータをロードするために設計されており、外部システムに対して通常の操作として効率的なクエリを実行するためのものではありません。より効率的な解決策は、データをStarRocksにロードすることです。

:::

StarRocksは、外部テーブルを使用して他のデータソースにアクセスすることをサポートしています。外部テーブルは、他のデータソースに保存されているデータテーブルに基づいて作成されます。StarRocksはデータテーブルのメタデータのみを保存します。外部テーブルを使用して、他のデータソースのデータを直接クエリすることができます。StarRocksは、MySQL、StarRocks、Elasticsearch、Apache Hive™、Apache Iceberg、Apache Hudiといったデータソースをサポートしています。**現在、他のStarRocksクラスターから現在のStarRocksクラスターにデータを書き込むことができますが、データを読み取ることはできません。StarRocks以外のデータソースからは、これらのデータソースからデータを読み取ることのみが可能です。**

v2.5以降、StarRocksは外部データソースのホットデータクエリを高速化するData Cache機能を提供しています。詳細は [Data Cache](data_cache.md) を参照してください。

## StarRocks外部テーブル

StarRocks 1.19以降、StarRocksはStarRocks外部テーブルを使用して、あるStarRocksクラスターから別のクラスターにデータを書き込むことを可能にしています。これにより、読み書きの分離が実現され、リソースの分離が向上します。まず、宛先StarRocksクラスターに宛先テーブルを作成します。その後、ソースStarRocksクラスターで、宛先テーブルと同じスキーマを持つStarRocks外部テーブルを作成し、`PROPERTIES`フィールドに宛先クラスターとテーブルの情報を指定します。

データは、INSERT INTOステートメントを使用してStarRocks外部テーブルに書き込むことで、ソースクラスターから宛先クラスターに書き込むことができます。これにより、以下の目標を実現できます：

* StarRocksクラスター間のデータ同期。
* 読み書きの分離。データはソースクラスターに書き込まれ、ソースクラスターからのデータ変更は宛先クラスターに同期され、クエリサービスを提供します。

以下のコードは、宛先テーブルと外部テーブルを作成する方法を示しています。

~~~SQL
# 宛先StarRocksクラスターに宛先テーブルを作成します。
CREATE TABLE t
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
)
ENGINE=olap
DISTRIBUTED BY HASH(k1);

# ソースStarRocksクラスターに外部テーブルを作成します。
CREATE EXTERNAL TABLE external_t
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
)
ENGINE=olap
DISTRIBUTED BY HASH(k1)
PROPERTIES
(
    "host" = "127.0.0.1",
    "port" = "9020",
    "user" = "user",
    "password" = "passwd",
    "database" = "db_test",
    "table" = "t"
);

# ソースクラスターから宛先クラスターにデータを書き込むには、StarRocks外部テーブルにデータを書き込みます。2番目のステートメントは本番環境で推奨されます。
insert into external_t values ('2020-10-11', 1, 1, 'hello', '2020-10-11 10:00:00');
insert into external_t select * from other_table;
~~~

パラメータ：

* **EXTERNAL:** このキーワードは、作成されるテーブルが外部テーブルであることを示します。
* **host:** このパラメータは、宛先StarRocksクラスターのLeader FEノードのIPアドレスを指定します。
* **port:** このパラメータは、宛先StarRocksクラスターのFEノードのRPCポートを指定します。

  :::note

  StarRocks外部テーブルが属するソースクラスターが宛先StarRocksクラスターにアクセスできるようにするには、ネットワークとファイアウォールを構成して、次のポートへのアクセスを許可する必要があります：

  * FEノードのRPCポート。FE構成ファイル **fe/fe.conf** の `rpc_port` を参照してください。デフォルトのRPCポートは `9020` です。
  * BEノードのbRPCポート。BE構成ファイル **be/be.conf** の `brpc_port` を参照してください。デフォルトのbRPCポートは `8060` です。

  :::

* **user:** このパラメータは、宛先StarRocksクラスターにアクセスするために使用されるユーザー名を指定します。
* **password:** このパラメータは、宛先StarRocksクラスターにアクセスするために使用されるパスワードを指定します。
* **database:** このパラメータは、宛先テーブルが属するデータベースを指定します。
* **table:** このパラメータは、宛先テーブルの名前を指定します。

StarRocks外部テーブルを使用する際の制限は以下の通りです：

* StarRocks外部テーブルでは、INSERT INTOおよびSHOW CREATE TABLEコマンドのみを実行できます。他のデータ書き込み方法はサポートされていません。また、StarRocks外部テーブルからデータをクエリしたり、外部テーブルでDDL操作を行うことはできません。
* 外部テーブルの作成構文は通常のテーブルと同じですが、外部テーブルの列名やその他の情報は宛先テーブルと同じでなければなりません。
* 外部テーブルは、宛先テーブルから10秒ごとにテーブルメタデータを同期します。宛先テーブルでDDL操作が行われた場合、2つのテーブル間のデータ同期に遅延が生じる可能性があります。

## JDBC互換データベース用外部テーブル

v2.3.0から、StarRocksはJDBC互換データベースをクエリするための外部テーブルを提供しています。この方法により、データをStarRocksにインポートすることなく、これらのデータベースのデータを非常に高速に分析できます。このセクションでは、StarRocksで外部テーブルを作成し、JDBC互換データベースのデータをクエリする方法について説明します。

### 前提条件

JDBC外部テーブルを使用してデータをクエリする前に、FEsとBEsがJDBCドライバのダウンロードURLにアクセスできることを確認してください。ダウンロードURLは、JDBCリソースを作成するためのステートメントで指定される `driver_url` パラメータによって指定されます。

### JDBCリソースの作成と管理

#### JDBCリソースの作成

データベースからデータをクエリするための外部テーブルを作成する前に、StarRocksでJDBCリソースを作成してデータベースの接続情報を管理する必要があります。データベースはJDBCドライバをサポートしている必要があり、「ターゲットデータベース」と呼ばれます。リソースを作成した後、外部テーブルを作成するために使用できます。

次のステートメントを実行して、`jdbc0` という名前のJDBCリソースを作成します：

~~~SQL
CREATE EXTERNAL RESOURCE jdbc0
PROPERTIES (
    "type"="jdbc",
    "user"="postgres",
    "password"="changeme",
    "jdbc_uri"="jdbc:postgresql://127.0.0.1:5432/jdbc_test",
    "driver_url"="https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar",
    "driver_class"="org.postgresql.Driver"
);
~~~

`PROPERTIES` で必要なパラメータは次の通りです：

* `type`: リソースのタイプ。値を `jdbc` に設定します。

* `user`: ターゲットデータベースに接続するために使用されるユーザー名。

* `password`: ターゲットデータベースに接続するために使用されるパスワード。

* `jdbc_uri`: JDBCドライバがターゲットデータベースに接続するために使用するURI。URIの形式はデータベースURIの構文を満たす必要があります。一般的なデータベースのURI構文については、[Oracle](https://docs.oracle.com/en/database/oracle/oracle-database/21/jjdbc/data-sources-and-URLs.html#GUID-6D8EFA50-AB0F-4A2B-88A0-45B4A67C361E)、[PostgreSQL](https://jdbc.postgresql.org/documentation/head/connect.html)、[SQL Server](https://docs.microsoft.com/en-us/sql/connect/jdbc/building-the-connection-url?view=sql-server-ver16) の公式ウェブサイトを参照してください。

> 注：URIにはターゲットデータベースの名前を含める必要があります。前述のコード例では、`jdbc_test` は接続したいターゲットデータベースの名前です。

* `driver_url`: JDBCドライバJARパッケージのダウンロードURL。HTTP URLまたはファイルURLがサポートされています。例：`https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar` または `file:///home/disk1/postgresql-42.3.3.jar`。

* `driver_class`: JDBCドライバのクラス名。一般的なデータベースのJDBCドライバクラス名は次の通りです：
  * MySQL: com.mysql.jdbc.Driver (MySQL 5.x以前)、com.mysql.cj.jdbc.Driver (MySQL 6.x以降)
  * SQL Server: com.microsoft.sqlserver.jdbc.SQLServerDriver
  * Oracle: oracle.jdbc.driver.OracleDriver
  * PostgreSQL: org.postgresql.Driver

リソースが作成されると、FEは `driver_url` パラメータで指定されたURLを使用してJDBCドライバJARパッケージをダウンロードし、チェックサムを生成し、BEsがダウンロードしたJDBCドライバを検証するためにチェックサムを使用します。

> 注：JDBCドライバJARパッケージのダウンロードに失敗した場合、リソースの作成も失敗します。

BEsがJDBC外部テーブルを初めてクエリし、対応するJDBCドライバJARパッケージがマシンに存在しない場合、BEsは `driver_url` パラメータで指定されたURLを使用してJDBCドライバJARパッケージをダウンロードし、すべてのJDBCドライバJARパッケージは `${STARROCKS_HOME}/lib/jdbc_drivers` ディレクトリに保存されます。

#### JDBCリソースの表示

次のステートメントを実行して、StarRocks内のすべてのJDBCリソースを表示します：

~~~SQL
SHOW RESOURCES;
~~~

> 注：`ResourceType` 列は `jdbc` です。

#### JDBCリソースの削除

次のステートメントを実行して、`jdbc0` という名前のJDBCリソースを削除します：

~~~SQL
DROP RESOURCE "jdbc0";
~~~

> 注：JDBCリソースが削除されると、そのJDBCリソースを使用して作成されたすべてのJDBC外部テーブルは使用できなくなります。ただし、ターゲットデータベース内のデータは失われません。ターゲットデータベース内のデータをStarRocksでクエリする必要がある場合は、JDBCリソースとJDBC外部テーブルを再作成できます。

### データベースの作成

次のステートメントを実行して、StarRocks内に `jdbc_test` という名前のデータベースを作成し、アクセスします：

~~~SQL
CREATE DATABASE jdbc_test; 
USE jdbc_test; 
~~~

> 注：前述のステートメントで指定するデータベース名は、ターゲットデータベースの名前と同じである必要はありません。

### JDBC外部テーブルの作成

次のステートメントを実行して、データベース `jdbc_test` に `jdbc_tbl` という名前のJDBC外部テーブルを作成します：

~~~SQL
create external table jdbc_tbl (
    `id` bigint NULL, 
    `data` varchar(200) NULL 
) ENGINE=jdbc 
properties (
    "resource" = "jdbc0",
    "table" = "dest_tbl"
);
~~~

`properties` で必要なパラメータは次の通りです：

* `resource`: 外部テーブルを作成するために使用されるJDBCリソースの名前。

* `table`: データベース内のターゲットテーブル名。

StarRocksとターゲットデータベース間のサポートされているデータ型とデータ型のマッピングについては、[Data type mapping](External_table.md#Data type mapping) を参照してください。

> 注：
>
> * インデックスはサポートされていません。
> * データ分布ルールを指定するためにPARTITION BYまたはDISTRIBUTED BYを使用することはできません。

### JDBC外部テーブルのクエリ

JDBC外部テーブルをクエリする前に、次のステートメントを実行してPipelineエンジンを有効にする必要があります：

~~~SQL
set enable_pipeline_engine=true;
~~~

> 注：Pipelineエンジンがすでに有効になっている場合、このステップをスキップできます。

次のステートメントを実行して、JDBC外部テーブルを使用してターゲットデータベース内のデータをクエリします。

~~~SQL
select * from JDBC_tbl;
~~~

StarRocksは、フィルタ条件をターゲットテーブルにプッシュダウンすることで、述語プッシュダウンをサポートしています。データソースにできるだけ近い場所でフィルタ条件を実行することで、クエリパフォーマンスを向上させることができます。現在、StarRocksは、二項比較演算子（`>`, `>=`, `=`, `<`, `<=`）、`IN`、`IS NULL`、`BETWEEN ... AND ...` を含む演算子をプッシュダウンできます。ただし、StarRocksは関数をプッシュダウンすることはできません。

### データ型のマッピング

現在、StarRocksはターゲットデータベース内の基本型のデータのみをクエリできます。たとえば、NUMBER、STRING、TIME、DATEなどです。ターゲットデータベース内のデータ値の範囲がStarRocksでサポートされていない場合、クエリはエラーを報告します。

ターゲットデータベースとStarRocks間のマッピングは、ターゲットデータベースのタイプに基づいて異なります。

#### **MySQLとStarRocks**

| MySQL        | StarRocks |
| ------------ | --------- |
| BOOLEAN      | BOOLEAN   |
| TINYINT      | TINYINT   |
| SMALLINT     | SMALLINT  |
| MEDIUMINTINT | INT       |
| BIGINT       | BIGINT    |
| FLOAT        | FLOAT     |
| DOUBLE       | DOUBLE    |
| DECIMAL      | DECIMAL   |
| CHAR         | CHAR      |
| VARCHAR      | VARCHAR   |
| DATE         | DATE      |
| DATETIME     | DATETIME  |

#### **OracleとStarRocks**

| Oracle          | StarRocks |
| --------------- | --------- |
| CHAR            | CHAR      |
| VARCHARVARCHAR2 | VARCHAR   |
| DATE            | DATE      |
| SMALLINT        | SMALLINT  |
| INT             | INT       |
| BINARY_FLOAT    | FLOAT     |
| BINARY_DOUBLE   | DOUBLE    |
| DATE            | DATE      |
| DATETIME        | DATETIME  |
| NUMBER          | DECIMAL   |

#### **PostgreSQLとStarRocks**

| PostgreSQL          | StarRocks |
| ------------------- | --------- |
| SMALLINTSMALLSERIAL | SMALLINT  |
| INTEGERSERIAL       | INT       |
| BIGINTBIGSERIAL     | BIGINT    |
| BOOLEAN             | BOOLEAN   |
| REAL                | FLOAT     |
| DOUBLE PRECISION    | DOUBLE    |
| DECIMAL             | DECIMAL   |
| TIMESTAMP           | DATETIME  |
| DATE                | DATE      |
| CHAR                | CHAR      |
| VARCHAR             | VARCHAR   |
| TEXT                | VARCHAR   |

#### **SQL ServerとStarRocks**

| SQL Server        | StarRocks |
| ----------------- | --------- |
| BOOLEAN           | BOOLEAN   |
| TINYINT           | TINYINT   |
| SMALLINT          | SMALLINT  |
| INT               | INT       |
| BIGINT            | BIGINT    |
| FLOAT             | FLOAT     |
| REAL              | DOUBLE    |
| DECIMALNUMERIC    | DECIMAL   |
| CHAR              | CHAR      |
| VARCHAR           | VARCHAR   |
| DATE              | DATE      |
| DATETIMEDATETIME2 | DATETIME  |

### 制限事項

* JDBC外部テーブルを作成する際、テーブルにインデックスを作成したり、PARTITION BYやDISTRIBUTED BYを使用してテーブルのデータ分布ルールを指定することはできません。

* JDBC外部テーブルをクエリする際、StarRocksは関数をテーブルにプッシュダウンすることはできません。

## (非推奨) Elasticsearch外部テーブル

StarRocksとElasticsearchは、2つの人気のある分析システムです。StarRocksは大規模な分散コンピューティングにおいて高性能であり、Elasticsearchは全文検索に最適です。StarRocksとElasticsearchを組み合わせることで、より完全なOLAPソリューションを提供できます。

### Elasticsearch外部テーブルの作成例

#### 構文

~~~sql
CREATE EXTERNAL TABLE elastic_search_external_table
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
)
ENGINE=ELASTICSEARCH
PROPERTIES (
    "hosts" = "http://192.168.0.1:9200,http://192.168.0.2:9200",
    "user" = "root",
    "password" = "root",
    "index" = "tindex",
    "type" = "_doc",
    "es.net.ssl" = "true"
);
~~~

以下の表は、パラメータを説明しています。

| **パラメータ**        | **必須** | **デフォルト値** | **説明**                                              |
| -------------------- | ------------ | ----------------- | ------------------------------------------------------------ |
| hosts                | Yes          | None              | Elasticsearchクラスターの接続アドレス。1つ以上のアドレスを指定できます。StarRocksは、このアドレスからElasticsearchのバージョンとインデックスシャードの割り当てを解析します。StarRocksは、`GET /_nodes/http` API操作によって返されるアドレスに基づいて、Elasticsearchクラスターと通信します。したがって、`host` パラメータの値は、`GET /_nodes/http` API操作によって返されるアドレスと同じでなければなりません。そうでない場合、BEsはElasticsearchクラスターと通信できない可能性があります。 |
| index                | Yes          | None              | StarRocksに作成されたテーブルに対して作成されたElasticsearchインデックスの名前。名前はエイリアスにすることができます。このパラメータはワイルドカード（\*）をサポートしています。たとえば、`index` を <code class="language-text">hello*</code> に設定すると、StarRocksは名前が `hello` で始まるすべてのインデックスを取得します。 |
| user                 | No           | Empty             | 基本認証が有効なElasticsearchクラスターにログインするために使用されるユーザー名。`/*cluster/state/*nodes/http` とインデックスにアクセスできることを確認してください。 |
| password             | No           | Empty             | Elasticsearchクラスターにログインするために使用されるパスワード。 |
| type                 | No           | `_doc`            | インデックスのタイプ。デフォルト値：`_doc`。Elasticsearch 8以降のバージョンでデータをクエリする場合、このパラメータを設定する必要はありません。Elasticsearch 8以降のバージョンでは、マッピングタイプが削除されています。 |
| es.nodes.wan.only    | No           | `false`           | StarRocksがElasticsearchクラスターにアクセスしてデータを取得するために、`hosts` で指定されたアドレスのみを使用するかどうかを指定します。<ul><li>`true`: StarRocksは、`hosts` で指定されたアドレスのみを使用してElasticsearchクラスターにアクセスしてデータを取得し、インデックスのシャードが存在するデータノードをスニッフしません。StarRocksがElasticsearchクラスター内のデータノードのアドレスにアクセスできない場合、このパラメータを `true` に設定する必要があります。</li><li>`false`: StarRocksは、`host` で指定されたアドレスを使用して、Elasticsearchクラスターインデックスのシャードが存在するデータノードをスニッフします。StarRocksがElasticsearchクラスター内のデータノードのアドレスにアクセスできる場合、デフォルト値 `false` を保持することをお勧めします。</li></ul> |
| es.net.ssl           | No           | `false`           | ElasticsearchクラスターにアクセスするためにHTTPSプロトコルを使用できるかどうかを指定します。StarRocks 2.4以降のバージョンのみがこのパラメータの設定をサポートしています。<ul><li>`true`: HTTPSおよびHTTPプロトコルの両方を使用してElasticsearchクラスターにアクセスできます。</li><li>`false`: HTTPプロトコルのみを使用してElasticsearchクラスターにアクセスできます。</li></ul> |
| enable_docvalue_scan | No           | `true`            | Elasticsearch列指向ストレージからターゲットフィールドの値を取得するかどうかを指定します。ほとんどの場合、列指向ストレージからデータを読み取る方が行指向ストレージからデータを読み取るよりも優れています。 |
| enable_keyword_sniff | No           | `true`            | ElasticsearchでTEXTタイプのフィールドをKEYWORDタイプのフィールドに基づいてスニッフするかどうかを指定します。このパラメータを `false` に設定すると、StarRocksはトークン化後にマッチングを行います。 |

##### 高速なクエリのための列指向スキャン

`enable_docvalue_scan` を `true` に設定すると、StarRocksはElasticsearchからデータを取得する際に次のルールに従います：

* **試してみる**: StarRocksは、ターゲットフィールドに対して列指向ストレージが有効かどうかを自動的に確認します。有効であれば、StarRocksはターゲットフィールドのすべての値を列指向ストレージから取得します。
* **自動ダウングレード**: ターゲットフィールドのいずれかが列指向ストレージで利用できない場合、StarRocksは行指向ストレージ（`_source`）からターゲットフィールドのすべての値を解析して取得します。

> **注**
>
> * ElasticsearchのTEXTタイプのフィールドには列指向ストレージが利用できません。したがって、TEXTタイプの値を含むフィールドをクエリする場合、StarRocksはフィールドの値を `_source` から取得します。
> * 多数のフィールド（25以上）をクエリする場合、`docvalue` からフィールド値を読み取ることは、`_source` からフィールド値を読み取ることと比較して顕著な利点を示しません。

##### KEYWORDタイプのフィールドをスニッフする

`enable_keyword_sniff` を `true` に設定すると、Elasticsearchはインデックスなしで直接データ取り込みを許可します。取り込み後に自動的にインデックスを作成します。STRINGタイプのフィールドに対して、ElasticsearchはTEXTとKEYWORDの両方のタイプを持つフィールドを作成します。これはElasticsearchのマルチフィールド機能の動作です。マッピングは次の通りです：

~~~SQL
"k4": {
   "type": "text",
   "fields": {
      "keyword": {   
         "type": "keyword",
         "ignore_above": 256
      }
   }
}
~~~

たとえば、`k4` に対して "=" フィルタリングを行う場合、StarRocks on Elasticsearchはフィルタリング操作をElasticsearchのTermQueryに変換します。

元のSQLフィルタは次の通りです：

~~~SQL
k4 = "StarRocks On Elasticsearch"
~~~

変換されたElasticsearchクエリDSLは次の通りです：

~~~SQL
"term" : {
    "k4": "StarRocks On Elasticsearch"

}
~~~

`k4` の最初のフィールドはTEXTであり、データ取り込み後に `k4` に設定されたアナライザー（またはアナライザーが設定されていない場合は標準アナライザー）によってトークン化されます。その結果、最初のフィールドは3つの用語にトークン化されます：`StarRocks`、`On`、`Elasticsearch`。詳細は次の通りです：

~~~SQL
POST /_analyze
{
  "analyzer": "standard",
  "text": "StarRocks On Elasticsearch"
}
~~~

トークン化結果は次の通りです：

~~~SQL
{
   "tokens": [
      {
         "token": "starrocks",
         "start_offset": 0,
         "end_offset": 5,
         "type": "<ALPHANUM>",
         "position": 0
      },
      {
         "token": "on",
         "start_offset": 6,
         "end_offset": 8,
         "type": "<ALPHANUM>",
         "position": 1
      },
      {
         "token": "elasticsearch",
         "start_offset": 9,
         "end_offset": 11,
         "type": "<ALPHANUM>",
         "position": 2
      }
   ]
}
~~~

次のようなクエリを実行するとします：

~~~SQL
"term" : {
    "k4": "StarRocks On Elasticsearch"
}
~~~

辞書内に `StarRocks On Elasticsearch` に一致する用語がないため、結果は返されません。

ただし、`enable_keyword_sniff` を `true` に設定している場合、StarRocksは `k4 = "StarRocks On Elasticsearch"` を `k4.keyword = "StarRocks On Elasticsearch"` に変換してSQLセマンティクスに一致させます。変換された `StarRocks On Elasticsearch` クエリDSLは次の通りです：

~~~SQL
"term" : {
    "k4.keyword": "StarRocks On Elasticsearch"
}
~~~

`k4.keyword` はKEYWORDタイプです。したがって、データはElasticsearchに完全な用語として書き込まれ、成功したマッチングが可能になります。

#### 列データ型のマッピング

外部テーブルを作成する際、Elasticsearchテーブルの列データ型に基づいて外部テーブルの列データ型を指定する必要があります。次の表は、列データ型のマッピングを示しています。

| **Elasticsearch** | **StarRocks**               |
| ----------------- | --------------------------- |
| BOOLEAN           | BOOLEAN                     |
| BYTE              | TINYINT/SMALLINT/INT/BIGINT |
| SHORT             | SMALLINT/INT/BIGINT         |
| INTEGER           | INT/BIGINT                  |
| LONG              | BIGINT                      |
| FLOAT             | FLOAT                       |
| DOUBLE            | DOUBLE                      |
| KEYWORD           | CHAR/VARCHAR                |
| TEXT              | CHAR/VARCHAR                |
| DATE              | DATE/DATETIME               |
| NESTED            | CHAR/VARCHAR                |
| OBJECT            | CHAR/VARCHAR                |
| ARRAY             | ARRAY                       |

> **注**
>
> * StarRocksは、JSON関連の関数を使用してNESTEDタイプのデータを読み取ります。
> * Elasticsearchは多次元配列を自動的に一次元配列にフラット化します。StarRocksも同様です。**ElasticsearchからのARRAYデータのクエリサポートはv2.5から追加されました。**

### 述語プッシュダウン

StarRocksは述語プッシュダウンをサポートしています。フィルタはElasticsearchにプッシュダウンされて実行され、クエリパフォーマンスが向上します。次の表は、述語プッシュダウンをサポートする演算子を示しています。

|   SQL構文  |   ES構文  |
| :---: | :---: |
|  `=`   |  term query   |
|  `in`   |  terms query   |
|  `>=,  <=, >, <`   |  range   |
|  `and`   |  bool.filter   |
|  `or`   |  bool.should   |
|  `not`   |  bool.must_not   |
|  `not in`   |  bool.must_not + terms   |
|  `esquery`   |  ES Query DSL  |

### 例

**esquery関数** は、**SQLで表現できないクエリ**（matchやgeoshapeなど）をElasticsearchにプッシュダウンしてフィルタリングするために使用されます。esquery関数の最初のパラメータはインデックスを関連付けるために使用されます。2番目のパラメータは、基本的なQuery DSLのJSON式であり、{}で囲まれています。**JSON式には1つのルートキーが必要ですが、1つだけです**。たとえば、match、geo_shape、またはboolです。

* matchクエリ

~~~sql
select * from es_table where esquery(k4, '{
    "match": {
       "k4": "StarRocks on elasticsearch"
    }
}');
~~~

* geo関連クエリ

~~~sql
select * from es_table where esquery(k4, '{
  "geo_shape": {
     "location": {
        "shape": {
           "type": "envelope",
           "coordinates": [
              [
                 13,
                 53
              ],
              [
                 14,
                 52
              ]
           ]
        },
        "relation": "within"
     }
  }
}');
~~~

* boolクエリ

~~~sql
select * from es_table where esquery(k4, ' {
     "bool": {
        "must": [
           {
              "terms": {
                 "k1": [
                    11,
                    12
                 ]
              }
           },
           {
              "terms": {
                 "k2": [
                    100
                 ]
              }
           }
        ]
     }
  }');
~~~

### 使用上の注意

* Elasticsearch 5.x以前のバージョンは、5.x以降のバージョンとは異なる方法でデータをスキャンします。現在、**5.x以降のバージョンのみがサポートされています。**
* HTTP基本認証が有効なElasticsearchクラスターがサポートされています。
* StarRocksからデータをクエリすることは、Elasticsearchから直接データをクエリするほど高速ではない場合があります。たとえば、カウント関連のクエリです。その理由は、Elasticsearchがターゲットドキュメントのメタデータを直接読み取り、実際のデータをフィルタリングする必要がないため、カウントクエリが高速化されるためです。

## (非推奨) Hive外部テーブル

Hive外部テーブルを使用する前に、サーバーにJDK 1.8がインストールされていることを確認してください。

### Hiveリソースの作成

HiveリソースはHiveクラスターに対応します。StarRocksで使用するHiveクラスターを構成する必要があります。たとえば、Hiveメタストアのアドレスを指定します。Hive外部テーブルで使用するHiveリソースを指定する必要があります。

* `hive0` という名前のHiveリソースを作成します。

~~~sql
CREATE EXTERNAL RESOURCE "hive0"
PROPERTIES (
  "type" = "hive",
  "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
);
~~~

* StarRocksで作成されたリソースを表示します。

~~~sql
SHOW RESOURCES;
~~~

* `hive0` という名前のリソースを削除します。

~~~sql
DROP RESOURCE "hive0";
~~~

StarRocks 2.3以降のバージョンでは、Hiveリソースの `hive.metastore.uris` を変更できます。詳細は [ALTER RESOURCE](../sql-reference/sql-statements/Resource/ALTER_RESOURCE.md) を参照してください。

### データベースの作成

~~~sql
CREATE DATABASE hive_test;
USE hive_test;
~~~

### Hive外部テーブルの作成

構文

~~~sql
CREATE EXTERNAL TABLE table_name (
  col_name col_type [NULL | NOT NULL] [COMMENT "comment"]
) ENGINE=HIVE
PROPERTIES (
  "key" = "value"
);
~~~

例：`hive0` リソースに対応するHiveクラスターの `rawdata` データベースに `profile_parquet_p7` という外部テーブルを作成します。

~~~sql
CREATE EXTERNAL TABLE `profile_wos_p7` (
  `id` bigint NULL,
  `first_id` varchar(200) NULL,
  `second_id` varchar(200) NULL,
  `p__device_id_list` varchar(200) NULL,
  `p__is_deleted` bigint NULL,
  `p_channel` varchar(200) NULL,
  `p_platform` varchar(200) NULL,
  `p_source` varchar(200) NULL,
  `p__city` varchar(200) NULL,
  `p__province` varchar(200) NULL,
  `p__update_time` bigint NULL,
  `p__first_visit_time` bigint NULL,
  `p__last_seen_time` bigint NULL
) ENGINE=HIVE
PROPERTIES (
  "resource" = "hive0",
  "database" = "rawdata",
  "table" = "profile_parquet_p7"
);
~~~

説明：

* 外部テーブルの列
  * 列名はHiveテーブルの列名と同じでなければなりません。
  * 列の順序はHiveテーブルの列の順序と同じである必要はありません。
  * Hiveテーブルの列の一部のみを選択できますが、すべての**パーティションキー列**を選択する必要があります。
  * 外部テーブルのパーティションキー列は `partition by` を使用して指定する必要はありません。他の列と同じ説明リストで定義する必要があります。パーティション情報を指定する必要はありません。StarRocksはこの情報をHiveテーブルから自動的に同期します。
  * `ENGINE` をHIVEに設定します。
* PROPERTIES:
  * **hive.resource**: 使用するHiveリソース。
  * **database**: Hiveデータベース。
  * **table**: Hiveのテーブル。**ビュー**はサポートされていません。
* 以下の表は、HiveとStarRocks間の列データ型のマッピングを示しています。

    |  Hiveの列タイプ   |  StarRocksの列タイプ   | 説明 |
    | --- | --- | ---|
    |   INT/INTEGER  | INT    |
    |   BIGINT  | BIGINT    |
    |   TIMESTAMP  | DATETIME    | TIMESTAMPデータをDATETIMEデータに変換する際、精度とタイムゾーン情報が失われます。セッション変数のタイムゾーンに基づいて、タイムゾーンオフセットのないDATETIMEデータに変換する必要があります。 |
    |  STRING  | VARCHAR   |
    |  VARCHAR  | VARCHAR   |
    |  CHAR  | CHAR   |
    |  DOUBLE | DOUBLE |
    | FLOAT | FLOAT|
    | DECIMAL | DECIMAL|
    | ARRAY | ARRAY |

> 注：
>
> * 現在、サポートされているHiveストレージ形式はParquet、ORC、CSVです。
CSVのストレージ形式の場合、引用符をエスケープ文字として使用することはできません。
> * SNAPPYおよびLZ4圧縮形式がサポートされています。
> * クエリできるHive文字列列の最大長は1 MBです。文字列列が1 MBを超える場合、null列として処理されます。

### Hive外部テーブルの使用

`profile_wos_p7` の総行数をクエリします。

~~~sql
select count(*) from profile_wos_p7;
~~~

### キャッシュされたHiveテーブルメタデータの更新

* Hiveのパーティション情報と関連するファイル情報はStarRocksにキャッシュされます。キャッシュは `hive_meta_cache_refresh_interval_s` で指定された間隔で更新されます。デフォルト値は7200です。`hive_meta_cache_ttl_s` はキャッシュのタイムアウト期間を指定し、デフォルト値は86400です。
  * キャッシュされたデータは手動で更新することもできます。
    1. Hiveでテーブルにパーティションが追加または削除された場合、`REFRESH EXTERNAL TABLE hive_t` コマンドを実行して、StarRocksにキャッシュされたテーブルメタデータを更新する必要があります。`hive_t` はStarRocks内のHive外部テーブルの名前です。
    2. Hiveの一部のパーティション内のデータが更新された場合、`REFRESH EXTERNAL TABLE hive_t PARTITION ('k1=01/k2=02', 'k1=03/k2=04')` コマンドを実行して、StarRocksにキャッシュされたデータを更新する必要があります。`hive_t` はStarRocks内のHive外部テーブルの名前です。`'k1=01/k2=02'` と `'k1=03/k2=04'` はデータが更新されたHiveパーティションの名前です。
    3. `REFRESH EXTERNAL TABLE hive_t` を実行すると、StarRocksはまずHive外部テーブルの列情報がHiveメタストアから返されたHiveテーブルの列情報と同じであるかどうかを確認します。Hiveテーブルのスキーマが変更された場合（列の追加や削除など）、StarRocksは変更をHive外部テーブルに同期します。同期後、Hive外部テーブルの列の順序はHiveテーブルの列の順序と同じままで、パーティション列が最後の列になります。
* HiveデータがParquet、ORC、CSV形式で保存されている場合、StarRocks 2.3以降のバージョンでは、Hiveテーブルのスキーマ変更（ADD COLUMNやREPLACE COLUMNなど）をHive外部テーブルに同期できます。

### オブジェクトストレージへのアクセス

* FE構成ファイルのパスは `fe/conf` であり、Hadoopクラスターをカスタマイズする必要がある場合、構成ファイルを追加できます。たとえば、HDFSクラスターが高可用性のネームサービスを使用している場合、`hdfs-site.xml` を `fe/conf` に配置する必要があります。HDFSがViewFsで構成されている場合、`core-site.xml` を `fe/conf` に配置する必要があります。
* BE構成ファイルのパスは `be/conf` であり、Hadoopクラスターをカスタマイズする必要がある場合、構成ファイルを追加できます。たとえば、高可用性のネームサービスを使用しているHDFSクラスターの場合、`hdfs-site.xml` を `be/conf` に配置する必要があります。HDFSがViewFsで構成されている場合、`core-site.xml` を `be/conf` に配置する必要があります。
* BEが存在するマシンで、BEの**起動スクリプト** `bin/start_be.sh` において、JDK環境としてJAVA_HOMEを構成します。たとえば、`export JAVA_HOME = <JDKパス>`。この構成をスクリプトの先頭に追加し、BEを再起動して構成を有効にする必要があります。
* Kerberosサポートを構成します：
  1. HiveおよびHDFSにアクセスするために、すべてのFE/BEマシンで `kinit -kt keytab_path principal` を使用してログインする必要があります。kinitコマンドのログインは一定期間のみ有効であり、定期的に実行するためにcrontabに追加する必要があります。
  2. `hive-site.xml/core-site.xml/hdfs-site.xml` を `fe/conf` に配置し、`core-site.xml/hdfs-site.xml` を `be/conf` に配置します。
  3. **$FE_HOME/conf/fe.conf** ファイルの `JAVA_OPTS` オプションの値に `-Djava.security.krb5.conf=/etc/krb5.conf` を追加します。**/etc/krb5.conf** は **krb5.conf** ファイルの保存パスです。オペレーティングシステムに基づいてパスを変更できます。
  4. **$BE_HOME/conf/be.conf** ファイルに直接 `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` を追加します。**/etc/krb5.conf** は **krb5.conf** ファイルの保存パスです。オペレーティングシステムに基づいてパスを変更できます。
  5. Hiveリソースを追加する際、`hive.metastore.uris` にドメイン名を渡す必要があります。さらに、**/etc/hosts** ファイルにHive/HDFSドメイン名とIPアドレスのマッピングを追加する必要があります。

* AWS S3のサポートを構成します：次の構成を `fe/conf/core-site.xml` および `be/conf/core-site.xml` に追加します。

   ~~~XML
   <configuration>
      <property>
         <name>fs.s3a.access.key</name>
         <value>******</value>
      </property>
      <property>
         <name>fs.s3a.secret.key</name>
         <value>******</value>
      </property>
      <property>
         <name>fs.s3a.endpoint</name>
         <value>s3.us-west-2.amazonaws.com</value>
      </property>
      <property>
      <name>fs.s3a.connection.maximum</name>
      <value>500</value>
      </property>
   </configuration>
   ~~~

   1. `fs.s3a.access.key`: AWSアクセスキーID。
   2. `fs.s3a.secret.key`: AWSシークレットキー。
   3. `fs.s3a.endpoint`: 接続するAWS S3エンドポイント。
   4. `fs.s3a.connection.maximum`: StarRocksからS3への同時接続の最大数。クエリ中に `Timeout waiting for connection from poll` エラーが発生した場合、このパラメータを大きな値に設定できます。

## (非推奨) Iceberg外部テーブル

v2.1.0以降、StarRocksは外部テーブルを使用してApache Icebergからデータをクエリすることを可能にしています。Icebergのデータをクエリするには、StarRocksにIceberg外部テーブルを作成する必要があります。テーブルを作成する際、クエリしたいIcebergテーブルと外部テーブルの間にマッピングを確立する必要があります。

### 始める前に

StarRocksがApache Icebergで使用されるメタデータサービス（Hiveメタストアなど）、ファイルシステム（HDFSなど）、オブジェクトストレージシステム（Amazon S3やAlibaba Cloud Object Storage Serviceなど）にアクセスする権限を持っていることを確認してください。

### 注意事項

* Iceberg外部テーブルは、次のタイプのデータのみをクエリするために使用できます：
  * Iceberg v1テーブル（分析データテーブル）。ORC形式のIceberg v2（行レベルの削除）テーブルはv3.0以降でサポートされ、Parquet形式のIceberg v2テーブルはv3.1以降でサポートされます。Iceberg v1テーブルとIceberg v2テーブルの違いについては、[Iceberg Table Spec](https://iceberg.apache.org/spec/) を参照してください。
  * gzip（デフォルト形式）、Zstd、LZ4、Snappy形式で圧縮されたテーブル。
  * ParquetまたはORC形式で保存されたファイル。

* StarRocks 2.3以降のバージョンでは、Iceberg外部テーブルはIcebergテーブルのスキーマ変更を同期することをサポートしていますが、StarRocks 2.3以前のバージョンではサポートしていません。Icebergテーブルのスキーマが変更された場合、対応する外部テーブルを削除し、新しい外部テーブルを作成する必要があります。

### 手順

#### ステップ1: Icebergリソースの作成

Iceberg外部テーブルを作成する前に、StarRocksでIcebergリソースを作成する必要があります。このリソースはIcebergアクセス情報を管理するために使用されます。さらに、外部テーブルを作成するためのステートメントでこのリソースを指定する必要があります。ビジネス要件に基づいてリソースを作成できます：

* IcebergテーブルのメタデータがHiveメタストアから取得される場合、リソースを作成し、catalogタイプを `HIVE` に設定できます。

* Icebergテーブルのメタデータが他のサービスから取得される場合、カスタムcatalogを作成する必要があります。その後、リソースを作成し、catalogタイプを `CUSTOM` に設定します。

##### catalogタイプが `HIVE` のリソースを作成する

たとえば、`iceberg0` という名前のリソースを作成し、catalogタイプを `HIVE` に設定します。

~~~SQL
CREATE EXTERNAL RESOURCE "iceberg0" 
PROPERTIES (
   "type" = "iceberg",
   "iceberg.catalog.type" = "HIVE",
   "iceberg.catalog.hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083" 
);
~~~

以下の表は関連するパラメータを説明しています。

| **パラメータ**                       | **説明**                                              |
| ----------------------------------- | ------------------------------------------------------------ |
| type                                | リソースのタイプ。値を `iceberg` に設定します。               |
| iceberg.catalog.type              | リソースのcatalogタイプ。Hive catalogとカスタムcatalogの両方がサポートされています。Hive catalogを指定する場合、値を `HIVE` に設定します。カスタムcatalogを指定する場合、値を `CUSTOM` に設定します。 |
| iceberg.catalog.hive.metastore.uris | HiveメタストアのURI。パラメータ値は次の形式です：`thrift://< IcebergメタデータのIPアドレス >:< ポート番号 >`。ポート番号はデフォルトで9083です。Apache IcebergはHive catalogを使用してHiveメタストアにアクセスし、Icebergテーブルのメタデータをクエリします。 |

##### catalogタイプが `CUSTOM` のリソースを作成する

カスタムcatalogは抽象クラスBaseMetastoreCatalogを継承する必要があり、IcebergCatalogインターフェースを実装する必要があります。さらに、カスタムcatalogのクラス名はStarRocksに既に存在するクラスの名前と重複してはなりません。catalogが作成された後、catalogとその関連ファイルをパッケージ化し、各フロントエンド（FE）の **fe/lib** パスに配置します。その後、各FEを再起動します。前述の操作を完了した後、catalogがカスタムcatalogであるリソースを作成できます。

たとえば、`iceberg1` という名前のリソースを作成し、catalogタイプを `CUSTOM` に設定します。

~~~SQL
CREATE EXTERNAL RESOURCE "iceberg1" 
PROPERTIES (
   "type" = "iceberg",
   "iceberg.catalog.type" = "CUSTOM",
   "iceberg.catalog-impl" = "com.starrocks.IcebergCustomCatalog" 
);
~~~

以下の表は関連するパラメータを説明しています。

| **パラメータ**          | **説明**                                              |
| ---------------------- | ------------------------------------------------------------ |
| type                   | リソースのタイプ。値を `iceberg` に設定します。               |
| iceberg.catalog.type | リソースのcatalogタイプ。Hive catalogとカスタムcatalogの両方がサポートされています。Hive catalogを指定する場合、値を `HIVE` に設定します。カスタムcatalogを指定する場合、値を `CUSTOM` に設定します。 |
| iceberg.catalog-impl   | カスタムcatalogの完全修飾クラス名。FEsはこの名前に基づいてcatalogを検索します。catalogにカスタム構成項目が含まれている場合、Iceberg外部テーブルを作成する際に `PROPERTIES` パラメータにキーと値のペアとして追加する必要があります。 |

StarRocks 2.3以降のバージョンでは、Icebergリソースの `hive.metastore.uris` と `iceberg.catalog-impl` を変更できます。詳細は [ALTER RESOURCE](../sql-reference/sql-statements/Resource/ALTER_RESOURCE.md) を参照してください。

##### Icebergリソースの表示

~~~SQL
SHOW RESOURCES;
~~~

##### Icebergリソースの削除

たとえば、`iceberg0` という名前のリソースを削除します。

~~~SQL
DROP RESOURCE "iceberg0";
~~~

Icebergリソースを削除すると、このリソースを参照するすべての外部テーブルが使用できなくなります。ただし、Apache Iceberg内の対応するデータは削除されません。Apache Iceberg内のデータを引き続きクエリする必要がある場合は、新しいリソースと新しい外部テーブルを作成してください。

#### ステップ2: (オプション) データベースの作成

たとえば、StarRocksに `iceberg_test` という名前のデータベースを作成します。

~~~SQL
CREATE DATABASE iceberg_test; 
USE iceberg_test; 
~~~

> 注：StarRocks内のデータベース名は、Apache Iceberg内のデータベース名と異なる場合があります。

#### ステップ3: Iceberg外部テーブルの作成

たとえば、データベース `iceberg_test` に `iceberg_tbl` という名前のIceberg外部テーブルを作成します。

~~~SQL
CREATE EXTERNAL TABLE `iceberg_tbl` ( 
    `id` bigint NULL, 
    `data` varchar(200) NULL 
) ENGINE=ICEBERG 
PROPERTIES ( 
    "resource" = "iceberg0", 
    "database" = "iceberg", 
    "table" = "iceberg_table" 
); 
~~~

以下の表は関連するパラメータを説明しています。

| **パラメータ** | **説明**                                              |
| ------------- | ------------------------------------------------------------ |
| ENGINE        | エンジン名。値を `ICEBERG` に設定します。                 |
| resource      | 外部テーブルが参照するIcebergリソースの名前。 |
| database      | Icebergテーブルが属するデータベースの名前。 |
| table         | Icebergテーブルの名前。                               |

> 注：
   >
   > * 外部テーブルの名前は、Icebergテーブルの名前と異なる場合があります。
   >
   > * 外部テーブルの列名は、Icebergテーブルの列名と同じでなければなりません。2つのテーブルの列の順序は異なる場合があります。

カスタムcatalogで構成項目を定義し、データをクエリする際に構成項目を有効にしたい場合、外部テーブルを作成する際に `PROPERTIES` パラメータにキーと値のペアとして構成項目を追加できます。たとえば、カスタムcatalogで `custom-catalog.properties` という構成項目を定義した場合、次のコマンドを実行して外部テーブルを作成できます。

~~~SQL
CREATE EXTERNAL TABLE `iceberg_tbl` ( 
    `id` bigint NULL, 
    `data` varchar(200) NULL 
) ENGINE=ICEBERG 
PROPERTIES ( 
    "resource" = "iceberg0", 
    "database" = "iceberg", 
    "table" = "iceberg_table",
    "custom-catalog.properties" = "my_property"
); 
~~~

外部テーブルを作成する際、Icebergテーブルの列データ型に基づいて外部テーブルの列データ型を指定する必要があります。次の表は、列データ型のマッピングを示しています。

| **Icebergテーブル** | **Iceberg外部テーブル** |
| ----------------- | -------------------------- |
| BOOLEAN           | BOOLEAN                    |
| INT               | TINYINT / SMALLINT / INT   |
| LONG              | BIGINT                     |
| FLOAT             | FLOAT                      |
| DOUBLE            | DOUBLE                     |
| DECIMAL(P, S)     | DECIMAL                    |
| DATE              | DATE / DATETIME            |
| TIME              | BIGINT                     |
| TIMESTAMP         | DATETIME                   |
| STRING            | STRING / VARCHAR           |
| UUID              | STRING / VARCHAR           |
| FIXED(L)          | CHAR                       |
| BINARY            | VARCHAR                    |
| LIST              | ARRAY                      |

StarRocksは、TIMESTAMPTZ、STRUCT、およびMAPのデータ型を持つIcebergデータのクエリをサポートしていません。

#### ステップ4: Apache Icebergのデータをクエリする

外部テーブルが作成された後、外部テーブルを使用してApache Icebergのデータをクエリできます。

~~~SQL
select count(*) from iceberg_tbl;
~~~

## (非推奨) Hudi外部テーブル

v2.2.0以降、StarRocksはHudi外部テーブルを使用してHudiデータレイクからデータをクエリすることを可能にしています。これにより、非常に高速なデータレイク分析が可能になります。このトピックでは、StarRocksクラスターにHudi外部テーブルを作成し、Hudiデータレイクからデータをクエリする方法について説明します。

### 始める前に

StarRocksクラスターがHiveメタストア、HDFSクラスター、またはHudiテーブルを登録できるバケットにアクセスする権限を持っていることを確認してください。

### 注意事項

* HudiのHudi外部テーブルは読み取り専用であり、クエリにのみ使用できます。
* StarRocksは、Copy on WriteテーブルとMerge On Readテーブル（MORテーブルはv2.5からサポートされています）をクエリすることをサポートしています。これら2つのタイプのテーブルの違いについては、[Table & Query Types](https://hudi.apache.org/docs/table_types/) を参照してください。
* StarRocksは、Hudiの次の2つのクエリタイプをサポートしています：スナップショットクエリと読み取り最適化クエリ（HudiはMerge On Readテーブルでのみ読み取り最適化クエリをサポートしています）。インクリメンタルクエリはサポートされていません。Hudiのクエリタイプの詳細については、[Table & Query Types](https://hudi.apache.org/docs/next/table_types/#query-types) を参照してください。
* StarRocksは、Hudiファイルの次の圧縮形式をサポートしています：gzip、zstd、LZ4、およびSnappy。Hudiファイルのデフォルトの圧縮形式はgzipです。
* StarRocksは、Hudi管理テーブルからのスキーマ変更を同期することはできません。詳細については、[Schema Evolution](https://hudi.apache.org/docs/schema_evolution/) を参照してください。Hudi管理テーブルのスキーマが変更された場合、StarRocksクラスターから関連するHudi外部テーブルを削除し、その外部テーブルを再作成する必要があります。

### 手順

#### ステップ1: Hudiリソースの作成と管理

StarRocksクラスターにHudiリソースを作成する必要があります。Hudiリソースは、StarRocksクラスターで作成するHudiデータベースと外部テーブルを管理するために使用されます。

##### Hudiリソースの作成

次のステートメントを実行して、`hudi0` という名前のHudiリソースを作成します：

~~~SQL
CREATE EXTERNAL RESOURCE "hudi0" 
PROPERTIES ( 
    "type" = "hudi", 
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
);
~~~

以下の表はパラメータを説明しています。

| パラメータ           | 説明                                                  |
| ------------------- | ------------------------------------------------------------ |
| type                | Hudiリソースのタイプ。値をhudiに設定します。         |
| hive.metastore.uris | Hudiリソースが接続するHiveメタストアのThrift URI。HudiリソースをHiveメタストアに接続した後、Hiveを使用してHudiテーブルを作成および管理できます。Thrift URIは `<HiveメタストアのIPアドレス>:<Hiveメタストアのポート番号>` 形式です。デフォルトのポート番号は9083です。 |

v2.3以降、StarRocksはHudiリソースの `hive.metastore.uris` 値の変更を許可しています。詳細は [ALTER RESOURCE](../sql-reference/sql-statements/Resource/ALTER_RESOURCE.md) を参照してください。

##### Hudiリソースの表示

次のステートメントを実行して、StarRocksクラスターに作成されたすべてのHudiリソースを表示します：

~~~SQL
SHOW RESOURCES;
~~~

##### Hudiリソースの削除

次のステートメントを実行して、`hudi0` という名前のHudiリソースを削除します：

~~~SQL
DROP RESOURCE "hudi0";
~~~

> 注：
>
> Hudiリソースを削除すると、そのHudiリソースを使用して作成されたすべてのHudi外部テーブルが使用できなくなります。ただし、Hudiに保存されているデータには影響しません。StarRocksを使用してHudiからデータをクエリする場合は、StarRocksクラスターでHudiリソース、Hudiデータベース、およびHudi外部テーブルを再作成する必要があります。

#### ステップ2: Hudiデータベースの作成

次のステートメントを実行して、StarRocksクラスターに `hudi_test` という名前のHudiデータベースを作成して開きます：

~~~SQL
CREATE DATABASE hudi_test; 
USE hudi_test; 
~~~

> 注：
>
> StarRocksクラスターで指定するHudiデータベースの名前は、関連するHudiデータベースと同じである必要はありません。

#### ステップ3: Hudi外部テーブルの作成

次のステートメントを実行して、`hudi_test` Hudiデータベースに `hudi_tbl` という名前のHudi外部テーブルを作成します：

~~~SQL
CREATE EXTERNAL TABLE `hudi_tbl` ( 
    `id` bigint NULL, 
    `data` varchar(200) NULL 
) ENGINE=HUDI 
PROPERTIES ( 
    "resource" = "hudi0", 
    "database" = "hudi", 
    "table" = "hudi_table" 
); 
~~~

以下の表はパラメータを説明しています。

| パラメータ | 説明                                                  |
| --------- | ------------------------------------------------------------ |
| ENGINE    | Hudi外部テーブルのクエリエンジン。値を `HUDI` に設定します。 |
| resource  | StarRocksクラスター内のHudiリソースの名前。     |
| database  | StarRocksクラスター内のHudi外部テーブルが属するHudiデータベースの名前。 |
| table     | Hudi外部テーブルが関連付けられているHudi管理テーブル。 |

> 注：
>
> * Hudi外部テーブルの名前は、関連するHudi管理テーブルの名前と同じである必要はありません。
>
> * Hudi外部テーブルの列は、関連するHudi管理テーブルの対応する列と同じ名前を持つ必要がありますが、順序は異なる場合があります。
>
> * 関連するHudi管理テーブルから一部またはすべての列を選択し、選択した列のみをHudi外部テーブルに作成できます。次の表は、Hudiがサポートするデータ型とStarRocksがサポートするデータ型のマッピングを示しています。

| Hudiがサポートするデータ型   | StarRocksがサポートするデータ型 |
| ----------------------------   | --------------------------------- |
| BOOLEAN                        | BOOLEAN                           |
| INT                            | TINYINT/SMALLINT/INT              |
| DATE                           | DATE                              |
| TimeMillis/TimeMicros          | TIME                              |
| TimestampMillis/TimestampMicros| DATETIME                          |
| LONG                           | BIGINT                            |
| FLOAT                          | FLOAT                             |
| DOUBLE                         | DOUBLE                            |
| STRING                         | CHAR/VARCHAR                      |
| ARRAY                          | ARRAY                             |
| DECIMAL                        | DECIMAL                           |

> **注**
>
> StarRocksは、STRUCTまたはMAPタイプのデータのクエリをサポートしていません。また、Merge On ReadテーブルのARRAYタイプのデータのクエリもサポートしていません。

#### ステップ4: Hudi外部テーブルからデータをクエリする

特定のHudi管理テーブルに関連付けられたHudi外部テーブルを作成した後、Hudi外部テーブルにデータをロードする必要はありません。Hudiからデータをクエリするには、次のステートメントを実行します：

~~~SQL
SELECT COUNT(*) FROM hudi_tbl;
~~~

## (非推奨) MySQL外部テーブル

スタースキーマでは、データは一般的にディメンジョンテーブルとファクトテーブルに分けられます。ディメンジョンテーブルはデータが少なく、UPDATE操作を含みます。現在、StarRocksは直接のUPDATE操作をサポートしていません（ユニークキーテーブルを使用して更新を実装できます）。いくつかのシナリオでは、ディメンジョンテーブルをMySQLに保存して直接データを読み取ることができます。

MySQLデータをクエリするには、StarRocksに外部テーブルを作成し、MySQLデータベース内のテーブルにマッピングする必要があります。テーブルを作成する際に、MySQL接続情報を指定する必要があります。

~~~sql
CREATE EXTERNAL TABLE mysql_external_table
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
)
ENGINE=mysql
PROPERTIES
(
    "host" = "127.0.0.1",
    "port" = "3306",
    "user" = "mysql_user",
    "password" = "mysql_passwd",
    "database" = "mysql_db_test",
    "table" = "mysql_table_test"
);
~~~

パラメータ：

* **host**: MySQLデータベースの接続アドレス
* **port**: MySQLデータベースのポート番号
* **user**: MySQLにログインするためのユーザー名
* **password**: MySQLにログインするためのパスワード
* **database**: MySQLデータベースの名前
* **table**: MySQLデータベース内のテーブルの名前