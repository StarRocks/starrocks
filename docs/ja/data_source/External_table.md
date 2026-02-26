---
displayed_sidebar: docs
---

# 外部テーブル

:::note

外部テーブル機能は、特定の特殊な使用ケースを除いて推奨されなくなり、将来のリリースで非推奨になる可能性があります。一般的なシナリオで外部データソースからデータを管理およびクエリするには、[外部カタログ](./catalog/catalog_overview.md)が推奨されます。

- v3.0以降、Hive、Iceberg、Hudiからデータをクエリするにはカタログを使用することをお勧めします。以下を参照してください。[Hiveカタログ](../data_source/catalog/hive_catalog.md)、[Icebergカタログ](./catalog/iceberg/iceberg_catalog.md)、および[Hudiカタログ](../data_source/catalog/hudi_catalog.md)。

- v3.1以降、MySQLおよびPostgreSQLからデータをクエリするには[JDBCカタログ](../data_source/catalog/jdbc_catalog.md)を使用し、Elasticsearchからデータをクエリするには[Elasticsearchカタログ](../data_source/catalog/elasticsearch_catalog.md)を使用することをお勧めします。

- v3.2.9およびv3.3.1以降、OracleおよびSQL Serverからデータをクエリするには[JDBCカタログ](../data_source/catalog/jdbc_catalog.md)を使用することをお勧めします。

- 外部テーブル機能は、StarRocksへのデータロードを支援するために設計されたものであり、通常の操作として外部システムに対して効率的なクエリを実行するためではありません。よりパフォーマンスの高いソリューションは、データをStarRocksにロードすることです。

:::

StarRocksは、外部テーブルを使用して他のデータソースへのアクセスをサポートしています。外部テーブルは、他のデータソースに保存されているデータテーブルに基づいて作成されます。StarRocksはデータテーブルのメタデータのみを保存します。外部テーブルを使用して、他のデータソースのデータを直接クエリできます。現在、StarRocks外部テーブルを除き、他のすべての外部テーブルは非推奨です。**別のStarRocksクラスターから現在のStarRocksクラスターにのみデータを書き込むことができます。そこからデータを読み取ることはできません。StarRocks以外のデータソースの場合、これらのデータソースからのみデータを読み取ることができます。**

2.5以降、StarRocksはデータキャッシュ機能を提供し、外部データソース上のホットデータクエリを高速化します。詳細については、[データキャッシュ](data_cache.md)を参照してください。

## StarRocks外部テーブル

StarRocks 1.19以降、StarRocksはStarRocks外部テーブルを使用して、あるStarRocksクラスターから別のStarRocksクラスターにデータを書き込むことを可能にします。これにより、読み書き分離が実現され、より良いリソース分離が提供されます。まず、宛先StarRocksクラスターに宛先テーブルを作成できます。次に、ソースStarRocksクラスターで、宛先テーブルと同じスキーマを持つStarRocks外部テーブルを作成し、宛先クラスターとテーブルの情報を`PROPERTIES`フィールドで指定します。

INSERT INTOステートメントを使用してStarRocks外部テーブルに書き込むことで、ソースクラスターから宛先クラスターにデータを書き込むことができます。これにより、以下の目標を達成できます。

- StarRocksクラスター間のデータ同期。
- 読み書き分離。データはソースクラスターに書き込まれ、ソースクラスターからのデータ変更は宛先クラスターに同期され、クエリサービスを提供します。

以下のコードは、宛先テーブルと外部テーブルの作成方法を示しています。

```SQL
# Create a destination table in the destination StarRocks cluster.
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

# Create an external table in the source StarRocks cluster.
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

# Write data from a source cluster to a destination cluster by writing data into the StarRocks external table. The second statement is recommended for the production environment.
insert into external_t values ('2020-10-11', 1, 1, 'hello', '2020-10-11 10:00:00');
insert into external_t select * from other_table;
```

パラメータ:

- **EXTERNAL:**このキーワードは、作成するテーブルが外部テーブルであることを示します。

- **host:**このパラメータは、宛先StarRocksクラスターのリーダーFEノードのIPアドレスを指定します。

- **port:**このパラメーターは、宛先StarRocksクラスターのFEノードのRPCポートを指定します。

  :::note

  StarRocks外部テーブルが属するソースクラスターが宛先StarRocksクラスターにアクセスできるようにするには、ネットワークとファイアウォールを構成して、以下のポートへのアクセスを許可する必要があります。

  - FEノードのRPCポート。`rpc_port`FE構成ファイル**fe/fe.conf**内を参照してください。デフォルトのRPCポートは`9020`です。
  - BEノードのbRPCポート。`brpc_port`BE構成ファイル**be/be.conf**内を参照してください。デフォルトのbRPCポートは`8060`です。

  :::

- **user:** このパラメーターは、宛先StarRocksクラスターへのアクセスに使用するユーザー名を指定します。

- **password:** このパラメーターは、宛先StarRocksクラスターへのアクセスに使用するパスワードを指定します。

- **database:** このパラメーターは、宛先テーブルが属するデータベースを指定します。

- **table:** このパラメーターは、宛先テーブルの名前を指定します。

StarRocks外部テーブルを使用する場合、以下の制限が適用されます。

- StarRocks外部テーブルに対しては、INSERT INTOおよびSHOW CREATE TABLEコマンドのみ実行できます。その他のデータ書き込み方法はサポートされていません。さらに、StarRocks外部テーブルからデータをクエリしたり、外部テーブルに対してDDL操作を実行したりすることはできません。
- 外部テーブルを作成する構文は通常のテーブルを作成する構文と同じですが、外部テーブルの列名およびその他の情報は宛先テーブルと同じである必要があります。
- 外部テーブルは、宛先テーブルからテーブルメタデータを10秒ごとに同期します。宛先テーブルでDDL操作が実行された場合、2つのテーブル間でデータ同期に遅延が発生する可能性があります。

## (非推奨) JDBC互換データベース用の外部テーブル

v2.3.0以降、StarRocksはJDBC互換データベースをクエリするための外部テーブルを提供しています。これにより、データをStarRocksにインポートすることなく、そのようなデータベースのデータを非常に高速に分析できます。このセクションでは、StarRocksで外部テーブルを作成し、JDBC互換データベースのデータをクエリする方法について説明します。

### 前提条件

JDBC外部テーブルを使用してデータをクエリする前に、FEとBEがJDBCドライバーのダウンロードURLにアクセスできることを確認してください。ダウンロードURLは、`driver_url`JDBCリソースを作成するために使用されるステートメントのパラメーターで指定されます。

### JDBCリソースの作成と管理

#### JDBCリソースの作成

データベースからデータをクエリするための外部テーブルを作成する前に、StarRocksでJDBCリソースを作成して、データベースの接続情報を管理する必要があります。データベースはJDBCドライバーをサポートしている必要があり、「ターゲットデータベース」と呼ばれます。リソースを作成した後、それを使用して外部テーブルを作成できます。

という名前のJDBCリソースを作成するには、以下のステートメントを実行します`jdbc0`:

```SQL
CREATE EXTERNAL RESOURCE jdbc0
PROPERTIES (
    "type"="jdbc",
    "user"="postgres",
    "password"="changeme",
    "jdbc_uri"="jdbc:postgresql://127.0.0.1:5432/jdbc_test",
    "driver_url"="https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar",
    "driver_class"="org.postgresql.Driver"
);
```

の必須パラメーターは`PROPERTIES`以下のとおりです。

- `type`: リソースのタイプ。値を`jdbc`に設定します。

- `user`: ターゲットデータベースへの接続に使用されるユーザー名。

- `password`: ターゲットデータベースへの接続に使用されるパスワード。

- `jdbc_uri`: JDBCドライバーがターゲットデータベースに接続するために使用するURI。URI形式はデータベースURI構文を満たす必要があります。一般的なデータベースのURI構文については、以下の公式サイトをご覧ください。[Oracle](https://docs.oracle.com/en/database/oracle/oracle-database/21/jjdbc/data-sources-and-URLs.html#GUID-6D8EFA50-AB0F-4A2B-88A0-45B4A67C361E)、[PostgreSQL](https://jdbc.postgresql.org/documentation/head/connect.html)、[SQL Server](https://docs.microsoft.com/en-us/sql/connect/jdbc/building-the-connection-url?view=sql-server-ver16)。

> 注: URIにはターゲットデータベースの名前を含める必要があります。例えば、上記のコード例では、`jdbc_test` は、接続したいターゲットデータベースの名前です。

- `driver_url`: JDBCドライバーJARパッケージのダウンロードURL。HTTP URLまたはファイルURLがサポートされています。例えば、`https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar` または `file:///home/disk1/postgresql-42.3.3.jar`。

- `driver_class`: JDBCドライバーのクラス名。一般的なデータベースのJDBCドライバークラス名は以下の通りです。
  - MySQL: com.mysql.jdbc.Driver (MySQL 5.x以前), com.mysql.cj.jdbc.Driver (MySQL 6.x以降)
  - SQL Server: com.microsoft.sqlserver.jdbc.SQLServerDriver
  - Oracle: oracle.jdbc.driver.OracleDriver
  - PostgreSQL: org.postgresql.Driver

リソースが作成される際、FEは「`driver_url`」パラメータで指定されたURLを使用してJDBCドライバーJARパッケージをダウンロードし、チェックサムを生成して、BEによってダウンロードされたJDBCドライバーを検証します。

> 注: JDBCドライバーJARパッケージのダウンロードが失敗した場合、リソースの作成も失敗します。

BEがJDBC外部テーブルを初めてクエリし、対応するJDBCドライバーJARパッケージが自身のマシンに存在しない場合、BEは「`driver_url`」パラメータで指定されたURLを使用してJDBCドライバーJARパッケージをダウンロードし、すべてのJDBCドライバーJARパッケージは「`${STARROCKS_HOME}/lib/jdbc_drivers`」ディレクトリに保存されます。

#### JDBCリソースの表示

StarRocks内のすべてのJDBCリソースを表示するには、以下のステートメントを実行します。

```SQL
SHOW RESOURCES;
```

> 注: 「`ResourceType`」列は「`jdbc`」です。

#### JDBCリソースの削除

「`jdbc0`」という名前のJDBCリソースを削除するには、以下のステートメントを実行します。

```SQL
DROP RESOURCE "jdbc0";
```

> 注: JDBCリソースが削除されると、そのJDBCリソースを使用して作成されたすべてのJDBC外部テーブルは利用できなくなります。ただし、ターゲットデータベース内のデータは失われません。StarRocksを使用してターゲットデータベース内のデータをクエリする必要がある場合は、JDBCリソースとJDBC外部テーブルを再度作成できます。

### データベースの作成

「`jdbc_test`」という名前のデータベースをStarRocksで作成し、アクセスするには、以下のステートメントを実行します。

```SQL
CREATE DATABASE jdbc_test; 
USE jdbc_test; 
```

> 注: 上記のステートメントで指定するデータベース名は、ターゲットデータベースの名前と同じである必要はありません。

### JDBC外部テーブルの作成

「`jdbc_tbl`データベース内`jdbc_test`:

```SQL
create external table jdbc_tbl (
    `id` bigint NULL, 
    `data` varchar(200) NULL 
) ENGINE=jdbc 
properties (
    "resource" = "jdbc0",
    "table" = "dest_tbl"
);
```

の必須パラメーターは次のとおりです。`properties`は次のとおりです。

- `resource`: 外部テーブルの作成に使用されるJDBCリソースの名前。

- `table`: データベース内のターゲットテーブル名。

サポートされているデータ型とStarRocksとターゲットデータベース間のデータ型マッピングについては、「[データ型マッピング](External_table.md#Data type mapping)」を参照してください。

> 注記：
>
> - インデックスはサポートされていません。
> - PARTITION BYまたはDISTRIBUTED BYを使用してデータ分散ルールを指定することはできません。

### JDBC外部テーブルのクエリ

JDBC外部テーブルをクエリする前に、次のステートメントを実行してパイプラインエンジンを有効にする必要があります。

```SQL
set enable_pipeline_engine=true;
```

> 注：パイプラインエンジンがすでに有効になっている場合は、この手順をスキップできます。

次のステートメントを実行して、JDBC外部テーブルを使用してターゲットデータベースのデータをクエリします。

```SQL
select * from JDBC_tbl;
```

StarRocksは、フィルター条件をターゲットテーブルにプッシュダウンすることで、述語プッシュダウンをサポートします。フィルター条件をデータソースのできるだけ近くで実行すると、クエリパフォーマンスが向上します。現在、StarRocksは、二項比較演算子（`>`, `>=`, `=`, `<`、および`<=`)、`IN`, `IS NULL`、および`BETWEEN ... AND ...`を含む演算子をプッシュダウンできます。ただし、StarRocksは関数をプッシュダウンできません。

### データ型マッピング

現在、StarRocksは、NUMBER、STRING、TIME、DATEなどのターゲットデータベースの基本型のデータのみをクエリできます。ターゲットデータベースのデータ値の範囲がStarRocksでサポートされていない場合、クエリはエラーを報告します。

ターゲットデータベースとStarRocks間のマッピングは、ターゲットデータベースのタイプによって異なります。

#### **MySQLとStarRocks**

| MySQL | StarRocks |
| ------------ | --------- |
| BOOLEAN | BOOLEAN |
| TINYINT | TINYINT |
| SMALLINT | SMALLINT |
| MEDIUMINTINT | INT |
| BIGINT | BIGINT |
| FLOAT | FLOAT |
| DOUBLE | DOUBLE |
| DECIMAL | DECIMAL |
| CHAR | CHAR |
| VARCHAR | VARCHAR |
| DATE | DATE |
| DATETIME | DATETIME |

#### **OracleとStarRocks**

| Oracle | StarRocks |
| --------------- | --------- |
| CHAR | CHAR |
| VARCHARVARCHAR2 | VARCHAR |
| DATE | DATE |
| SMALLINT | SMALLINT |
| INT | INT |
| BINARY_FLOAT | FLOAT |
| BINARY_DOUBLE | DOUBLE |
| DATE | DATE |
| DATETIME | DATETIME |
| NUMBER | DECIMAL |

#### **PostgreSQLとStarRocks**

| PostgreSQL | StarRocks |
| ------------------- | --------- |
| SMALLINTSMALLSERIAL | SMALLINT |
| INTEGERSERIAL | INT |
| BIGINTBIGSERIAL | BIGINT |
| BOOLEAN | BOOLEAN |
| REAL | FLOAT |
| DOUBLE PRECISION | DOUBLE |
| DECIMAL | DECIMAL |
| TIMESTAMP | DATETIME |
| DATE | DATE |
| CHAR | CHAR |
| VARCHAR | VARCHAR |
| TEXT | VARCHAR |

#### **SQL ServerとStarRocks**

| SQL Server | StarRocks |
| ----------------- | --------- |
| BOOLEAN | BOOLEAN |
| TINYINT | TINYINT |
| SMALLINT | SMALLINT |
| INT | INT |
| BIGINT | BIGINT |
| FLOAT | FLOAT |
| REAL | DOUBLE |
| DECIMALNUMERIC | DECIMAL |
| CHAR | CHAR |
| VARCHAR | VARCHAR |
| DATE | DATE |
| DATETIMEDATETIME2 | DATETIME |

### 制限事項

- JDBC外部テーブルを作成する際、テーブルにインデックスを作成したり、PARTITION BYおよびDISTRIBUTED BYを使用してテーブルのデータ分散ルールを指定したりすることはできません。

- JDBC外部テーブルをクエリする際、StarRocksは関数をテーブルにプッシュダウンできません。

## (非推奨) Elasticsearch外部テーブル

StarRocksとElasticsearchは、2つの人気のある分析システムです。StarRocksは大規模な分散コンピューティングで高性能を発揮します。Elasticsearchは全文検索に最適です。StarRocksとElasticsearchを組み合わせることで、より完全なOLAPソリューションを提供できます。

### Elasticsearch外部テーブルの作成例

#### 構文

```sql
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
```

次の表は、パラメーターについて説明しています。

| **パラメーター**        | **必須** | **デフォルト値** | **説明**                                              |
| -------------------- | ------------ | ----------------- | ------------------------------------------------------------ |
| hosts                | Yes          | None              | Elasticsearchクラスターの接続アドレス。1つ以上のアドレスを指定できます。StarRocksはこのアドレスからElasticsearchのバージョンとインデックスシャードの割り当てを解析できます。StarRocksは、`GET /_nodes/http` API操作によって返されるアドレスに基づいてElasticsearchクラスターと通信します。したがって、`host`パラメーターの値は、`GET /_nodes/http` API操作によって返されるアドレスと同じである必要があります。そうでない場合、BEはElasticsearchクラスターと通信できない可能性があります。|`index`を<code class="language-text">hello*</code>に設定すると、StarRocksは名前が`hello`で始まるすべてのインデックスを取得します。|`/*cluster/state/*nodes/http`とインデックスにアクセスできることを確認してください。|`_doc`            | インデックスのタイプ。デフォルト値: `_doc`。Elasticsearch 8以降のバージョンでデータをクエリする場合、マッピングタイプがElasticsearch 8以降のバージョンで削除されているため、このパラメーターを設定する必要はありません。|`false`           | StarRocksが、指定されたアドレスのみを使用して`hosts`でElasticsearchクラスターにアクセスし、データをフェッチするかどうかを指定します。<ul><li>`true`: StarRocksは、指定されたアドレスのみを使用して`hosts`でElasticsearchクラスターにアクセスし、データをフェッチし、Elasticsearchインデックスのシャードが配置されているデータノードをスニッフィングしません。StarRocksがElasticsearchクラスター内のデータノードのアドレスにアクセスできない場合、このパラメーターを`true`に設定する必要があります。</li><li>`false`: StarRocksは、指定されたアドレスを使用して`host`でElasticsearchクラスターインデックスのシャードが配置されているデータノードをスニッフィングします。StarRocksがクエリ実行計画を生成した後、関連するBEはElasticsearchクラスター内のデータノードに直接アクセスして、インデックスのシャードからデータをフェッチします。StarRocksがElasticsearchクラスター内のデータノードのアドレスにアクセスできる場合、デフォルト値の`false`を保持することをお勧めします。</li></ul> |`false`           | HTTPSプロトコルを使用してElasticsearchクラスターにアクセスできるかどうかを指定します。StarRocks 2.4以降のバージョンのみがこのパラメーターの設定をサポートしています。<ul><li>`true`: HTTPSとHTTPの両方のプロトコルを使用してElasticsearchクラスターにアクセスできます。</li><li>`false`: HTTPプロトコルのみを使用してElasticsearchクラスターにアクセスできます。</li></ul> |
| enable_docvalue_scan | No           | `true`            | Elasticsearchの列指向ストレージからターゲットフィールドの値を取得するかどうかを指定します。ほとんどの場合、列指向ストレージからのデータ読み取りは、行指向ストレージからのデータ読み取りよりもパフォーマンスが優れています。|`true`            | KEYWORD型フィールドに基づいてElasticsearch内のTEXT型フィールドをスニッフィングするかどうかを指定します。このパラメーターが`false`に設定されている場合、StarRocksはトークン化後にマッチングを実行します。|

##### 高速クエリのための列指向スキャン

を`enable_docvalue_scan`に設定すると、`true`、StarRocks は Elasticsearch からデータを取得する際に、以下のルールに従います。

- **試行**: StarRocks は、対象フィールドでカラム型ストレージが有効になっているかどうかを自動的にチェックします。有効になっている場合、StarRocks は対象フィールド内のすべての値をカラム型ストレージから取得します。
- **自動ダウングレード**: 対象フィールドのいずれかがカラム型ストレージで利用できない場合、StarRocks は対象フィールド内のすべての値をパースし、行型ストレージから取得します (`_source`)。

> **注**
>
> - Elasticsearch の TEXT 型フィールドでは、カラム型ストレージは利用できません。したがって、TEXT 型の値を含むフィールドをクエリする場合、StarRocks はそのフィールドの値を `_source`から取得します。
> - 多数 (25 以上) のフィールドをクエリする場合、フィールド値を `docvalue`から読み取ることは、フィールド値を `_source`から読み取る場合と比較して、顕著なメリットはありません。

##### KEYWORD 型フィールドの検出

もし `enable_keyword_sniff`を `true`に設定すると、Elasticsearch はインデックスなしで直接データ取り込みを許可します。これは、取り込み後に自動的にインデックスが作成されるためです。STRING 型フィールドの場合、Elasticsearch は TEXT 型と KEYWORD 型の両方を持つフィールドを作成します。これが Elasticsearch のマルチフィールド機能の仕組みです。マッピングは以下のとおりです。

```SQL
"k4": {
   "type": "text",
   "fields": {
      "keyword": {   
         "type": "keyword",
         "ignore_above": 256
      }
   }
}
```

例えば、`k4`に対して「=」フィルタリングを実行する場合、Elasticsearch 上の StarRocks はそのフィルタリング操作を Elasticsearch TermQuery に変換します。

元の SQL フィルターは以下のとおりです。

```SQL
k4 = "StarRocks On Elasticsearch"
```

変換された Elasticsearch クエリ DSL は以下のとおりです。

```SQL
"term" : {
    "k4": "StarRocks On Elasticsearch"

}
```

`k4`の最初のフィールドは TEXT 型であり、`k4`用に設定されたアナライザー (または `k4`用にアナライザーが設定されていない場合は標準アナライザー) によって、データ取り込み後にトークン化されます。その結果、最初のフィールドは 3 つのタームにトークン化されます。`StarRocks`、`On`、および `Elasticsearch`。詳細は以下のとおりです。

```SQL
POST /_analyze
{
  "analyzer": "standard",
  "text": "StarRocks On Elasticsearch"
}
```

トークン化の結果は以下のとおりです。

```SQL
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
```

以下のようにクエリを実行するとします。

```SQL
"term" : {
    "k4": "StarRocks On Elasticsearch"
}
```

辞書には、ターム `StarRocks On Elasticsearch`に一致するタームがないため、結果は返されません。

ただし、`enable_keyword_sniff`を `true`に設定している場合、StarRocks は `k4 = "StarRocks On Elasticsearch"`を `k4.keyword = "StarRocks On Elasticsearch"`に変換して SQL セマンティクスに一致させます。変換された `StarRocks On Elasticsearch`クエリ DSL は以下のとおりです。

```SQL
"term" : {
    "k4.keyword": "StarRocks On Elasticsearch"
}
```

`k4.keyword`は KEYWORD 型です。したがって、データは完全なタームとして Elasticsearch に書き込まれ、正常なマッチングが可能になります。

#### カラムデータ型のマッピング

外部テーブルを作成する際、Elasticsearch テーブルのカラムのデータ型に基づいて、外部テーブルのカラムのデータ型を指定する必要があります。以下の表は、カラムデータ型のマッピングを示しています。

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
> - StarRocksは、JSON関連関数を使用してNESTED型のデータを読み取ります。
> - Elasticsearchは多次元配列を自動的に一次元配列にフラット化します。StarRocksも同様です。**ElasticsearchからのARRAYデータのクエリのサポートはv2.5から追加されました。**

### 述語プッシュダウン

StarRocksは述語プッシュダウンをサポートしています。フィルターはElasticsearchにプッシュダウンして実行できるため、クエリパフォーマンスが向上します。次の表は、述語プッシュダウンをサポートする演算子を示しています。

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

「**esquery関数**」は、**SQLで表現できないクエリ** (matchやgeoshapeなど) をElasticsearchにプッシュダウンしてフィルタリングするために使用されます。esquery関数の最初のパラメーターはインデックスを関連付けるために使用されます。2番目のパラメーターは、基本的なQuery DSLのJSON式であり、角括弧「{}」で囲まれています。**JSON式には、ルートキーが1つだけ必要です**。例えば、match、geo_shape、boolなどです。

- matchクエリ

```sql
select * from es_table where esquery(k4, '{
    "match": {
       "k4": "StarRocks on elasticsearch"
    }
}');
```

- 地理関連クエリ

```sql
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
```

- boolクエリ

```sql
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
```

### 使用上の注意

- Elasticsearch 5.x以前は、5.x以降とは異なる方法でデータをスキャンします。現在、**5.x以降のバージョンのみ**がサポートされています。
- HTTP基本認証が有効なElasticsearchクラスターがサポートされています。
- StarRocksからデータをクエリする速度は、Elasticsearchから直接データをクエリする速度ほど速くない場合があります（カウント関連のクエリなど）。その理由は、Elasticsearchが実際のデータをフィルタリングする必要なく、ターゲットドキュメントのメタデータを直接読み取るため、カウントクエリが高速化されるためです。

## (非推奨) Hive外部テーブル

Hive外部テーブルを使用する前に、サーバーにJDK 1.8がインストールされていることを確認してください。

### Hiveリソースを作成する

HiveリソースはHiveクラスターに対応します。StarRocksが使用するHiveクラスター（Hiveメタストアアドレスなど）を設定する必要があります。Hive外部テーブルが使用するHiveリソースを指定する必要があります。

- hive0という名前のHiveリソースを作成する。

```sql
CREATE EXTERNAL RESOURCE "hive0"
PROPERTIES (
  "type" = "hive",
  "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
);
```

- StarRocksで作成されたリソースを表示する。

```sql
SHOW RESOURCES;
```

- という名前のリソースを削除する`hive0`。

```sql
DROP RESOURCE "hive0";
```

変更できます`hive.metastore.uris`StarRocks 2.3以降のバージョンでは、Hiveリソースの。詳細については、を参照してください。[ALTER RESOURCE](../sql-reference/sql-statements/Resource/ALTER_RESOURCE.md)。

### データベースを作成する

```sql
CREATE DATABASE hive_test;
USE hive_test;
```

### Hive外部テーブルを作成する

構文

```sql
CREATE EXTERNAL TABLE table_name (
  col_name col_type [NULL | NOT NULL] [COMMENT "comment"]
) ENGINE=HIVE
PROPERTIES (
  "key" = "value"
);
```

例: 外部テーブルを作成する`profile_parquet_p7`の下に`rawdata`データベースを、に対応するHiveクラスター内に。`hive0`リソース。

```sql
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
```

説明:

- 外部テーブルの列
  - 列名はHiveテーブルの列名と同じである必要があります。
  - 列の順序は**である必要はありません**Hiveテーブルの列の順序と同じ。
  - 選択できるのは**Hiveテーブルの一部の列のみ**ですが、すべての**パーティションキー列**を選択する必要があります。
  - 外部テーブルのパーティションキー列は、を使用して指定する必要はありません。`partition by`。他の列と同じ記述リストで定義する必要があります。パーティション情報を指定する必要はありません。StarRocksはHiveテーブルからこの情報を自動的に同期します。
  - を設定する`ENGINE`をHIVEに。
- PROPERTIES:
  - **hive.resource**: 使用されるHiveリソース。
  - **database**: Hiveデータベース。
  - **table**: Hive内のテーブル。**ビュー**はサポートされていません。
- 次の表は、HiveとStarRocks間のカラムデータ型マッピングについて説明しています。

  | Hiveのカラム型 | StarRocksのカラム型 | 説明 |
| --- | --- | ---|
| INT/INTEGER | INT | |
| BIGINT | BIGINT | |
| TIMESTAMP | DATETIME | TIMESTAMPデータをDATETIMEデータに変換すると、精度とタイムゾーン情報が失われます。sessionVariableのタイムゾーンに基づいて、タイムゾーンオフセットを持たないDATETIMEデータにTIMESTAMPデータを変換する必要があります。 |
| STRING | VARCHAR | |
| VARCHAR | VARCHAR | |
| CHAR | CHAR | |
| DOUBLE | DOUBLE | |
| FLOAT | FLOAT| |
| DECIMAL | DECIMAL| |
| ARRAY | ARRAY | |

> 注:
>
> - 現在サポートされているHiveストレージ形式は、Parquet、ORC、およびCSVです。
ストレージ形式がCSVの場合、引用符をエスケープ文字として使用することはできません。
> - SNAPPYおよびLZ4圧縮形式がサポートされています。
> - クエリ可能なHive文字列カラムの最大長は1 MBです。文字列カラムが1 MBを超える場合、NULLカラムとして処理されます。

### Hive外部テーブルを使用する

の総行数をクエリする`profile_wos_p7`。

```sql
select count(*) from profile_wos_p7;
```

### キャッシュされたHiveテーブルメタデータを更新する

- Hiveパーティション情報および関連ファイル情報はStarRocksにキャッシュされます。キャッシュは、によって指定された間隔で更新されます。`hive_meta_cache_refresh_interval_s`。デフォルト値は7200です。
  - キャッシュされたデータは手動で更新することもできます。
    1. Hiveのテーブルにパーティションが追加または削除された場合、`REFRESH EXTERNAL TABLE hive_t`コマンドを実行して、StarRocksにキャッシュされたテーブルメタデータを更新する必要があります。`hive_t`はStarRocksのHive外部テーブルの名前です。
    2. 一部のHiveパーティションのデータが更新された場合、`REFRESH EXTERNAL TABLE hive_t PARTITION ('k1=01/k2=02', 'k1=03/k2=04')`コマンドを実行して、StarRocksのキャッシュデータを更新する必要があります。`hive_t`はStarRocksのHive外部テーブルの名前です。`'k1=01/k2=02'`と`'k1=03/k2=04'`は、データが更新されたHiveパーティションの名前です。
    3. を実行すると、`REFRESH EXTERNAL TABLE hive_t`StarRocksはまず、Hive外部テーブルのカラム情報がHive Metastoreによって返されるHiveテーブルのカラム情報と同じであるかどうかを確認します。カラムの追加や削除など、Hiveテーブルのスキーマが変更された場合、StarRocksはその変更をHive外部テーブルに同期します。同期後、Hive外部テーブルのカラム順序はHiveテーブルのカラム順序と同じになり、パーティションカラムが最後のカラムになります。
- HiveデータがParquet、ORC、およびCSV形式で保存されている場合、StarRocks 2.3以降のバージョンでは、Hiveテーブルのスキーマ変更（ADD COLUMNやREPLACE COLUMNなど）をHive外部テーブルに同期できます。

### オブジェクトストレージにアクセスする

- FE設定ファイルのパスは`fe/conf`であり、Hadoopクラスターをカスタマイズする必要がある場合、設定ファイルを追加できます。例えば、HDFSクラスターが高可用性ネームサービスを使用している場合、`hdfs-site.xml`を`fe/conf`の下に配置する必要があります。HDFSがViewFsで構成されている場合、`core-site.xml`を`fe/conf`の下に配置する必要があります。

- BE設定ファイルのパスは`be/conf`であり、Hadoopクラスターをカスタマイズする必要がある場合、設定ファイルを追加できます。例えば、HDFSクラスターが高可用性ネームサービスを使用している場合、`hdfs-site.xml`を`be/conf`の下に配置する必要があります。HDFSがViewFsで構成されている場合、`core-site.xml`を`be/conf`の下に配置する必要があります。

- BEが配置されているマシンでは、BEの**起動スクリプトで、JAVA_HOMEをJRE環境ではなくJDK環境として構成します。** `bin/start_be.sh`、例えば、`export JAVA_HOME = <JDK path>`。この設定はスクリプトの先頭に追加し、設定を有効にするにはBEを再起動する必要があります。

- Kerberosサポートの設定:
  1. でログインするには、`kinit -kt keytab_path principal`すべてのFE/BEマシンにアクセスするには、HiveとHDFSへのアクセスが必要です。kinitコマンドによるログインは一定期間のみ有効であり、定期的に実行されるようにcrontabに設定する必要があります。
  2. を`hive-site.xml/core-site.xml/hdfs-site.xml`の下に置き、`fe/conf`、そして`core-site.xml/hdfs-site.xml`を`be/conf`の下に置きます。
  3. を`-Djava.security.krb5.conf=/etc/krb5.conf`オプションの値に`JAVA_OPTS`追加します。**$FE_HOME/conf/fe.conf**ファイルに。**/etc/krb5.conf**は**krb5.conf**ファイルの保存パスです。オペレーティングシステムに基づいてパスを変更できます。
  4. を直接`JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"`に**$BE_HOME/conf/be.conf**ファイルに追加します。**/etc/krb5.conf**は**krb5.conf**ファイルの保存パスです。オペレーティングシステムに基づいてパスを変更できます。
  5. Hiveリソースを追加する際、ドメイン名を`hive.metastore.uris`に渡す必要があります。さらに、Hive/HDFSのドメイン名とIPアドレスのマッピングを**/etc/hosts**ファイルに追加する必要があります。

- AWS S3のサポートを設定: 以下の設定を`fe/conf/core-site.xml`と`be/conf/core-site.xml`に追加します。

  ```XML
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
  ```

  1. `fs.s3a.access.key`：AWSアクセスキーID。
  2. `fs.s3a.secret.key`：AWSシークレットキー。
  3. `fs.s3a.endpoint`：接続するAWS S3エンドポイント。
  4. `fs.s3a.connection.maximum`：StarRocksからS3への同時接続の最大数。クエリ中にエラー`Timeout waiting for connection from poll`が発生した場合、このパラメータをより大きな値に設定できます。

## (非推奨) Iceberg外部テーブル

v2.1.0以降、StarRocksでは外部テーブルを使用してApache Icebergからデータをクエリできます。Icebergのデータをクエリするには、StarRocksにIceberg外部テーブルを作成する必要があります。テーブルを作成する際には、外部テーブルとクエリしたいIcebergテーブルとのマッピングを確立する必要があります。

### 開始する前に

StarRocksが、Apache Icebergが使用するメタデータサービス（Hive metastoreなど）、ファイルシステム（HDFSなど）、およびオブジェクトストレージシステム（Amazon S3やAlibaba Cloud Object Storage Serviceなど）にアクセスする権限を持っていることを確認してください。

### 注意事項

- Iceberg外部テーブルは、以下の種類のデータのみをクエリするために使用できます。
  - Iceberg v1テーブル（分析データテーブル）。ORC形式のIceberg v2（行レベル削除）テーブルはv3.0以降でサポートされ、Parquet形式のIceberg v2テーブルはv3.1以降でサポートされます。Iceberg v1テーブルとIceberg v2テーブルの違いについては、以下を参照してください。[Icebergテーブル仕様](https://iceberg.apache.org/spec/)。
  - gzip（デフォルト形式）、Zstd、LZ4、またはSnappy形式で圧縮されたテーブル。
  - ParquetまたはORC形式で保存されたファイル。

- StarRocks 2.3以降のバージョンのIceberg外部テーブルは、Icebergテーブルのスキーマ変更の同期をサポートしていますが、StarRocks 2.3より前のバージョンのIceberg外部テーブルはサポートしていません。Icebergテーブルのスキーマが変更された場合、対応する外部テーブルを削除し、新しいテーブルを作成する必要があります。

### 手順

#### ステップ1：Icebergリソースを作成する

Iceberg外部テーブルを作成する前に、StarRocksにIcebergリソースを作成する必要があります。このリソースはIcebergへのアクセス情報を管理するために使用されます。さらに、外部テーブルを作成するステートメントでこのリソースを指定する必要があります。ビジネス要件に基づいてリソースを作成できます。

- IcebergテーブルのメタデータがHive metastoreから取得される場合、リソースを作成し、カタログタイプを「`HIVE`」に設定できます。

- Icebergテーブルのメタデータが他のサービスから取得される場合、カスタムカタログを作成する必要があります。その後、リソースを作成し、カタログタイプを「`CUSTOM`」に設定します。

##### カタログタイプが「`HIVE`

例えば、「`iceberg0`」という名前のリソースを作成し、カタログタイプを「`HIVE`」に設定します。

```SQL
CREATE EXTERNAL RESOURCE "iceberg0" 
PROPERTIES (
   "type" = "iceberg",
   "iceberg.catalog.type" = "HIVE",
   "iceberg.catalog.hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083" 
);
```

以下の表は関連するパラメーターについて説明しています。

| **パラメーター**                       | **説明**                                              |
| ----------------------------------- | ------------------------------------------------------------ |
| type                                | リソースタイプ。値を「`iceberg`」に設定します。|
| iceberg.catalog.type              | リソースのカタログタイプ。Hiveカタログとカスタムカタログの両方がサポートされています。Hiveカタログを指定する場合は、値を「`HIVE`」に設定します。カスタムカタログを指定する場合は、値を「`CUSTOM`」に設定します。|
| iceberg.catalog.hive.metastore.uris | Hive metastoreのURI。パラメーター値は以下の形式です。`thrift://< IP address of Iceberg metadata >:< port number >`。ポート番号はデフォルトで9083です。Apache IcebergはHiveカタログを使用してHive metastoreにアクセスし、Icebergテーブルのメタデータをクエリします。|

##### カタログタイプが「`CUSTOM`

カスタムカタログは抽象クラスBaseMetastoreCatalogを継承し、IcebergCatalogインターフェースを実装する必要があります。さらに、カスタムカタログのクラス名は、StarRocksに既に存在するクラス名と重複してはなりません。カタログが作成されたら、カタログとその関連ファイルをパッケージ化し、各フロントエンド（FE）の「**fe/lib**」パスの下に配置します。その後、各FEを再起動します。上記の操作を完了すると、カタログがカスタムカタログであるリソースを作成できます。

例えば、「`iceberg1`」という名前のリソースを作成し、カタログタイプを「`CUSTOM`」に設定します。

```SQL
CREATE EXTERNAL RESOURCE "iceberg1" 
PROPERTIES (
   "type" = "iceberg",
   "iceberg.catalog.type" = "CUSTOM",
   "iceberg.catalog-impl" = "com.starrocks.IcebergCustomCatalog" 
);
```

以下の表は関連するパラメーターについて説明しています。

| **パラメーター**          | **説明**                                              |
| ---------------------- | ------------------------------------------------------------ |
| type                   | リソースタイプ。値を「`iceberg`」に設定します。               |`HIVE`」に設定します。カスタムカタログを指定する場合は、値を「`CUSTOM`」に設定します。 |`PROPERTIES`」パラメーターにキーと値のペアとして追加する必要があります。 |

Icebergリソースの「`hive.metastore.uris`」と「`iceberg.catalog-impl`」は、StarRocks 2.3以降のバージョンで変更可能です。詳細については、「[ALTER RESOURCE](../sql-reference/sql-statements/Resource/ALTER_RESOURCE.md)」を参照してください。

##### Icebergリソースの表示

```SQL
SHOW RESOURCES;
```

##### Icebergリソースの削除

例えば、「`iceberg0`」という名前のリソースを削除します。

```SQL
DROP RESOURCE "iceberg0";
```

Icebergリソースを削除すると、このリソースを参照するすべての外部テーブルが利用できなくなります。ただし、Apache Iceberg内の対応するデータは削除されません。Apache Iceberg内のデータを引き続きクエリする必要がある場合は、新しいリソースと新しい外部テーブルを作成してください。

#### ステップ2: (オプション) データベースの作成

例えば、StarRocksで「`iceberg_test`」という名前のデータベースを作成します。

```SQL
CREATE DATABASE iceberg_test; 
USE iceberg_test; 
```

> 注: StarRocksのデータベース名は、Apache Icebergのデータベース名と異なる場合があります。

#### ステップ3: Iceberg外部テーブルの作成

例えば、データベース「`iceberg_tbl`」に「`iceberg_test`」という名前のIceberg外部テーブルを作成します。

```SQL
CREATE EXTERNAL TABLE `iceberg_tbl` ( 
    `id` bigint NULL, 
    `data` varchar(200) NULL 
) ENGINE=ICEBERG 
PROPERTIES ( 
    "resource" = "iceberg0", 
    "database" = "iceberg", 
    "table" = "iceberg_table" 
); 
```

次の表は、関連するパラメーターについて説明しています。

| **パラメーター** | **説明**                                              |
| ------------- | ------------------------------------------------------------ |
| ENGINE        | エンジン名。値を「`ICEBERG`」に設定します。                 |
| resource      | 外部テーブルが参照するIcebergリソースの名前。 |
| database      | Icebergテーブルが属するデータベースの名前。 |
| table         | Icebergテーブルの名前。                               |

> 注:
>
> - 外部テーブルの名前は、Icebergテーブルの名前と異なる場合があります。
>
> - 外部テーブルの列名は、Icebergテーブルの列名と同じである必要があります。両テーブルの列の順序は異なる場合があります。

」パラメーターにキーと値のペアとして追加できます。例えば、カスタムカタログで設定項目「`PROPERTIES`」を定義した場合、次のコマンドを実行して外部テーブルを作成できます。`custom-catalog.properties`.

```SQL
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
```

外部テーブルを作成する際、Icebergテーブルの列のデータ型に基づいて、外部テーブルの列のデータ型を指定する必要があります。以下の表は、列のデータ型のマッピングを示しています。

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

StarRocksは、データ型がTIMESTAMPTZ、STRUCT、MAPであるIcebergデータのクエリをサポートしていません。

#### ステップ4: Apache Icebergのデータをクエリする

外部テーブルが作成された後、その外部テーブルを使用してApache Icebergのデータをクエリできます。

```SQL
select count(*) from iceberg_tbl;
```

## (非推奨) Hudi外部テーブル

v2.2.0以降、StarRocksはHudi外部テーブルを使用してHudiデータレイクからデータをクエリすることを可能にし、これにより超高速なデータレイク分析を促進します。このトピックでは、StarRocksクラスターでHudi外部テーブルを作成し、そのHudi外部テーブルを使用してHudiデータレイクからデータをクエリする方法について説明します。

### 開始する前に

StarRocksクラスターが、Hudiテーブルを登録できるHiveメタストア、HDFSクラスター、またはバケットへのアクセス権を持っていることを確認してください。

### 注意事項

- HudiのHudi外部テーブルは読み取り専用であり、クエリにのみ使用できます。
- StarRocksは、Copy on WriteおよびMerge On Readテーブルのクエリをサポートしています（MORテーブルはv2.5以降でサポート）。これら2種類のテーブルの違いについては、以下を参照してください。[テーブルとクエリの種類](https://hudi.apache.org/docs/table_types/).
- StarRocksは、Hudiの以下の2つのクエリタイプをサポートしています: スナップショットクエリと読み取り最適化クエリ（HudiはMerge On Readテーブルでのみ読み取り最適化クエリの実行をサポートしています）。増分クエリはサポートされていません。Hudiのクエリタイプに関する詳細については、以下を参照してください。[テーブルとクエリの種類](https://hudi.apache.org/docs/next/table_types/#query-types).
- StarRocksは、Hudiファイルに対して以下の圧縮形式をサポートしています: gzip、zstd、LZ4、Snappy。Hudiファイルのデフォルトの圧縮形式はgzipです。
- StarRocksは、Hudi管理テーブルからのスキーマ変更を同期できません。詳細については、以下を参照してください。[スキーマ進化](https://hudi.apache.org/docs/schema_evolution/)。Hudi管理テーブルのスキーマが変更された場合、関連するHudi外部テーブルをStarRocksクラスターから削除し、その外部テーブルを再作成する必要があります。

### 手順

#### ステップ1: Hudiリソースの作成と管理

StarRocksクラスターにHudiリソースを作成する必要があります。Hudiリソースは、StarRocksクラスターで作成するHudiデータベースと外部テーブルを管理するために使用されます。

##### Hudiリソースを作成する

以下のステートメントを実行して、という名前のHudiリソースを作成します。`hudi0`:

```SQL
CREATE EXTERNAL RESOURCE "hudi0" 
PROPERTIES ( 
    "type" = "hudi", 
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
);
```

以下の表は、パラメーターについて説明しています。

| パラメーター | 説明 |
| ------------------- | ------------------------------------------------------------ |
| type | Hudiリソースのタイプ。値をhudiに設定します。 |
| hive.metastore.uris | Hudiリソースが接続するHiveメタストアのThrift URI。HudiリソースをHiveメタストアに接続した後、Hiveを使用してHudiテーブルを作成および管理できます。Thrift URIは`<IP address of the Hive metastore>:<Port number of the Hive metastore>` 形式です。デフォルトのポート番号は9083です。 |

v2.3以降、StarRocksは`hive.metastore.uris` Hudiリソースの値を変更できます。詳細については、以下を参照してください。[ALTER RESOURCE](../sql-reference/sql-statements/Resource/ALTER_RESOURCE.md).

##### Hudiリソースを表示する

StarRocksクラスターで作成されたすべてのHudiリソースを表示するには、以下のステートメントを実行します。

```SQL
SHOW RESOURCES;
```

##### Hudiリソースを削除する

という名前のHudiリソースを削除するには、次のステートメントを実行します。`hudi0`：

```SQL
DROP RESOURCE "hudi0";
```

> 注意：
>
> Hudiリソースを削除すると、そのHudiリソースを使用して作成されたすべてのHudi外部テーブルが利用できなくなります。ただし、削除はHudiに保存されているデータには影響しません。StarRocksを使用してHudiからデータをクエリしたい場合は、StarRocksクラスターでHudiリソース、Hudiデータベース、およびHudi外部テーブルを再作成する必要があります。

#### ステップ2：Hudiデータベースを作成する

という名前のHudiデータベースを作成して開くには、次のステートメントを実行します。`hudi_test`をStarRocksクラスターで実行します。

```SQL
CREATE DATABASE hudi_test; 
USE hudi_test; 
```

> 注意：
>
> StarRocksクラスターでHudiデータベースに指定する名前は、Hudi内の関連データベースと同じである必要はありません。

#### ステップ3：Hudi外部テーブルを作成する

という名前のHudi外部テーブルを作成するには、次のステートメントを実行します。`hudi_tbl`の`hudi_test`Hudiデータベースで：

```SQL
CREATE EXTERNAL TABLE `hudi_tbl` ( 
    `id` bigint NULL, 
    `data` varchar(200) NULL 
) ENGINE=HUDI 
PROPERTIES ( 
    "resource" = "hudi0", 
    "database" = "hudi", 
    "table" = "hudi_table" 
); 
```

次の表は、パラメーターについて説明しています。

| パラメーター | 説明 |
| --------- | ------------------------------------------------------------ |
| ENGINE | Hudi外部テーブルのクエリエンジン。値を「`HUDI`」に設定します。 |

> resource | StarRocksクラスター内のHudiリソースの名前。 |
>
> - database | Hudi外部テーブルが属するStarRocksクラスター内のHudiデータベースの名前。 |
>
> - table | Hudi外部テーブルが関連付けられているHudiマネージドテーブル。 |
>
> - 注意：

Hudi外部テーブルに指定する名前は、関連するHudiマネージドテーブルと同じである必要はありません。

> **Hudi外部テーブルの列は、関連するHudiマネージドテーブルの対応する列と比較して、同じ名前である必要がありますが、順序が異なる場合があります。**
>
> 関連するHudiマネージドテーブルから一部またはすべての列を選択し、選択した列のみをHudi外部テーブルに作成できます。次の表は、Hudiでサポートされているデータ型とStarRocksでサポートされているデータ型のマッピングを示しています。

#### | Hudiでサポートされているデータ型 | StarRocksでサポートされているデータ型 |
| ---------------------------- | --------------------------------- |
| BOOLEAN | BOOLEAN |
| INT | TINYINT/SMALLINT/INT |
| DATE | DATE |
| TimeMillis/TimeMicros | TIME |
| TimestampMillis/TimestampMicros| DATETIME |
| LONG | BIGINT |
| FLOAT | FLOAT |
| DOUBLE | DOUBLE |
| STRING | CHAR/VARCHAR |
| ARRAY | ARRAY |
| DECIMAL | DECIMAL |

注意

```SQL
SELECT COUNT(*) FROM hudi_tbl;
```

## StarRocksは、STRUCT型またはMAP型のデータのクエリをサポートしていません。また、Merge On ReadテーブルでのARRAY型のデータのクエリもサポートしていません。

ステップ4：Hudi外部テーブルからデータをクエリする

特定のHudiマネージドテーブルに関連付けられたHudi外部テーブルを作成した後、Hudi外部テーブルにデータをロードする必要はありません。Hudiからデータをクエリするには、次のステートメントを実行します。

```sql
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
```

(非推奨) MySQL外部テーブル

- **スター型スキーマでは、データは通常、ディメンションテーブルとファクトテーブルに分けられます。ディメンションテーブルはデータ量が少ないですが、UPDATE操作を伴います。現在、StarRocksは直接UPDATE操作をサポートしていません（UPDATEはUnique Keyテーブルを使用して実装できます）。一部のシナリオでは、直接データ読み取りのためにディメンションテーブルをMySQLに保存できます。**MySQLデータをクエリするには、StarRocksに外部テーブルを作成し、それをMySQLデータベース内のテーブルにマッピングする必要があります。テーブルを作成する際に、MySQL接続情報を指定する必要があります。
- **パラメーター：**host
- **：MySQLデータベースの接続アドレス**port
- **：MySQLデータベースのポート番号**user
- **：MySQLにログインするためのユーザー名**password
- **テーブル**: MySQLデータベース内のテーブルの名前
