---
displayed_sidebar: docs
---

# 外部テーブル

StarRocks は外部テーブルを使用して他のデータソースにアクセスすることをサポートしています。外部テーブルは、他のデータソースに保存されているデータテーブルに基づいて作成されます。StarRocks はデータテーブルのメタデータのみを保存します。外部テーブルを使用して、他のデータソースのデータを直接クエリすることができます。StarRocks は次のデータソースをサポートしています: MySQL、Elasticsearch、Hive、StarRocks、Apache Iceberg、Apache Hudi。**現在、他の StarRocks クラスターから現在の StarRocks クラスターにデータを書き込むことのみが可能です。データを読み取ることはできません。StarRocks 以外のデータソースからは、これらのデータソースからデータを読み取ることのみが可能です。**

バージョン 2.5 以降、StarRocks は外部データソース上のホットデータクエリを高速化する Data Cache 機能を提供しています。詳細は [Data Cache](data_cache.md) を参照してください。

## MySQL 外部テーブル

スタースキーマでは、データは一般的にディメンジョンテーブルとファクトテーブルに分けられます。ディメンジョンテーブルはデータ量が少なく、UPDATE 操作が含まれます。現在、StarRocks は直接の UPDATE 操作をサポートしていません（ユニークキーテーブルを使用して更新を実装できます）。いくつかのシナリオでは、ディメンジョンテーブルを MySQL に保存して直接データを読み取ることができます。

MySQL データをクエリするには、StarRocks に外部テーブルを作成し、MySQL データベース内のテーブルにマッピングする必要があります。テーブルを作成する際に、MySQL の接続情報を指定する必要があります。

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

パラメータ:

* **host**: MySQL データベースの接続アドレス
* **port**: MySQL データベースのポート番号
* **user**: MySQL にログインするためのユーザー名
* **password**: MySQL にログインするためのパスワード
* **database**: MySQL データベースの名前
* **table**: MySQL データベース内のテーブルの名前

## StarRocks 外部テーブル

StarRocks 1.19 以降、StarRocks は StarRocks 外部テーブルを使用して、ある StarRocks クラスターから別のクラスターにデータを書き込むことを可能にします。これにより、読み書きの分離が実現され、より良いリソースの分離が提供されます。最初に、宛先 StarRocks クラスターに宛先テーブルを作成します。次に、ソース StarRocks クラスターで、宛先テーブルと同じスキーマを持つ StarRocks 外部テーブルを作成し、`PROPERTIES` フィールドに宛先クラスターとテーブルの情報を指定します。

ソースクラスターから宛先クラスターにデータを書き込むには、StarRocks 外部テーブルにデータを書き込むために INSERT INTO ステートメントを使用します。これにより、次の目標を達成できます:

* StarRocks クラスター間のデータ同期。
* 読み書きの分離。データはソースクラスターに書き込まれ、ソースクラスターからのデータ変更は宛先クラスターに同期され、クエリサービスを提供します。

以下のコードは、宛先テーブルと外部テーブルを作成する方法を示しています。

~~~SQL
# 宛先 StarRocks クラスターに宛先テーブルを作成します。
CREATE TABLE t
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
)
ENGINE=olap
DISTRIBUTED BY HASH(k1) BUCKETS 10;

# ソース StarRocks クラスターに外部テーブルを作成します。
CREATE EXTERNAL TABLE external_t
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
)
ENGINE=olap
DISTRIBUTED BY HASH(k1) BUCKETS 10
PROPERTIES
(
    "host" = "127.0.0.1",
    "port" = "9020",
    "user" = "user",
    "password" = "passwd",
    "database" = "db_test",
    "table" = "t"
);

# ソースクラスターから宛先クラスターにデータを書き込むには、StarRocks 外部テーブルにデータを書き込みます。2番目のステートメントは本番環境で推奨されます。
insert into external_t values ('2020-10-11', 1, 1, 'hello', '2020-10-11 10:00:00');
insert into external_t select * from other_table;
~~~

パラメータ:

* **EXTERNAL:** このキーワードは、作成されるテーブルが外部テーブルであることを示します。
* **host:** このパラメータは、宛先 StarRocks クラスターの Leader FE ノードの IP アドレスを指定します。
* **port:** このパラメータは、宛先 StarRocks クラスターの FE ノードの RPC ポートを指定します。

  :::note

  StarRocks 外部テーブルが属するソースクラスターが宛先 StarRocks クラスターにアクセスできるようにするには、ネットワークとファイアウォールを構成して、次のポートへのアクセスを許可する必要があります:

  * FE ノードの RPC ポート。FE 設定ファイル **fe/fe.conf** の `rpc_port` を参照してください。デフォルトの RPC ポートは `9020` です。
  * BE ノードの bRPC ポート。BE 設定ファイル **be/be.conf** の `brpc_port` を参照してください。デフォルトの bRPC ポートは `8060` です。

  :::

* **user:** このパラメータは、宛先 StarRocks クラスターにアクセスするために使用されるユーザー名を指定します。
* **password:** このパラメータは、宛先 StarRocks クラスターにアクセスするために使用されるパスワードを指定します。
* **database:** このパラメータは、宛先テーブルが属するデータベースを指定します。
* **table:** このパラメータは、宛先テーブルの名前を指定します。

StarRocks 外部テーブルを使用する際の制限事項は次のとおりです:

* StarRocks 外部テーブルでは、INSERT INTO と SHOW CREATE TABLE コマンドのみを実行できます。他のデータ書き込み方法はサポートされていません。また、StarRocks 外部テーブルからデータをクエリしたり、外部テーブルに対して DDL 操作を実行することはできません。
* 外部テーブルの作成構文は通常のテーブルの作成と同じですが、外部テーブルの列名やその他の情報は宛先テーブルと同じでなければなりません。
* 外部テーブルは宛先テーブルから10秒ごとにテーブルメタデータを同期します。宛先テーブルで DDL 操作が行われた場合、2つのテーブル間のデータ同期に遅延が生じる可能性があります。

## Elasticsearch 外部テーブル

StarRocks と Elasticsearch は、2つの人気のある分析システムです。StarRocks は大規模な分散コンピューティングで高性能を発揮し、Elasticsearch は全文検索に最適です。StarRocks と Elasticsearch を組み合わせることで、より完全な OLAP ソリューションを提供できます。

### Elasticsearch 外部テーブルの作成例

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

次の表は、パラメータを説明しています。

| **Parameter**        | **Required** | **Default value** | **Description**                                              |
| -------------------- | ------------ | ----------------- | ------------------------------------------------------------ |
| hosts                | Yes          | None              | Elasticsearch クラスターの接続アドレス。1つ以上のアドレスを指定できます。StarRocks はこのアドレスから Elasticsearch のバージョンとインデックスシャードの割り当てを解析します。StarRocks は `GET /_nodes/http` API 操作で返されるアドレスに基づいて Elasticsearch クラスターと通信します。したがって、`host` パラメータの値は `GET /_nodes/http` API 操作で返されるアドレスと同じでなければなりません。そうでない場合、BEs は Elasticsearch クラスターと通信できない可能性があります。 |
| index                | Yes          | None              | StarRocks のテーブルに作成された Elasticsearch インデックスの名前。名前はエイリアスにすることができます。このパラメータはワイルドカード（\*）をサポートします。たとえば、`index` を <code class="language-text">hello*</code> に設定すると、StarRocks は名前が `hello` で始まるすべてのインデックスを取得します。 |
| user                 | No           | Empty             | 基本認証が有効な Elasticsearch クラスターにログインするために使用されるユーザー名。`/*cluster/state/*nodes/http` とインデックスにアクセスできることを確認してください。 |
| password             | No           | Empty             | Elasticsearch クラスターにログインするために使用されるパスワード。 |
| type                 | No           | `_doc`            | インデックスのタイプ。デフォルト値: `_doc`。Elasticsearch 8 以降のバージョンでデータをクエリする場合、このパラメータを設定する必要はありません。Elasticsearch 8 以降のバージョンではマッピングタイプが削除されています。 |
| es.nodes.wan.only    | No           | `false`           | StarRocks が Elasticsearch クラスターにアクセスしてデータを取得するために `hosts` で指定されたアドレスのみを使用するかどうかを指定します。<ul><li>`true`: StarRocks は Elasticsearch クラスターにアクセスしてデータを取得するために `hosts` で指定されたアドレスのみを使用し、Elasticsearch インデックスのシャードが存在するデータノードをスニッフしません。StarRocks が Elasticsearch クラスター内のデータノードのアドレスにアクセスできない場合、このパラメータを `true` に設定する必要があります。</li><li>`false`: StarRocks は Elasticsearch クラスターインデックスのシャードが存在するデータノードをスニッフするために `host` で指定されたアドレスを使用します。StarRocks が Elasticsearch クラスター内のデータノードのアドレスにアクセスできる場合、デフォルト値 `false` を保持することをお勧めします。</li></ul> |
| es.net.ssl           | No           | `false`           | HTTPS プロトコルを使用して Elasticsearch クラスターにアクセスできるかどうかを指定します。StarRocks 2.4 以降のバージョンのみがこのパラメータの設定をサポートしています。<ul><li>`true`: HTTPS および HTTP プロトコルの両方を使用して Elasticsearch クラスターにアクセスできます。</li><li>`false`: HTTP プロトコルのみを使用して Elasticsearch クラスターにアクセスできます。</li></ul> |
| enable_docvalue_scan | No           | `true`            | Elasticsearch 列指向（カラムナ）ストレージからターゲットフィールドの値を取得するかどうかを指定します。ほとんどの場合、列指向ストレージからデータを読み取る方が行指向ストレージからデータを読み取るよりも優れています。 |
| enable_keyword_sniff | No           | `true`            | Elasticsearch で TEXT 型フィールドを KEYWORD 型フィールドに基づいてスニッフするかどうかを指定します。このパラメータを `false` に設定すると、StarRocks はトークン化後にマッチングを実行します。 |

##### クエリを高速化するための列指向スキャン

`enable_docvalue_scan` を `true` に設定すると、StarRocks は Elasticsearch からデータを取得する際に次のルールに従います:

* **試してみる**: StarRocks はターゲットフィールドに対して列指向ストレージが有効かどうかを自動的にチェックします。有効な場合、StarRocks はターゲットフィールドのすべての値を列指向ストレージから取得します。
* **自動ダウングレード**: ターゲットフィールドのいずれかが列指向ストレージで利用できない場合、StarRocks は行指向ストレージ（`_source`）からターゲットフィールドのすべての値を解析して取得します。

> **注意**
>
> * Elasticsearch では TEXT 型フィールドに対して列指向ストレージは利用できません。したがって、TEXT 型の値を含むフィールドをクエリする場合、StarRocks はフィールドの値を `_source` から取得します。
> * 多数（25 以上）のフィールドをクエリする場合、`docvalue` からフィールド値を読み取ることは `_source` からフィールド値を読み取ることと比較して顕著な利点を示しません。

##### KEYWORD 型フィールドのスニッフ

`enable_keyword_sniff` を `true` に設定すると、Elasticsearch はインデックスなしで直接データ取り込みを許可します。取り込み後に自動的にインデックスを作成します。STRING 型フィールドの場合、Elasticsearch は TEXT 型と KEYWORD 型の両方を持つフィールドを作成します。これは Elasticsearch の Multi-Field 機能の動作です。マッピングは次のようになります:

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

たとえば、`k4` に対して "=" フィルタリングを行う場合、StarRocks は Elasticsearch 上でフィルタリング操作を Elasticsearch TermQuery に変換します。

元の SQL フィルターは次のとおりです:

~~~SQL
k4 = "StarRocks On Elasticsearch"
~~~

変換された Elasticsearch クエリ DSL は次のとおりです:

~~~SQL
"term" : {
    "k4": "StarRocks On Elasticsearch"

}
~~~

`k4` の最初のフィールドは TEXT であり、データ取り込み後に `k4` に設定されたアナライザー（またはアナライザーが設定されていない場合は標準アナライザー）によってトークン化されます。その結果、最初のフィールドは `StarRocks`、`On`、`Elasticsearch` の3つのトークンに分割されます。詳細は次のとおりです:

~~~SQL
POST /_analyze
{
  "analyzer": "standard",
  "text": "StarRocks On Elasticsearch"
}
~~~

トークン化の結果は次のとおりです:

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

たとえば、次のようにクエリを実行するとします:

~~~SQL
"term" : {
    "k4": "StarRocks On Elasticsearch"
}
~~~

辞書に `StarRocks On Elasticsearch` に一致する用語がないため、結果は返されません。

しかし、`enable_keyword_sniff` を `true` に設定している場合、StarRocks は `k4 = "StarRocks On Elasticsearch"` を `k4.keyword = "StarRocks On Elasticsearch"` に変換して SQL セマンティクスに一致させます。変換された `StarRocks On Elasticsearch` クエリ DSL は次のとおりです:

~~~SQL
"term" : {
    "k4.keyword": "StarRocks On Elasticsearch"
}
~~~

`k4.keyword` は KEYWORD 型です。したがって、データは Elasticsearch に完全な用語として書き込まれ、成功したマッチングが可能になります。

#### 列データ型のマッピング

外部テーブルを作成する際には、Elasticsearch テーブル内の列データ型に基づいて外部テーブル内の列データ型を指定する必要があります。次の表は、列データ型のマッピングを示しています。

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

> **注意**
>
> * StarRocks は、NESTED 型のデータを JSON 関連の関数を使用して読み取ります。
> * Elasticsearch は多次元配列を自動的に一次元配列にフラット化します。StarRocks も同様です。**Elasticsearch からの ARRAY データのクエリサポートは v2.5 から追加されました。**

### プレディケートプッシュダウン

StarRocks はプレディケートプッシュダウンをサポートしています。フィルタは Elasticsearch にプッシュダウンされて実行され、クエリパフォーマンスが向上します。次の表は、プレディケートプッシュダウンをサポートするオペレーターを示しています。

|   SQL 構文  |   ES 構文  |
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

**esquery 関数**は、**SQL で表現できないクエリ**（たとえば、match や geoshape）を Elasticsearch にプッシュダウンしてフィルタリングするために使用されます。esquery 関数の最初のパラメータはインデックスを関連付けるために使用されます。2番目のパラメータは基本的な Query DSL の JSON 式で、{} で囲まれています。**JSON 式は 1 つだけのルートキーを持たなければなりません**。たとえば、match、geo_shape、または bool です。

* match クエリ

~~~sql
select * from es_table where esquery(k4, '{
    "match": {
       "k4": "StarRocks on elasticsearch"
    }
}');
~~~

* geo 関連クエリ

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

* bool クエリ

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

* Elasticsearch 5.x より前のバージョンは、5.x 以降のバージョンとは異なる方法でデータをスキャンします。現在、**5.x 以降のバージョンのみがサポートされています。**
* HTTP 基本認証が有効な Elasticsearch クラスターがサポートされています。
* StarRocks からデータをクエリすることは、Elasticsearch から直接データをクエリするほど速くない場合があります。たとえば、count 関連のクエリです。その理由は、Elasticsearch がターゲットドキュメントのメタデータを直接読み取り、実際のデータをフィルタリングする必要がないため、count クエリが高速化されるからです。

## JDBC 互換データベース用の外部テーブル

バージョン 2.3.0 以降、StarRocks は JDBC 互換データベースをクエリするための外部テーブルを提供しています。これにより、データを StarRocks にインポートすることなく、これらのデータベースのデータを高速に分析できます。このセクションでは、StarRocks で外部テーブルを作成し、JDBC 互換データベースのデータをクエリする方法について説明します。

### 前提条件

JDBC 外部テーブルを使用してデータをクエリする前に、FEs と BEs が JDBC ドライバのダウンロード URL にアクセスできることを確認してください。ダウンロード URL は、JDBC リソースを作成するためのステートメントで `driver_url` パラメータによって指定されます。

### JDBC リソースの作成と管理

#### JDBC リソースの作成

データベースからデータをクエリするための外部テーブルを作成する前に、StarRocks で JDBC リソースを作成してデータベースの接続情報を管理する必要があります。データベースは JDBC ドライバをサポートしている必要があり、「ターゲットデータベース」と呼ばれます。リソースを作成した後、それを使用して外部テーブルを作成できます。

次のステートメントを実行して、`jdbc0` という名前の JDBC リソースを作成します:

~~~SQL
create external resource jdbc0
properties (
    "type"="jdbc",
    "user"="postgres",
    "password"="changeme",
    "jdbc_uri"="jdbc:postgresql://127.0.0.1:5432/jdbc_test",
    "driver_url"="https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar",
    "driver_class"="org.postgresql.Driver"
);
~~~

`properties` で必要なパラメータは次のとおりです:

* `type`: リソースのタイプ。値を `jdbc` に設定します。

* `user`: ターゲットデータベースに接続するために使用されるユーザー名。

* `password`: ターゲットデータベースに接続するために使用されるパスワード。

* `jdbc_uri`: JDBC ドライバがターゲットデータベースに接続するために使用する URI。URI の形式はデータベース URI 構文に従う必要があります。一般的なデータベースの URI 構文については、[Oracle](https://docs.oracle.com/en/database/oracle/oracle-database/21/jjdbc/data-sources-and-URLs.html#GUID-6D8EFA50-AB0F-4A2B-88A0-45B4A67C361E)、[PostgreSQL](https://jdbc.postgresql.org/documentation/head/connect.html)、[SQL Server](https://docs.microsoft.com/en-us/sql/connect/jdbc/building-the-connection-url?view=sql-server-ver16) の公式ウェブサイトを参照してください。

> 注意: URI にはターゲットデータベースの名前を含める必要があります。たとえば、前述のコード例では、`jdbc_test` は接続したいターゲットデータベースの名前です。

* `driver_url`: JDBC ドライバ JAR パッケージのダウンロード URL。HTTP URL またはファイル URL がサポートされています。たとえば、`https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar` または `file:///home/disk1/postgresql-42.3.3.jar` です。

* `driver_class`: JDBC ドライバのクラス名。一般的なデータベースの JDBC ドライバクラス名は次のとおりです:
  * MySQL: com.mysql.jdbc.Driver (MySQL 5.x およびそれ以前)、com.mysql.cj.jdbc.Driver (MySQL 6.x およびそれ以降)
  * SQL Server: com.microsoft.sqlserver.jdbc.SQLServerDriver
  * Oracle: oracle.jdbc.driver.OracleDriver
  * PostgreSQL: org.postgresql.Driver

リソースが作成されると、FE は `driver_url` パラメータで指定された URL を使用して JDBC ドライバ JAR パッケージをダウンロードし、チェックサムを生成し、BEs によってダウンロードされた JDBC ドライバを検証するためにチェックサムを使用します。

> 注意: JDBC ドライバ JAR パッケージのダウンロードに失敗した場合、リソースの作成も失敗します。

BEs が JDBC 外部テーブルを初めてクエリし、対応する JDBC ドライバ JAR パッケージがマシンに存在しない場合、BEs は `driver_url` パラメータで指定された URL を使用して JDBC ドライバ JAR パッケージをダウンロードし、すべての JDBC ドライバ JAR パッケージは `${STARROCKS_HOME}/lib/jdbc_drivers` ディレクトリに保存されます。

#### JDBC リソースの表示

次のステートメントを実行して、StarRocks 内のすべての JDBC リソースを表示します:

~~~SQL
SHOW RESOURCES;
~~~

> 注意: `ResourceType` 列は `jdbc` です。

#### JDBC リソースの削除

次のステートメントを実行して、`jdbc0` という名前の JDBC リソースを削除します:

~~~SQL
DROP RESOURCE "jdbc0";
~~~

> 注意: JDBC リソースが削除されると、その JDBC リソースを使用して作成されたすべての JDBC 外部テーブルは使用できなくなります。ただし、ターゲットデータベース内のデータは失われません。StarRocks を使用してターゲットデータベース内のデータをクエリする必要がある場合は、再度 JDBC リソースと JDBC 外部テーブルを作成できます。

### データベースの作成

次のステートメントを実行して、StarRocks 内に `jdbc_test` という名前のデータベースを作成してアクセスします:

~~~SQL
CREATE DATABASE jdbc_test; 
USE jdbc_test; 
~~~

> 注意: 前述のステートメントで指定するデータベース名は、ターゲットデータベースの名前と同じである必要はありません。

### JDBC 外部テーブルの作成

次のステートメントを実行して、データベース `jdbc_test` に `jdbc_tbl` という名前の JDBC 外部テーブルを作成します:

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

`properties` で必要なパラメータは次のとおりです:

* `resource`: 外部テーブルを作成するために使用される JDBC リソースの名前。

* `table`: データベース内のターゲットテーブル名。

サポートされているデータ型と StarRocks とターゲットデータベース間のデータ型マッピングについては、[データ型マッピング](External_table.md#Data type mapping) を参照してください。

> 注意:
>
> * インデックスはサポートされていません。
> * データ分布ルールを指定するために PARTITION BY または DISTRIBUTED BY を使用することはできません。

### JDBC 外部テーブルのクエリ

JDBC 外部テーブルをクエリする前に、次のステートメントを実行してパイプラインエンジンを有効にする必要があります:

~~~SQL
set enable_pipeline_engine=true;
~~~

> 注意: パイプラインエンジンがすでに有効になっている場合、このステップをスキップできます。

次のステートメントを実行して、JDBC 外部テーブルを使用してターゲットデータベース内のデータをクエリします。

~~~SQL
select * from JDBC_tbl;
~~~

StarRocks は、フィルタ条件をターゲットテーブルにプッシュダウンすることによってプレディケートプッシュダウンをサポートしています。データソースにできるだけ近い場所でフィルタ条件を実行することで、クエリパフォーマンスが向上します。現在、StarRocks はバイナリ比較演算子（`>`, `>=`, `=`, `<`, `<=`）、`IN`, `IS NULL`, `BETWEEN ... AND ...` を含むオペレーターをプッシュダウンできます。ただし、StarRocks は関数をプッシュダウンすることはできません。

### データ型マッピング

現在、StarRocks はターゲットデータベース内の基本型（NUMBER、STRING、TIME、DATE など）のデータのみをクエリできます。ターゲットデータベース内のデータ値の範囲が StarRocks によってサポートされていない場合、クエリはエラーを報告します。

ターゲットデータベースと StarRocks の間のマッピングは、ターゲットデータベースのタイプに基づいて異なります。

#### **MySQL と StarRocks**

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

#### **Oracle と StarRocks**

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

#### **PostgreSQL と StarRocks**

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

#### **SQL Server と StarRocks**

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

* JDBC 外部テーブルを作成する際、テーブルにインデックスを作成したり、テーブルのデータ分布ルールを指定するために PARTITION BY や DISTRIBUTED BY を使用することはできません。

* JDBC 外部テーブルをクエリする際、StarRocks は関数をテーブルにプッシュダウンすることはできません。

## Hive 外部テーブル

Hive 外部テーブルを使用する前に、サーバーに JDK 1.8 がインストールされていることを確認してください。

### Hive リソースの作成

Hive リソースは Hive クラスターに対応します。StarRocks が使用する Hive クラスターを構成する必要があります。たとえば、Hive メタストアのアドレスを指定する必要があります。Hive 外部テーブルで使用する Hive リソースを指定する必要があります。

* `hive0` という名前の Hive リソースを作成します。

~~~sql
CREATE EXTERNAL RESOURCE "hive0"
PROPERTIES (
  "type" = "hive",
  "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
);
~~~

* StarRocks で作成されたリソースを表示します。

~~~sql
SHOW RESOURCES;
~~~

* `hive0` という名前のリソースを削除します。

~~~sql
DROP RESOURCE "hive0";
~~~

StarRocks 2.3 以降のバージョンでは、StarRocks の Hive リソースで `hive.metastore.uris` を変更できます。詳細については、[ALTER RESOURCE](../sql-reference/sql-statements/data-definition/ALTER_RESOURCE.md) を参照してください。

### データベースの作成

~~~sql
CREATE DATABASE hive_test;
USE hive_test;
~~~

### Hive 外部テーブルの作成

構文

~~~sql
CREATE EXTERNAL TABLE table_name (
  col_name col_type [NULL | NOT NULL] [COMMENT "comment"]
) ENGINE=HIVE
PROPERTIES (
  "key" = "value"
);
~~~

例: `hive0` リソースに対応する Hive クラスターの `rawdata` データベースに `profile_parquet_p7` という外部テーブルを作成します。

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

説明:

* 外部テーブルの列
  * 列名は Hive テーブルの列名と同じでなければなりません。
  * 列の順序は Hive テーブルの列の順序と同じである必要はありません。
  * Hive テーブルの一部の列のみを選択できますが、すべての **パーティションキー列** を選択する必要があります。
  * 外部テーブルのパーティションキー列は `partition by` を使用して指定する必要はありません。他の列と同じ説明リストで定義する必要があります。パーティション情報を指定する必要はありません。StarRocks は Hive テーブルからこの情報を自動的に同期します。
  * `ENGINE` を HIVE に設定します。
* PROPERTIES:
  * **hive.resource**: 使用する Hive リソース。
  * **database**: Hive データベース。
  * **table**: Hive のテーブル。**ビュー**はサポートされていません。
* 次の表は、Hive と StarRocks の間の列データ型のマッピングを示しています。

    |  Hive の列タイプ   |  StarRocks の列タイプ   | 説明 |
    | --- | --- | ---|
    |   INT/INTEGER  | INT    |
    |   BIGINT  | BIGINT    |
    |   TIMESTAMP  | DATETIME    | TIMESTAMP データを DATETIME データに変換する際に精度とタイムゾーン情報が失われます。セッション変数のタイムゾーンに基づいて、タイムゾーンオフセットのない DATETIME データに TIMESTAMP データを変換する必要があります。 |
    |  STRING  | VARCHAR   |
    |  VARCHAR  | VARCHAR   |
    |  CHAR  | CHAR   |
    |  DOUBLE | DOUBLE |
    | FLOAT | FLOAT|
    | DECIMAL | DECIMAL|
    | ARRAY | ARRAY |

> 注意:
>
> * 現在、サポートされている Hive ストレージ形式は Parquet、ORC、CSV です。
CSV の場合、引用符をエスケープ文字として使用することはできません。
> * SNAPPY および LZ4 圧縮形式がサポートされています。
> * クエリ可能な Hive 文字列列の最大長は 1 MB です。文字列列が 1 MB を超える場合、それは null 列として処理されます。

### Hive 外部テーブルの使用

`profile_wos_p7` の行数の合計をクエリします。

~~~sql
select count(*) from profile_wos_p7;
~~~

### キャッシュされた Hive テーブルメタデータの更新

* Hive のパーティション情報と関連するファイル情報は StarRocks にキャッシュされます。キャッシュは `hive_meta_cache_refresh_interval_s` で指定された間隔で更新されます。デフォルト値は 7200 です。`hive_meta_cache_ttl_s` はキャッシュのタイムアウト期間を指定し、デフォルト値は 86400 です。
  * キャッシュされたデータは手動で更新することもできます。
    1. Hive のテーブルにパーティションが追加または削除された場合、StarRocks にキャッシュされたテーブルメタデータを更新するために `REFRESH EXTERNAL TABLE hive_t` コマンドを実行する必要があります。`hive_t` は StarRocks の Hive 外部テーブルの名前です。
    2. 一部の Hive パーティションのデータが更新された場合、StarRocks にキャッシュされたデータを更新するために `REFRESH EXTERNAL TABLE hive_t PARTITION ('k1=01/k2=02', 'k1=03/k2=04')` コマンドを実行する必要があります。`hive_t` は StarRocks の Hive 外部テーブルの名前です。`'k1=01/k2=02'` と `'k1=03/k2=04'` はデータが更新された Hive パーティションの名前です。
    3. `REFRESH EXTERNAL TABLE hive_t` を実行すると、StarRocks は最初に Hive 外部テーブルの列情報が Hive メタストアから返された Hive テーブルの列情報と同じであるかどうかを確認します。Hive テーブルのスキーマが変更された場合（列の追加や削除など）、StarRocks は変更を Hive 外部テーブルに同期します。同期後、Hive 外部テーブルの列順序は Hive テーブルの列順序と同じままであり、パーティション列が最後の列になります。
* Hive データが Parquet、ORC、CSV 形式で保存されている場合、StarRocks 2.3 以降のバージョンでは Hive テーブルのスキーマ変更（ADD COLUMN や REPLACE COLUMN など）を Hive 外部テーブルに同期できます。

### オブジェクトストレージへのアクセス

* FE 設定ファイルのパスは `fe/conf` であり、Hadoop クラスターをカスタマイズする必要がある場合は設定ファイルを追加できます。たとえば、HDFS クラスターが高可用性の名前サービスを使用している場合、`hdfs-site.xml` を `fe/conf` に配置する必要があります。HDFS が ViewFs で構成されている場合、`core-site.xml` を `fe/conf` に配置する必要があります。
* BE 設定ファイルのパスは `be/conf` であり、Hadoop クラスターをカスタマイズする必要がある場合は設定ファイルを追加できます。たとえば、高可用性の名前サービスを使用する HDFS クラスターの場合、`hdfs-site.xml` を `be/conf` に配置する必要があります。HDFS が ViewFs で構成されている場合、`core-site.xml` を `be/conf` に配置する必要があります。
* BE が配置されているマシンで、BE **起動スクリプト** `bin/start_be.sh` において、JDK 環境として JAVA_HOME を構成し、JRE 環境ではなく、たとえば `export JAVA_HOME = <JDK path>` とします。この構成をスクリプトの先頭に追加し、BE を再起動して構成を有効にする必要があります。
* Kerberos サポートの構成:
  1. すべての FE/BE マシンで `kinit -kt keytab_path principal` を使用してログインし、Hive と HDFS にアクセスする必要があります。kinit コマンドのログインは一定期間のみ有効であり、定期的に実行されるように crontab に追加する必要があります。
  2. `hive-site.xml/core-site.xml/hdfs-site.xml` を `fe/conf` に配置し、`core-site.xml/hdfs-site.xml` を `be/conf` に配置します。
  3. **$FE_HOME/conf/fe.conf** ファイルの `JAVA_OPTS` オプションの値に `-Djava.security.krb5.conf=/etc/krb5.conf` を追加します。**/etc/krb5.conf** は **krb5.conf** ファイルの保存パスです。オペレーティングシステムに基づいてパスを変更できます。
  4. **$BE_HOME/conf/be.conf** ファイルに直接 `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` を追加します。**/etc/krb5.conf** は **krb5.conf** ファイルの保存パスです。オペレーティングシステムに基づいてパスを変更できます。
  5. Hive リソースを追加する際、`hive.metastore.uris` にドメイン名を渡す必要があります。さらに、**/etc/hosts** ファイルに Hive/HDFS ドメイン名と IP アドレスのマッピングを追加する必要があります。

* AWS S3 のサポートを構成する: `fe/conf/core-site.xml` と `be/conf/core-site.xml` に次の構成を追加します。

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

   1. `fs.s3a.access.key`: AWS アクセスキー ID。
   2. `fs.s3a.secret.key`: AWS シークレットキー。
   3. `fs.s3a.endpoint`: 接続する AWS S3 エンドポイント。
   4. `fs.s3a.connection.maximum`: StarRocks から S3 への同時接続の最大数。クエリ中に `Timeout waiting for connection from poll` エラーが発生した場合、このパラメータを大きな値に設定できます。

## (非推奨) Iceberg 外部テーブル

バージョン 2.1.0 以降、StarRocks は外部テーブルを使用して Apache Iceberg からデータをクエリすることを可能にします。Iceberg のデータをクエリするには、StarRocks に Iceberg 外部テーブルを作成する必要があります。テーブルを作成する際に、クエリしたい Iceberg テーブルと外部テーブルの間のマッピングを確立する必要があります。

### 開始前の準備

StarRocks が Apache Iceberg で使用されるメタデータサービス（Hive メタストアなど）、ファイルシステム（HDFS など）、オブジェクトストレージシステム（Amazon S3 や Alibaba Cloud Object Storage Service など）にアクセスする権限を持っていることを確認してください。

### 注意事項

* Iceberg 外部テーブルは次のタイプのデータのみをクエリするために使用できます:
  * バージョン 1（分析データテーブル）テーブル。バージョン 2（行レベル削除）テーブルはサポートされていません。バージョン 1 テーブルとバージョン 2 テーブルの違いについては、[Iceberg Table Spec](https://iceberg.apache.org/spec/) を参照してください。
  * gzip（デフォルト形式）、Zstd、LZ4、または Snappy 形式で圧縮されたテーブル。
  * Parquet または ORC 形式で保存されたファイル。

* StarRocks 2.3 以降のバージョンの Iceberg 外部テーブルは、Iceberg テーブルのスキーマ変更を同期することをサポートしていますが、StarRocks 2.3 より前のバージョンの Iceberg 外部テーブルはサポートしていません。Iceberg テーブルのスキーマが変更された場合、対応する外部テーブルを削除し、新しいものを作成する必要があります。

### 手順

#### ステップ 1: Iceberg リソースの作成

Iceberg 外部テーブルを作成する前に、StarRocks に Iceberg リソースを作成する必要があります。このリソースは Iceberg のアクセス情報を管理するために使用されます。さらに、このリソースを外部テーブルを作成するためのステートメントで指定する必要があります。ビジネス要件に基づいてリソースを作成できます:

* Iceberg テーブルのメタデータが Hive メタストアから取得される場合、リソースを作成し、カタログタイプを `HIVE` に設定できます。

* Iceberg テーブルのメタデータが他のサービスから取得される場合、カスタムカタログを作成する必要があります。その後、リソースを作成し、カタログタイプを `CUSTOM` に設定します。

##### カタログタイプが `HIVE` のリソースを作成する

たとえば、`iceberg0` という名前のリソースを作成し、カタログタイプを `HIVE` に設定します。

~~~SQL
CREATE EXTERNAL RESOURCE "iceberg0" 
PROPERTIES (
   "type" = "iceberg",
   "iceberg.catalog.type" = "HIVE",
   "iceberg.catalog.hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083" 
);
~~~

次の表は、関連するパラメータを説明しています。

| **Parameter**                       | **Description**                                              |
| ----------------------------------- | ------------------------------------------------------------ |
| type                                | リソースのタイプ。値を `iceberg` に設定します。               |
| iceberg.catalog.type              | リソースのカタログタイプ。Hive カタログとカスタムカタログの両方がサポートされています。Hive カタログを指定する場合、値を `HIVE` に設定します。カスタムカタログを指定する場合、値を `CUSTOM` に設定します。 |
| iceberg.catalog.hive.metastore.uris | Hive メタストアの URI。パラメータ値は次の形式です: `thrift://< Iceberg メタデータの IP アドレス >:< ポート番号 >`。ポート番号はデフォルトで 9083 です。Apache Iceberg は Hive カタログを使用して Hive メタストアにアクセスし、Iceberg テーブルのメタデータをクエリします。 |

##### カタログタイプが `CUSTOM` のリソースを作成する

カスタムカタログは抽象クラス BaseMetastoreCatalog を継承する必要があり、IcebergCatalog インターフェースを実装する必要があります。さらに、カスタムカタログのクラス名は StarRocks に既に存在するクラスの名前と重複してはなりません。カタログが作成された後、カタログとその関連ファイルをパッケージ化し、各フロントエンド（FE）の **fe/lib** パスに配置します。その後、各 FE を再起動します。前述の操作を完了した後、カタログがカスタムカタログであるリソースを作成できます。

たとえば、`iceberg1` という名前のリソースを作成し、カタログタイプを `CUSTOM` に設定します。

~~~SQL
CREATE EXTERNAL RESOURCE "iceberg1" 
PROPERTIES (
   "type" = "iceberg",
   "iceberg.catalog.type" = "CUSTOM",
   "iceberg.catalog-impl" = "com.starrocks.IcebergCustomCatalog" 
);
~~~

次の表は、関連するパラメータを説明しています。

| **Parameter**          | **Description**                                              |
| ---------------------- | ------------------------------------------------------------ |
| type                   | リソースのタイプ。値を `iceberg` に設定します。               |
| iceberg.catalog.type | リソースのカタログタイプ。Hive カタログとカスタムカタログの両方がサポートされています。Hive カタログを指定する場合、値を `HIVE` に設定します。カスタムカタログを指定する場合、値を `CUSTOM` に設定します。 |
| iceberg.catalog-impl   | カスタムカタログの完全修飾クラス名。FEs はこの名前に基づいてカタログを検索します。カタログにカスタム構成項目が含まれている場合、Iceberg 外部テーブルを作成する際に `PROPERTIES` パラメータにキーと値のペアとして追加する必要があります。 |

StarRocks 2.3 以降のバージョンでは、Iceberg リソースの `hive.metastore.uris` と `iceberg.catalog-impl` を変更できます。詳細については、[ALTER RESOURCE](../sql-reference/sql-statements/data-definition/ALTER_RESOURCE.md) を参照してください。

##### Iceberg リソースの表示

~~~SQL
SHOW RESOURCES;
~~~

##### Iceberg リソースの削除

たとえば、`iceberg0` という名前のリソースを削除します。

~~~SQL
DROP RESOURCE "iceberg0";
~~~

Iceberg リソースを削除すると、このリソースを参照するすべての外部テーブルが使用できなくなります。ただし、Apache Iceberg の対応するデータは削除されません。Apache Iceberg のデータを引き続きクエリする必要がある場合は、新しいリソースと新しい外部テーブルを作成してください。

#### ステップ 2: (オプション) データベースの作成

たとえば、StarRocks に `iceberg_test` という名前のデータベースを作成します。

~~~SQL
CREATE DATABASE iceberg_test; 
USE iceberg_test; 
~~~

> 注意: StarRocks のデータベース名は、Apache Iceberg のデータベース名と異なる場合があります。

#### ステップ 3: Iceberg 外部テーブルの作成

たとえば、`iceberg_test` データベースに `iceberg_tbl` という名前の Iceberg 外部テーブルを作成します。

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

次の表は、関連するパラメータを説明しています。

| **Parameter** | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| ENGINE        | エンジン名。値を `ICEBERG` に設定します。                 |
| resource      | 外部テーブルが参照する Iceberg リソースの名前。 |
| database      | Iceberg テーブルが属するデータベースの名前。 |
| table         | Iceberg テーブルの名前。                               |

> 注意:
   >
   > * 外部テーブルの名前は、Iceberg テーブルの名前と異なる場合があります。
   >
   > * 外部テーブルの列名は Iceberg テーブルの列名と同じでなければなりません。2 つのテーブルの列順序は異なる場合があります。

カスタムカタログで構成項目を定義し、データをクエリする際に構成項目を有効にしたい場合、外部テーブルを作成する際に `PROPERTIES` パラメータにキーと値のペアとして構成項目を追加できます。たとえば、カスタムカタログで `custom-catalog.properties` という構成項目を定義した場合、次のコマンドを実行して外部テーブルを作成できます。

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

外部テーブルを作成する際には、Iceberg テーブル内の列データ型に基づいて外部テーブル内の列データ型を指定する必要があります。次の表は、列データ型のマッピングを示しています。

| **Iceberg テーブル** | **Iceberg 外部テーブル** |
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

StarRocks は、TIMESTAMPTZ、STRUCT、および MAP のデータ型を持つ Iceberg データのクエリをサポートしていません。

#### ステップ 4: Apache Iceberg のデータをクエリする

外部テーブルが作成された後、外部テーブルを使用して Apache Iceberg のデータをクエリできます。

~~~SQL
select count(*) from iceberg_tbl;
~~~

## Hudi 外部テーブル

バージョン 2.2.0 以降、StarRocks は Hudi 外部テーブルを使用して Hudi データレイクからデータをクエリすることを可能にします。これにより、高速なデータレイク分析が可能になります。このトピックでは、StarRocks クラスターで Hudi 外部テーブルを作成し、Hudi データレイクからデータをクエリする方法について説明します。

### 開始前の準備

StarRocks クラスターが Hive メタストア、HDFS クラスター、または Hudi テーブルを登録できるバケットにアクセスする権限を持っていることを確認してください。

### 注意事項

* Hudi 外部テーブルは読み取り専用であり、クエリのみに使用できます。
* StarRocks は、Copy on Write テーブルと Merge On Read テーブル（MOR テーブルは v2.5 からサポート）をクエリすることをサポートしています。これら 2 つのタイプのテーブルの違いについては、[Table & Query Types](https://hudi.apache.org/docs/table_types/) を参照してください。
* StarRocks は、Hudi の次の 2 つのクエリタイプをサポートしています: スナップショットクエリと読み取り最適化クエリ（Hudi は Merge On Read テーブルでのみ読み取り最適化クエリを実行することをサポートしています）。インクリメンタルクエリはサポートされていません。Hudi のクエリタイプの詳細については、[Table & Query Types](https://hudi.apache.org/docs/next/table_types/#query-types) を参照してください。
* StarRocks は、Hudi ファイルの次の圧縮形式をサポートしています: gzip、zstd、LZ4、および Snappy。Hudi ファイルのデフォルトの圧縮形式は gzip です。
* StarRocks は Hudi 管理テーブルからのスキーマ変更を同期することはできません。詳細については、[Schema Evolution](https://hudi.apache.org/docs/schema_evolution/) を参照してください。Hudi 管理テーブルのスキーマが変更された場合、StarRocks クラスターから関連する Hudi 外部テーブルを削除し、その外部テーブルを再作成する必要があります。

### 手順

#### ステップ 1: Hudi リソースの作成と管理

StarRocks クラスターに Hudi リソースを作成する必要があります。Hudi リソースは、StarRocks クラスターで作成する Hudi データベースと外部テーブルを管理するために使用されます。

##### Hudi リソースの作成

次のステートメントを実行して、`hudi0` という名前の Hudi リソースを作成します:

~~~SQL
CREATE EXTERNAL RESOURCE "hudi0" 
PROPERTIES ( 
    "type" = "hudi", 
    "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
);
~~~

次の表は、パラメータを説明しています。

| Parameter           | Description                                                  |
| ------------------- | ------------------------------------------------------------ |
| type                | Hudi リソースのタイプ。値を `hudi` に設定します。         |
| hive.metastore.uris | Hudi リソースが接続する Hive メタストアの Thrift URI。Hudi リソースを Hive メタストアに接続した後、Hive を使用して Hudi テーブルを作成および管理できます。Thrift URI は `<Hive メタストアの IP アドレス>:<Hive メタストアのポート番号>` 形式です。デフォルトのポート番号は 9083 です。 |

バージョン 2.3 以降、StarRocks は Hudi リソースの `hive.metastore.uris` 値を変更することを許可しています。詳細については、[ALTER RESOURCE](../sql-reference/sql-statements/data-definition/ALTER_RESOURCE.md) を参照してください。

##### Hudi リソースの表示

次のステートメントを実行して、StarRocks クラスターに作成されたすべての Hudi リソースを表示します:

~~~SQL
SHOW RESOURCES;
~~~

##### Hudi リソースの削除

次のステートメントを実行して、`hudi0` という名前の Hudi リソースを削除します:

~~~SQL
DROP RESOURCE "hudi0";
~~~

> 注意:
>
> Hudi リソースを削除すると、その Hudi リソースを使用して作成されたすべての Hudi 外部テーブルが使用できなくなります。ただし、Hudi に保存されたデータには影響しません。StarRocks を使用して Hudi からデータをクエリする場合は、StarRocks クラスターで Hudi リソース、Hudi データベース、および Hudi 外部テーブルを再作成する必要があります。

#### ステップ 2: Hudi データベースの作成

次のステートメントを実行して、StarRocks クラスターに `hudi_test` という名前の Hudi データベースを作成して開きます:

~~~SQL
CREATE DATABASE hudi_test; 
USE hudi_test; 
~~~

> 注意:
>
> StarRocks クラスターで指定する Hudi データベースの名前は、関連する Hudi データベースと同じである必要はありません。

#### ステップ 3: Hudi 外部テーブルの作成

次のステートメントを実行して、`hudi_test` Hudi データベースに `hudi_tbl` という名前の Hudi 外部テーブルを作成します:

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

次の表は、パラメータを説明しています。

| Parameter | Description                                                  |
| --------- | ------------------------------------------------------------ |
| ENGINE    | Hudi 外部テーブルのクエリエンジン。値を `HUDI` に設定します。 |
| resource  | StarRocks クラスター内の Hudi リソースの名前。     |
| database  | StarRocks クラスター内の Hudi 外部テーブルが属する Hudi データベースの名前。 |
| table     | Hudi 外部テーブルが関連付けられている Hudi 管理テーブル。 |

> 注意:
>
> * Hudi 外部テーブルに指定する名前は、関連する Hudi 管理テーブルと同じである必要はありません。
>
> * Hudi 外部テーブルの列は、関連する Hudi 管理テーブルの列と同じ名前を持つ必要がありますが、順序は異なる場合があります。
>
> * 関連する Hudi 管理テーブルから一部またはすべての列を選択し、Hudi 外部テーブルに選択した列のみを作成できます。次の表は、Hudi がサポートするデータ型と StarRocks がサポートするデータ型のマッピングを示しています。

| Hudi がサポートするデータ型   | StarRocks がサポートするデータ型 |
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

> **注意**
>
> StarRocks は、STRUCT または MAP 型のデータのクエリをサポートしていません。また、Merge On Read テーブルの ARRAY 型データのクエリもサポートしていません。

#### ステップ 4: Hudi 外部テーブルからデータをクエリする

特定の Hudi 管理テーブルに関連付けられた Hudi 外部テーブルを作成した後、Hudi 外部テーブルにデータをロードする必要はありません。Hudi からデータをクエリするには、次のステートメントを実行します:

~~~SQL
SELECT COUNT(*) FROM hudi_tbl;
~~~