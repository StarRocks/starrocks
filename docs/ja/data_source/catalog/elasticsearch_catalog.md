---
displayed_sidebar: docs
toc_max_heading_level: 4
---

# Elasticsearch catalog

StarRocks は v3.1 以降、Elasticsearch catalog をサポートしています。

StarRocks と Elasticsearch はどちらも人気のある分析システムで、それぞれ異なる強みを持っています。StarRocks は大規模な分散コンピューティングに優れ、外部テーブルを通じて Elasticsearch からデータをクエリすることができます。Elasticsearch は全文検索機能で知られています。StarRocks と Elasticsearch の組み合わせにより、より包括的な OLAP ソリューションが提供されます。Elasticsearch catalog を使用すると、データ移行の必要なく、StarRocks 上で SQL ステートメントを使用して Elasticsearch クラスター内のすべてのインデックスデータを直接分析できます。

他のデータソースの catalog とは異なり、Elasticsearch catalog には作成時に `default` という名前のデータベースが 1 つだけ含まれています。各 Elasticsearch インデックスは自動的にデータテーブルにマッピングされ、`default` データベースにマウントされます。

## Elasticsearch catalog の作成

### 構文

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES ("key"="value", ...)
```

### パラメータ

#### `catalog_name`

Elasticsearch catalog の名前です。命名規則は以下の通りです。

- 名前には文字、数字 (0-9)、アンダースコア (_) を含めることができます。文字で始める必要があります。
- 名前は大文字と小文字を区別し、長さは 1023 文字を超えてはなりません。

#### `comment`

Elasticsearch catalog の説明です。このパラメータはオプションです。

#### PROPERTIES

Elasticsearch catalog のプロパティです。以下の表は、Elasticsearch catalog に対してサポートされているプロパティを説明しています。

| パラメータ                   | 必須     | デフォルト値 | 説明                                                  |
| --------------------------- | -------- | ------------- | ------------------------------------------------------------ |
| hosts                       | はい      | なし          | Elasticsearch クラスターの接続アドレスです。1 つ以上のアドレスを指定できます。StarRocks はこのアドレスから Elasticsearch のバージョンとインデックスシャードの割り当てを解析できます。StarRocks は `GET /_nodes/http` API 操作によって返されるアドレスに基づいて Elasticsearch クラスターと通信します。したがって、`hosts` パラメータの値は `GET /_nodes/http` API 操作によって返されるアドレスと同じでなければなりません。そうでない場合、BEs または CNs は Elasticsearch クラスターと通信できない可能性があります。 |
| type                        | はい      | なし          | データソースのタイプです。Elasticsearch catalog を作成する際には、このパラメータを `es` に設定します。 |
| user                        | いいえ   | 空            | HTTP ベーシック認証が有効な Elasticsearch クラスターにログインするために使用されるユーザー名です。`/cluster/state/nodes/http` などのパスにアクセスする権限があり、インデックスを読み取る権限があることを確認してください。 |
| password                    | いいえ   | 空            | Elasticsearch クラスターにログインするために使用されるパスワードです。 |
| es.type                     | いいえ   | _doc          | インデックスのタイプです。Elasticsearch 8 以降のバージョンでデータをクエリする場合、このパラメータを設定する必要はありません。Elasticsearch 8 以降のバージョンではマッピングタイプが削除されています。 |
| es.nodes.wan.only           | いいえ   | FALSE         | StarRocks が Elasticsearch クラスターにアクセスしてデータを取得するために `hosts` で指定されたアドレスのみを使用するかどうかを指定します。<ul><li>`true`: StarRocks は Elasticsearch クラスターにアクセスしてデータを取得するために `hosts` で指定されたアドレスのみを使用し、Elasticsearch インデックスのシャードが存在するデータノードをスニッフしません。StarRocks が Elasticsearch クラスター内のデータノードのアドレスにアクセスできない場合、このパラメータを `true` に設定する必要があります。</li><li>`false`: StarRocks は Elasticsearch クラスターのインデックスのシャードが存在するデータノードをスニッフするために `hosts` で指定されたアドレスを使用します。StarRocks が Elasticsearch クラスター内のデータノードのアドレスにアクセスできる場合、デフォルト値 `false` を保持することをお勧めします。</li></ul> |
| es.net.ssl | いいえ   | FALSE         | Elasticsearch クラスターにアクセスするために HTTPS プロトコルを使用できるかどうかを指定します。StarRocks v2.4 以降のみ、このパラメータの設定をサポートしています。<ul><li>`true`: Elasticsearch クラスターにアクセスするために HTTPS および HTTP プロトコルの両方を使用できます。</li><li>`false`: Elasticsearch クラスターにアクセスするために HTTP プロトコルのみを使用できます。</li></ul> |
| enable_docvalue_scan        | いいえ   | TRUE          | Elasticsearch の列指向（カラムナ）ストレージからターゲットフィールドの値を取得するかどうかを指定します。ほとんどの場合、列指向（カラムナ）ストレージからデータを読み取る方が行指向（ロウ）ストレージから読み取るよりも優れています。 |
| enable_keyword_sniff        | いいえ   | TRUE          | Elasticsearch で KEYWORD タイプのフィールドに基づいて TEXT タイプのフィールドをスニッフするかどうかを指定します。このパラメータが `false` に設定されている場合、StarRocks はトークン化後にマッチングを実行します。 |

### 例

次の例では、`es_test` という名前の Elasticsearch catalog を作成します。

```SQL
CREATE EXTERNAL CATALOG es_test
COMMENT 'test123'
PROPERTIES
(
    "type" = "es",
    "es.type" = "_doc",
    "hosts" = "https://xxx:9200",
    "es.net.ssl" = "true",
    "user" = "admin",
    "password" = "xxx",
    "es.nodes.wan.only" = "true"
);
```

## Predicate pushdown

StarRocks は、Elasticsearch テーブルに対して指定されたクエリの述語を Elasticsearch にプッシュダウンして実行することをサポートしています。これにより、クエリエンジンとストレージソースの間の距離が最小化され、クエリパフォーマンスが向上します。次の表は、Elasticsearch にプッシュダウンできるオペレーターを示しています。

| SQL 構文   | Elasticsearch 構文  |
| ------------ | --------------------- |
| `=`            | term query            |
| `in`           | terms query           |
| `>=, <=, >, <` | range                 |
| `and`          | bool.filter           |
| `or`           | bool.should           |
| `not`          | bool.must_not         |
| `not in`       | bool.must_not + terms |
| `esquery`      | ES Query DSL          |

## クエリ例

`esquery()` 関数を使用して、SQL で表現できないマッチクエリやジオシェイプクエリなどの Elasticsearch クエリを Elasticsearch にプッシュダウンしてフィルタリングと処理を行うことができます。`esquery()` 関数では、最初のパラメータがインデックスと関連付けるための列名を指定し、2 番目のパラメータが Elasticsearch クエリの Elasticsearch Query DSL ベースの JSON 表現で、波括弧 (`{}`) で囲まれています。JSON 表現には、`match`、`geo_shape`、`bool` などのルートキーが 1 つだけ含まれている必要があります。

- マッチクエリ

  ```SQL
  SELECT * FROM es_table WHERE esquery(k4, '{
     "match": {
        "k4": "StarRocks on elasticsearch"
     }
  }');
  ```

- ジオシェイプクエリ

  ```SQL
  SELECT * FROM es_table WHERE esquery(k4, '{
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

- ブールクエリ

  ```SQL
  SELECT * FROM es_table WHERE esquery(k4, ' {
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

## 使用上の注意

- v5.x 以降、Elasticsearch は異なるデータスキャン方法を採用しています。StarRocks は Elasticsearch v5.x 以降からのデータクエリのみをサポートしています。
- StarRocks は HTTP ベーシック認証が有効な Elasticsearch クラスターからのデータクエリのみをサポートしています。
- `count()` を含むクエリなど、一部のクエリは StarRocks 上で Elasticsearch よりもはるかに遅く実行されます。これは、Elasticsearch がクエリ条件を満たす指定されたドキュメント数に関連するメタデータを直接読み取ることができ、要求されたデータをフィルタリングする必要がないためです。