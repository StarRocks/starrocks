---
displayed_sidebar: docs
---

# HTTP SQL API

## 説明

StarRocks v3.2.0 は、ユーザーが HTTP を使用してさまざまなタイプのクエリを実行できる HTTP SQL API を導入しました。現在、この API は SELECT、SHOW、EXPLAIN、および KILL ステートメントをサポートしています。

curl コマンドを使用した構文:

```shell
curl -X POST 'http://<fe_ip>:<fe_http_port>/api/v1/catalogs/<catalog_name>/databases/<database_name>/sql' \
   -u '<username>:<password>'  -d '{"query": "<sql_query>;", "sessionVariables":{"<var_name>":<var_value>}}' \
   --header "Content-Type: application/json"
```

## リクエストメッセージ

### リクエストライン

```shell
POST 'http://<fe_ip>:<fe_http_port>/api/v1/catalogs/<catalog_name>/databases/<database_name>/sql'
```

| フィールド              | 説明                                                  |
| ------------------------ | :----------------------------------------------------------- |
|  fe_ip                   | FE ノードの IP アドレス。                                                  |
|  fe_http_port            | FE HTTP ポート。                                           |
|  catalog_name            | catalog 名。v3.2.0 では、この API を使用して StarRocks 内部テーブルのみをクエリできます。つまり、`<catalog_name>` は `default_catalog` にのみ設定できます。v3.2.1 以降、この API を使用して [external catalogs](../data_source/catalog/catalog_overview.md) のテーブルをクエリできます。 |
|  database_name           | データベース名。リクエストラインでデータベース名が指定されておらず、SQL クエリでテーブル名が使用されている場合、テーブル名の前にそのデータベース名を付ける必要があります。例: `database_name.table_name`。 |

- 指定された catalog 内のデータベースを跨いでデータをクエリします。SQL クエリでテーブルが使用されている場合、テーブル名の前にそのデータベース名を付ける必要があります。

   ```shell
   POST /api/v1/catalogs/<catalog_name>/sql
   ```

- 指定された catalog およびデータベースからデータをクエリします。

   ```shell
   POST /api/v1/catalogs/<catalog_name>/databases/<database_name>/sql
   ```

### 認証方法

```shell
Authorization: Basic <credentials>
```

基本認証が使用されます。つまり、`credentials` に対してユーザー名とパスワードを入力します（`-u '<username>:<password>'`）。ユーザー名にパスワードが設定されていない場合、`<username>:` のみを渡し、パスワードを空のままにすることができます。たとえば、root アカウントを使用する場合、`-u 'root:'` と入力できます。

### リクエストボディ

```shell
-d '{"query": "<sql_query>;", "sessionVariables":{"<var_name>":<var_value>}}'
```

| フィールド              | 説明                                                  |
| ------------------------ | :----------------------------------------------------------- |
| query                    | SQL クエリ、STRING 形式。SELECT、SHOW、EXPLAIN、および KILL ステートメントのみがサポートされています。HTTP リクエストに対して実行できる SQL クエリは 1 つだけです。 |
| sessionVariables         | クエリに対して設定したい [セッション変数](./System_variable.md)、JSON 形式。このフィールドはオプションです。デフォルトは空です。設定したセッション変数は同じ接続に対して有効であり、接続が閉じられると無効になります。 |

### リクエストヘッダー

```shell
--header "Content-Type: application/json"
```

このヘッダーは、リクエストボディが JSON 文字列であることを示します。

## レスポンスメッセージ

### ステータスコード

- 200: HTTP リクエストが成功し、データがクライアントに送信される前にサーバーが正常です。
- 4xx: HTTP リクエストエラー、クライアントエラーを示します。
- `500 Internal Server Error`: HTTP リクエストが成功しましたが、データがクライアントに送信される前にサーバーがエラーに遭遇しました。
- 503: HTTP リクエストが成功しましたが、FE がサービスを提供できません。

### レスポンスヘッダー

`content-type` はレスポンスボディの形式を示します。改行区切りの JSON が使用され、レスポンスボディは `\n` で区切られた複数の JSON オブジェクトで構成されます。

|                      | 説明                                                  |
| -------------------- | :----------------------------------------------------------- |
| content-type         | 形式は改行区切りの JSON で、デフォルトは "application/x-ndjson charset=UTF-8" です。 |
| X-StarRocks-Query-Id | クエリ ID。                                                          |

### レスポンスボディ

#### リクエストが送信される前に失敗

リクエストがクライアント側で失敗するか、サーバーがクライアントにデータを返す前にエラーに遭遇します。レスポンスボディは以下の形式で、`msg` はエラー情報です。

```json
{
   "status":"FAILED",
   "msg":"xxx"
}
```

#### リクエストが送信された後に失敗

結果の一部が返され、HTTP ステータスコードは 200 です。データ送信が中断され、接続が閉じられ、エラーが記録されます。

#### 成功

レスポンスメッセージの各行は JSON オブジェクトです。JSON オブジェクトは `\n` で区切られています。

- SELECT ステートメントの場合、以下の JSON オブジェクトが返されます。

| オブジェクト       | 説明                                                  |
| ------------ | :----------------------------------------------------------- |
| `connectionId` | 接続 ID。長時間保留中のクエリをキャンセルするには、KILL `<connectionId>` を呼び出します。 |
| `meta`        | 列を表します。キーは `meta` で、値は JSON 配列であり、配列内の各オブジェクトは列を表します。 |
| `data`         | データ行。キーは `data` で、値はデータの行を含む JSON 配列です。 |
| `statistics`   | クエリの統計情報。                                       |

- SHOW ステートメントの場合、`meta`、`data`、および `statistics` が返されます。
- EXPLAIN ステートメントの場合、クエリの詳細な実行計画を示す `explain` オブジェクトが返されます。

以下の例では、`\n` をセパレータとして使用しています。StarRocks は HTTP チャンクモードを使用してデータを送信します。FE がデータチャンクを取得するたびに、そのデータチャンクをクライアントにストリーミングします。クライアントは行ごとにデータを解析でき、データキャッシュの必要性や全データを待つ必要がなくなり、クライアントのメモリ消費を削減します。

```json
{"connectionId": 7}\n
{"meta": [
    {
      "name": "stock_symbol",
      "type": "varchar"
    },
    {
      "name": "closing_price",
      "type": "decimal64(8, 2)"
    },
    {
      "name": "closing_date",
      "type": "datetime"
    }
  ]}\n
{"data": ["JDR", 12.86, "2014-10-02 00:00:00"]}\n
{"data": ["JDR",14.8, "2014-10-10 00:00:00"]}\n
...
{"statistics": {"scanRows": 0,"scanBytes": 0,"returnRows": 9}}
```

## 例

### SELECT クエリを実行

- StarRocks 内部テーブルからデータをクエリします（`catalog_name` は `default_catalog` です）。

  ```shell
  curl -X POST 'http://127.0.0.1:8030/api/v1/catalogs/default_catalog/databases/test/sql' -u 'root:' -d '{"query": "select * from agg;"}' --header "Content-Type: application/json"
  ```

  結果:

  ```json
  {"connectionId":49}
  {"meta":[{"name":"no","type":"int(11)"},{"name":"k","type":"decimal64(10, 2)"},{"name":"v","type":"decimal64(10, 2)"}]}
  {"data":[1,"10.00",null]}
  {"data":[2,"10.00","11.00"]}
  {"data":[2,"20.00","22.00"]}
  {"data":[2,"25.00",null]}
  {"data":[2,"30.00","35.00"]}
  {"statistics":{"scanRows":0,"scanBytes":0,"returnRows":5}}
  ```

- Iceberg テーブルからデータをクエリします。

  ```shell
  curl -X POST 'http://172.26.93.145:8030/api/v1/catalogs/iceberg_catalog/databases/ywb/sql' -u 'root:' -d '{"query": "select * from iceberg_analyze;"}' --header "Content-Type: application/json"
  ```

  結果:

  ```json
  {"connectionId":13}
  {"meta":[{"name":"k1","type":"int(11)"},{"name":"k2","type":"int(11)"}]}
  {"data":[1,2]}
  {"data":[1,1]}
  {"statistics":{"scanRows":0,"scanBytes":0,"returnRows":2}}
  ```

### クエリをキャンセル

予期せぬ長時間実行されるクエリをキャンセルするには、接続を閉じることができます。StarRocks は接続が閉じられたことを検出すると、このクエリをキャンセルします。

また、KILL `connectionId` を呼び出してこのクエリをキャンセルすることもできます。例:

```shell
curl -X POST 'http://127.0.0.1:8030/api/v1/catalogs/default_catalog/databases/test/sql' -u 'root:' -d '{"query": "kill 17;"}' --header "Content-Type: application/json"
```

`connectionId` はレスポンスボディから取得するか、SHOW PROCESSLIST を呼び出して取得できます。例:

```shell
curl -X POST 'http://127.0.0.1:8030/api/v1/catalogs/default_catalog/databases/test/sql' -u 'root:' -d '{"query": "show processlist;"}' --header "Content-Type: application/json"
```

### セッション変数を設定してクエリを実行

```shell
curl -X POST 'http://127.0.0.1:8030/api/v1/catalogs/default_catalog/databases/test/sql' -u 'root:'  -d '{"query": "SHOW VARIABLES;", "sessionVariables":{"broadcast_row_limit":14000000}}'  --header "Content-Type: application/json"
```