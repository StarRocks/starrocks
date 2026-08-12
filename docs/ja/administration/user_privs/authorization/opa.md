---
displayed_sidebar: docs
sidebar_position: 45
---

# Open Policy Agent で権限を管理する

[Open Policy Agent](https://www.openpolicyagent.org/) (OPA) は、複数のシステムの認可判断を一元化できるポリシーエンジンです。StarRocks は、既存の認証、Group Provider、アナライザー、行アクセス制御、列マスキングの仕組みを引き続き使用しながら、権限チェックを OPA に委譲できます。

StarRocks は OPA に StarRocks ネイティブの JSON ペイロードを送信します。ポリシーのレスポンスは、操作を許可するかどうかを制御し、必要に応じて行フィルターや列マスク用の SQL 式を返すことができます。

## 機能

- System、Catalog、Database、Table、Column、View、Materialized View、Function、Resource、Resource Group、Storage Volume、Pipe、Warehouse などの StarRocks オブジェクトを認可します。
- SQL フィルター式を返すことで行アクセスポリシーを適用します。
- SQL マスキング式を返すことで列マスキングポリシーを適用します。
- 多数の列を参照するクエリで、バッチ列マスキングにより OPA への往復回数を削減します。

OPA 認可は fail-closed です。OPA サービスが `false` を返す、`result` を省略する、無効な JSON を返す、2xx 以外の HTTP ステータスを返す、またはタイムアウトした場合、StarRocks は操作を拒否します。

`GRANT`、`REVOKE`、ユーザー管理、ロール管理などの権限管理操作は、引き続き StarRocks ネイティブの権限チェックを使用します。OPA はオブジェクト認可、行フィルター、列マスクを制御しますが、ネイティブの権限管理モデルを置き換えるものではありません。

## OPA アクセス制御を設定する

各 FE ノードの FE 設定ファイル **fe.conf** に、次の設定項目を追加します。

```properties
access_control = opa
opa_policy_url = http://opa.example.com:8181/v1/data/starrocks/allow

# Optional policy endpoints.
opa_row_filters_url = http://opa.example.com:8181/v1/data/starrocks/row_filters
opa_column_masking_url = http://opa.example.com:8181/v1/data/starrocks/column_mask
opa_batch_column_masking_url = http://opa.example.com:8181/v1/data/starrocks/batch_column_masks
```

`access_control` または任意の `opa_*` 設定項目を変更した後は、すべての FE ノードを再起動してください。

External Catalog では、カタログプロパティ `"catalog.access.control" = "opa"` を設定して OPA を有効にすることもできます。このプロパティが設定されていない場合、カタログはグローバルな `access_control` の値を使用します。

```sql
CREATE EXTERNAL CATALOG hive_catalog
PROPERTIES (
    "type" = "hive",
    "hive.metastore.type" = "hive",
    "hive.metastore.uris" = "thrift://127.0.0.1:9083",
    "catalog.access.control" = "opa"
);
```

## 認可リクエスト

StarRocks は OPA Data API 形式で `opa_policy_url` に POST リクエストを送信します。

```json
{
  "input": {
    "context": {
      "user": "alice",
      "groups": ["finance"],
      "host": "%",
      "queryId": "5e4b8e2d-2c80-4c49-94c2-1d8d7c11f8cc",
      "catalog": "default_catalog",
      "database": "sales"
    },
    "action": {
      "operation": "check",
      "privilege": "SELECT",
      "objectType": "COLUMN",
      "resource": {
        "catalog": "default_catalog",
        "database": "sales",
        "table": "orders",
        "column": "amount"
      }
    }
  }
}
```

リクエストを許可するには `{"result": true}` を返します。それ以外の結果は拒否されます。

## ポリシー例

以下の例では `package starrocks` を使用しているため、`allow` ルールは `/v1/data/starrocks/allow` で利用できます。

### データベースレベルの SELECT

`SELECT` クエリでは、StarRocks は列プルーニング後にスキャン対象の各列を認可します。OPA でデータベースレベルの `SELECT` を表現するには、`COLUMN` リクエストに一致させ、データベース権限を確認するときに `input.action.resource.table` と `input.action.resource.column` を無視します。

クエリ例:

```sql
SELECT amount FROM sales.orders;
```

Rego ポリシー例:

```rego
package starrocks

import rego.v1

default allow := false

allow if {
    input.action.operation == "check"
    input.action.privilege == "SELECT"
    input.action.objectType == "COLUMN"
    input.context.user == "alice"
    input.action.resource.catalog == "default_catalog"
    input.action.resource.database == "sales"
}
```

### データベース可視性チェック

データベース可視性チェックは、データベースが `SHOW DATABASES` に表示されるか、`USE <db>` で選択できるかを制御します。データベースレベルの権限では、`privilege: "ANY"` を持つ `DATABASE` リクエストを処理します。

クエリ例:

```sql
SHOW DATABASES;
USE sales;
```

Rego ポリシー例:

```rego
package starrocks

import rego.v1

default allow := false

allow if {
    input.action.operation == "check"
    input.action.privilege == "ANY"
    input.action.objectType == "DATABASE"
    input.context.user == "alice"
    input.action.resource.catalog == "default_catalog"
    input.action.resource.database == "sales"
}
```

どちらのステートメントも、`privilege: "ANY"` を持つ同じ `DATABASE` チェックを生成します。

## 行フィルター

`opa_row_filters_url` が設定されている場合、StarRocks はクエリの書き換え前に OPA に行アクセス式を問い合わせます。行フィルターは `result` 配列で返します。

```json
{
  "result": [
    {"expression": "region = 'EMEA'"},
    {"expression": "tenant_id = 100"}
  ]
}
```

StarRocks は、返された複数の式を `AND` で結合します。式は有効な StarRocks SQL 式である必要があります。

## 列マスク

`opa_column_masking_url` が設定されている場合、StarRocks は各列の列マスクを OPA に問い合わせます。マスキング式は `result.expression` で返します。

```json
{
  "result": {
    "expression": "NULL"
  }
}
```

`opa_batch_column_masking_url` が設定されている場合、StarRocks は列ごとのリクエストの代わりにこれを使用します。マスキング式は `result` 配列で返します。

```json
{
  "result": [
    {"column": "phone", "expression": "NULL"},
    {"column": "email", "expression": "concat('***@', split_part(email, '@', 2))"}
  ]
}
```

バッチリクエストでは、返される各マスクは `column` 名、または `action.filterResources` 内の 0 から始まる `index` で対象列を識別できます。

式は有効な StarRocks SQL 式である必要があります。無効な行フィルター式またはマスク式は、クエリの失敗を引き起こします。

## FE 設定項目

OPA 関連のすべての FE 設定項目については、[FE 設定 - ユーザー、ロール、権限](../../management/FE_parameters/user_query_loading.md#ユーザーロール権限) を参照してください。
