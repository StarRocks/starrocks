---
displayed_sidebar: docs
---

# Rill

Rill は、外部テーブルを使用して Rill ダッシュボードを強化するための OLAP コネクタとして、StarRocks への接続をサポートしています。Rill は、StarRocks の内部データと外部データの両方をクエリおよび可視化できます。

## Connection

Rill は MySQL プロトコルを使用して StarRocks に接続します。接続は、接続パラメータまたは DSN 接続文字列を使用して構成できます。

### 接続パラメータ

Rill でデータソースを追加する際に、[**StarRocks**] を選択し、以下のパラメータを設定します。

- **Host**: StarRocks クラスタの FE ホスト IP アドレスまたはホスト名
- **Port**: StarRocks FE の MySQL プロトコルポート (デフォルト: `9030`)
- **Username**: 認証用のユーザー名 (デフォルト: `root`)
- **Password**: 認証用のパスワード
- **Catalog**: StarRocks の catalog 名 (デフォルト: `default_catalog`)。 内部 catalog と external catalog (例: Iceberg, Hive) の両方をサポートします。
- **Database**: StarRocks のデータベース名
- **SSL**: SSL/TLS 暗号化を有効にする (デフォルト: `false`)

### Connection String (DSN)

あるいは、MySQL 形式の DSN 接続文字列を使用できます。

```
user:password@tcp(host:9030)/database?parseTime=true
```

外部 catalog の場合、catalog とデータベースを別々の property として指定します。

## external catalog

Rill は、Hive、Iceberg、Delta Lake などの外部データソースを含む、StarRocks の external catalog からのデータクエリをサポートしています。`catalog` プロパティを external catalog 名 (例: `iceberg_catalog`) に、`database` プロパティをその catalog 内のデータベースに設定します。

## その他の情報

詳細な構成オプション、例、トラブルシューティング、および最新の情報については、[Rill Data StarRocks connector のドキュメント](https://docs.rilldata.com/developers/build/connectors/olap/starrocks) を参照してください。
