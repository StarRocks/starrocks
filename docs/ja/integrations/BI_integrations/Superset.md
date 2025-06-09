---
displayed_sidebar: docs
---

# Apache Superset

Apache Superset は、StarRocks 内部データと外部データの両方のクエリと可視化をサポートしています。

## 前提条件

以下のインストールを完了していることを確認してください。

1. Apache Superset サーバーに StarRocks 用の Python クライアントをインストールします。

   ```SQL
   pip install starrocks
   ```

2. 最新バージョンの Apache Superset をインストールします。詳細については、[Installing Superset](https://superset.apache.org/docs/intro) を参照してください。

## 統合

Apache Superset にデータベースを作成します。

![Apache Superset - 1](../../_assets/BI_superset_1.png)

![Apache Superset - 2](../../_assets/BI_superset_2.png)

次の点に注意してください。

- **SUPPORTED DATABASES** には、データソースとして使用する **StarRocks** を選択します。
- **SQLALCHEMY** **URI** には、以下のように StarRocks SQLAlchemy URI 形式で URI を入力します。

  ```SQL
  starrocks://<User>:<Password>@<Host>:<Port>/<Catalog>.<Database>
  ```

  URI のパラメータは次のように説明されています。

  - `User`: StarRocks クラスターにログインするためのユーザー名、例: `admin`。
  - `Password`: StarRocks クラスターにログインするためのパスワード。
  - `Host`: StarRocks クラスターの FE ホスト IP アドレス。
  - `Port`: StarRocks クラスターの FE クエリポート、例: `9030`。
  - `Catalog`: StarRocks クラスター内の対象カタログ。内部カタログと外部カタログの両方がサポートされています。
  - `Database`: StarRocks クラスター内の対象データベース。内部データベースと外部データベースの両方がサポートされています。