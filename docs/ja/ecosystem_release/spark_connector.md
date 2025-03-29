---
displayed_sidebar: docs
---

# Releases of StarRocks Connector for Spark

## Notifications

**ユーザーガイド:**

- [Spark connector を使用して StarRocks にデータをロードする](https://docs.starrocks.io/docs/loading/Spark-connector-starrocks/)
- [Spark connector を使用して StarRocks からデータを読み取る](https://docs.starrocks.io/docs/unloading/Spark_connector/)

**ソースコード**: [starrocks-connector-for-apache-spark](https://github.com/StarRocks/starrocks-connector-for-apache-spark)

**JAR ファイルの命名形式**: `starrocks-spark-connector-${spark_version}_${scala_version}-${connector_version}.jar`

**JAR ファイルの取得方法:**

- [Maven Central Repository](https://repo1.maven.org/maven2/com/starrocks) から直接 Spark connector JAR ファイルをダウンロードします。
- Maven プロジェクトの `pom.xml` ファイルに Spark connector を依存関係として追加し、ダウンロードします。具体的な手順については、[ユーザーガイド](https://docs.starrocks.io/docs/loading/Spark-connector-starrocks/#obtain-spark-connector) を参照してください。
- ソースコードをコンパイルして Spark connector JAR ファイルを作成します。具体的な手順については、[ユーザーガイド](https://docs.starrocks.io/docs/loading/Spark-connector-starrocks/#obtain-spark-connector) を参照してください。

**バージョン要件:**

| Spark connector | Spark            | StarRocks     | Java | Scala |
| --------------- | ---------------- | ------------- | ---- | ----- |
| 1.1.1           | 3.2, 3.3, or 3.4 | 2.5 and later | 8    | 2.12  |
| 1.1.0           | 3.2, 3.3, or 3.4 | 2.5 and later | 8    | 2.12  |

## リリースノート

### 1.1

### 1.1.2

**機能**

- Spark v3.5 をサポートします。[#89](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/89)
- Spark SQL を使用して StarRocks からデータを読み取る際に `starrocks.filter.query` パラメータをサポートします。[#92](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/92)
- StarRocks から JSON 型のカラムを読み取ることをサポートします。[#100](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/100)

**改善**

- エラーメッセージを最適化しました。Spark connector が StarRocks からデータを読み取る際に、`starrocks.columns` パラメータで指定されたカラムが StarRocks テーブルに存在しない場合、返されるエラーメッセージに存在しないカラム名が明示的に表示されます。[#97](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/97)
- Spark connector が HTTP 経由で StarRocks FE からクエリプランを要求する際に例外が発生した場合、FE は例外情報を HTTP ステータスとエンティティを通じて Spark connector に返します。[#98](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/98)

#### 1.1.1

このリリースには、主に StarRocks へのデータロードに関するいくつかの機能と改善が含まれています。

> **注意**
>
> Spark connector をこのバージョンにアップグレードする際のいくつかの変更に注意してください。詳細については、[Spark connector のアップグレード](https://docs.starrocks.io/docs/loading/Spark-connector-starrocks/#upgrade-from-version-110-to-111) を参照してください。

**機能**

- シンクがリトライをサポートします。[#61](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/61)
- BITMAP および HLL カラムへのデータロードをサポートします。[#67](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/67)
- ARRAY 型データのロードをサポートします。[#74](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/74)
- バッファされた行数に基づいてフラッシュすることをサポートします。[#78](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/78)

**改善**

- 不要な依存関係を削除し、Spark connector JAR ファイルを軽量化しました。[#55](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/55) [#57](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/57)
- fastjson を jackson に置き換えました。[#58](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/58)
- 欠落していた Apache ライセンスヘッダーを追加しました。[#60](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/60)
- MySQL JDBC ドライバーを Spark connector JAR ファイルにパッケージしないようにしました。[#63](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/63)
- タイムゾーンパラメータを設定し、Spark Java8 API の日時と互換性を持たせることをサポートします。[#64](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/64)
- CPU コストを削減するために行文字列コンバーターを最適化しました。[#68](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/68)
- `starrocks.fe.http.url` パラメータが http スキームを追加することをサポートします。[#71](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/71)
- インターフェース BatchWrite#useCommitCoordinator が DataBricks 13.1 で動作するように実装されました。[#79](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/79)
- エラーログに権限とパラメータを確認するヒントを追加しました。[#81](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/81)

**バグ修正**

- CSV 関連パラメータ `column_separator` および `row_delimiter` のエスケープ文字を解析します。[#85](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/85)

**ドキュメント**

- ドキュメントをリファクタリングしました。[#66](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/66)
- BITMAP および HLL カラムへのデータロードの例を追加しました。[#70](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/70)
- Python で書かれた Spark アプリケーションの例を追加しました。[#72](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/72)
- ARRAY 型データのロードの例を追加しました。[#75](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/75)
- 主キーテーブルでの部分更新と条件付き更新を実行する例を追加しました。[#80](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/80)

**1.1.0**

**機能**

- StarRocks にデータをロードすることをサポートします。

### 1.0

**機能**

- StarRocks からデータをアンロードすることをサポートします。