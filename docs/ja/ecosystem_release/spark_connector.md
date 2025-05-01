---
displayed_sidebar: docs
---

# Spark connector

## **通知**

**ユーザーガイド:**

- [Spark connector を使用して StarRocks にデータをロードする](../loading/Spark-connector-starrocks.md)
- [Spark connector を使用して StarRocks からデータを読み取る](../unloading/Spark_connector.md)

**ソースコード**: [starrocks-connector-for-apache-spark](https://github.com/StarRocks/starrocks-connector-for-apache-spark)

**JAR ファイルの命名形式**: `starrocks-spark-connector-${spark_version}_${scala_version}-${connector_version}.jar`

**JAR ファイルの取得方法:**

- [Maven Central Repository](https://repo1.maven.org/maven2/com/starrocks) から直接 Spark connector JAR ファイルをダウンロードします。
- Maven プロジェクトの `pom.xml` ファイルに Spark connector を依存関係として追加し、ダウンロードします。具体的な手順については、[ユーザーガイド](../loading/Spark-connector-starrocks.md#obtain-spark-connector) を参照してください。
- ソースコードをコンパイルして Spark connector JAR ファイルを作成します。具体的な手順については、[ユーザーガイド](../loading/Spark-connector-starrocks.md#obtain-spark-connector) を参照してください。

**バージョン要件:**

| Spark connector | Spark            | StarRocks     | Java | Scala |
| --------------- | ---------------- | ------------- | ---- | ----- |
| 1.1.1           | 3.2, 3.3, or 3.4 | 2.5 and later | 8    | 2.12  |
| 1.1.0           | 3.2, 3.3, or 3.4 | 2.5 and later | 8    | 2.12  |

## **リリースノート**

### 1.1

**1.1.1**

このリリースには、主に StarRocks へのデータロードに関するいくつかの機能と改善が含まれています。

> **注意**
>
> Spark connector をこのバージョンにアップグレードする際のいくつかの変更点に注意してください。詳細は [Upgrade Spark connector](../loading/Spark-connector-starrocks.md#upgrade-from-version-110-to-111) を参照してください。

**機能**

- シンクがリトライをサポートします。 [#61](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/61)
- BITMAP および HLL カラムへのデータロードをサポートします。 [#67](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/67)
- ARRAY 型データのロードをサポートします。 [#74](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/74)
- バッファされた行数に応じてフラッシュをサポートします。 [#78](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/78)

**改善**

- 不要な依存関係を削除し、Spark connector JAR ファイルを軽量化しました。 [#55](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/55) [#57](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/57)
- fastjson を jackson に置き換えました。 [#58](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/58)
- 欠落していた Apache ライセンスヘッダーを追加しました。 [#60](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/60)
- Spark connector JAR ファイルに MySQL JDBC ドライバーをパッケージしないようにしました。 [#63](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/63)
- タイムゾーンパラメータを設定し、Spark Java8 API の日時と互換性を持たせることをサポートしました。 [#64](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/64)
- CPU コストを削減するために行文字列コンバーターを最適化しました。 [#68](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/68)
- `starrocks.fe.http.url` パラメータが http スキームを追加することをサポートします。 [#71](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/71)
- インターフェース BatchWrite#useCommitCoordinator が DataBricks 13.1 で動作するように実装されました。 [#79](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/79)
- エラーログに権限とパラメータを確認するヒントを追加しました。 [#81](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/81)

**バグ修正**

- CSV 関連のパラメータ `column_seperator` と `row_delimiter` におけるエスケープ文字を解析します。 [#85](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/85)

**ドキュメント**

- ドキュメントをリファクタリングしました。 [#66](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/66)
- BITMAP および HLL カラムにデータをロードする例を追加しました。 [#70](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/70)
- Python で書かれた Spark アプリケーションの例を追加しました。 [#72](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/72)
- ARRAY 型データをロードする例を追加しました。 [#75](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/75)
- 主キーテーブルで部分更新と条件付き更新を実行する例を追加しました。 [#80](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/80)

**1.1.0**

**機能**

- StarRocks にデータをロードすることをサポートします。

### 1.0

**機能**

- StarRocks からデータをアンロードすることをサポートします。