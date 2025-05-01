---
displayed_sidebar: docs
---

# Flink コネクタ

## 通知

**ユーザーガイド:**

- [Flink コネクタを使用して StarRocks にデータをロードする](../loading/Flink-connector-starrocks.md)
- [Flink コネクタを使用して StarRocks からデータを読み取る](../unloading/Flink_connector.md)

**ソースコード:** [starrocks-connector-for-apache-flink](https://github.com/StarRocks/starrocks-connector-for-apache-flink)

**JAR ファイルの命名形式:**

- Flink 1.15 以降: `flink-connector-starrocks-${connector_version}_flink-${flink_version}.jar`
- Flink 1.15 より前: `flink-connector-starrocks-${connector_version}_flink-${flink_version}_${scala_version}.jar`

**JAR ファイルを取得する方法:**

- [Maven Central Repository](https://repo1.maven.org/maven2/com/starrocks) から直接 Flink コネクタ JAR ファイルをダウンロードします。
- Flink コネクタを Maven プロジェクトの `pom.xml` ファイルに依存関係として追加し、ダウンロードします。具体的な手順については、[ユーザーガイド](../loading/Flink-connector-starrocks.md#obtain-flink-connector) を参照してください。
- ソースコードをコンパイルして Flink コネクタ JAR ファイルを作成します。具体的な手順については、[ユーザーガイド](../loading/Flink-connector-starrocks.md#obtain-flink-connector) を参照してください。

**バージョン要件:**

| コネクタ | Flink                    | StarRocks     | Java | Scala     |
| --------- | ------------------------ | ------------- | ---- | --------- |
| 1.2.9 | 1.15,1.16,1.17,1.18 | 2.1 以降| 8 | 2.11,2.12 |
| 1.2.8     | 1.13,1.14,1.15,1.16,1.17 | 2.1 以降 | 8    | 2.11,2.12 |
| 1.2.7     | 1.11,1.12,1.13,1.14,1.15 | 2.1 以降 | 8    | 2.11,2.12 |

> **注意**
>
> 一般的に、Flink コネクタの最新バージョンは、Flink の最新の 3 つのバージョンとのみ互換性を維持します。

## リリースノート

### 1.2

#### 1.2.9

このリリースにはいくつかの機能とバグ修正が含まれています。注目すべき変更点は、Flink コネクタが [Flink CDC 3.0](https://ververica.github.io/flink-cdc-connectors/master/content/overview/cdc-pipeline.html) と統合され、CDC ソース（MySQL や Kafka など）から StarRocks へのストリーミング ELT パイプラインを簡単に構築できるようになったことです。詳細は [Flink CDC 同期](../loading/Flink-connector-starrocks.md#flink-cdc-synchronization-schema-change-supported) を参照してください。

**機能**

- Flink CDC 3.0 をサポートするための catalog を実装しました。[#295](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/295)
- Flink CDC 3.0 をサポートするために [FLP-191](https://cwiki.apache.org/confluence/display/FLINK/FLIP-191%3A+Extend+unified+Sink+interface+to+support+small+file+compaction) で新しいシンク API を実装しました。[#301](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/301)
- Flink 1.18 をサポートしました。[#305](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/305)

**バグ修正**

- 誤解を招くスレッド名とログを修正しました。[#290](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/290)
- 複数のテーブルに書き込む際に使用される誤った stream-load-sdk 設定を修正しました。[#298](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/298)

#### 1.2.8

このリリースにはいくつかの改善とバグ修正が含まれています。注目すべき変更点は以下の通りです。

- Flink 1.16 と 1.17 をサポートしました。
- シンクが設定されている場合に `sink.label-prefix` を設定することを推奨します。これにより、厳密な一度だけのセマンティクスが保証されます。具体的な手順については、[Exactly Once](../loading/Flink-connector-starrocks.md#exactly-once) を参照してください。

**改善点**

- Stream Load トランザクションインターフェースを使用して少なくとも一度を保証するかどうかを設定するサポートを追加しました。[#228](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/228)
- シンク V1 のリトライメトリクスを追加しました。[#229](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/229)
- EXISTING_JOB_STATUS が FINISHED の場合、getLabelState を取得する必要がありません。[#231](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/231)
- シンク V1 の不要なスタックトレースログを削除しました。[#232](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/232)
- [リファクタリング] StarRocksSinkManagerV2 を stream-load-sdk に移動しました。[#233](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/233)
- Flink テーブルのスキーマに基づいて部分更新を自動的に検出するようにしました。ユーザーが明示的に指定する `sink.properties.columns` パラメータは不要です。[#235](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/235)
- [リファクタリング] probeTransactionStreamLoad を stream-load-sdk に移動しました。[#240](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/240)
- stream-load-sdk に git-commit-id-plugin を追加しました。[#242](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/242)
- DefaultStreamLoader#close に info ログを使用しました。[#243](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/243)
- 依存関係なしで stream-load-sdk JAR ファイルを生成するサポートを追加しました。[#245](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/245)
- stream-load-sdk で fastjson を jackson に置き換えました。[#247](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/247)
- update_before レコードを処理するサポートを追加しました。[#250](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/250)
- ファイルに Apache ライセンスを追加しました。[#251](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/251)
- stream-load-sdk で例外を取得するサポートを追加しました。[#252](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/252)
- `strip_outer_array` と `ignore_json_size` をデフォルトで有効にしました。[#259](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/259)
- Flink ジョブが復元され、シンクセマンティクスが厳密な一度だけの場合に、残存するトランザクションをクリーンアップしようとします。[#271](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/271)
- リトライが失敗した後、最初の例外を返します。[#279](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/279)

**バグ修正**

- StarRocksStreamLoadVisitor のタイプミスを修正しました。[#230](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/230)
- fastjson のクラスローダーリークを修正しました。[#260](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/260)

**テスト**

- Kafka から StarRocks へのロードのためのテストフレームワークを追加しました。[#249](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/249)

**ドキュメント**

- ドキュメントをリファクタリングしました。[#262](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/262)
- シンクに関するドキュメントを改善しました。[#268](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/268) [#275](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/275)
- シンクのための DataStream API の例を追加しました。[#253](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/253)