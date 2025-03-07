---
displayed_sidebar: docs
---

# StarRocks Connector for Flink のリリース

## 通知

**ユーザーガイド:**

- [Flink コネクタを使用して StarRocks にデータをロードする](https://docs.starrocks.io/docs/loading/Flink-connector-starrocks/)
- [Flink コネクタを使用して StarRocks からデータを読み取る](https://docs.starrocks.io/docs/unloading/Flink_connector/)

**ソースコード:** [starrocks-connector-for-apache-flink](https://github.com/StarRocks/starrocks-connector-for-apache-flink)

**JAR ファイルの命名形式:**

- Flink 1.15 以降: `flink-connector-starrocks-${connector_version}_flink-${flink_version}.jar`
- Flink 1.15 より前: `flink-connector-starrocks-${connector_version}_flink-${flink_version}_${scala_version}.jar`

**JAR ファイルを取得する方法:**

- [Maven Central Repository](https://repo1.maven.org/maven2/com/starrocks) から直接 Flink コネクタ JAR ファイルをダウンロードします。
- Flink コネクタを Maven プロジェクトの `pom.xml` ファイルに依存関係として追加し、ダウンロードします。具体的な手順は [ユーザーガイド](https://docs.starrocks.io/docs/loading/Flink-connector-starrocks/#obtain-flink-connector) を参照してください。
- ソースコードをコンパイルして Flink コネクタ JAR ファイルを作成します。具体的な手順は [ユーザーガイド](https://docs.starrocks.io/docs/loading/Flink-connector-starrocks/#obtain-flink-connector) を参照してください。

**バージョン要件:**

| コネクタ | Flink                    | StarRocks     | Java | Scala     |
| --------- | ------------------------ | ------------- | ---- | --------- |
| 1.2.9 | 1.15,1.16,1.17,1.18 | 2.1 以降| 8 | 2.11,2.12 |
| 1.2.8     | 1.13,1.14,1.15,1.16,1.17 | 2.1 以降 | 8    | 2.11,2.12 |
| 1.2.7     | 1.11,1.12,1.13,1.14,1.15 | 2.1 以降 | 8    | 2.11,2.12 |

> **注意**
>
> 一般に、Flink コネクタの最新バージョンは、Flink の最新の 3 つのバージョンとのみ互換性があります。

## リリースノート

### 1.2

#### 1.2.10

**機能**

- JSON カラムの読み取りをサポートします。[#334](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/334)
- ARRAY、STRUCT、および MAP カラムの読み取りをサポートします。[#347](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/347)
- JSON 形式でデータをシンクする際の LZ4 圧縮をサポートします。[#354](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/354)
- Flink 1.19 をサポートします。[#379](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/379)

**改善**

- ソケットタイムアウトの設定をサポートします。[#319](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/319)
- Stream Load トランザクションインターフェースが非同期の `prepare` および `commit` 操作をサポートします。[#328](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/328)
- StarRocks テーブルのカラムのサブセットを Flink ソーステーブルにマッピングすることをサポートします。[#352](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/352)
- Stream Load トランザクションインターフェースを使用する際に特定のウェアハウスを設定することをサポートします。[#361](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/361)

**バグ修正**

以下の問題を修正しました:

- `StarRocksDynamicLookupFunction` 内の `StarRocksSourceBeReader` がデータ読み取り完了後に閉じられない問題。[#351](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/351)
- 空の JSON 文字列を JSON カラムにロードする際に例外が発生する問題。[#380](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/380)

#### 1.2.9

このリリースにはいくつかの機能とバグ修正が含まれています。注目すべき変更は、Flink コネクタが Flink CDC 3.0 と統合され、CDC ソース（MySQL や Kafka など）から StarRocks へのストリーミング ELT パイプラインを簡単に構築できるようになったことです。詳細は [Flink CDC 3.0 とのデータ同期 (schema change 対応)](https://docs.starrocks.io/docs/loading/Flink-connector-starrocks/#synchronize-data-with-flink-cdc-30-with-schema-change-supported) を参照してください。

**機能**

- Flink CDC 3.0 をサポートするために catalog を実装します。[#295](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/295)
- Flink CDC 3.0 をサポートするために新しいシンク API を [FLP-191](https://cwiki.apache.org/confluence/display/FLINK/FLIP-191%3A+Extend+unified+Sink+interface+to+support+small+file+compaction) で実装します。[#301](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/301)
- Flink 1.18 をサポートします。[#305](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/305)

**バグ修正**

- 誤解を招くスレッド名とログを修正します。[#290](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/290)
- 複数のテーブルに書き込む際に使用される誤った stream-load-sdk 設定を修正します。[#298](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/298)

#### 1.2.8

このリリースにはいくつかの改善とバグ修正が含まれています。注目すべき変更は以下の通りです:

- Flink 1.16 および 1.17 をサポートします。
- シンクが設定されている場合に `sink.label-prefix` を設定して、正確に一度のセマンティクスを保証することを推奨します。具体的な手順は [Exactly Once](https://docs.starrocks.io/docs/loading/Flink-connector-starrocks/#exactly-once) を参照してください。

**改善**

- Stream Load トランザクションインターフェースを使用して少なくとも一度の保証をするかどうかを設定することをサポートします。[#228](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/228)
- シンク V1 のリトライメトリクスを追加します。[#229](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/229)
- EXISTING_JOB_STATUS が FINISHED の場合、getLabelState を取得する必要はありません。[#231](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/231)
- シンク V1 の不要なスタックトレースログを削除します。[#232](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/232)
- [リファクタリング] StarRocksSinkManagerV2 を stream-load-sdk に移動します。[#233](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/233)
- Flink テーブルのスキーマに基づいて部分更新を自動的に検出し、ユーザーが明示的に指定する `sink.properties.columns` パラメータを不要にします。[#235](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/235)
- [リファクタリング] probeTransactionStreamLoad を stream-load-sdk に移動します。[#240](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/240)
- stream-load-sdk に git-commit-id-plugin を追加します。[#242](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/242)
- DefaultStreamLoader#close に info ログを使用します。[#243](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/243)
- 依存関係なしで stream-load-sdk JAR ファイルを生成することをサポートします。[#245](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/245)
- stream-load-sdk で fastjson を jackson に置き換えます。[#247](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/247)
- update_before レコードを処理することをサポートします。[#250](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/250)
- ファイルに Apache ライセンスを追加します。[#251](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/251)
- stream-load-sdk で例外を取得することをサポートします。[#252](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/252)
- `strip_outer_array` と `ignore_json_size` をデフォルトで有効にします。[#259](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/259)
- Flink ジョブが復元され、シンクセマンティクスが正確に一度の場合に、残存するトランザクションをクリーンアップしようとします。[#271](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/271)
- リトライが失敗した後、最初の例外を返します。[#279](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/279)

**バグ修正**

- StarRocksStreamLoadVisitor のタイプミスを修正します。[#230](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/230)
- fastjson のクラスローダーリークを修正します。[#260](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/260)

**テスト**

- Kafka から StarRocks へのロードのためのテストフレームワークを追加します。[#249](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/249)

**ドキュメント**

- ドキュメントをリファクタリングします。[#262](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/262)
- シンクのドキュメントを改善します。[#268](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/268) [#275](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/275)
- シンクのための DataStream API の例を追加します。[#253](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/253)