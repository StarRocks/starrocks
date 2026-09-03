---
displayed_sidebar: docs
description: "StarRocks 4.1 リリースノート：マルチテナントのレンジベース tablet 自動分割、大容量 tablet サポート（100 GB を目標）、高速スキーマ進化 V2..."
---

# StarRocks バージョン 4.1 {#starrocks-version-41}

:::danger

**コンテナイメージの問題（v4.1.0）**

v4.1.0 のコンテナイメージにおける不安定なロード順序の問題により、コンテナ環境で BE プロセスが正常に起動しないことがあります。**コンテナ環境をお使いの場合は v4.1.0 へのアップグレードをNOT行ってください。**この修正を含む v4.1.1 のリリースをお待ちください（[#71825](https://github.com/StarRocks/starrocks/pull/71825)）。

:::

:::warning

**ダウングレードに関する注意事項**

- StarRocks を v4.1 にアップグレードした後、v4.0.6 より前の v4.0 バージョンへのダウングレードはNOT行わないでください。

  v4.1 で導入された内部的なデータレイアウトの変更（tablet の分割および分散メカニズムに関連）により、v4.1 にアップグレードされたクラスタでは、以前のバージョンと完全な互換性のないメタデータおよびストレージ構造が生成される場合があります。その結果、v4.1 からのダウングレードは v4.0.6 以降のバージョンにのみサポートされます。v4.0.6 より前のバージョンへのダウングレードはサポートされていません。この制限は、以前のバージョンが tablet のレイアウトおよび分散メタデータを解釈する方法における後方互換性の制約によるものです。

:::

## 4.1.4 {#414}

リリース日：2026年8月5日

### 動作の変更 {#behavior-changes}

- 共有データモードにおいて、`TABLESAMPLE` / `SAMPLE` 句および `ANALYZE SAMPLE TABLE` が lake テーブルに対して有効になりました。以前はサンプルオプションが BE に到達せず、フルスキャンが実行されていました。[#71874](https://github.com/StarRocks/starrocks/pull/71874)
- `ALTER TABLE` の `flat_json` 設定への変更は、バージョン管理されたタスクを通じて BE ノードに伝播されるようになり、確実に反映されるようになりました。[#74747](https://github.com/StarRocks/starrocks/pull/74747)
- GIN 転置インデックスによって処理される `NOT MATCH` 述語が、NULL 行を返さなくなりました。NULL 行は結果から正しく除外されるようになりました。[#75578](https://github.com/StarRocks/starrocks/pull/75578)
- `ANALYZE ... UPDATE HISTOGRAM ON` は、オプティマイザが使用しない char 系列の列に対してヒストグラムを計算しなくなり、MCV のみを計算するようになったため、分析コストが削減されました。[#75968](https://github.com/StarRocks/starrocks/pull/75968)
- `rewrite_manifests` は、順序を保持するパーティション範囲によって出力マニフェストをクラスタリングするようになり、マニフェストごとのパーティション範囲がより厳密になり、パーティション数の多いテーブルでのマニフェストプルーニングが改善されました。[#76193](https://github.com/StarRocks/starrocks/pull/76193)
- マルチテーブルストリームロードは、結合トランザクションログを使用するかどうかを決定する際に、グローバルな `lake_use_combined_txn_log` 設定のみに従うのではなく、各テーブルの `file_bundling` プロパティを尊重するようになりました。[#76806](https://github.com/StarRocks/starrocks/pull/76806)
- 主キーテーブルにおいて、GIN 転置インデックスと列モードの部分更新を組み合わせた場合に、破損した結果が返されなくなりました。インデックスはデルタ列グループセグメントから提供されるようになりました。[#76271](https://github.com/StarRocks/starrocks/pull/76271)
- 整数配列に対する `array_difference` は、`BIGINT` に拡張する前に隣接する差分を 64 ビットで計算するようになり、int32 のオーバーフローが修正されました。[#76569](https://github.com/StarRocks/starrocks/pull/76569)
- 定数でない除数を持つ除算式は単調であるとみなされなくなり、誤った結果を生む可能性のある不正な ZoneMap プルーニングが修正されました。[#76744](https://github.com/StarRocks/starrocks/pull/76744)
- 配列およびマップのコンストラクタは、チャンクのフラット化された結果が 4 GB を超える場合、データを無言で破損させる代わりにエラーを発生させるようになりました。[#76419](https://github.com/StarRocks/starrocks/pull/76419)
- 主キーの挿入時に、L1/L2 永続性インデックスに既に存在するキーが正しく拒否されるようになりました。以前は存在チェックがスキップされていました。[#76591](https://github.com/StarRocks/starrocks/pull/76591)
- レンジコロケートテーブルにおけるサンプルベースの tablet の事前分割は、新しいシャードをソースの tablet のワーカーに詰め込む代わりに、コンピュートノード全体に分散させるようになり、ハッシュに比べて約 3 倍遅かったレンジのバッチインポートが修正されました。[#76608](https://github.com/StarRocks/starrocks/pull/76608)
- インクリメンタルまたは AUTO のマテリアライズドビューは、`AUTO` リフレッシュモードにおける PCT フォールバックのためのメンテナンスクエリを再構築するようになり、IVM がサポートしない形状に対するリフレッシュの失敗が修正されました。[#75961](https://github.com/StarRocks/starrocks/pull/75961)
- `ALTER MATERIALIZED VIEW ... ACTIVE` が非集計の INCREMENTAL マテリアライズドビューに対して機能するようになりました。ストレージで満たされる `__ROW_ID__` 列は MV の DDL 列リストから除外されるようになり、非アクティブな IVM を再アクティブ化できるようになりました。[#77017](https://github.com/StarRocks/starrocks/pull/77017)
- Iceberg のパーティションキャッシュは、（エントリ数だけでなく）メモリによって制限されるようになり、その使用状況が公開されるようになったため、パーティション数の多いテーブルでの無制限な増大が防止されます。[#76165](https://github.com/StarRocks/starrocks/pull/76165)
- OAuth2 クライアント資格情報を使用する Iceberg REST カタログは、バックグラウンドのトークンリフレッシュタスクが停止した後も、以降のすべてのリクエストを失敗させるのではなく、自己修復するようになりました。[#76457](https://github.com/StarRocks/starrocks/pull/76457)
- Lake のフルバキュームは、以前はスキップされていた孤立した `.lcrm`（Lake Compaction Rows Mapper）ファイルを回収するようになりました。[#76522](https://github.com/StarRocks/starrocks/pull/76522)
- 共有データの公開処理は、tablet が欠落しているバンドル tablet のメタデータファイルの書き込みを拒否するようになり、パーティションが公開時に永久にスタックすることを防止します。[#76850](https://github.com/StarRocks/starrocks/pull/76850)
- 放棄された外部（Spark/Flink コネクタ）スキャンコンテキストは、回収時にパイプラインフラグメントをキャンセルするようになり、外部スキャンプランは QueryContext の生存期間を制限するために `query_delivery_timeout` を設定するようになりました。[#76535](https://github.com/StarRocks/starrocks/pull/76535) [#76536](https://github.com/StarRocks/starrocks/pull/76536)
- マテリアライズドビューのピン留め範囲マップが（テーブル UUID によって）正しくキー付けされるようになり、OLAP テーブルに対する MV のブートストラップピン留めが無効化されていた問題と、異なるデータベース内の同名テーブルの誤った処理が修正されました。[#76320](https://github.com/StarRocks/starrocks/pull/76320) [#76351](https://github.com/StarRocks/starrocks/pull/76351)
- leader FE に転送されたステートメントの監査ログは、CTE のエイリアスや未修飾のテーブル名ではなく、完全修飾されたクエリ対象のリレーション（leader と一致）を記録するようになりました。[#76387](https://github.com/StarRocks/starrocks/pull/76387)
- 完全な `STRUCT` 列を `ROLLUP`、`CUBE`、または `GROUPING SETS` と一緒に集計しても、プラン作成時に `usedStructFiledPos` エラーで失敗しなくなりました。[#76804](https://github.com/StarRocks/starrocks/pull/76804)
- FE メモリの `Estimator` はコンテナのオーバーヘッドをカウントするようになり、Parquet スキャナは完全なバッチサイズではなく制限されたチャンクサイズを使用して欠落した列をパディングするようになったため、メモリ計上の精度が向上しました。[#75971](https://github.com/StarRocks/starrocks/pull/75971) [#75981](https://github.com/StarRocks/starrocks/pull/75981)

### 改善 {#improvements}

- Routine Load において、Kafka/Pulsar のメッセージメタデータ（パーティション、オフセット、タイムスタンプなど）を公開するための `INCLUDE METADATA` 句のサポートが追加されました。[#73840](https://github.com/StarRocks/starrocks/pull/73840)
- Routine Load のメタデータエイリアスを任意選択にしました。[#76294](https://github.com/StarRocks/starrocks/pull/76294)
- `information_schema.materialized_views` に `LAST_FRESHNESS_CONFIRMED_AT` 列が追加されました。[#74585](https://github.com/StarRocks/starrocks/pull/74585)
- `/metrics` エンドポイントを通じてデータキャッシュメトリクスを公開し、FE の Compaction メトリクス、`ALTER TABLE` 列操作メトリクスと所要時間、および exchange sink 上の `CompressedInputBytes` メトリクスを追加しました。 [#58204](https://github.com/StarRocks/starrocks/pull/58204) [#72941](https://github.com/StarRocks/starrocks/pull/72941) [#76247](https://github.com/StarRocks/starrocks/pull/76247) [#76309](https://github.com/StarRocks/starrocks/pull/76309)
- コネクタのメタデータ操作における認証および接続エラーの詳細をエンドユーザーに公開するようにしました。 [#75490](https://github.com/StarRocks/starrocks/pull/75490)
- 大きな列の容量制限チェックに関するエラーメッセージを改善しました。 [#76303](https://github.com/StarRocks/starrocks/pull/76303)
- `hdfs_backend_selector_cache_replica_num` 変数を追加し、オブジェクトストレージクライアントのキャッシュサイズをランタイムで変更可能にしました。 [#75023](https://github.com/StarRocks/starrocks/pull/75023) [#75851](https://github.com/StarRocks/starrocks/pull/75851)
- サンプルベースの tablet 事前分割メタ層リーダーの対応範囲を、`CHAR`、複合ソートキー、UTC 調整済みの Parquet の `TIMESTAMP` / ORC の `TIMESTAMP_INSTANT` を含む、より多くのソートキータイプに拡張しました。 [#75937](https://github.com/StarRocks/starrocks/pull/75937) [#76011](https://github.com/StarRocks/starrocks/pull/76011) [#76114](https://github.com/StarRocks/starrocks/pull/76114)
- 外部テーブルの統計情報収集を改善しました。述語で使用される列の使用状況が追跡されるようになり、複数の列を単一のスキャンで収集し、Iceberg テーブルに対しては上限付きコストのスキャン予算が適用されるようになりました。 [#75938](https://github.com/StarRocks/starrocks/pull/75938) [#76638](https://github.com/StarRocks/starrocks/pull/76638) [#76549](https://github.com/StarRocks/starrocks/pull/76549)
- 右外部結合、semi、anti、および full-outer のレンジコロケートジョインをサポートしました。 [#76040](https://github.com/StarRocks/starrocks/pull/76040)
- Query Queue V2 のコスト推定器を最適化しました。 [#76609](https://github.com/StarRocks/starrocks/pull/76609)
- `FILES()` グロブに対して、ワイルドカードのリテラルプレフィックスを S3 の `ListObjectsV2` 呼び出しにプッシュダウンし、リストされるオブジェクト数を削減しました。 [#76210](https://github.com/StarRocks/starrocks/pull/76210)
- `remove_orphan_files` で Iceberg のマニフェストエントリをスキャンする際、`file_path` 列のみが投影されるようになりました。 [#76020](https://github.com/StarRocks/starrocks/pull/76020)
- レイクの主キーテーブレットに対する base Compaction、および分離ソートキー方式のクラウドネイティブ主キーテーブルに対するロードスピルと積極的な PK インデックス SST をサポートしました。 [#76794](https://github.com/StarRocks/starrocks/pull/76794) [#76094](https://github.com/StarRocks/starrocks/pull/76094)
- 共有データのレンジ分散テーブルにおいて、末尾のソートキー列をメタデータのみで追加することをサポートしました。 [#76341](https://github.com/StarRocks/starrocks/pull/76341)
- フラグメントのキャンセル時に実行中の exchange sink RPC をキャンセルするようにし、reshard クリーニング中にリシャードされたパーティション上で実行中の Compaction をキャンセルするようにしました。 [#75613](https://github.com/StarRocks/starrocks/pull/75613) [#76759](https://github.com/StarRocks/starrocks/pull/76759)
- 複数ステートメントの Stream Load チャネルを待機前に発火させるようにし、ロードのレイテンシを削減しました。 [#76715](https://github.com/StarRocks/starrocks/pull/76715)
- `Analytor::process` における列アップグレードに対するメモリ制限チェックを追加しました。 [#75821](https://github.com/StarRocks/starrocks/pull/75821)
- 非同期デルタライターが停止した際に、実際のエラー状態を保持するようにしました。 [#76216](https://github.com/StarRocks/starrocks/pull/76216)
- レイクの PK インデックス SSTable に世代バージョンを記録し、レイクの主キー公開時に `op_write.seg_delvecs` を適用し、Compaction 公開の競合解決中に出力セグメントフッターのオープンをスキップするようにしました。 [#76208](https://github.com/StarRocks/starrocks/pull/76208) [#76474](https://github.com/StarRocks/starrocks/pull/76474) [#76657](https://github.com/StarRocks/starrocks/pull/76657)
- 診断を容易にするため、レイクのロードおよび公開バージョンパスにスタックトレースと詳細なトレースカウンタを追加しました。 [#75901](https://github.com/StarRocks/starrocks/pull/75901) [#76810](https://github.com/StarRocks/starrocks/pull/76810)

### セキュリティ {#security}

- [CVE-2026-44891] STOMP サブフレームデコーダーにおけるメモリ枯渇（DoS）脆弱性を修正するため、Netty を 4.1.136.Final にアップデートしました。 [#76555](https://github.com/StarRocks/starrocks/pull/76555)
- [CVE-2026-55971] [CVE-2026-43871] C++ バインディングにおけるヒープベースのバッファオーバーフローと無限ループの脆弱性を修正するため、Apache Thrift を 0.24.0 にアップデートしました。 [#76922](https://github.com/StarRocks/starrocks/pull/76922)
- [CVE-2026-10050] 脆弱性のある Jetty の jar（クライアント側の Digest 認証バイパスで、Hadoop 経由で推移的に取り込まれていたもの）を除外し、pgjdbc を 42.7.12 にアップデートしました。 [#76783](https://github.com/StarRocks/starrocks/pull/76783)
- [CVE-2011-4969] [CVE-2014-6071] 脆弱性のある jQuery 1.4.2（およびその他の jQuery XSS の CVE）が同梱された、未使用の `avro-ipc` jar を除外しました。 [#76270](https://github.com/StarRocks/starrocks/pull/76270)
- [CVE-2024-29857] 修正済みの対応バージョンと共に出荷されていた、脆弱で古い推移的依存関係（例えば `bcprov-jdk15on` 1.70 や EOL となった `okhttp` 2.x）を削除し、退行を防ぐために依存関係の禁止設定を追加しました。 [#76097](https://github.com/StarRocks/starrocks/pull/76097)

### バグ修正 {#bug-fixes}

以下の問題が修正されました：

- ジョイン述語の導出において、空のレンジでプランナーがクラッシュする問題。 [#75011](https://github.com/StarRocks/starrocks/pull/75011)
- NULL でない定数の `ELSE` を持つ `CASE` に対して、集計が誤ってプッシュダウンされる問題。 [#75037](https://github.com/StarRocks/starrocks/pull/75037)
- `PARTITION-TOP-N` の partition-by が、プルーニングされた辞書スロットに書き換えられてしまう問題。 [#75956](https://github.com/StarRocks/starrocks/pull/75956)
- ジョインの shuffle-join 出力プロパティの分岐における演算子優先順位の誤り、および `predicateCommonOperators` がジョインオペレーターの構築を通じて引き継がれない問題。 [#76203](https://github.com/StarRocks/starrocks/pull/76203) [#76330](https://github.com/StarRocks/starrocks/pull/76330) [#76388](https://github.com/StarRocks/starrocks/pull/76388)
- 分析時における、ビュー列および `ROLLUP` キーの NULL 許容性の誤り。 [#75684](https://github.com/StarRocks/starrocks/pull/75684) [#76149](https://github.com/StarRocks/starrocks/pull/76149)
- 長さゼロのキャプチャグループに対する `regexp_extract_all` の無限ループ。 [#75798](https://github.com/StarRocks/starrocks/pull/75798)
- `LargeOrCalculatingVisitor` における誤った `nullsFraction` のクランプ処理。 [#75864](https://github.com/StarRocks/starrocks/pull/75864)
- 文字列から数値へのスキーマ変更変換における `CAST` セマンティクスの誤り。 [#75538](https://github.com/StarRocks/starrocks/pull/75538)
- 同一列に対する `min`/`max` において、同期マテリアライズドビューの書き換えでロールアップ列が失われる問題。 [#75528](https://github.com/StarRocks/starrocks/pull/75528)
- スキャン述語を引き上げる際、`array_map` ラムダ内部で誤って書き換えが適用される問題。 [#76380](https://github.com/StarRocks/starrocks/pull/76380)
- バケット認識実行下の Iceberg バケットテーブルにおける `COUNT(DISTINCT)` の過剰カウント。 [#76601](https://github.com/StarRocks/starrocks/pull/76601)
- 必須の Iceberg 列に対する `UNNEST` + `GROUP BY` において、NULL 許容出力に対するエラーが発生する問題。 [#76730](https://github.com/StarRocks/starrocks/pull/76730)
- `INSERT OVERWRITE` コードパス全体でのロック競合を削減し、CBO のテーブルプルーニング中に Memo の自己参照を回避するため `Operator` のソルトを復元しました。 [#75828](https://github.com/StarRocks/starrocks/pull/75828) [#76542](https://github.com/StarRocks/starrocks/pull/76542)
- 文字列の日付パーティション列を時間値と比較した場合に、Iceberg のパーティションプルーニング、マニフェストの行数推定、メタデータ削除、等価削除の適用、および Delta Lake のパーティションプルーニングが誤った結果になる問題。 [#76068](https://github.com/StarRocks/starrocks/pull/76068) [#76107](https://github.com/StarRocks/starrocks/pull/76107) [#76197](https://github.com/StarRocks/starrocks/pull/76197) [#76280](https://github.com/StarRocks/starrocks/pull/76280) [#76348](https://github.com/StarRocks/starrocks/pull/76348)
- Iceberg の読み取りが対象スナップショットのスキーマとパーティション仕様を考慮していなかった問題、パーティションフィールドを削除した後に Iceberg V1 テーブルでクエリが失敗する問題、`rollback_to_snapshot` の後に MV 書き換えが古い結果を返す問題を修正しました。[#74711](https://github.com/StarRocks/starrocks/pull/74711) [#75149](https://github.com/StarRocks/starrocks/pull/75149) [#75924](https://github.com/StarRocks/starrocks/pull/75924)
- Iceberg のマニフェストデータファイルキャッシュが不完全なファイルセットを返していた問題、増分スキャン範囲イテレータが並行クローズに対して安全でなかった問題を修正し、分析時のタイムトラベルスナップショットバインディングをベストエフォート方式にしました。[#76215](https://github.com/StarRocks/starrocks/pull/76215) [#75953](https://github.com/StarRocks/starrocks/pull/75953) [#76448](https://github.com/StarRocks/starrocks/pull/76448)
- Iceberg/Delta のメタデータ由来の統計情報に `StatsSource=TABLE_METADATA` のタグが付与されるようになりました。[#76560](https://github.com/StarRocks/starrocks/pull/76560)
- Delta Lake および Kudu の非パーティション化マテリアライズドビューのクエリ書き換え。[#76359](https://github.com/StarRocks/starrocks/pull/76359)
- `gcs-connector` の 3.x 設定名変更により GCS の vended credentials が無視されていた問題を修正しました。[#75979](https://github.com/StarRocks/starrocks/pull/75979)
- Hive の `getTable()` が、`get_table_req` フォールバックの前に再接続するようになり、テーブルが見つからない場合をメッセージテキストではなく例外タイプで検出するようになりました。[#76456](https://github.com/StarRocks/starrocks/pull/76456) [#76459](https://github.com/StarRocks/starrocks/pull/76459)
- Parquet の列インデックス統計情報で `BOOLEAN` の最小値/最大値がサポートされていませんでした。[#74752](https://github.com/StarRocks/starrocks/pull/74752)
- 名前に `.` を含むキーに対する Flat-JSON サブフィールド読み取り、および NULL を返す代わりに中間の Flat-JSON オブジェクトを再構築してしまう問題を修正しました。サブフィールドキーが大文字小文字を区別せずに衝突する場合、JSON のサブフィールドプッシュダウンがスキップされるようになりました。[#75583](https://github.com/StarRocks/starrocks/pull/75583) [#75764](https://github.com/StarRocks/starrocks/pull/75764) [#76594](https://github.com/StarRocks/starrocks/pull/76594)
- 片方のブランチのみがビットマップインデックス化されている場合でも、OR ネスト述語を保持するようにしました。[#76275](https://github.com/StarRocks/starrocks/pull/76275)
- 主キーテーブルの自動増分列の部分更新適用時に BE がクラッシュする問題。[#76119](https://github.com/StarRocks/starrocks/pull/76119)
- ロードのスピル中に発生する `LoadChunkSpiller` の初期化競合による BE クラッシュ。[#76098](https://github.com/StarRocks/starrocks/pull/76098)
- 不完全なネストされたレイクスキーマに対するネイティブ Parquet リーダーのクラッシュ、および Avro の複合型カラムにネストされた `BOOLEAN` に対する ASAN クラッシュ。[#76455](https://github.com/StarRocks/starrocks/pull/76455) [#76041](https://github.com/StarRocks/starrocks/pull/76041)
- 無効な文字列を `NOT NULL` 数値カラムに変換する際、schema change 中に `bad_variant_access` が発生する問題。[#76707](https://github.com/StarRocks/starrocks/pull/76707)
- `SimdJsonConverter` のエラーパスにおけるヒープバッファオーバーフロー、およびバッファ拡張をまたぐ複数文字の CSV 区切り文字における use-after-free。[#76752](https://github.com/StarRocks/starrocks/pull/76752) [#76718](https://github.com/StarRocks/starrocks/pull/76718)
- スキャン終了処理中の `MorselQueueFactory` における use-after-free、キャンセル時のスピル可能ジョインビルド `set_finishing` における use-after-free、および `PipelineDriver` デストラクタにおけるグローバルランタイムフィルタタイマーのリーク／スケジュールされていないタイマー。[#76259](https://github.com/StarRocks/starrocks/pull/76259) [#76633](https://github.com/StarRocks/starrocks/pull/76633) [#76252](https://github.com/StarRocks/starrocks/pull/76252)
- 物理分割による空の tablet による CN クラッシュを修正するための null-safe な `SparseRangeIterator::has_more()`、および bRPC スタブキャッシュのクリーンアップタイマーのリーク。[#75985](https://github.com/StarRocks/starrocks/pull/75985) [#75973](https://github.com/StarRocks/starrocks/pull/75973)
- クエリデプロイワーカー上で `ConnectContext` が復元されていなかった問題。[#76366](https://github.com/StarRocks/starrocks/pull/76366)
- 共有データの主キーテーブルにおいて、トランザクション内での upsert/delete の順序を保持するようにし、スピルマージを操作を意識したものにしました。[#75338](https://github.com/StarRocks/starrocks/pull/75338) [#75366](https://github.com/StarRocks/starrocks/pull/75366)
- `prev_garbage_version` の宙ぶらりん状態を回避するために `cal_new_base_version` で永続メタデータを読み取るようにし、`publish_version` 内の `base_version` を `base_metadata` と同期させ、通常のパスでマージされた並列 Compaction トランザクションログを永続化するようにしました。[#75904](https://github.com/StarRocks/starrocks/pull/75904) [#76313](https://github.com/StarRocks/starrocks/pull/76313) [#76460](https://github.com/StarRocks/starrocks/pull/76460)
- レンジ Colocate の修正：アライメントジョブのストームを停止し、アライメントされていない Colocate Join を fail-close するようにしました。また、バケットシャッフル下で null-safe ジョインが一致をドロップしてしまう問題を修正し、プランフィードバックにおけるレンジ Colocate ジョインの検出を改善しました。[#75930](https://github.com/StarRocks/starrocks/pull/75930) [#76104](https://github.com/StarRocks/starrocks/pull/76104) [#76121](https://github.com/StarRocks/starrocks/pull/76121)
- tablet 分割／再シャードの修正：分割時にテーブルの楽観バージョンを上げることで並行クエリが再プランされるようにし、同一 tablet の再シャード前に PK インデックスのメモリテーブルをフラッシュするようにし、再シャードをまたいでバージョン区間ごとにレイクの vacuum ファイルを保持するようにし、分割不可能なレンジ分散 tablet に対する自動分割ジョブのループを停止するようにし、分割後のレンジ分散ソートキーにおける `IS NULL` のプルーニングを修正しました。[#76123](https://github.com/StarRocks/starrocks/pull/76123) [#76367](https://github.com/StarRocks/starrocks/pull/76367) [#76209](https://github.com/StarRocks/starrocks/pull/76209) [#76663](https://github.com/StarRocks/starrocks/pull/76663) [#76797](https://github.com/StarRocks/starrocks/pull/76797)
- キー由来のレンジ分散テーブルのキー列の順序を変更してしまう全列 `ORDER BY` を拒否するようにしました。[#76256](https://github.com/StarRocks/starrocks/pull/76256)
- ロードスピルの並列マージ結果をフラッシュ順に統合し、ファイルバンドリングの引き継ぎにおいて `metaId` によってタッチされたインデックスを除外するようにしました。[#75951](https://github.com/StarRocks/starrocks/pull/75951) [#76368](https://github.com/StarRocks/starrocks/pull/76368)
- NULL を吸収する辞書マッピングの group-by キーのために辞書サイズ + 1 を予約するようにし、プロファイルレポートの再スケジュール前にドライバーの準備状況を確認するようにしました。[#75357](https://github.com/StarRocks/starrocks/pull/75357) [#75725](https://github.com/StarRocks/starrocks/pull/75725)
- `PlannerMetaLocker` が、ロックが一度も成功しなかった場合にアンロックをスキップしていました。[#74041](https://github.com/StarRocks/starrocks/pull/74041)
- リソースグループを warehouse でフィルタリングするようにしました。[#73209](https://github.com/StarRocks/starrocks/pull/73209)
- `SHOW CREATE ROUTINE LOAD` の出力において `jsonpaths` の値をエスケープするようにしました。[#75755](https://github.com/StarRocks/starrocks/pull/75755)
- Arrow Flight のプリペアドステートメント転送の問題を修正しました。[#76310](https://github.com/StarRocks/starrocks/pull/76310)
- 辞書の更新間隔のオーバーフローおよび意図しない自動更新の問題を修正しました。[#76634](https://github.com/StarRocks/starrocks/pull/76634)
- `ERROR_IF_OVERFLOW` 配下で最小値／最大値が空の場合に統計情報キャッシュのロードが失敗する問題を修正しました。[#76684](https://github.com/StarRocks/starrocks/pull/76684)
- catalog の削除の存在チェックを、書き込みロック下でアトミックに行うようにしました。[#76778](https://github.com/StarRocks/starrocks/pull/76778)

## 4.1.3 {#413}

リリース日：2026 年 7 月 14 日

### 動作の変更 {#behavior-changes-1}

- `CTAS` は、明示的に宣言された `VARCHAR(N)` カラムの長さを `VARCHAR(MAX)` に広げるのではなく、そのまま保持するようになりました。既存のテーブルへの影響はありません。`CTAS` で作成された新しいテーブルは、以降の書き込み時に宣言された長さを強制するようになります。[#73498](https://github.com/StarRocks/starrocks/pull/73498)
- `OPERATE ON SYSTEM` 権限を持たずに `sys.fe_memory_usage` または `sys.fe_locks` をクエリした場合、誤解を招くノード検索失敗ではなく、明確なアクセス拒否エラーが返されるようになりました。[#73567](https://github.com/StarRocks/starrocks/pull/73567)
- `FILES()` およびブローカー／ストリームロードは、`isAdjustedToUTC=false` で書き込まれた `INT64` の Parquet タイムスタンプに対して、セッションのタイムゾーンシフトを適用しなくなりました。これらのタイムスタンプは、現在ではウォールクロック値として扱われ、そのままロードされます。v4.1.3 より前にこのようなファイルからロードされたデータは、アップグレード後にロードされたデータと異なる場合があります。整合性が必要な場合は再ロードしてください。[#73674](https://github.com/StarRocks/starrocks/pull/73674)
- 正常にコミットされたマルチテーブルトランザクションのストリームロードジョブが、`PREPARING` のまま停止し続けるのではなく、`information_schema.loads` および `SHOW STREAM LOAD` において正しく `VISIBLE` として表示されるようになりました。[#74386](https://github.com/StarRocks/starrocks/pull/74386)
- コネクタの増分スキャン範囲スケジューリングは、デプロイされたフラグメントのドライバーレイアウトを一貫して再利用するようになり、スキャン範囲が存在しないドライバーに誤って割り当てられることを防止します。[#74674](https://github.com/StarRocks/starrocks/pull/74674)
- `LIKE` の定数畳み込みが MySQL 8 のバックスラッシュエスケープのセマンティクスに一致するようになり、`'a\\\\b'` のようなパターンが以前は逆の結果を返していたケースを修正しました。[#74814](https://github.com/StarRocks/starrocks/pull/74814)
- Routine Load は `property.kafka_partition_discovery` プロパティをサポートするようになりました。これにより、正確な開始オフセットを指定するために `kafka_partitions` と `kafka_offsets` が指定されている場合でも、パーティションの自動検出を継続できます。`property.kafka_default_offsets` が設定されていない場合、ジョブがすでに消費の進捗を持っている状態で検出されたパーティションのデフォルト開始オフセットは `OFFSET_END` から `OFFSET_BEGINNING` に変わります。これは新しいプロパティを使用しているジョブだけでなく、**すべて**の自動検出ジョブに適用されます。[#74729](https://github.com/StarRocks/starrocks/pull/74729)
- グループなし集計は、マージ前に `UNION ALL` 分岐を通じてプッシュダウンされるようになり、ユニオンに対して集計を行うクエリのネットワーク転送量とメモリ使用量が削減されます。[#73930](https://github.com/StarRocks/starrocks/pull/73930)
- IVM のメンテナンスクエリは、`CREATE` 時に保存された固定のクエリテキストを使用するのではなく、リフレッシュのたびに現在のビュー定義から再導出されるようになりました。既存の MV は再作成しなくても、リライターのバグ修正の恩恵を自動的に受けられます。[#74881](https://github.com/StarRocks/starrocks/pull/74881)
- サンプルベースの tablet 事前分割では、ソース側の tablet のワーカー（`PACK` 配置）にシャードを詰め込むのではなく、事前分割されたシャードをすべてのコンピュートノードに分散配置（`SPREAD` 配置）するようになり、ロードの並行性が向上しました。[#75514](https://github.com/StarRocks/starrocks/pull/75514)
- 列コメントのみを変更する `ALTER TABLE ... MODIFY COLUMN` は、完全な schema change ジョブを起動する代わりに軽量なメタデータのみのパスを使用するようになり、この動作は主キー列でも機能するようになりました。[#75325](https://github.com/StarRocks/starrocks/pull/75325)
- `FLOOR` と `CEIL` は予約されていないキーワードとして扱われるようになり、クォートなしで列名として使用できます。[#75241](https://github.com/StarRocks/starrocks/pull/75241)
- `SHOW FUNCTIONS` の出力には、UDF および UDAF の Properties 列に `isolation` プロパティ（`shared` または `isolated`）が常に含まれるようになりました。[#75255](https://github.com/StarRocks/starrocks/pull/75255)
- `lake_vacuum_min_batch_delete_size` のデフォルト値が 100 から 200 に引き上げられ、`DeleteObjects` リクエストごとにより多くの古いファイルの削除をバッチ処理することで、オブジェクトストレージ上での vacuum のスループットが向上しました。[#74304](https://github.com/StarRocks/starrocks/pull/74304)
- vended credentials を使用する Iceberg REST catalog テーブルはキャッシュされ、その認証情報はバックグラウンドでリフレッシュされるようになりました。これにより、AWS Lake Formation のレート制限を引き起こしていた `getTable()` ごとの `GetDataAccess` 呼び出しが不要になります。[#75431](https://github.com/StarRocks/starrocks/pull/75431)
- IVM の `bitmap_union`、`hll_union`、`percentile_union` の集計状態は、マテリアライズドビュー内で（可視列と非表示の `__AGG_STATE_` 列に）2 回ではなく 1 回だけ保存されるようになり、これらのスケッチタイプのストレージが半分になりました。[#75760](https://github.com/StarRocks/starrocks/pull/75760)
- インクリメンタルマテリアライズドビューは `bitmap_agg`、`hll_union`、`percentile_union`、`bitmap_union` の集計関数をサポートするようになり、正確な重複排除カウントとスケッチベースの集計をインクリメンタルに維持できるようになりました。[#75587](https://github.com/StarRocks/starrocks/pull/75587) [#75610](https://github.com/StarRocks/starrocks/pull/75610)
- サンプルベースの tablet 事前分割の tablet 数は、均等な分散のためにアクティブなコンピュートノード数の最も近い倍数に切り上げられるようになり、小規模なロードでの過度な断片化を避けるために最小の tablet サイズで下限が設定されるようになりました。[#75360](https://github.com/StarRocks/starrocks/pull/75360) [#75584](https://github.com/StarRocks/starrocks/pull/75584)

### 改善 {#improvements-1}

- `ngram_search` 関数は、非定数の検索対象引数を受け入れるようになりました。[#74675](https://github.com/StarRocks/starrocks/pull/74675)
- `enable_http_auth` FE 設定によって制御される HTTP 認証フレームワークが追加され、すべての外部 HTTP エンドポイントに対する認証と RBAC の強制をゲート制御します。[#73822](https://github.com/StarRocks/starrocks/pull/73822)
- `information_schema.materialized_views` に、リフレッシュと配置の可観測性列（`refresh_warehouse`、`refresh_resource_group`、`refresh_mode`、`refresh_type`、`last_refresh_details`）が追加されました。[#74342](https://github.com/StarRocks/starrocks/pull/74342)
- 外部メタストアの遅延や停止が FE のジャーナルリプレイや起動を妨げないようにするため、新しい FE 設定で制御されるオプトインの外部統計キャッシュのジャーナルリプレイ時遅延リフレッシュが追加されました。[#74371](https://github.com/StarRocks/starrocks/pull/74371)
- `VARCHAR` の長さの増加は、データの書き換えを行わずに高速な schema change を介して、レンジ分散（共有データ）のソートキー列で許可されるようになりました。[#74698](https://github.com/StarRocks/starrocks/pull/74698)
- 共有データのトランザクションログの書き込みが設定可能なしきい値を超えた場合にスタックトレースをダンプする機能が追加され、遅い `put_txn_log` / `put_combined_txn_log` 呼び出しの診断が容易になりました。[#74704](https://github.com/StarRocks/starrocks/pull/74704)
- tablet 事前分割のメタ層フッターリーダーが `DATE`、`DATETIME`、`DECIMAL`、`VARCHAR`、および ORC の `TIMESTAMP` ソートキーをサポートするようになり、データ層のサンプリングにフォールバックする必要のあるロード数が削減されました。[#74710](https://github.com/StarRocks/starrocks/pull/74710) [#74739](https://github.com/StarRocks/starrocks/pull/74739) [#74792](https://github.com/StarRocks/starrocks/pull/74792) [#74902](https://github.com/StarRocks/starrocks/pull/74902) [#74955](https://github.com/StarRocks/starrocks/pull/74955) [#75186](https://github.com/StarRocks/starrocks/pull/75186) [#75209](https://github.com/StarRocks/starrocks/pull/75209) [#75427](https://github.com/StarRocks/starrocks/pull/75427) [#75697](https://github.com/StarRocks/starrocks/pull/75697)
- サンプルベースの tablet 事前分割は、`INSERT INTO ... SELECT ... FROM <OLAP table>` のロードに加えて、すべてのソートキー列を含む列リスト指定の `INSERT` ステートメントにも適用されるようになりました。[#74828](https://github.com/StarRocks/starrocks/pull/74828) [#75345](https://github.com/StarRocks/starrocks/pull/75345)
- 共有データの tablet メタデータおよびトランザクションログファイルに対する Adler-32 チェックサム保護が追加され、読み取り時にサイレントな破損を検出できるようになりました。[#74924](https://github.com/StarRocks/starrocks/pull/74924)
- データベースごとの `txn_max_committed_pending_publish_ms` FE メトリクスが追加され、コミット済みだがまだパブリッシュされていないトランザクションのうち最も古いものの経過時間を報告することで、バージョンのパブリッシュの停滞を検出しやすくなりました。[#75025](https://github.com/StarRocks/starrocks/pull/75025)
- tablet の分割 / マージが、パブリッシュバージョンのレスポンスからリアルタイムでトリガーされるようになり、ロードが完了してから自動分割 / マージが開始されるまでの遅延が短縮されました。[#75010](https://github.com/StarRocks/starrocks/pull/75010)
- no-SST の条件マージタスクを `pk_index_execution` スレッドプールにルーティングすることで、lake 主キーテーブルの条件付き更新の比較フェーズを最適化しました。[#74572](https://github.com/StarRocks/starrocks/pull/74572)
- lake の schema change およびロールアップジョブのロックのスコープを、データベース全体ではなくテーブルレベルに限定し、同じデータベース内の他のテーブルに対する同時操作でのロック競合を削減しました。[#75087](https://github.com/StarRocks/starrocks/pull/75087)
- 共有なしモードでのいくつかのデータベースレベルの書き込みロックを、テーブルスコープの集中書き込みロックに縮小し、BE のレポートコールバックとクールダウン操作中のロック競合を削減しました。[#74521](https://github.com/StarRocks/starrocks/pull/74521) [#74523](https://github.com/StarRocks/starrocks/pull/74523)
- Avro の Routine Load は、`MAP` と `STRUCT` のターゲット列をネイティブにサポートするようになりました。[#74901](https://github.com/StarRocks/starrocks/pull/74901)
- レンジコロケートの tablet の安定性ゲーティングは、グループを安定としてマークする前に StarOS の配置収束を待つようになり、Colocate Join がホストローカル実行を達成できるようになりました。[#75290](https://github.com/StarRocks/starrocks/pull/75290) [#75656](https://github.com/StarRocks/starrocks/pull/75656) [#75883](https://github.com/StarRocks/starrocks/pull/75883)
- 外部テーブルの CBO 統計を改善しました。オプティマイザは、Iceberg のマニフェストから完全なファイル列挙を行うことなく行数を推定するようになり、Parquet/ORC 圧縮に対する Hive / Hudi の行数の過小評価を修正し、JDBC コネクタ向けの非同期行数統計を追加し、Puffin 統計が利用できない場合の Iceberg および外部コネクタに対する NDV 推定のフォールバックを提供します。[#75280](https://github.com/StarRocks/starrocks/pull/75280) [#75082](https://github.com/StarRocks/starrocks/pull/75082) [#75083](https://github.com/StarRocks/starrocks/pull/75083) [#75092](https://github.com/StarRocks/starrocks/pull/75092) [#75097](https://github.com/StarRocks/starrocks/pull/75097) [#75382](https://github.com/StarRocks/starrocks/pull/75382) [#75474](https://github.com/StarRocks/starrocks/pull/75474)
- Iceberg のマニフェスト列統計は、クラスタ化された列のみに対して選択的にキャッシュされるようになり、多数のデータファイルを持つワイドテーブルでの FE ヒープ消費が削減されました。[#75395](https://github.com/StarRocks/starrocks/pull/75395)
- 外部テーブルの統計収集が、FE の再起動や HA フェイルオーバーをまたいだ永続的な述語列トラッキングをサポートするようになり、自動 ANALYZE が正しい列をターゲットにできるようになりました。[#75653](https://github.com/StarRocks/starrocks/pull/75653)
- スケジューリングから実行までの外部テーブル統計収集のライフサイクル全体をカバーする、構造化された `[ExternalStats]` ログ行が追加されました。[#75335](https://github.com/StarRocks/starrocks/pull/75335) [#75529](https://github.com/StarRocks/starrocks/pull/75529)
- `SHOW ANALYZE STATUS` には、外部テーブル統計ジョブの Properties 列にパーティション、列、スナップショットのメタデータが含まれるようになりました。[#75630](https://github.com/StarRocks/starrocks/pull/75630)
- 各外部テーブルの統計ソース（`TABLE_METADATA`、`ANALYZE`、または `NONE`）が、クエリのランタイムプロファイルに公開されるようになりました。[#75253](https://github.com/StarRocks/starrocks/pull/75253)
- Iceberg および Delta Lake の外部テーブルに対して、パーティションフィルタの必須化とパーティション数の上限（従来は Hive、Hudi、Paimon のみで利用可能）のサポートが追加されました。[#75790](https://github.com/StarRocks/starrocks/pull/75790)
- `TABLE SAMPLE` およびヒストグラムの `ANALYZE` において 1% 未満のサンプリング比率をサポートし、計算された比率がゼロに切り捨てられることで大規模テーブルで発生していた失敗を修正しました。[#74551](https://github.com/StarRocks/starrocks/pull/74551)
- `jemalloc_conf` BE 設定項目が追加され、jemalloc のランタイムオプションが `information_schema.be_configs` を通じて確認できるようになりました。[#75344](https://github.com/StarRocks/starrocks/pull/75344)
- 保持されているチャンク容量を解放することで、共有なしモードでの主キー Compaction 中のメモリ使用量を削減する `compaction_chunk_reset_memory_tracker_threshold_percent` BE 設定が追加されました。[#75091](https://github.com/StarRocks/starrocks/pull/75091)
- staros を v4.1.1 にアップグレードしました。これには、再起動をまたいだ `datacache.enable` の永続化、ワーカーグループごとのシャードウォームアップタイムアウトのオーバーライド、S3 リトライジッターの改善が含まれます。[#75204](https://github.com/StarRocks/starrocks/pull/75204)
- SQL 文字列に資格情報マーカーが含まれていない場合に正規表現スキャンをスキップすることで、監査ホットパス上の SQL 資格情報のマスキングを最適化しました。[#74812](https://github.com/StarRocks/starrocks/pull/74812)
- Parquet スキャナーの式駆動オンデマンド遅延列ロードにより、マルチブランチの `OR` クエリでの不要な I/O が削減されました。[#74886](https://github.com/StarRocks/starrocks/pull/74886)
- `ds_hll_count_distinct` / `DataSketchesHll` は、順序依存の HIP エスティメーターの代わりに複合エスティメーターを使用することで、安定したカーディナリティ推定を生成するようになりました。[#75053](https://github.com/StarRocks/starrocks/pull/75053)

### セキュリティ {#security-1}

- [CVE-2026-45416] [CVE-2026-44249] [CVE-2026-45673] Netty を 4.1.135.Final にアップグレードし、SNI ハンドラーのヒープ枯渇（DoS）、IPv6 サブネットフィルターのバイパス、および DNS キャッシュポイズニングを修正しました。[#74668](https://github.com/StarRocks/starrocks/pull/74668)
- [CVE-2026-54512] [CVE-2026-54513] `jackson-databind` を 2.21.4 にアップグレードし、2 件のデシリアライゼーション脆弱性を修正しました。[#75373](https://github.com/StarRocks/starrocks/pull/75373)
- [GHSA-2r2c-cx56-8933] [GHSA-47qp-hqvx-6r3f] 未認証の Telnet サーバー DoS 脆弱性を修正するため、Hadoop の推移的依存関係から `org.jline:jline-remote-telnet` を除外しました。[#75066](https://github.com/StarRocks/starrocks/pull/75066)
- [CVE-2026-39822] pprof バイナリの脆弱性を修正するため、pprof プレビルドを更新しました。[#76248](https://github.com/StarRocks/starrocks/pull/76248) [#74669](https://github.com/StarRocks/starrocks/pull/74669)
- 述語の値に含まれる単一引用符がリテラル境界をエスケープする可能性があった、`information_schema.task_runs` における SQL インジェクションを修正しました。[#75520](https://github.com/StarRocks/starrocks/pull/75520)
- `tencent.cos.access_key`、`tencent.cos.secret_key`、および `iceberg.catalog.jdbc.password` が `SHOW CREATE CATALOG` の出力でマスクされるようになりました。[#74696](https://github.com/StarRocks/starrocks/pull/74696)
- 入力が切り詰められたパーセントエスケープシーケンスで終わる場合に `url_decode` で発生していた境界外読み取りを修正しました。[#75139](https://github.com/StarRocks/starrocks/pull/75139)
- `HyperLogLog::deserialize` が範囲外の `SPARSE` レジスタインデックスを受け入れてしまい、不正な形式の入力でヒープメモリが破損して BE がクラッシュする可能性があった問題を修正しました。[#75521](https://github.com/StarRocks/starrocks/pull/75521)
- `bar()` が負の幅の値を受け入れてしまい、無制限の文字列成長と BE のメモリ枯渇が発生していた問題を修正しました。[#75143](https://github.com/StarRocks/starrocks/pull/75143)

### バグ修正 {#bug-fixes-1}

以下の問題が修正されました。

- `add_files` が、論理的な型付き値ではなく Parquet の物理エンコーディングバイトで Iceberg のファイル範囲を設定していたため、ファイルレベルの最小値/最大値プルーニングが不正確になっていました（例えば `DECIMAL` 列で発生）。[#69207](https://github.com/StarRocks/starrocks/pull/69207)
- `ApplyTuningGuideRule` は、入力リストが不変の `List.of(...)` として構築されたプランノードを走査する際に `UnsupportedOperationException` をスローしていました。[#70785](https://github.com/StarRocks/starrocks/pull/70785)
- `INSERT OVERWRITE` の 2 フェーズ再プランでは、最初のプランニングセッションから古いラムダ引数の列参照 ID が生成されることがあり、`expr_type does not match slot_type` エラーが発生していました。[#73273](https://github.com/StarRocks/starrocks/pull/73273)
- GIN（転置）インデックスを持つテーブルに対する部分更新において、更新対象から GIN インデックス付き列が除外されている場合に、クエリが無期限にハングアップまたは失敗していました。[#73773](https://github.com/StarRocks/starrocks/pull/73773)
- ロウセットのスキーマと tablet のスキーマの間でスキーマドリフトが発生した場合、Lake PCU（部分列更新）がクラッシュするか、データが暗黙的に破損していました。[#74005](https://github.com/StarRocks/starrocks/pull/74005)
- 複数のスキーマ句を持つ外部 Iceberg テーブルに対する結合 `ALTER TABLE` が、句のディスパッチのたびにキューに入っていたすべてのアクションを誤って再実行していました。[#74036](https://github.com/StarRocks/starrocks/pull/74036)
- パーティションのフラッシュとリソースグループのキャンセルが交錯した際に `num_rows` のスナップショットが実際のチャンク行数を超えていたため、`PartitionedSpillerWriter` が `SIGSEGV` でクラッシュしていました。[#74081](https://github.com/StarRocks/starrocks/pull/74081)
- BE のシグナル初期化で SIGPIPE が無視されていなかったため、BE プロセスが起動中（通常はデプロイ直後）に予期せず終了することがありました。[#74424](https://github.com/StarRocks/starrocks/pull/74424)
- 行範囲フィルタリングによりストラクト VARCHAR サブフィールドの埋め込みがスキップされた場合に、Parquet の一時的な dict-code 列が上位層に漏洩し、型不一致が発生していました。[#74452](https://github.com/StarRocks/starrocks/pull/74452)
- `SELECT ... INTO OUTFILE` が、実際にエクスポートされた行数ではなく `ReturnRows=0` を監査ログに記録していました。[#74467](https://github.com/StarRocks/starrocks/pull/74467)
- ロックタイプの不一致により `TabletChecker.doCheck()` が `blockingAddTabletCtxToScheduler` で `IllegalMonitorStateException` をスローし、チェッカーのラウンド全体が黙って中断していました。[#74596](https://github.com/StarRocks/starrocks/pull/74596)
- `information_schema.COLUMNS` が `DATETIME_PRECISION` に対して常に `NULL` を返しており、そのフィールドから列サイズを導出する MySQL プロトコルクライアントが動作しなくなっていました。[#74623](https://github.com/StarRocks/starrocks/pull/74623)
- クエリが異なるデータベースまたはカタログ間で同じ非修飾名を持つ 2 つのテーブルを結合した場合、MV のリフレッシュが `Duplicate key` で失敗していました。[#74730](https://github.com/StarRocks/starrocks/pull/74730)
- 特定の条件下で、スピル可能なハッシュジョインのプローブがクラッシュしていました。[#74978](https://github.com/StarRocks/starrocks/pull/74978) [#75140](https://github.com/StarRocks/starrocks/pull/75140)
- 幅またはバケット数の引数がゼロの場合、Iceberg の `truncate` および `bucket` 変換関数が `SIGFPE` で BE をクラッシュさせていました。[#74998](https://github.com/StarRocks/starrocks/pull/74998)
- 被除数が `TYPE_MIN` で除数が `-1` の場合、`mod()` および `pmod()` が `SIGFPE` で BE をクラッシュさせていました。[#74980](https://github.com/StarRocks/starrocks/pull/74980)
- `bucket_num` がゼロまたは負の値の場合、`histogram()` が `SIGFPE` で BE をクラッシュさせていました。[#75041](https://github.com/StarRocks/starrocks/pull/75041)
- すべての入力行が `NULL` の場合、`encode_fingerprint_sha256` が `SIGSEGV` でクラッシュしていました。[#75042](https://github.com/StarRocks/starrocks/pull/75042)
- 単一文字のワイルドカード `_` を含む `LIKE` パターンが、GIN 転置インデックス経由で評価された際に不正な結果を返していました。[#75551](https://github.com/StarRocks/starrocks/pull/75551)
- 対象のセグメントが空の場合、AND のみの `MATCH` クエリが GIN 転置インデックスに対して不当なエラーを返していました。[#75161](https://github.com/StarRocks/starrocks/pull/75161)
- CLucene の `match_all` クエリが不正な結果を返していた問題を、CLucene の依存関係をアップグレードすることで解決しました。[#75180](https://github.com/StarRocks/starrocks/pull/75180)
- ベクトルインデックスの書き換えが合成距離列を共有テーブルスキーマ上に直接登録してしまい、同じテーブルに対する無関係な同時実行クエリで `Multiple entries with same key` エラーが発生していました。[#74785](https://github.com/StarRocks/starrocks/pull/74785)
- ジョインの並べ替えプルーニングにより、スキャン述語がまだ参照している列がプルーニングされることがあり、統計推定が `missing statistic of col` をスローしていました。[#74791](https://github.com/StarRocks/starrocks/pull/74791)
- `avg(DISTINCT x)` が sum/count マテリアライズドビュー経由で誤って書き換えられ、`DISTINCT` が暗黙的に削除され、重複が存在する場合に誤った結果が返されていました。[#75071](https://github.com/StarRocks/starrocks/pull/75071)
- `ALTER TABLE ... MODIFY COLUMN ... AFTER <nonexistent_col>` が、明確な意味的エラーではなく内部の `NullPointerException` をスローしていました。[#75073](https://github.com/StarRocks/starrocks/pull/75073)
- `SHOW CREATE ROUTINE LOAD` は、`COLUMNS TERMINATED BY` 句を持たないジョブにおいて、最初のロード記述句の前に余計な先頭カンマを出力していました。[#75522](https://github.com/StarRocks/starrocks/pull/75522)
- CTE を持つ `SECURITY INVOKER` ビューは、CTE 名が実際のテーブル参照と誤認された場合に、NPE により権限チェックが失敗することがありました。[#74813](https://github.com/StarRocks/starrocks/pull/74813)
- `ReduceCastRule` は、date/datetime の境界リテラルのシフトが表現可能な範囲をオーバーフローする場合（例えば `<= '9999-12-31'`）、`SemanticException` によってクエリプランニングを中断していました。[#75036](https://github.com/StarRocks/starrocks/pull/75036)
- `SplitJoinORToUnionRule` は、ジョイン条件が null-safe-equal（`<=>`）の論理和を使用している場合に、重複した行を出力していました。[#75038](https://github.com/StarRocks/starrocks/pull/75038)
- 複数テーブルの外部クエリにおいて並列メタデータ準備スレッド間で共有されていた `Tracers` により、`enable_profile=true` の下で `IllegalStateException` が発生していました。[#74746](https://github.com/StarRocks/starrocks/pull/74746)
- `ChunksPartitioner` 内のパーティションコンシューマエラーが黙って破棄されていたため、パーティション化された TopN がエラーを表面化させずに部分的または誤った結果を返すことがありました。[#74693](https://github.com/StarRocks/starrocks/pull/74693)
- BE の vacuum タスクは、FE 呼び出し元のタイムアウトが経過した後もゾンビとして実行され続け、`RELEASE_SNAPSHOT` スレッドプールを枯渇させ、vacuum のスループットを崩壊させていました。[#74694](https://github.com/StarRocks/starrocks/pull/74694)
- autovacuum のレースにより、実行中のトランザクションより1つ大きい `minActiveTxnId` が一時的に計算されることがあり、BE がまだ必要な結合トランザクションログを削除してしまい、publish が永続的に詰まってしまうことがありました。[#74906](https://github.com/StarRocks/starrocks/pull/74906)
- FE の EOS-cancel と BE のステージ2デプロイの間のレースにより、正常に完了したクエリがキャンセルされたと誤って表示されていました。[#75009](https://github.com/StarRocks/starrocks/pull/75009)
- 集計 TopN のランタイムフィルタのビルドキーが `ConstColumn` である場合、BE は `AggTopNRuntimeFilterUpdaterImpl` 内で `SIGSEGV` によりクラッシュしていました。[#74809](https://github.com/StarRocks/starrocks/pull/74809) [#74941](https://github.com/StarRocks/starrocks/pull/74941)
- すべての非 null 配列が空の場合、`array_map` / `transform` は `NULL` 行を黙って除外し、誤った行数を返していました。[#75141](https://github.com/StarRocks/starrocks/pull/75141)
- 2^64 を超える `LARGEINT` / `DECIMAL128` リテラルは、JIT コンパイルされた式内で黙って64ビットに切り詰められていました。[#75137](https://github.com/StarRocks/starrocks/pull/75137)
- UTF-8 文字列関数（`split`、`split_part`、`str_to_map`）は、最後の文字が切り詰められた、または不正なマルチバイトのリードバイトを持ち、区切り文字が空の場合に、文字列の終端を超えて読み取っていました。[#75068](https://github.com/StarRocks/starrocks/pull/75068)
- `parse_json()` は、不正な形式の JSON に対して、`ALLOW_THROW_EXCEPTION` SQL モードであってもクエリを失敗させる代わりに黙って `NULL` を返していました。[#74976](https://github.com/StarRocks/starrocks/pull/74976)
- 厳密モードの数値縮小キャストが、スロットデータが未定義である `NULL` 行に対して誤ってオーバーフローエラーを発生させていました。[#74903](https://github.com/StarRocks/starrocks/pull/74903)
- 1970年より前の Parquet `INT64` タイムスタンプで、サブ秒部分がゼロでない場合、負の切り捨て除算の余りにより不正な値にデコードされていました。[#75207](https://github.com/StarRocks/starrocks/pull/75207)
- 1970年より前の ORC `TIMESTAMP` 値は、ロード時にサブ秒コンポーネントが失われていました。[#75432](https://github.com/StarRocks/starrocks/pull/75432)
- ORC ストライプの min/max タイムスタンプ統計は、1970年より前およびサブ秒の境界において誤ってデコードされ、データファイルが誤ってプルーニングされる原因となっていました。[#75543](https://github.com/StarRocks/starrocks/pull/75543)
- `ARRAY`、`MAP`、または `STRUCT` 列内にネストされた `INT96` の Parquet タイムスタンプは、ロード時に1つのセッションタイムゾーンオフセットを失っていました。[#74868](https://github.com/StarRocks/starrocks/pull/74868)
- Parquet の `UINT_32` 値は、`BIGINT` 列にロードされる際、ゼロ拡張ではなく符号拡張されており、高ビットの符号なし整数に対して黙って負の値を格納していました。[#75002](https://github.com/StarRocks/starrocks/pull/75002)
- `HiveDataSource` のデストラクタは、`_scanner_ctx`（それらのノードを参照する述語を保持する）よりも先に `_pool`（およびその `Expr` ノード）を破棄していたため、heap-use-after-free を引き起こしていました。[#74818](https://github.com/StarRocks/starrocks/pull/74818)
- OpenX SerDe を使用して gzip 圧縮された JSON Hive 外部テーブルを読み取る際、マルチバイトの UTF-8 文字が 8 MB の解凍バッファ境界をまたぐ場合、`UTF8_ERROR` で失敗していました。[#74827](https://github.com/StarRocks/starrocks/pull/74827)
- ADLS2 `ListPaths` は、非 HNS アカウントにおいて、クライアントが無条件にアクセスする欠落した JSON フィールドが原因で `SIGSEGV` によりクラッシュしていました。[#75166](https://github.com/StarRocks/starrocks/pull/75166)
- 複数の `UNNEST` オペレータが同じ入力配列列を共有し、異なるサブフィールドを消費する場合、`unnest` はクラッシュするか誤った結果を返していました。[#75012](https://github.com/StarRocks/starrocks/pull/75012) [#75445](https://github.com/StarRocks/starrocks/pull/75445) [#76002](https://github.com/StarRocks/starrocks/pull/76002)
- `query_mem_limit` が `unnest` の実行中に強制されておらず、大きな配列に対する `unnest` がクエリを失敗させる代わりに BE を OOM でキルさせることがありました。[#75179](https://github.com/StarRocks/starrocks/pull/75179)
- `RANK` 境界を持つ `TopN` は、ランク制限がちょうどチャンク境界に落ちる場合に1行を失っていました。[#75045](https://github.com/StarRocks/starrocks/pull/75045)
- `PushDownDistinctAggregateRule` の後の列プルーニングにより、空の分析（ウィンドウ）オペレータが生成され、プランニングまたは実行のエラーを引き起こすことがありました。[#74810](https://github.com/StarRocks/starrocks/pull/74810)
- `EliminateSortColumnWithEqualityPredicateRule` は、グローバルな制限を設定せずにスキャンオペレータにのみ行数制限を設定していたため、制限付きサブクエリに対する `COUNT(*)` が同時実行下で予想以上の行数を返すことがありました。[#74983](https://github.com/StarRocks/starrocks/pull/74983)
- Lake の主キー永続性インデックスの再構築は、セグメント範囲モードで誤ったセグメントイテレータの位置を使用しており、キー範囲フィルタの適用が不正になっていました。[#74887](https://github.com/StarRocks/starrocks/pull/74887) [#75206](https://github.com/StarRocks/starrocks/pull/75206)
- `DROP PERSISTENT INDEX` はテーブルロックなしで `rebuildPindexVersion` を変更していました。復元後の `RestoreJob` は、DB READ ロックのみの下で MV のベーステーブル情報を変更していました。`FinalizeCreateTableAction` はイテレータの作成をまたいで DB レベルのロックを渡していました。[#74968](https://github.com/StarRocks/starrocks/pull/74968)
- `dumpImage` は、データベースごとのロックの取得がループ途中で例外を投げた場合、グローバルなメタロックを無期限に取り残してしまうことがありました。[#75488](https://github.com/StarRocks/starrocks/pull/75488)
- マルチステートメントの Stream Load は、トランザクションごとに1つの `TxnStateCallbackFactory` エントリをリークしており、無制限に増大して最終的に FE のヒープを枯渇させていました。[#75188](https://github.com/StarRocks/starrocks/pull/75188)
- catalog、データベース名、テーブル名、またはパーティション名が長い場合、ヒストグラム統計の `information_schema.task_runs` の行数が `primary_key_limit_size`（128バイト）をオーバーフローすることがありました。[#75735](https://github.com/StarRocks/starrocks/pull/75735)
- BE の JVM メトリクスは、無効な Prometheus の `# TYPE` 行（メトリック名内にラベルセットを含む）を出力しており、Prometheus がスクレイプ全体を中断する原因となっていました。[#75240](https://github.com/StarRocks/starrocks/pull/75240)
- `SHOW PARTITIONS` と `information_schema.partitions_meta` は、共有データテーブルにおいて、各パーティションの実際のバケット数ではなく、すべての物理パーティションのバケット数をテーブルレベルのデフォルト値として報告していました。[#75734](https://github.com/StarRocks/starrocks/pull/75734)
- `SHOW PROC '.../index_schema/<id>'` は、共有データ（`CLOUD_NATIVE`）テーブル上のすべてのロールアップインデックスに対して、ベーステーブルのスキーマを返していました。[#76069](https://github.com/StarRocks/starrocks/pull/76069)
- `ALTER TABLE ... MODIFY COLUMN` の no-op 句が誤って軽量コメントパスにルーティングされており、バッチ `ALTER TABLE` ステートメントで `MODIFY COLUMN COMMENT can not be combined with other alter operations` エラーを引き起こしていました。[#75736](https://github.com/StarRocks/starrocks/pull/75736)
- `isCommentOnlyModification` は、`isKey` / `aggregationType` の正規化が不正であったため、キー列や集計列をコメントのみの変更と誤認することがありました。[#75545](https://github.com/StarRocks/starrocks/pull/75545)
- `ALTER VIEW` は、循環参照するビュー定義をコミットしてしまうことがあり、その後の `SELECT` が `StackOverflowError` をスローする原因となっていました。[#75033](https://github.com/StarRocks/starrocks/pull/75033)
- `OrderedPartitionExchanger` がまだ前のチャンクへのポインタを保持している間に、下流のコンシューマがそのチャンクを変更した場合、heap-use-after-free が発生していました。[#75279](https://github.com/StarRocks/starrocks/pull/75279)
- ビルド側のスロット記述子が非 nullable であるにもかかわらず、ランタイム状態が nullable だった場合に NLJoin がクラッシュする問題を修正しました。[#75343](https://github.com/StarRocks/starrocks/pull/75343) [#75788](https://github.com/StarRocks/starrocks/pull/75788)
- 構造体フィールド名が解析可能な JSON パスでない場合に、`CAST(json/variant AS struct)` がフラグメント準備時に BE をクラッシュさせる問題を修正しました。[#75355](https://github.com/StarRocks/starrocks/pull/75355)
- ネストされた辞書式の Dict-decode 処理で、プロデューサーフラグメントとコンシューマーフラグメント間で互換性のない辞書変換が生成され、実行時に `Dict Decode failed` エラーが発生する場合がある問題を修正しました。[#75246](https://github.com/StarRocks/starrocks/pull/75246)
- `get_rowset_by_version` が `nullptr` を返し、`gtid` の比較が null チェックより前に置かれている場合に、schema change がヌルポインタ参照でクラッシュする問題を修正しました。[#74855](https://github.com/StarRocks/starrocks/pull/74855)
- スナップショットマネージャが親 tablet のメタデータを回収するかどうかを判断する際に reshard ジョブを考慮していなかったため、tablet の分割/マージ後に共有データクラスタのスナップショットが復元不可能になる問題を修正しました。[#75638](https://github.com/StarRocks/starrocks/pull/75638)
- ファイルバンドリングの vacuum が、兄弟 tablet のゼロ行バンドルセグメントを誤って非共有としてフラグ付けし、他の tablet がまだ参照している状態でそのバンドルファイルが削除されてしまう問題を修正しました。[#75689](https://github.com/StarRocks/starrocks/pull/75689)
- 共有データテーブルの Compaction publish で、Compaction トランザクション開始後に可視化された rollup/同期 MV インデックスが削除され、バンドルファイルにそれらのインデックスが含まれなくなる問題を修正しました。[#76105](https://github.com/StarRocks/starrocks/pull/76105)
- tablet の reshard 中に Compaction が破棄された場合、共有データの永続性インデックス Compaction がパススルー再利用中の SSTable ファイルを誤って削除する問題を修正しました。[#75726](https://github.com/StarRocks/starrocks/pull/75726)
- `NOT NULL` から nullable への flat-JSON 列スキーマ進化により、Compaction の読み取りパスで `CHECK` クラッシュが発生する問題を修正しました。[#75680](https://github.com/StarRocks/starrocks/pull/75680)
- nullable 列に対する `count_combine` が、ストリーミング事前集計のパススルーパスで `SIGSEGV` により BE をクラッシュさせる問題を修正しました。[#75298](https://github.com/StarRocks/starrocks/pull/75298)
- JDK 21 で削除されたリフレクティブな `DirectByteBuffer` コンストラクタルックアップが原因で、Java UDF が JDK 21 以降でロードに失敗する問題を修正しました。[#75666](https://github.com/StarRocks/starrocks/pull/75666)
- パーサーが `CREATE TABLE AS SELECT` 内の `ENGINE` 句をサポートしていなかったため、Unified catalog（Hive メタストア）への `CTAS` が常に失敗する問題を修正しました。[#75771](https://github.com/StarRocks/starrocks/pull/75771)
- `JoinTuningGuide` のフィードバック駆動型ジョイン再構築で `predicateCommonOperators` が失われ、共通部分式の再利用を含むプランで `InputDependenciesChecker` の検証エラーが発生する問題を修正しました。[#75773](https://github.com/StarRocks/starrocks/pull/75773)
- サブパーティションを持つテーブルで、バージョンリスト構築前に空のサブパーティションがプルーニングされる場合に、クエリキャッシュの正規化が `Preconditions.checkState` でクラッシュする問題を修正しました。[#75789](https://github.com/StarRocks/starrocks/pull/75789)
- `replayFromJson` がレガシーエイリアスで保存されたセッション変数を静かにスキップし、クエリダンプのリプレイがデフォルト値にフォールバックしてしまう問題を修正しました。[#75813](https://github.com/StarRocks/starrocks/pull/75813)
- 行グループの開始オフセットが二重に計算されることが原因で、Parquet の行グループを複数持つデータファイルに対して Iceberg の `_row_id` 仮想列が誤った値を返す問題を修正しました。[#75758](https://github.com/StarRocks/starrocks/pull/75758)
- Iceberg DELETE/UPDATE プランナーが物理テーブル ID ではなく合成テーブル ID でマッチングを行っていたため、対象のスキャンノードを特定できず、ベーススナップショット ID と競合検出フィルタが失われる問題を修正しました。[#76013](https://github.com/StarRocks/starrocks/pull/76013)
- `PipelineExecutorSet::start()` が呼び出される前に `cancel_plan_fragment` RPC が到着した場合に、`FragmentContext::set_final_status` が `SIGSEGV` でクラッシュする問題を修正しました。[#75030](https://github.com/StarRocks/starrocks/pull/75030)
- `FragmentExecutor` がまだフラグメントの終了処理中であるにもかかわらず `QueryContext` が回収され、`ResGuard::reset()` で heap-use-after-free が発生する問題を修正しました。[#74978](https://github.com/StarRocks/starrocks/pull/74978)
- `StringSearch::_pattern` が未初期化のままだったため、デフォルト構築された `search()` が未初期化のポインタを参照してしまう問題を修正しました。[#75614](https://github.com/StarRocks/starrocks/pull/75614)
- `DATETIME` マイクロ秒が JVM のデフォルトロケールの数字セットを使用してレンダリングされていたため、アラビア語やペルシャ語などのロケールで非 ASCII の数字が使用され、tablet の事前分割において境界値の解析が壊れる問題を修正しました。[#75001](https://github.com/StarRocks/starrocks/pull/75001)
- tablet の統計情報がリフレッシュされる前に `INSERT OVERWRITE` の後、パーティション行数が `_statistics_.column_statistics` にゼロとして書き込まれる可能性があり、オプティマイザがパーティションのカーディナリティ推定を崩してしまう問題を修正しました。[#74801](https://github.com/StarRocks/starrocks/pull/74801)
- グローバル設定が無効化されている場合、`enable_statistic_collect_on_first_load` のテーブルレベルのオーバーライドで初回ロード統計収集を有効にできない問題を修正しました。[#74794](https://github.com/StarRocks/starrocks/pull/74794)
- UNION の分岐に入力行がない場合に `PushDownNonGroupedAggregateBelowUnion` が非 nullable な宣言タイプで nullable な出力を生成し、BE の `CHECK` 失敗を引き起こす問題を修正しました。[#76101](https://github.com/StarRocks/starrocks/pull/76101)

## 4.1.2 {#412}

リリース日：2026 年 6 月 18 日

### 動作の変更 {#behavior-changes-2}

- ユーザーが権限を持たないデータベースへの接続時に、接続を ERROR 2013 で閉じる代わりに、正しい MySQL エラーパケットを返すようになりました。[#70072](https://github.com/StarRocks/starrocks/pull/70072)
- `SHOW FUNCTIONS` は、関数レベルの権限を通じて関数を参照できるが create-function スコープ権限を保有していないユーザーに対して、UDF のファイルパスおよびオブジェクトファイルパスを `***` としてマスクするようになりました。[#73425](https://github.com/StarRocks/starrocks/pull/73425)
- 外部 catalog から Hive のビューをクエリする際に、Ranger の行フィルタと列マスキングポリシーが正しく適用されるようになりました。[#73265](https://github.com/StarRocks/starrocks/pull/73265)
- `ALTER TABLE ... ADD COLUMN ... DEFAULT current_timestamp` は `current_timestamp` の生成式を正しく保持するようになりました。`DESCRIBE` と `information_schema` は、バックフィル時のリテラル値ではなく式を反映するようになりました。[#73455](https://github.com/StarRocks/starrocks/pull/73455)
- セッションのタイムゾーンが UTC+8 と異なるクラスタでも、`information_schema.loads` のロード時フィルタリングでフィルタ範囲がずれなくなりました。ロード時刻は FE と BE の境界を跨いで UTC エポックミリ秒として交換されるようになりました。[#73365](https://github.com/StarRocks/starrocks/pull/73365)
- `connector_max_split_size` セッション変数が、常にデフォルト値を使用するのではなく、Paimon のスキャン分割計算に正しく適用されるようになりました。[#71756](https://github.com/StarRocks/starrocks/pull/71756)
- `pipeline_enable_large_column_checker` はデフォルトで有効になりました。[#72798](https://github.com/StarRocks/starrocks/pull/72798)
- Hive のパーティション統計情報は、タイマーによってキー単位で自動的にリフレッシュされなくなりました。パーティション統計情報は、明示的な `refreshTable()` の呼び出し時にのみリフレッシュされるようになり、大規模なパーティションテーブルにおける HMS の負荷が軽減されました。[#73563](https://github.com/StarRocks/starrocks/pull/73563)
- Iceberg または外部 catalog のベーステーブルでスキーマドリフト（列タイプの変更、列の削除、テーブルの削除）が発生した場合、依存するマテリアライズドビューは、NULL 行や不明瞭なエラーを黙って生成する代わりに、次回のリフレッシュ時に非アクティブとしてマークされるようになりました。[#73770](https://github.com/StarRocks/starrocks/pull/73770)
- Iceberg コネクタは、`AND` の複合述語で一方の側のみ変換可能な場合に述語全体を破棄する代わりに、変換可能な側を部分的にプッシュダウンするようになり、パーティションプルーニングとデータスキッピングが改善されました。[#70293](https://github.com/StarRocks/starrocks/pull/70293)
- 明示的トランザクションの `COMMIT` は、データベースの書き込みロックに対して（ミリ秒ではなく）`query_timeout` 秒まで正しく待機するようになり、短時間の同時書き込みアクティビティ下での不要なロックタイムアウト障害を防止します。[#73549](https://github.com/StarRocks/starrocks/pull/73549)
- IVM リフレッシュは、フィルタされた行を黙って破棄する代わりに、strict-load フィルタエラーを呼び出し元に表示するようになりました。[#73938](https://github.com/StarRocks/starrocks/pull/73938)
- `count_combine(nullable_col)` は `COUNT(col)` のセマンティクスに合わせて、NULL 行を正しく除外するようになりました。`COUNT(<nullable column>)` を利用したインクリメンタル MV は、以前は水増しされたカウントをマテリアライズしていました。[#74029](https://github.com/StarRocks/starrocks/pull/74029)
- `SHOW ALTER TABLE COLUMN` は現在、クラウドネイティブ（共有データ）テーブルの `file_bundling` や `enable_persistent_index` などのプロパティに対して `ALTER TABLE ... SET (...)` によってトリガーされる非同期メタデータのみの alter ジョブも表示するようになりました。[#74198](https://github.com/StarRocks/starrocks/pull/74198)
- 集計関数を参照する `HAVING` 句を持つインクリメンタル MV を作成すると、初回リフレッシュ時に内部プランエラーを発生させる代わりに、`CREATE` 時に明確なエラーで失敗するようになりました。[#74054](https://github.com/StarRocks/starrocks/pull/74054)
- IVM は、インクリメンタルマテリアライズドビューにおいて `MIN`/`MAX(DECIMAL)` 集計関数をサポートするようになりました。[#73969](https://github.com/StarRocks/starrocks/pull/73969)
- IVM のアダプティブリフレッシュは、最初のデルタ特性がすでに `mv_max_rows_per_refresh` を超えている場合にデルタウィンドウを正しく制限するようになり、1回のタスク実行でバックログ全体がリフレッシュされることを防ぎます。[#74464](https://github.com/StarRocks/starrocks/pull/74464)
- GROUP-BY 専用のインクリメンタル MV（例：`SELECT k FROM t GROUP BY k`）は、`__ROW_ID__` を VARCHAR として正しくエンコードするようになり、2回目のリフレッシュ時のクラッシュが修正されました。[#74030](https://github.com/StarRocks/starrocks/pull/74030)

### 改善 {#improvements-2}

- Paimon ビューをサポートし、`CREATE`/`REPLACE`/`DROP`、`SHOW`/`DESC`、および外部 catalog からの Paimon ビューのクエリを含みます。Paimon ビュー内のテーブル参照は、`default_catalog` の代わりに Paimon catalog に対して解決されるようになりました。[#56058](https://github.com/StarRocks/starrocks/pull/56058) [#70217](https://github.com/StarRocks/starrocks/pull/70217)
- スキーマドリフトや複雑なネスト型を持つファイルを読み取る際に安定したスキーマ制御を行うため、`FILES()` で明示的な `schema` パラメータをサポートします。[#72033](https://github.com/StarRocks/starrocks/pull/72033)
- `get_query_profile()` は、接続している FE だけでなく、すべての FE ノードにわたるクエリプロファイル情報を取得するようになりました。[#71123](https://github.com/StarRocks/starrocks/pull/71123)
- 現在実行中のクエリの UUID を返す `query_id()` 組み込み関数が追加されました。[#73621](https://github.com/StarRocks/starrocks/pull/73621)
- 共有データモードの `CREATE`/`ALTER STORAGE VOLUME` は、メタデータを永続化する前にストレージロケーションのアクセス可能性（認証情報とエンドポイント）を検証するようになり、設定ミスに対して早期に失敗するようになりました。[#70053](https://github.com/StarRocks/starrocks/pull/70053)
- FE の既存の `AWS_S3_USE_WEB_IDENTITY_TOKEN_FILE` サポートに合わせて、BE における AWS S3 認証情報用の `WebIdentity` トークンプロバイダーサポートが追加されました。[#69966](https://github.com/StarRocks/starrocks/pull/69966)
- `txnlog` の欠落やセグメントの喪失、リモート I/O の遅延によって publish が恒久的にブロックされている場合に、共有データテーブル上のスタックした `COMMITTED` トランザクションのブロックを解除する `ADMIN SKIP COMMITTED TRANSACTION` コマンドが追加されました。[#73553](https://github.com/StarRocks/starrocks/pull/73553)
- `information_schema.tables_config` は `table_name` 述語を FE にプッシュダウンするようになり、単一テーブルのルックアップにおけるオーバーヘッドを大幅に削減します。[#73210](https://github.com/StarRocks/starrocks/pull/73210)
- 接続時のイントロスペクションで MySQL 8 のスキーマを検査する BI ツールや JDBC ドライバとの互換性を向上させるため、`information_schema` テーブルに欠落していた MySQL 8 の列が追加されました。[#73370](https://github.com/StarRocks/starrocks/pull/73370)
- `false` に設定された場合にセッション単位の変数を上書きする、クラスタ全体のキルスイッチとしての `enable_pipeline_event_scheduler` BE 設定が追加されました。[#73264](https://github.com/StarRocks/starrocks/pull/73264)
- 複数の幅広な文字列列を持つテーブルで統計情報を収集する際のクエリごとのメモリピークを削減するため、統計情報収集用のオプトインの幅広文字列列分離が追加されました。[#73258](https://github.com/StarRocks/starrocks/pull/73258)
- スロー lock のロギングは、高いロック競合下での JVM セーフポイントの停滞を防ぐため、イベントごとのレート制限と構成可能なスタックキャプチャ制御をサポートするようになりました。[#73647](https://github.com/StarRocks/starrocks/pull/73647)
- MV リフレッシュのログエントリにはプレフィックスにデータベース名が含まれるようになり、同じ MV 名が複数のスキーマに存在するマルチテナント環境でログ行を区別できるようになりました。[#73521](https://github.com/StarRocks/starrocks/pull/73521)
- `enable_profile_log` FE 設定は現在可変になり、FE の再起動なしに `ADMIN SET FRONTEND CONFIG` を介して実行時に切り替えることができます。[#73894](https://github.com/StarRocks/starrocks/pull/73894)
- ロードプロファイル（stream load、routine load、broker load、および merge-commit load）を `fe.profile.log` に書き込むための `enable_print_load_profile_to_log` FE 設定（デフォルト `false`）が追加され、クエリプロファイルのバーストによってインメモリストアが退避された場合でもプロファイルが保持されるようになりました。[#74150](https://github.com/StarRocks/starrocks/pull/74150)
- `SHOW ROUTINE LOAD` は、Java オブジェクト参照の代わりに `JobProperties` で列マッピングを正しくレンダリングするようになりました。[#74199](https://github.com/StarRocks/starrocks/pull/74199)
- `CachingIcebergCatalog` は catalog レベルのロックの代わりにテーブルレベルのロックを使用するようになり、多数の同時アクティブテーブルを持つ catalog でのリフレッシュのシリアル化遅延を削減します。[#73079](https://github.com/StarRocks/starrocks/pull/73079)
- メタスキャン（バックグラウンドの統計情報収集）は、変更後のセグメントファイルで not-found エラーとして失敗する代わりに、`ADD COLUMN`、`DROP COLUMN`、`RENAME COLUMN`、`REORDER COLUMN` のスキーマ変更を適切に処理するようになりました。[#72901](https://github.com/StarRocks/starrocks/pull/72901)
- サンプルベースの tablet プリスプリットは、マルチパーティションのレンジ分散テーブルと Broker Load もカバーするようになり、既存のデータ層ベースラインなしで初回ロードの並行性を実現します。[#73101](https://github.com/StarRocks/starrocks/pull/73101) [#73912](https://github.com/StarRocks/starrocks/pull/73912) [#74048](https://github.com/StarRocks/starrocks/pull/74048)
- MySQL 結果のシリアライゼーションは行ごとの仮想ディスパッチを使用しなくなり、型付きの列ライターがチャンクごとに1回だけ構築されるようになったことで、幅広または大規模な結果セットのシリアライゼーションオーバーヘッドが削減されました。[#66316](https://github.com/StarRocks/starrocks/pull/66316)
- `DATETIME`/`DATE` から文字列への cast は出力バッファに直接書き込むようになり、行ごとのヒープ割り当てが不要になりました。[#73801](https://github.com/StarRocks/starrocks/pull/73801)
- クエリ統計のマージパスは `SpinLock` をロックフリーの並列マップに置き換え、ワーカーが中間または最終的な統計情報を送信する際の大規模クラスタでの CPU 使用率を削減します。[#73796](https://github.com/StarRocks/starrocks/pull/73796)
- 集計のハッシュマップおよびハッシュセットのプリフェッチは L2 キャッシュの常駐状況によって制御されるようになり、バケット配列が L2 に収まる場合の4〜9%の性能低下を回避します。プリフェッチの距離は現在設定可能です。[#73943](https://github.com/StarRocks/starrocks/pull/73943)
- 共有データ Primary Key テーブルの軽量 compaction publish 用に、パイプライン化されたセグメントごとの `.lcrm` 読み取りが行われ、順次的なオブジェクトストレージへの往復が削減されます。[#73992](https://github.com/StarRocks/starrocks/pull/73992)
- 共有データモードにおけるコールド PK インデックスの再構築スキャンはセグメント間で並列化されるようになり、セグメントの読み取りがリモート I/O に律速される場合の再構築時間を削減します。[#74249](https://github.com/StarRocks/starrocks/pull/74249)
- 内部クエリ（統計情報収集、タスク実行、MV リフレッシュ）は `SHOW PROC '/current_queries'` に表示されるようになり、`KILL QUERY` で kill できるようになりました。[#74488](https://github.com/StarRocks/starrocks/pull/74488)
- S3 スロットリングの監視と `lake_vacuum_min_batch_delete_size` のチューニングのために、lake vacuum のバッチサイズおよびリトライ回数の bvar メトリクスが追加されました。[#74112](https://github.com/StarRocks/starrocks/pull/74112)
- リサイクルビンの増加が FE ヒープを圧迫する前に把握できるよう、`CatalogRecycleBin` サイズゲージメトリクスが追加されました。[#74440](https://github.com/StarRocks/starrocks/pull/74440)
- `LIST` パーティション化されたテーブルは、レンジパーティションテーブル向けに設計された latest-N ヒューリスティックを適用する代わりに、`OlapTableSink` ですべてのパーティションを開くようになり、増分オープンの RPC オーバーヘッドを削減します。[#74099](https://github.com/StarRocks/starrocks/pull/74099)
- `FILES()` または Broker Load を介して、`LARGE_LIST` および `FIXED_SIZE_LIST` の Arrow 型を JSON 列にロードすることをサポートします。[#73714](https://github.com/StarRocks/starrocks/pull/73714) [#73718](https://github.com/StarRocks/starrocks/pull/73718)
- 共有データテーブル上の merge-commit（`FRONTEND_STREAMING`）ロードにおいて、他のロードタイプと整合させるため、トランザクションログとファイルのバンドルを組み合わせてサポートします。[#74460](https://github.com/StarRocks/starrocks/pull/74460)
- FE の再起動なしに lake publish フェーズ内訳の警告しきい値を制御するため、可変な FE 設定 `slow_publish_partition_log_threshold_ms`（デフォルト 3000 ms）が追加されました。[#74043](https://github.com/StarRocks/starrocks/pull/74043)

### セキュリティ {#security-2}

- [CVE-2026-43869] 証明書のホスト検証の不備に対応するため、`libthrift` を 0.23.0 にバンプしました。[#73243](https://github.com/StarRocks/starrocks/pull/73243)
- [CVE-2026-41293] HTTP/2 リクエストヘッダー検証に対応するため、Apache Tomcat を 9.0.118 にアップグレードしました。[#73797](https://github.com/StarRocks/starrocks/pull/73797)
- [CVE-2026-45416] [CVE-2026-44249] [CVE-2026-45673] SNI ハンドラーのヒープ枯渇（DoS）、IPv6 サブネットフィルターのバイパス、DNS キャッシュポイズニングに対応するため、Netty を 4.1.135.Final にアップグレードしました。[#74668](https://github.com/StarRocks/starrocks/pull/74668)
- Go 標準ライブラリのセキュリティ修正を含めるため、pprof のビルド済みバイナリを Go 1.25.11 にアップグレードしました。[#73545](https://github.com/StarRocks/starrocks/pull/73545) [#74669](https://github.com/StarRocks/starrocks/pull/74669)

### バグ修正 {#bug-fixes-2}

以下の問題が修正されました：

- URL に `host:port` パターン外で `:` が含まれる場合、`parse_url()` が誤ったホストを返していました。[#63542](https://github.com/StarRocks/starrocks/pull/63542)
- 辞書変換された式が、成立しないケース（例：`IF(col = '1', NULL, 'ok')`）でも `f(null) = null` を前提としてしまっていました。[#69376](https://github.com/StarRocks/starrocks/pull/69376)
- トランザクションストリームロードがユーザー指定のタイムアウトではなくデフォルトの RPC タイムアウトを使用していたため、タイムアウトが早期に発生していました。[#67584](https://github.com/StarRocks/starrocks/pull/67584)
- アイデンティティ列に NULL 値を持つ Iceberg の等価削除ファイルは、ジョイン述語で `NULL = NULL` が UNKNOWN と評価されるため、一致する行を削除できていませんでした。[#67321](https://github.com/StarRocks/starrocks/pull/67321)
- `INJECTED` パーティションプロジェクション列を持つテーブルのエラーメッセージが、問題の原因となっている列をより明確に示すよう改善されました。[#68052](https://github.com/StarRocks/starrocks/pull/68052)
- insert-overwrite 操作が認識されなかったため、insert-only ACID Hive テーブルに対するクエリが期待より多くの行を返していました。[#71460](https://github.com/StarRocks/starrocks/pull/71460)
- 並行読み取り中に Iceberg のメタデータエントリがピン留めされた際、ディスクキャッシュが設定容量を超過していました。[#71651](https://github.com/StarRocks/starrocks/pull/71651)
- 外部カタログをクエリする際、Paimon の主キー列が誤って非 NULL 許容としてマークされていました。[#71660](https://github.com/StarRocks/starrocks/pull/71660)
- 複数の `ARRAY_AGG(DISTINCT <const>)` 入力を持つクエリに対して `MultiDistinctByMultiFuncRewriter` が同じルールを繰り返し適用したため、オプティマイザがタイムアウトしていました。[#70605](https://github.com/StarRocks/starrocks/pull/70605)
- `DATE`/`TIMESTAMP` キーワードなしでプッシュダウンされた Oracle JDBC の日付述語が NLS 形式エラーを引き起こしていました。[#71412](https://github.com/StarRocks/starrocks/pull/71412)
- パーティション TopN が、その子オペレーターから必要な出力列を失う場合がありました。[#72848](https://github.com/StarRocks/starrocks/pull/72848)
- パーティション進化を持つ Iceberg テーブルでは、パーティション化されていない MV を作成できませんでした。[#72285](https://github.com/StarRocks/starrocks/pull/72285)
- `information_schema.be_cloud_native_compactions` において、並列サブタスクの Compaction タスク統計が上書きされて失われていました。[#72331](https://github.com/StarRocks/starrocks/pull/72331)
- 同期 MV に対する `SHOW CREATE MATERIALIZED VIEW` が「Table is not found」エラーで失敗していました。[#73396](https://github.com/StarRocks/starrocks/pull/73396)
- 文の `.log` ファイルが 4 セグメントのパスに配置された場合、schema change 中に lake publish のマルチステートメントトランザクションがデッドロックしていました。[#73423](https://github.com/StarRocks/starrocks/pull/73423)
- ソートマージプロバイダーのエラーがフラグメントコンテキストに伝播されず、サイレントなクエリ失敗を引き起こしていました。[#73337](https://github.com/StarRocks/starrocks/pull/73337)
- 長時間稼働している Follower FE 上で `ConnectorTableId` が `int` から負の値へオーバーフローし、Iceberg および Hive のクエリが誤解を招く「Invalid table type」エラーで失敗していました。[#73344](https://github.com/StarRocks/starrocks/pull/73344)
- 空の最適化句（distribution も partition spec もなし）を伴う `ALTER TABLE` が誤ってパースされ、FE のリプレイ時にテーブルのデフォルトの distribution が破損する可能性がありました。[#73352](https://github.com/StarRocks/starrocks/pull/73352)
- `AZURE_PATH_KEY` が有効な `StorageVolumeMgr` パラメーターとして認識されなかったため、ADLS2 共有データディザスタリカバリ時に FE の起動が失敗していました。[#73509](https://github.com/StarRocks/starrocks/pull/73509)
- オプティマイザがネストされた型の一部を `UNKNOWN_TYPE` にプルーニングした場合、または NULL 許容の array、map、struct スキーマが使用された場合に、Avro の複合型デコードが失敗していました。[#73474](https://github.com/StarRocks/starrocks/pull/73474)
- 2 つの `NullableColumn` が同じ `NullColumn` オブジェクトを共有していたため、COW 列変更の最適化が `map_apply` および類似の関数でクラッシュを引き起こしていました。[#73480](https://github.com/StarRocks/starrocks/pull/73480)
- プロバイダーが FE 上で即座にインスタンス化されていたため、カスタム `LocationProvider` を持つ Iceberg テーブルで `ClassNotFoundException` を伴う `SELECT` クエリが失敗していました。[#73482](https://github.com/StarRocks/starrocks/pull/73482)
- JDBC の `getTable()` はキャッシュミスのたびに余分な `getTableComment()` の往復通信を行っており、負荷の高いプランニングフェーズのロック保持時間を延ばし、並行 DDL をブロックしていました。[#73488](https://github.com/StarRocks/starrocks/pull/73488)
- ネストされた MV が `FULL` または `UNKNOWN` の適時性を返した際、ネストされた MV のリフレッシュで `NullPointerException` がスローされていました。[#73644](https://github.com/StarRocks/starrocks/pull/73644)
- FE のワーカーが、速度の遅い MySQL クライアントへのクエリ結果送信で無期限にブロックしていました。結果送信パスに書き込みタイムアウトが適用されるようになりました。[#73646](https://github.com/StarRocks/starrocks/pull/73646)
- V1 エンコード（共有なし）から V2 エンコード（共有データ）クラスタへ、または 2 つの共有データクラスタ間で Primary Key テーブルをレプリケートする際、PK `.del` ファイルがトランスコードされていませんでした。[#73649](https://github.com/StarRocks/starrocks/pull/73649) [#73958](https://github.com/StarRocks/starrocks/pull/73958)
- `VERSION_INCOMPLETE` の復旧時に、古いレプリカ参照が生きているレプリカを追加する前に削除されなかったため、`TabletInvertedIndex` 内に重複したレプリカが蓄積していました。[#73661](https://github.com/StarRocks/starrocks/pull/73661)
- `REPLICATE_SNAPSHOT` タスクとファイル単位のコピーサブタスクが同じスレッドプールを共有していたため、共有データ lake レプリケーションのファイルコピーが CN をクラッシュさせていました。[#73666](https://github.com/StarRocks/starrocks/pull/73666)
- BE によって単位カウンターが `.000` の小数接尾辞でフォーマットされた際、`RuntimeProfileParser` が `NumberFormatException` をスローしていました。[#73683](https://github.com/StarRocks/starrocks/pull/73683)
- 共有データ PK の tablet 分割における共有セグメントの物理 rowid エンコーディングが不正で、`rss_rowid` エントリが誤っていました。[#73686](https://github.com/StarRocks/starrocks/pull/73686)
- レンジコロケートとハッシュ分散が混在する `JOIN` クエリが、有効な結果ではなく `Unknown error` を返していました。[#73702](https://github.com/StarRocks/starrocks/pull/73702)
- `TimeUtils.longToTimeString` は固定の UTC+8 フォーマッタを使用していましたが、出力はセッションの `time_zone` を尊重するようになりました。[#73619](https://github.com/StarRocks/starrocks/pull/73619)
- すべての値が `NULL` で、その列が NULL 許容の単項関数パスを通過した場合、Decimal 型の列がスケールを失い、後続の結果型が破損していました。[#73789](https://github.com/StarRocks/starrocks/pull/73789)
- ネストされた型に対する JSON の部分追加が ASAN クラッシュを引き起こしていました。[#73715](https://github.com/StarRocks/starrocks/pull/73715)
- `public` ロールの権限キャッシュが `GRANT`/`REVOKE` 時に無効化されず、失効するまで古い権限が有効なままになっていました。[#73717](https://github.com/StarRocks/starrocks/pull/73717)
- サブライターに追加がなかった場合に `FlatJson` がクラッシュする問題。[#73730](https://github.com/StarRocks/starrocks/pull/73730)
- MV 自体が `HAVING` 述語を持つ場合に集計 MV の書き換えが誤って適用され、不完全な結果が返される可能性がある問題。[#73610](https://github.com/StarRocks/starrocks/pull/73610)
- 並列マージモードに入る際にスピルライターの `auto_flush` フラグでデータ競合が発生し、ARM 上で意図しないセグメントフラッシュが発生する問題。[#73616](https://github.com/StarRocks/starrocks/pull/73616)
- Routine Load スケジューラーが Kafka または Pulsar のパーティションメタデータを取得するためにブロッキングの BE RPC を実行している間、ジョブ単位の書き込みロックを保持し続け、最大 33.6 秒のロック保持時間が発生する問題。[#73591](https://github.com/StarRocks/starrocks/pull/73591)
- `tablet_sched_disable_colocate_balance` が有効な場合に、停止した BE 上の Colocate Join tablet が誤って `HEALTHY` として報告される問題。[#73550](https://github.com/StarRocks/starrocks/pull/73550)
- MISSING（ファントム）レプリカ行が存在する場合に `ADMIN SHOW REPLICA STATUS` が MySQL の結果ストリームを非同期化させ、クライアントのハングまたは切断を引き起こす問題。[#74393](https://github.com/StarRocks/starrocks/pull/74393)
- 共有データモードにおいて、パーティションごとのコーディネータークレームが各送信者の `open` RPC のたびに再記録されず、一部の送信者がコーディネーター選出から漏れ、`combined_txn_log` ファイルが書き込まれないまま残る問題。[#73962](https://github.com/StarRocks/starrocks/pull/73962)
- `_statistics_` データベースまたはテーブルが削除された後、`_statistics_.pipe_file_list` 内部テーブルが再作成されない問題。[#73970](https://github.com/StarRocks/starrocks/pull/73970)
- `TaskCleaner` によって強制終了されたタスク実行がアーカイブされず、`information_schema.task_runs` から痕跡もなく消えてしまう問題。[#74146](https://github.com/StarRocks/starrocks/pull/74146)
- `RENAME TABLE` および `SWAP TABLE`/`SWAP MATERIALIZED VIEW` がデータベース書き込みロックではなく集中的なテーブルロックのみを保持していたため、並行する読み取りが不完全な中間の名前とテーブルのマッピングを観測できてしまう問題。[#74100](https://github.com/StarRocks/starrocks/pull/74100)
- PK インデックスの Compaction 出力 sstable が tablet メタデータなしで開かれ、恒久的な `metadata is null when loading delvec` の失敗を引き起こす問題。[#74037](https://github.com/StarRocks/starrocks/pull/74037)
- 同一トランザクション内ですでに変更されたテーブルを対象とした明示的トランザクション内の部分更新 `INSERT` が、`COMMIT` において暗黙的にデータを破損させる問題。[#74344](https://github.com/StarRocks/starrocks/pull/74344)
- レンジ分散と互換性のない `ALTER TABLE` 操作（schema change、ソートキーの変更）が、メタデータを暗黙的に破損させる代わりに、実行可能なエラーとともに拒否されるようになった。[#74020](https://github.com/StarRocks/starrocks/pull/74020)
- オプティマイザで子要素の型が一致しない集計関数が誤ったクエリ結果を引き起こす問題。[#74159](https://github.com/StarRocks/starrocks/pull/74159)
- 予約語をテーブル名に使用した `ALTER ROUTINE LOAD` が解析不能な `origStmt` を書き込み、FE 再起動後に列マッピングが失われる問題。[#74188](https://github.com/StarRocks/starrocks/pull/74188)
- IVM `state_union` の互換性チェックがネストされた型（例：`ARRAY<VARCHAR>`）まで再帰しないため、`ARRAY_AGG` の IMV に対して `CREATE MATERIALIZED VIEW` が失敗する問題。[#73627](https://github.com/StarRocks/starrocks/pull/73627)
- スキャン範囲が完全にフィルタリングされた場合、Parquet の一時的な辞書コード列が上位レイヤーに漏れ出し、下流での型不一致を引き起こす問題。[#74452](https://github.com/StarRocks/starrocks/pull/74452)
- 浮動小数点数と整数が混在した `WHEN` および結果タイプを持つ `CASE WHEN` が無効な JIT IR を生成し、誤った結果やクラッシュを引き起こす問題。[#74382](https://github.com/StarRocks/starrocks/pull/74382)
- JIT コンパイル失敗により `LLVMContext` の use-after-free が発生し、SIGSEGV を引き起こす問題。[#74396](https://github.com/StarRocks/starrocks/pull/74396)
- バックグラウンドの統計タスクがセッションの `WAREHOUSE` 設定を上書きし、同じ接続コンテキスト上のその後のユーザークエリに影響を与える問題。[#74385](https://github.com/StarRocks/starrocks/pull/74385)
- クラスタスナップショットが一度も正常に完了していない場合に `CatalogRecycleBin` がエントリの削除を停止し、`INSERT OVERWRITE` の負荷が高いワークロード下で FE のメモリが無制限に増加する問題。[#74379](https://github.com/StarRocks/starrocks/pull/74379)
- 非 PK レプリカのバージョン欠落が FE によって検出されず、クエリが固定された `max_version` を持つ欠落レプリカに恒久的にルーティングされる問題。[#74408](https://github.com/StarRocks/starrocks/pull/74408)
- `MaterializedIndexMeta.updateSchemaBackendId`（共有読み取りロック下で変更される `HashSet`）でのデータ競合により、エントリの喪失またはセットの破損が発生する可能性がある問題。[#74412](https://github.com/StarRocks/starrocks/pull/74412)
- 保持境界メタデータがすでに vacuum 済みの場合に vacuum ウォーターマークが正しく報告されず、`file_bundling` のバージョン切り替えクリーンアップが停止する問題。[#74429](https://github.com/StarRocks/starrocks/pull/74429)
- Lake vacuum のリトライが決定論的な指数バックオフを使用していた問題を修正し、S3 のスロットリング下で CN 間にリトライを分散させるために decorrelated jitter を追加した。[#74108](https://github.com/StarRocks/starrocks/pull/74108)
- クエリメモリプールから割り当てられた RPC リクエストがプロセスコンテキストで解放されていたため、`OlapTableSink` におけるメモリ計上が膨張していた問題。[#73807](https://github.com/StarRocks/starrocks/pull/73807)
- 自動パーティション作成が `_incremental_open_node_channel` を並行してトリガーした場合の `TabletSinkSender::_send_chunk_by_node` における競合状態。[#73820](https://github.com/StarRocks/starrocks/pull/73820)
- アップストリームの変更のバックポートによって作成された UDAF コンテキストが、`unique_ptr::release` 経由でメモリリークを引き起こす問題。[#74025](https://github.com/StarRocks/starrocks/pull/74025)
- `append_selective` におけるメモリ計上の不正確さに起因する、パーティション化されたジョインプローブでの範囲外アクセスの可能性。[#74315](https://github.com/StarRocks/starrocks/pull/74315)
- `azure_adls2_oauth2_client_endpoint` 設定フィールドの名前にタイプミスがあった問題。[#74581](https://github.com/StarRocks/starrocks/pull/74581)
- `StarMgrMetaSyncer` がレンジ Colocate Join の PACK シャードグループを孤立とみなして誤って回収し、共有データモードでアクティブなシャードを恒久的に削除してしまう問題。[#74117](https://github.com/StarRocks/starrocks/pull/74117)
- PRIMARY KEY テーブルおよび明示的な `ORDER BY` を持たないテーブルにおいて、Colocate Join の tablet 分割のソートキーのアリティがマテリアライズドスキーマではなくベーススキーマから解決されていたため、分割ジョブが tablet のサイズを縮小せずに完了してしまう問題。[#74409](https://github.com/StarRocks/starrocks/pull/74409)
- パーティションデータがマージしきい値を下回っている場合に、自動マージデーモンが事前分割済みの tablet を再統合してしまい、サンプルベースの事前分割による並行性の利点が損なわれる問題。[#74583](https://github.com/StarRocks/starrocks/pull/74583)
- `RESTORE ... AS <new_db>` 後、関数の `FunctionName.db` がソースデータベースを指し続けていたため、Follower FE 上で db レベルの UDF が欠落する問題。[#74313](https://github.com/StarRocks/starrocks/pull/74313)
- ウェアハウスに複数の CN グループがある場合に、共有データ `DISTRIBUTED BY RANDOM` の CTAS/INSERT において、不変パーティションの tablet ロケーションに誤った CN グループが割り当てられる問題。[#74316](https://github.com/StarRocks/starrocks/pull/74316)
- 統計推定中にパーティションが並行して削除された場合の `StatisticsCalcUtils` における `NullPointerException`。[#73711](https://github.com/StarRocks/starrocks/pull/73711)
- `InformationSchemaDataSource` および `FrontendServiceImpl` のメタデータ RPC ハンドラーが完全なデータベース READ ロックを保持し、無関係なテーブルへの DDL をブロックする問題。[#73936](https://github.com/StarRocks/starrocks/pull/73936) [#73913](https://github.com/StarRocks/starrocks/pull/73913)
- 共有コンテキストのオブザーバーに通知せずに完了状態を切り替えるパイプラインオペレーターが、イベントスケジューラー下でピアドライバーを停止させる可能性がある問題。[#74055](https://github.com/StarRocks/starrocks/pull/74055) [#74056](https://github.com/StarRocks/starrocks/pull/74056)
- 述語プッシュダウンにおける非ルートの複合述語が、UNION 下で不可能なネストされた AND ブランチが存在する場合に、スキャンレベルの EOF ではなく `NotPushDown` を返し、`OlapScanNode` が行を出力しなくなる問題。[#74218](https://github.com/StarRocks/starrocks/pull/74218)
- 単一の記憶媒体を持つ BE 上で `BackendLoadStatistic.init` がレプリカごとに高コストなスキャンを実行していた問題を修正し、同種ディスクの BE ではチェックが O(1) になった。[#73555](https://github.com/StarRocks/starrocks/pull/73555)
- データディレクトリロードスレッドにおけるスレッド名設定の競合により、BE起動のたびにノイズの多い `failed to set thread name` 警告が発生していました。[#73862](https://github.com/StarRocks/starrocks/pull/73862)
- タスクマネージャーが不正な `RUNNING→RUNNING` 編集ログを書き込み、タスク実行が実行中マップに無期限に停止したまま見える問題を修正しました。[#73882](https://github.com/StarRocks/starrocks/pull/73882)
- PKマルチステートメントバッチトランザクションが複合ロウセット全体で `num_rows`、`data_size`、`num_dels` を累積せず、共有データの主キーテーブルで行数統計が不正確になる問題を修正しました。[#74059](https://github.com/StarRocks/starrocks/pull/74059)
- LakeロードのスピルクリーンアップがトランザクションID基準のバキューム駆動型回収を使用するようになり、BEのクラッシュやOOM発生後に孤立したスピルファイルが残らないようになりました。[#73064](https://github.com/StarRocks/starrocks/pull/73064)
- `0000` 年以降のPostgreSQL JDBCの時刻値で、タイプマッピングの結果が不正確になる問題を修正しました。[#70842](https://github.com/StarRocks/starrocks/pull/70842)
- schema change 中にロウセットから `gtid` を読み取る前のNULLチェックが欠落しており、NPEクラッシュを引き起こしていました。[#74855](https://github.com/StarRocks/starrocks/pull/74855)

## 4.1.1 {#411}

リリース日：2026年5月29日

### 動作の変更 {#behavior-changes-3}

- Hive コネクタは、デフォルトでJNI Avroスキャナーの代わりにネイティブC++ Avroスキャナーを使用するようになりました。[#73237](https://github.com/StarRocks/starrocks/pull/73237) [#73569](https://github.com/StarRocks/starrocks/pull/73569)
- INCREMENTAL / AUTOマテリアライズドビューに対するクエリの書き換えが無効化され、INCREMENTAL / AUTOマテリアライズドビューに対する FORCE リフレッシュおよびパーティションリフレッシュが拒否されるようになりました。[#72890](https://github.com/StarRocks/starrocks/pull/72890) [#72336](https://github.com/StarRocks/starrocks/pull/72336) [#71355](https://github.com/StarRocks/starrocks/pull/71355)

### 改善 {#improvements-3}

- Java UDF/UDAF/UDTFがより多くのタイプをサポートするようになりました：UDAF/UDTFの引数と戻り値としての STRUCT、ネストされた ARRAY / MAP タイプ、DATE / DATETIME、DECIMAL、および可変長引数です。[#72911](https://github.com/StarRocks/starrocks/pull/72911) [#72283](https://github.com/StarRocks/starrocks/pull/72283) [#72337](https://github.com/StarRocks/starrocks/pull/72337) [#72208](https://github.com/StarRocks/starrocks/pull/72208) [#68596](https://github.com/StarRocks/starrocks/pull/68596)
- スカラーUDFが STRUCT 引数をサポートするようになりました。[#72620](https://github.com/StarRocks/starrocks/pull/72620)
- Python UDFがネストされた ARRAY / MAP タイプをサポートするようになりました。[#72210](https://github.com/StarRocks/starrocks/pull/72210)
- UDAFは一度だけロードおよび初期化され、クエリ間で再利用されるようになり、クエリごとのオーバーヘッドが削減されました。[#72038](https://github.com/StarRocks/starrocks/pull/72038)
- Hive コネクタ用のJNI AvroスキャナーをネイティブC++スキャナーに置き換え、直接バイナリデコードと `avro.schema.literal` および `avro.schema.url` のサポートを追加しました。[#73237](https://github.com/StarRocks/starrocks/pull/73237) [#73283](https://github.com/StarRocks/starrocks/pull/73283) [#73257](https://github.com/StarRocks/starrocks/pull/73257) [#73569](https://github.com/StarRocks/starrocks/pull/73569)
- CTAS文でのTrinoの `WITH` 句をサポートします。[#71960](https://github.com/StarRocks/starrocks/pull/71960)
- シンクパスにおけるIcebergの `timestamptz` パーティション変換のサポートを完成させました。[#73397](https://github.com/StarRocks/starrocks/pull/73397)
- Icebergテーブルの集計に対してTopNランタイムフィルタープッシュダウンを有効化しました。[#72332](https://github.com/StarRocks/starrocks/pull/72332)
- Icebergの日時最小値/最大値最適化をサポートします。[#71870](https://github.com/StarRocks/starrocks/pull/71870)
- 複数のHDFSクラスタへのアクセスをサポートするため、Catalog および BEにおけるHDFS HA構成のパススルーを許可します。[#71521](https://github.com/StarRocks/starrocks/pull/71521)
- 外部テーブルクエリに対するパーティションスキャン数の上限を追加しました。[#68480](https://github.com/StarRocks/starrocks/pull/68480)
- サポートされていないIceberg V3機能に対して早期に失敗するようになりました。[#70242](https://github.com/StarRocks/starrocks/pull/70242)
- INSERT INTO FILES 経由のCSVエクスポートで `csv.enclose` および `csv.escape` をサポートします。[#71589](https://github.com/StarRocks/starrocks/pull/71589)
- `files()` への完全なスキーマプッシュダウンのための `enable_push_down_schema` INSERT プロパティを追加しました。[#70978](https://github.com/StarRocks/starrocks/pull/70978)
- Routine Load ジョブは、再試行不可能なエラー（例：主キーサイズの超過）が発生した場合に一時停止されるようになりました。[#71161](https://github.com/StarRocks/starrocks/pull/71161)
- 2つの子から生じる複雑な式に対するジョインの並び替えをサポートします。[#71615](https://github.com/StarRocks/starrocks/pull/71615)
- `date_trunc`、`array_map`、CASE WHEN、IS NULL、UNION、および定数に対するMCV/NULL割合の伝播を含む、CBO統計推定を改善しました。[#72233](https://github.com/StarRocks/starrocks/pull/72233) [#70372](https://github.com/StarRocks/starrocks/pull/70372) [#70221](https://github.com/StarRocks/starrocks/pull/70221) [#70865](https://github.com/StarRocks/starrocks/pull/70865) [#70989](https://github.com/StarRocks/starrocks/pull/70989) [#71000](https://github.com/StarRocks/starrocks/pull/71000)
- スキュージョインの検出を改善しました：すべてのジョインキーがスキューしている場合にのみスキューが検出されるようになり、スキュールールを強制する `force_group_by_skew_eliminate_when_skewed` スイッチが追加されました。[#72753](https://github.com/StarRocks/starrocks/pull/72753) [#71382](https://github.com/StarRocks/starrocks/pull/71382)
- FEにおける `regexp_replace` の定数畳み込みをサポートします。[#70804](https://github.com/StarRocks/starrocks/pull/70804)
- 定数パーティション値を持つ日付パーティション列に対する MIN / MAX を最適化しました。[#69880](https://github.com/StarRocks/starrocks/pull/69880)
- マテリアライズドビューのリフレッシュにおいて、`ASYNC` の同義語として `SCHEDULE` キーワードを導入しました。[#72329](https://github.com/StarRocks/starrocks/pull/72329)
- 共有データモードのLakeテーブルに対する tablet 作成の再試行をサポートします。[#71068](https://github.com/StarRocks/starrocks/pull/71068)
- Lakeカラムモードの部分更新に対する条件付き更新をサポートします。[#71961](https://github.com/StarRocks/starrocks/pull/71961)
- 取り込みスループットを向上させるため、部分更新のパブリッシュ、永続性インデックスの初期化、SSTableのオープンを並列化しました。[#71652](https://github.com/StarRocks/starrocks/pull/71652) [#71217](https://github.com/StarRocks/starrocks/pull/71217) [#72112](https://github.com/StarRocks/starrocks/pull/72112) [#71145](https://github.com/StarRocks/starrocks/pull/71145) [#72986](https://github.com/StarRocks/starrocks/pull/72986)
- 共有なしから共有データへのレプリケーション中のDCGファイル同期をサポートします。[#69339](https://github.com/StarRocks/starrocks/pull/69339)
- キー列と非キー列の両方における VARCHAR 長の拡張に対するスキーマ進化をサポートします。[#70747](https://github.com/StarRocks/starrocks/pull/70747)
- クラスタスナップショットの整合性チェック用に `snapshot_meta.json` マーカーを追加しました。[#71209](https://github.com/StarRocks/starrocks/pull/71209)
- DNパターンを介したLDAPダイレクトバインド認証をサポートします。[#71559](https://github.com/StarRocks/starrocks/pull/71559)
- クエリのトラブルシューティングを容易にする `get_query_dump_from_query_id` メタ関数を追加しました。[#72875](https://github.com/StarRocks/starrocks/pull/72875)
- 監査ログでクエリされたリレーションの監査をサポートしました。[#71596](https://github.com/StarRocks/starrocks/pull/71596)
- MySQL バイナリ結果エンコーディング用のセッション変数を追加しました。[#71415](https://github.com/StarRocks/starrocks/pull/71415)
- 共有データクラスタ向けの `tablet_num`、`MemtableIOSpeed`、`staros_shard_count`、および Iceberg メタデータテーブルクエリメトリクスなど、可観測性向上のためのメトリクスを追加しました。[#71444](https://github.com/StarRocks/starrocks/pull/71444) [#69842](https://github.com/StarRocks/starrocks/pull/69842) [#73096](https://github.com/StarRocks/starrocks/pull/73096) [#70825](https://github.com/StarRocks/starrocks/pull/70825)
- FE 設定 `deploy_serialization_min_thread_pool_size` を追加しました。[#72274](https://github.com/StarRocks/starrocks/pull/72274)
- MergeTabletJob の作成を無効にする `tablet_reshard_enable_tablet_merge` 設定を追加しました。[#70906](https://github.com/StarRocks/starrocks/pull/70906)
- `SO_REUSEPORT` によって HTTP サーバーの accept でのサンダリングハード問題を解消しました。[#72956](https://github.com/StarRocks/starrocks/pull/72956)
- `CREATE FUNCTION ... AS <sql_body>` を介した SQL UDF の作成をサポートしました。[#67558](https://github.com/StarRocks/starrocks/pull/67558)
- S3 からの UDF のロードをサポートしました。[#64541](https://github.com/StarRocks/starrocks/pull/64541)
- 時間順に並んだ UUID v7 値を生成する `uuid_v7` 関数を追加しました。[#67694](https://github.com/StarRocks/starrocks/pull/67694)
- external catalog の可観測性向上のため、catalog タイプごとのクエリメトリクスを追加しました。[#70533](https://github.com/StarRocks/starrocks/pull/70533)
- ウィンドウ関数向けの明示的なスキューヒントをサポートし、パーティションキーに偏りのあるウィンドウ関数を UNION に分割することで自動的に最適化します。[#68739](https://github.com/StarRocks/starrocks/pull/68739)

### セキュリティ {#security-3}

- [CVE] Netty を 4.1.133.Final にアップグレードしました。[#72905](https://github.com/StarRocks/starrocks/pull/72905)
- [CVE-2026-42198] [CVE-2026-5598] pgjdbc を 42.7.11（無制限の SCRAM PBKDF2 反復回数によるクライアント側 DoS）に、BouncyCastle を 1.84（FrodoKEM 秘密鍵の漏洩）にバンプしました。[#72797](https://github.com/StarRocks/starrocks/pull/72797)
- [CVE-2026-32280] [CVE-2026-32282] Golang の CVE を解消するため、pprof を go1.25.9 でビルドしました。[#71944](https://github.com/StarRocks/starrocks/pull/71944) [#73545](https://github.com/StarRocks/starrocks/pull/73545)
- jetty-http を 9.4.58.v20250814 にアップグレードしました。[#71762](https://github.com/StarRocks/starrocks/pull/71762)
- Broker 依存関係の CVE をクリーンアップし、`wildfly-openssl` を削除しました。[#72184](https://github.com/StarRocks/starrocks/pull/72184) [#71908](https://github.com/StarRocks/starrocks/pull/71908)
- INSERT INTO FILES のエラーメッセージ内の認証情報をマスクしました。[#71245](https://github.com/StarRocks/starrocks/pull/71245)

### バグ修正 {#bug-fixes-3}

以下の問題を修正しました：

- `hash_util` の静的初期化順序に起因する起動時の CN セグメンテーション違反。[#71825](https://github.com/StarRocks/starrocks/pull/71825)
- 物理分割が有効な状態で空の tablet をスキャンした際の CN クラッシュ。[#70281](https://github.com/StarRocks/starrocks/pull/70281)
- `information_schema.warehouse_queries` をクエリした際の BE クラッシュ。[#72019](https://github.com/StarRocks/starrocks/pull/72019)
- rowset の `num_rows` がゼロの場合の Lake Compaction における SIGFPE。[#71742](https://github.com/StarRocks/starrocks/pull/71742)
- ExecutionDAG フラグメント接続におけるゼロ除算。[#67918](https://github.com/StarRocks/starrocks/pull/67918)
- SinkBuffer での正常終了時のクラッシュ。[#73202](https://github.com/StarRocks/starrocks/pull/73202)
- スピル可能なハッシュジョインのプローブでのクラッシュ。[#72397](https://github.com/StarRocks/starrocks/pull/72397)
- 一時的な `std::string` へのフォーマット時のスタックバッファオーバーフロー。[#72728](https://github.com/StarRocks/starrocks/pull/72728)
- `reverse(DecimalV3)` でのクラッシュ。[#71834](https://github.com/StarRocks/starrocks/pull/71834)
- 一時的な `shared_ptr` の破棄に起因する `LoadChannel::get_load_replica_status` での use-after-free。[#71843](https://github.com/StarRocks/starrocks/pull/71843)
- スレッド作成が失敗した際の `ThreadPool::do_submit` での use-after-free。[#71276](https://github.com/StarRocks/starrocks/pull/71276)
- フラグメントの解体処理中に発生する Hive パーティションディスクリプタの use-after-free。[#73176](https://github.com/StarRocks/starrocks/pull/73176)
- インフォメーションスキーマのシンクにおける use-after-free。[#71513](https://github.com/StarRocks/starrocks/pull/71513)
- HttpClient インスタンスの再利用による FE のファイルディスクリプタリーク。[#73239](https://github.com/StarRocks/starrocks/pull/73239)
- `JDBCScanner::_init_jdbc_scanner` における JNI ローカルリファレンスリーク。[#72913](https://github.com/StarRocks/starrocks/pull/72913)
- MV プランコンテキストのキャッシュ時に発生するメモリリーク。[#72300](https://github.com/StarRocks/starrocks/pull/72300)
- ローカル交換における予期しないメモリ過剰使用。[#72262](https://github.com/StarRocks/starrocks/pull/72262)
- Lake `publish_version` における `response->tablet_metas` の競合。[#73274](https://github.com/StarRocks/starrocks/pull/73274)
- `DeltaWriter::commit()` における `SegmentFlushTask` の並行競合。[#73371](https://github.com/StarRocks/starrocks/pull/73371)
- シリアライゼーション中の `RuntimeProfile` min/max レース。[#72904](https://github.com/StarRocks/starrocks/pull/72904)
- クエリコンテキスト破棄中の `PipelineTimerTask` におけるレースコンディション。[#73082](https://github.com/StarRocks/starrocks/pull/73082)
- `_all_global_rf_ready_or_timeout` におけるレースコンディション。[#70920](https://github.com/StarRocks/starrocks/pull/70920)
- `map_apply` と `array_length` における共有 `NullColumn` の問題。[#71258](https://github.com/StarRocks/starrocks/pull/71258)
- パーティションバージョンのギャップによって引き起こされるバッチパブリッシュのデッドロック。[#71483](https://github.com/StarRocks/starrocks/pull/71483)
- 共有なしモードで rowset メタデータの LRU キャッシュをウォームアップする際のデッドロック。[#71459](https://github.com/StarRocks/starrocks/pull/71459)
- `Locker` のロールバックが例外安全ではなく、アンロック順序が誤っている問題。[#72789](https://github.com/StarRocks/starrocks/pull/72789)
- 読み取り専用パスおよびメタデータパスにおける複数の DB ロックによって引き起こされる、DDL と StarOS RPC のロック競合。[#73067](https://github.com/StarRocks/starrocks/pull/73067) [#72475](https://github.com/StarRocks/starrocks/pull/72475) [#72108](https://github.com/StarRocks/starrocks/pull/72108) [#72218](https://github.com/StarRocks/starrocks/pull/72218) [#72178](https://github.com/StarRocks/starrocks/pull/72178)
- プロジェクトノードの欠落による誤った shuffle 分散。[#71075](https://github.com/StarRocks/starrocks/pull/71075)
- AGG TopN ランタイムフィルターの `exprOrder` 不一致によるクラッシュと誤った結果。[#71479](https://github.com/StarRocks/starrocks/pull/71479)
- dict-merge GROUP BY による誤った結果。[#70866](https://github.com/StarRocks/starrocks/pull/70866)
- Query Cache とローカル shuffle 集計の競合。[#73194](https://github.com/StarRocks/starrocks/pull/73194)
- flat JSON におけるグローバル辞書生成の不整合。[#72953](https://github.com/StarRocks/starrocks/pull/72953)
- Flat JSON のマージ時の空値の不整合。[#72973](https://github.com/StarRocks/starrocks/pull/72973)
- 明示的なキー/値タイプが宣言されている場合の map リテラルにおけるタイプの不一致。[#71316](https://github.com/StarRocks/starrocks/pull/71316)
- JOIN USING トランスフォーマーにおいて、COALESCE の子要素が共通のタイプにキャストされない問題。[#72338](https://github.com/StarRocks/starrocks/pull/72338)
- グローバル変数を用いた reduce-cast 後に VARCHAR の長さが保持されない問題。[#70269](https://github.com/StarRocks/starrocks/pull/70269)
- MySQL 結果セット内のネストされたタイプにおいて VARBINARY が誤ってエンコードされる問題。[#71346](https://github.com/StarRocks/starrocks/pull/71346)
- 小さな LIMIT で集計のスピルを無効化した際の check-having-clause の問題。[#72705](https://github.com/StarRocks/starrocks/pull/72705)
- 日付解析前に引用符が除去されない問題、および PostgreSQL の日付/時刻のバグ。[#48517](https://github.com/StarRocks/starrocks/pull/48517) [#71016](https://github.com/StarRocks/starrocks/pull/71016)
- データファイルの共有フラグが失われ、vacuum が兄弟の split tablet からまだ参照されているファイルを削除してしまう問題。[#71585](https://github.com/StarRocks/starrocks/pull/71585)
- split→compaction→merge のシーケンスにおける tablet マージの正確性の問題。[#72350](https://github.com/StarRocks/starrocks/pull/72350)
- tablet の split 中にクロスパブリッシュされたトランザクションログの num_rows/data_size が肥大化する問題。[#71144](https://github.com/StarRocks/starrocks/pull/71144)
- 同一パブリッシュバッチ内での compaction 前の書き込みによって発生する Delvec の孤立エントリ。[#71001](https://github.com/StarRocks/starrocks/pull/71001)
- StarMgr ジャーナルのリプレイを同期させることで、follower FE 上の「クエリ可能なレプリカがありません」問題を解消。[#71263](https://github.com/StarRocks/starrocks/pull/71263)
- 通常の rowset コミット適用時に `merge_condition` が保持されない問題。[#72542](https://github.com/StarRocks/starrocks/pull/72542)
- 誤ったスナップショット ID とフィルターを使用した Iceberg DELETE の競合検出。[#73354](https://github.com/StarRocks/starrocks/pull/73354)
- 無効な Iceberg 変換引数による NPE。[#71917](https://github.com/StarRocks/starrocks/pull/71917)
- プランナーによって注入された余分な列が原因で、Iceberg の min/max 最適化がスキップされる問題。[#71863](https://github.com/StarRocks/starrocks/pull/71863)
- Iceberg ベーステーブルにおける集計ジョインプッシュダウンの MV 書き換え。[#71856](https://github.com/StarRocks/starrocks/pull/71856)
- INSERT OVERWRITE コミット前に Hive パーティションディレクトリが欠落している問題。[#71810](https://github.com/StarRocks/starrocks/pull/71810)
- JNI スキャナーに AWS assume-role が適用されない問題。[#71422](https://github.com/StarRocks/starrocks/pull/71422)
- プルーニングされた子要素とネストされた nullable なスキーマに対する Avro の複合タイプデコード。[#73474](https://github.com/StarRocks/starrocks/pull/73474)
- Parquet の Broker Load エラーにファイル/列/行のコンテキストが欠落している問題。[#73236](https://github.com/StarRocks/starrocks/pull/73236)
- Parquet スキャナーにおける Arrow 辞書値のサポート不足。[#71855](https://github.com/StarRocks/starrocks/pull/71855)
- Paimon テーブルの Primary Key が SHOW CREATE に表示されず、DESC の戻り値にも表示されない問題。[#70535](https://github.com/StarRocks/starrocks/pull/70535)
- PostgreSQL/Oracle の JDBC タイプ互換性、および末尾にスラッシュが付く JDBC URL の構築の問題。[#70626](https://github.com/StarRocks/starrocks/pull/70626) [#70992](https://github.com/StarRocks/starrocks/pull/70992)
- JDBC catalog における SQL Server テーブルでのマテリアライズドビューリフレッシュの問題。[#72962](https://github.com/StarRocks/starrocks/pull/72962)
- 外部ジョインを含むマテリアライズドビューにおける遅延実体化スロットの nullability の問題。[#72621](https://github.com/StarRocks/starrocks/pull/72621)
- AUTO および INCREMENTAL のマテリアライズドビューパーティションリフレッシュが拒否される問題。[#71355](https://github.com/StarRocks/starrocks/pull/71355)
- マテリアライズドビューが非アクティブになった後もマテリアライズドビュースケジューラが停止しない。 [#71265](https://github.com/StarRocks/starrocks/pull/71265)
- MySQL クライアントの互換性のために `SHOW GRANTS FOR CURRENT_USER()` のサポートが不足している。 [#71959](https://github.com/StarRocks/starrocks/pull/71959)
- SHOW ステートメントが明示的なトランザクション内で許可されていない。 [#72954](https://github.com/StarRocks/starrocks/pull/72954)
- Arrow Flight が空の結果セットに対して列名 `r` を返す。 [#71534](https://github.com/StarRocks/starrocks/pull/71534)
- Java UDF コードで JNI 例外処理のチェックが不足している。 [#71734](https://github.com/StarRocks/starrocks/pull/71734)
- `ai_query` 関数の登録に関する問題。 [#72103](https://github.com/StarRocks/starrocks/pull/72103)
- `enable_load_profile` を使用した場合の Stream Load プロファイル収集の問題。 [#71952](https://github.com/StarRocks/starrocks/pull/71952)
- プロファイルの START_TIME/END_TIME がセッションのタイムゾーンで表示されない。 [#71429](https://github.com/StarRocks/starrocks/pull/71429)
- `star_mgr_meta_sync_interval_sec` が実行時に変更できない。 [#71675](https://github.com/StarRocks/starrocks/pull/71675)
- `information_schema.tables` が等価述語内の特殊文字をエスケープしない。 [#71273](https://github.com/StarRocks/starrocks/pull/71273)
- エラーパスでの並列セグメント/ロウセットロード時に解放後使用（use-after-free）が発生する。 [#71083](https://github.com/StarRocks/starrocks/pull/71083)
- 集計スピル `set_finishing` におけるハッシュテーブルのデータ損失の可能性。 [#70851](https://github.com/StarRocks/starrocks/pull/70851)
- ディスクの再マイグレーション（A→B→A）中の GC 競合によって引き起こされる PK tablet のロウセットメタ損失。 [#70727](https://github.com/StarRocks/starrocks/pull/70727)
- `SharedDataStorageVolumeMgr` における DB 読み取りロックのリーク。 [#70987](https://github.com/StarRocks/starrocks/pull/70987)
- IVM リフレッシュが不完全な PCT パーティションメタデータを記録する。 [#71092](https://github.com/StarRocks/starrocks/pull/71092)
- 参照先の列が存在しない場合、Stream Load/Broker Load で生成列を解析する際に NPE が発生する。 [#71116](https://github.com/StarRocks/starrocks/pull/71116)
- 短絡ポイントルックアップでパーティション述語が欠落している。 [#71124](https://github.com/StarRocks/starrocks/pull/71124)

## 4.1.0 {#410}

リリース日: 2026 年 4 月 13 日

### 共有データアーキテクチャ {#shared-data-architecture}

- **新しいマルチテナントデータ管理**

  共有データクラスタは、レンジベースのデータ分散、および tablet の自動分割・マージをサポートするようになりました。tablet がサイズ過大やホットスポットになった場合、schema change、SQL の変更、データの再取り込みを必要とせずに自動的に分割できます。この機能はユーザビリティを大幅に向上させ、マルチテナントワークロードにおけるデータスキューやホットスポットの問題に直接対処します。 [#65199](https://github.com/StarRocks/starrocks/pull/65199) [#66342](https://github.com/StarRocks/starrocks/pull/66342) [#67056](https://github.com/StarRocks/starrocks/pull/67056) [#67386](https://github.com/StarRocks/starrocks/pull/67386) [#68342](https://github.com/StarRocks/starrocks/pull/68342) [#68569](https://github.com/StarRocks/starrocks/pull/68569) [#66743](https://github.com/StarRocks/starrocks/pull/66743) [#67441](https://github.com/StarRocks/starrocks/pull/67441) [#68497](https://github.com/StarRocks/starrocks/pull/68497) [#68591](https://github.com/StarRocks/starrocks/pull/68591) [#66672](https://github.com/StarRocks/starrocks/pull/66672) [#69155](https://github.com/StarRocks/starrocks/pull/69155)

- **大容量 tablet サポート（フェーズ 1）**

  共有データクラスタが tablet ごとに格納できるデータ量を大幅に増やせるようにし、長期的な目標として tablet あたり 100 GB を目指します。フェーズ 1 では、取り込み、Primary Key の更新、Compaction の全パイプラインにわたる tablet 内並列処理を導入し、Lake tablet が大きくなってもシングルスレッドのボトルネックにならないようにしました。改善点には、単一の tablet 内での並列 Compaction（セグメントレベルの分割を含む）、Lake ロード（ロードスピルパスを含む）における MemTable の並列ファイナライズ・フラッシュ・マージ、Primary Key テーブルの tablet 内並列パブリッシュおよび並列条件更新、リモートストレージマッパーファイルをサポートするクラウドネイティブ Primary Key インデックスのレンジ分割/並列/サイズ階層型 Compaction が含まれます。これらの変更により、大規模 tablet ワークロードにおける取り込みメモリのオーバーヘッド、Compaction の増幅、FE メタデータへの負荷が大幅に削減されます。 [#66424](https://github.com/StarRocks/starrocks/pull/66424) [#66522](https://github.com/StarRocks/starrocks/pull/66522) [#66778](https://github.com/StarRocks/starrocks/pull/66778) [#66586](https://github.com/StarRocks/starrocks/pull/66586) [#67432](https://github.com/StarRocks/starrocks/pull/67432) [#67478](https://github.com/StarRocks/starrocks/pull/67478) [#67554](https://github.com/StarRocks/starrocks/pull/67554) [#66796](https://github.com/StarRocks/starrocks/pull/66796) [#67392](https://github.com/StarRocks/starrocks/pull/67392) [#67878](https://github.com/StarRocks/starrocks/pull/67878) [#65908](https://github.com/StarRocks/starrocks/pull/65908) [#68677](https://github.com/StarRocks/starrocks/pull/68677) [#68123](https://github.com/StarRocks/starrocks/pull/68123) [#69865](https://github.com/StarRocks/starrocks/pull/69865)

- **Fast Schema Evolution V2**

  共有データクラスタは Fast Schema Evolution V2 をサポートするようになり、schema 操作の秒単位での DDL 実行が可能になりました。さらに、このサポートはマテリアライズドビューにも拡張されています。 [#65726](https://github.com/StarRocks/starrocks/pull/65726) [#66774](https://github.com/StarRocks/starrocks/pull/66774) [#67915](https://github.com/StarRocks/starrocks/pull/67915)

- **[Beta] 共有データ上の転置インデックス**

  共有データクラスタで組み込みの転置インデックスを有効にし、テキストフィルタリングや全文検索ワークロードを高速化します。 [#66541](https://github.com/StarRocks/starrocks/pull/66541)

- **キャッシュの可観測性**

  クエリレベルのキャッシュヒット率が監査ログおよびモニタリングシステムに公開されるようになり、キャッシュの透明性とレイテンシー診断が向上しました。追加の Data Cache メトリクスには、メモリおよびディスクのクォータ使用状況とページキャッシュの統計が含まれます。 [#63964](https://github.com/StarRocks/starrocks/pull/63964)

- Lake テーブル向けにセグメントメタデータフィルターを追加し、スキャン時にソートキーの範囲に基づいて無関係なセグメントをスキップすることで、範囲述語クエリの I/O を削減します。 [#68124](https://github.com/StarRocks/starrocks/pull/68124)

- Lake DeltaWriter の高速キャンセルをサポートし、共有データクラスタでキャンセルされた取り込みジョブのレイテンシーを削減します。 [#68877](https://github.com/StarRocks/starrocks/pull/68877)

- 自動化されたクラスタスナップショットの間隔ベースのスケジューリングをサポートしました。 [#67525](https://github.com/StarRocks/starrocks/pull/67525)

- MemTable のフラッシュとマージのパイプライン実行をサポートし、共有データクラスタにおけるクラウドネイティブテーブルの取り込みスループットを向上させます。 [#67878](https://github.com/StarRocks/starrocks/pull/67878)

- クラウドネイティブテーブルの修復に `dry_run` モードをサポートし、実行前に修復アクションをプレビューできるようにしました。 [#68494](https://github.com/StarRocks/starrocks/pull/68494)

- 共有なしクラスタにおけるトランザクションのパブリッシュ用スレッドプールを追加し、パブリッシュのスループットを向上させました。 [#67797](https://github.com/StarRocks/starrocks/pull/67797)

### データレイク分析 {#data-lake-analytics}

- **Iceberg DELETE のサポート**

  Iceberg テーブル向けのポジションデリートファイルの書き込みをサポートし、StarRocks から直接 Iceberg テーブルに対して DELETE 操作を実行できるようにしました。このサポートは、Plan、Sink、Commit、Audit の全パイプラインをカバーしています。 [#67259](https://github.com/StarRocks/starrocks/pull/67259) [#67277](https://github.com/StarRocks/starrocks/pull/67277) [#67421](https://github.com/StarRocks/starrocks/pull/67421) [#67567](https://github.com/StarRocks/starrocks/pull/67567)

- **Hive テーブルおよび Iceberg テーブル向けの TRUNCATE**

  外部 Hive および Iceberg テーブルで TRUNCATE TABLE をサポートします。[#64768](https://github.com/StarRocks/starrocks/pull/64768) [#65016](https://github.com/StarRocks/starrocks/pull/65016)

- **Iceberg 上のインクリメンタルマテリアライズドビュー**

  インクリメンタルマテリアライズドビューリフレッシュのサポートを Iceberg の append-only テーブルにも拡張し、テーブル全体のリフレッシュなしでクエリアクセラレーションを可能にします。[#65469](https://github.com/StarRocks/starrocks/pull/65469) [#62699](https://github.com/StarRocks/starrocks/pull/62699)

- **Iceberg における半構造化データ向けの VARIANT タイプ**

  Iceberg Catalog で VARIANT データタイプをサポートし、半構造化データに対して柔軟なスキーマオンリード方式でのストレージとクエリを実現します。読み取り、書き込み、タイプキャスト、および Parquet との統合をサポートします。[#63639](https://github.com/StarRocks/starrocks/pull/63639) [#66539](https://github.com/StarRocks/starrocks/pull/66539)

- **Iceberg v3 サポート**

  Iceberg v3 のデフォルト値機能と行系列（row lineage）のサポートを追加しました。[#69525](https://github.com/StarRocks/starrocks/pull/69525) [#69633](https://github.com/StarRocks/starrocks/pull/69633)

- **Iceberg テーブルメンテナンス手続き**

  `rewrite_manifests` プロシージャのサポートを追加し、より細かい粒度でのテーブルメンテナンスを行えるよう `expire_snapshots` および `remove_orphan_files` プロシージャに追加の引数を拡張しました。[#68817](https://github.com/StarRocks/starrocks/pull/68817) [#68898](https://github.com/StarRocks/starrocks/pull/68898)

- Iceberg テーブルからファイルパスおよび行位置のメタデータ列を読み取ることをサポートします。[#67003](https://github.com/StarRocks/starrocks/pull/67003)

- Iceberg v3 テーブルから `_row_id` を読み取ることをサポートし、Iceberg v3 向けのグローバル後期実体化をサポートします。[#62318](https://github.com/StarRocks/starrocks/pull/62318) [#64133](https://github.com/StarRocks/starrocks/pull/64133)

- カスタムプロパティを持つ Iceberg ビューの作成をサポートし、SHOW CREATE VIEW の出力にプロパティを表示します。[#65938](https://github.com/StarRocks/starrocks/pull/65938)

- 特定のブランチ、タグ、バージョン、またはタイムスタンプを指定した Paimon テーブルのクエリをサポートします。[#63316](https://github.com/StarRocks/starrocks/pull/63316)

- Paimon テーブルの複合タイプ（ARRAY、MAP、STRUCT）をサポートします。[#66784](https://github.com/StarRocks/starrocks/pull/66784)

- Iceberg テーブル作成時にかっこ構文を使用した Partition Transforms をサポートします。[#68945](https://github.com/StarRocks/starrocks/pull/68945)

- データ編成の改善のため、Transform Partition に基づく Iceberg グローバルシャッフルをサポートします。[#70009](https://github.com/StarRocks/starrocks/pull/70009)

- Iceberg テーブルシンクにおけるグローバルシャッフルの動的な有効化をサポートします。[#67442](https://github.com/StarRocks/starrocks/pull/67442)

- Iceberg テーブルシンクに Commit キューを導入し、同時実行による Commit の競合を回避します。[#68084](https://github.com/StarRocks/starrocks/pull/68084)

- データ編成と読み取りパフォーマンスの向上のため、Iceberg テーブルシンクにホストレベルのソートを追加しました。[#68121](https://github.com/StarRocks/starrocks/pull/68121)

- ETL 実行モードにおいて、明示的な設定なしで INSERT INTO SELECT、CREATE TABLE AS SELECT、および類似のバッチ操作のパフォーマンスを向上させる追加の最適化をデフォルトで有効にしました。[#66841](https://github.com/StarRocks/starrocks/pull/66841)

- Iceberg テーブルに対する INSERT および DELETE 操作のコミット監査情報を追加しました。[#69198](https://github.com/StarRocks/starrocks/pull/69198)

- Iceberg REST Catalog におけるビューエンドポイント操作の有効化・無効化をサポートします。[#66083](https://github.com/StarRocks/starrocks/pull/66083)

- CachingIcebergCatalog におけるキャッシュ検索の効率を最適化しました。[#66388](https://github.com/StarRocks/starrocks/pull/66388)

- さまざまな Iceberg catalog タイプに対して EXPLAIN をサポートします。[#66563](https://github.com/StarRocks/starrocks/pull/66563)

- AWS Glue Catalog のテーブルに対するパーティションプロジェクションをサポートします。[#67601](https://github.com/StarRocks/starrocks/pull/67601)

- AWS Glue の `GetDatabases` API に対するリソース共有タイプのサポートを追加しました。[#69056](https://github.com/StarRocks/starrocks/pull/69056)

- エンドポイントインジェクション（`azblob`/`adls2`）を伴う Azure ABFS/WASB パスマッピングをサポートします。[#67847](https://github.com/StarRocks/starrocks/pull/67847)

- リモート RPC のオーバーヘッドと外部システム障害の影響を軽減するため、JDBC catalog 向けのデータベースメタデータキャッシュを追加しました。[#68256](https://github.com/StarRocks/starrocks/pull/68256)

- `information_schema` における PostgreSQL テーブルの列コメントをサポートします。[#70520](https://github.com/StarRocks/starrocks/pull/70520)

- Oracle および PostgreSQL の JDBC タイプマッピングを改善しました。[#70315](https://github.com/StarRocks/starrocks/pull/70315) [#70566](https://github.com/StarRocks/starrocks/pull/70566)

### クエリエンジン {#query-engine}

- **再帰 CTE**

  階層的なトラバーサル、グラフクエリ、および反復的な SQL 計算のための再帰共通テーブル式（CTE）をサポートします。[#65932](https://github.com/StarRocks/starrocks/pull/65932)

- 統計情報に基づくスキュー検出、ヒストグラムサポート、および NULL スキューの認識機能を備えた Skew Join v2 の書き換えを改善しました。[#68680](https://github.com/StarRocks/starrocks/pull/68680) [#68886](https://github.com/StarRocks/starrocks/pull/68886)

- ウィンドウにおける COUNT DISTINCT を改善し、複数の distinct 集計を融合してサポートするようになりました。[#67453](https://github.com/StarRocks/starrocks/pull/67453)

- ウィンドウ関数に対する明示的なスキューヒントをサポートし、スキューしたパーティションキーを持つウィンドウ関数を UNION に分割することで自動的に最適化します。[#67944](https://github.com/StarRocks/starrocks/pull/67944)

- Trino Parser における INSERT ステートメントに対して EXPLAIN および EXPLAIN ANALYZE をサポートします。[#70174](https://github.com/StarRocks/starrocks/pull/70174)

- クエリキューの可視性向上のため EXPLAIN をサポートします。[#69933](https://github.com/StarRocks/starrocks/pull/69933)

### 関数と SQL 構文 {#functions-and-sql-syntax}

- 以下の関数を追加しました：
  - `array_top_n`: 値でランク付けされた配列から上位 N 個の要素を返します。[#63376](https://github.com/StarRocks/starrocks/pull/63376)
  - `arrays_zip`: 複数の配列を要素ごとに結合して構造体の配列にします。[#65556](https://github.com/StarRocks/starrocks/pull/65556)
  - `json_pretty`: JSON 文字列をインデント付きでフォーマットします。[#66695](https://github.com/StarRocks/starrocks/pull/66695)
  - `json_set`: JSON 文字列内の指定されたパスに値を設定します。[#66193](https://github.com/StarRocks/starrocks/pull/66193)
  - `initcap`: 各単語の先頭文字を大文字に変換します。[#66837](https://github.com/StarRocks/starrocks/pull/66837)
  - `sum_map`: 同じキーを持つ行にわたって MAP の値を合計します。[#67482](https://github.com/StarRocks/starrocks/pull/67482)
  - `current_timezone`: 現在のセッションのタイムゾーンを返します。[#63653](https://github.com/StarRocks/starrocks/pull/63653)
  - `current_warehouse`: 現在の warehouse の名前を返します。[#66401](https://github.com/StarRocks/starrocks/pull/66401)
  - `sec_to_time`: 秒数を TIME 値に変換します。[#62797](https://github.com/StarRocks/starrocks/pull/62797)
  - `ai_query`: 推論ワークロード向けに SQL から外部 AI モデルを呼び出します。[#61583](https://github.com/StarRocks/starrocks/pull/61583)
  - `raise_error`: SQL 式内でユーザー定義エラーを発生させます。[#69661](https://github.com/StarRocks/starrocks/pull/69661)
- 以下の関数または構文拡張を提供します：
  - カスタムソート順序のために `array_sort` でラムダコンパレータをサポートします。[#66607](https://github.com/StarRocks/starrocks/pull/66607)
  - SQL 標準のセマンティクスに準拠した FULL OUTER JOIN 用の USING 句をサポートします。[#65122](https://github.com/StarRocks/starrocks/pull/65122)
  - ORDER BY/PARTITION BY を用いたフレーム付きウィンドウ関数における DISTINCT 集計をサポートします。[#65815](https://github.com/StarRocks/starrocks/pull/65815) [#65030](https://github.com/StarRocks/starrocks/pull/65030) [#67453](https://github.com/StarRocks/starrocks/pull/67453)
  - `lead`/`lag`/`first_value`/`last_value` ウィンドウ関数において ARRAY タイプをサポートします。[#63547](https://github.com/StarRocks/starrocks/pull/63547)
  - 重複排除カウントに類似した集計関数向けに VARBINARY をサポートします。[#68442](https://github.com/StarRocks/starrocks/pull/68442)
  - IN 式における日付型と文字列型のキャストをサポートします。[#61746](https://github.com/StarRocks/starrocks/pull/61746)
  - BEGIN/START TRANSACTION 用の WITH LABEL 構文をサポートします。[#68320](https://github.com/StarRocks/starrocks/pull/68320)
  - SHOW ステートメントにおける WHERE/ORDER/LIMIT 句をサポートします。[#68834](https://github.com/StarRocks/starrocks/pull/68834)
  - タスク管理のための `ALTER TASK` ステートメントをサポートします。[#68675](https://github.com/StarRocks/starrocks/pull/68675)
  - CSV ファイルのエクスポートに対して複数の圧縮形式（GZIP/SNAPPY/ZSTD/LZ4/DEFLATE/ZLIB/BZIP2）をサポートします。[#68054](https://github.com/StarRocks/starrocks/pull/68054)
  - 名前ベースの構造体フィールドマッチングのために `STRUCT_CAST_BY_NAME` SQL モードをサポートします。[#69845](https://github.com/StarRocks/starrocks/pull/69845)

### 管理と可観測性 {#management--observability}

- マルチ warehouse の CPU リソース分離を改善するために、リソースグループに `warehouses`、`cpu_weight_percent`、`exclusive_cpu_weight` の属性をサポートします。[#66947](https://github.com/StarRocks/starrocks/pull/66947)
- FE のスレッド状態を確認するための `information_schema.fe_threads` システムビューを導入します。[#65431](https://github.com/StarRocks/starrocks/pull/65431)
- クラスタレベルで特定のクエリパターンをブロックするための SQL Digest Blacklist をサポートします。[#66499](https://github.com/StarRocks/starrocks/pull/66499)
- ネットワークトポロジの制約によりアクセスできないノードからの Arrow Flight データ取得をサポートします。[#66348](https://github.com/StarRocks/starrocks/pull/66348)
- 再接続することなく既存の接続にグローバル変数の変更を伝播する REFRESH CONNECTIONS コマンドを導入します。[#64964](https://github.com/StarRocks/starrocks/pull/64964)
- クエリプロファイルを分析し、フォーマットされた SQL を表示するための組み込み UI 機能を追加し、クエリチューニングをより利用しやすくしました。[#63867](https://github.com/StarRocks/starrocks/pull/63867)
- 構造化されたクラスタ概要を提供する `ClusterSummaryActionV2` API エンドポイントを実装します。[#68836](https://github.com/StarRocks/starrocks/pull/68836)
- 現在のクラスタの実行モード（共有データまたは共有なし）を確認するためのグローバルな読み取り専用システム変数 `@@run_mode` を追加しました。[#69247](https://github.com/StarRocks/starrocks/pull/69247)
- クエリキュー管理を改善するために、デフォルトで `query_queue_v2` を有効にしました。[#67462](https://github.com/StarRocks/starrocks/pull/67462)
- Stream Load および Merge Commit 操作のためのユーザーレベルのデフォルト warehouse をサポートします。[#68106](https://github.com/StarRocks/starrocks/pull/68106) [#68616](https://github.com/StarRocks/starrocks/pull/68616)
- 必要に応じてバックエンドのブラックリスト検証をバイパスするための `skip_black_list` セッション変数を追加しました。[#67467](https://github.com/StarRocks/starrocks/pull/67467)
- メトリクス API に `enable_table_metrics_collect` オプションを追加しました。[#68691](https://github.com/StarRocks/starrocks/pull/68691)
- クエリ詳細 HTTP API にユーザーの偽装（impersonate）サポートを追加しました。[#68674](https://github.com/StarRocks/starrocks/pull/68674)
- テーブルレベルのプロパティとして `table_query_timeout` を追加しました。[#67547](https://github.com/StarRocks/starrocks/pull/67547)
- FE observer ノードの追加をサポートします。[#67778](https://github.com/StarRocks/starrocks/pull/67778)
- ロードジョブの可視性を高めるために、`information_schema.loads` で Merge Commit 情報をサポートします。[#67879](https://github.com/StarRocks/starrocks/pull/67879)
- より優れたトラブルシューティングのために、クラウドネイティブテーブルで tablet ステータスの表示をサポートしました。[#69616](https://github.com/StarRocks/starrocks/pull/69616)

### セキュリティ {#security-4}

- [CVE-2026-33870] [CVE-2026-33871] AWS バンドルを置き換え、Netty を 4.1.132.Final にアップグレードしました。[#71017](https://github.com/StarRocks/starrocks/pull/71017)
- [CVE-2025-27821] Hadoop を v3.4.2 にアップグレードしました。[#68529](https://github.com/StarRocks/starrocks/pull/68529)
- [CVE-2025-54920] `spark-core_2.12` を 3.5.7 にアップグレードしました。[#70862](https://github.com/StarRocks/starrocks/pull/70862)

### バグ修正 {#bug-fixes-4}

以下の問題が修正されました：

- レンジ分散の tablet に対してデータファイルの削除をスキップすることで、tablet 分割後のデータ損失を修正しました。[#71135](https://github.com/StarRocks/starrocks/pull/71135)
- 複雑な型に対する `DefaultValueColumnIterator` のメモリリークを修正しました。[#71142](https://github.com/StarRocks/starrocks/pull/71142)
- `shared_ptr` の `BatchUnit` と `FetchTaskContext` 間の循環参照によるメモリリークを修正しました。[#71126](https://github.com/StarRocks/starrocks/pull/71126)
- getline への同時アクセスに起因する SystemMetrics での二重解放クラッシュを修正しました。[#71040](https://github.com/StarRocks/starrocks/pull/71040)
- eager merge がすべてのブロックを消費した際に SpillMemTableSink でクラッシュする問題を修正しました。[#69046](https://github.com/StarRocks/starrocks/pull/69046)
- TTL クリーナーによって自動作成パーティションが削除された際の NPE を修正しました。[#68257](https://github.com/StarRocks/starrocks/pull/68257)
- スナップショットが期限切れの場合の `IcebergCatalog.getPartitionLastUpdatedTime` における NPE を修正しました。[#68925](https://github.com/StarRocks/starrocks/pull/68925)
- 定数側の列参照を持つ outer join における不正な述語書き換えを修正しました。[#67072](https://github.com/StarRocks/starrocks/pull/67072)
- 共有データモードで CHAR 列の長さを変更した後にクエリ結果が誤る問題を修正しました。[#68808](https://github.com/StarRocks/starrocks/pull/68808)
- 複数テーブルの場合の MV リフレッシュのバグを修正しました。[#61763](https://github.com/StarRocks/starrocks/pull/61763)
- 強制リフレッシュ時の MV リサイクル時間の誤りを修正しました。[#68673](https://github.com/StarRocks/starrocks/pull/68673)
- 同期 MV におけるすべて NULL 値の処理バグを修正しました。[#69136](https://github.com/StarRocks/starrocks/pull/69136)
- 高速 schema change ADD COLUMN 後に MV をクエリした際の重複列 ID エラーを修正しました。[#71072](https://github.com/StarRocks/starrocks/pull/71072)
- 共有 DecodeInfo に起因する低基数書き換えの NPE を修正しました。[#68799](https://github.com/StarRocks/starrocks/pull/68799)
- 低基数ジョインの述語型不一致を修正しました。[#68568](https://github.com/StarRocks/starrocks/pull/68568)
- `null_counts` が空の場合の Parquet Page Index Filter でのセグメンテーション違反を修正しました。[#68463](https://github.com/StarRocks/starrocks/pull/68463)
- 同一パス上での JSON フラット化された配列とオブジェクトの競合を修正しました。[#68804](https://github.com/StarRocks/starrocks/pull/68804)
- Iceberg キャッシュの重み付けの不正確さを修正しました。[#69058](https://github.com/StarRocks/starrocks/pull/69058)
- Iceberg テーブルキャッシュのメモリ上限を修正しました。[#67769](https://github.com/StarRocks/starrocks/pull/67769)
- Iceberg の削除列における NULL 許容性の問題を修正しました。[#68649](https://github.com/StarRocks/starrocks/pull/68649)
- コンテナを含むように Azure ABFS/WASB FileSystem キャッシュキーを修正しました。[#68901](https://github.com/StarRocks/starrocks/pull/68901)
- HMS 接続プールが満杯の場合のデッドロックを修正しました。[#68033](https://github.com/StarRocks/starrocks/pull/68033)
- Paimon Catalog における VARCHAR フィールド型の長さの誤りを修正しました。[#68383](https://github.com/StarRocks/starrocks/pull/68383)
- ObjectTable での ClassCastException による Paimon catalog リフレッシュのクラッシュを修正しました。[#70224](https://github.com/StarRocks/starrocks/pull/70224)
- 定数サブクエリを伴う FULL OUTER JOIN USING を修正しました。[#69028](https://github.com/StarRocks/starrocks/pull/69028)
- CTE スコープにおける join on 句のバグを修正しました。[#68809](https://github.com/StarRocks/starrocks/pull/68809)
- bindScope() パターンを使用することで ConnectContext のメモリリークを修正しました。[#68215](https://github.com/StarRocks/starrocks/pull/68215)
- 共有なしクラスタにおける `CatalogRecycleBin.asyncDeleteForTables` のメモリリークを修正しました。[#68275](https://github.com/StarRocks/starrocks/pull/68275)
- 例外が発生した際に Thrift accept スレッドが終了してしまう問題を修正しました。[#68644](https://github.com/StarRocks/starrocks/pull/68644)
- Routine Load の列マッピングにおける UDF 解決を修正しました。[#68201](https://github.com/StarRocks/starrocks/pull/68201)
- `DROP FUNCTION IF EXISTS` が `ifExists` フラグを無視する問題を修正しました。[#69216](https://github.com/StarRocks/starrocks/pull/69216)
- dict ページが大きすぎる場合のスキャン結果エラーを修正しました。[#68258](https://github.com/StarRocks/starrocks/pull/68258)
- レンジパーティションの重複を修正しました。[#68255](https://github.com/StarRocks/starrocks/pull/68255)
- クエリキューの割り当て時間と保留タイムアウトを修正しました。[#65802](https://github.com/StarRocks/starrocks/pull/65802)
- null リテラル配列を処理する際に `array_map` がクラッシュする問題を修正しました。[#70629](https://github.com/StarRocks/starrocks/pull/70629)
- `to_base64` に関するスタックオーバーフローを修正しました。[#70623](https://github.com/StarRocks/starrocks/pull/70623)
- LDAP 認証における大文字小文字を区別しないユーザー名の正規化を修正しました。[#67966](https://github.com/StarRocks/starrocks/pull/67966)
- API `proc_file` の SSRF リスクを軽減しました。[#68997](https://github.com/StarRocks/starrocks/pull/68997)
- 監査ログおよび SQL の編集処理においてユーザー認証文字列をマスクするようにしました。[#70360](https://github.com/StarRocks/starrocks/pull/70360)

### 動作の変更 {#behavior-changes-4}

- ETL 実行モードの最適化がデフォルトで有効になりました。これにより、明示的な設定変更なしに INSERT INTO SELECT や CREATE TABLE AS SELECT 、および同様のバッチワークロードで恩恵を受けられます。[#66841](https://github.com/StarRocks/starrocks/pull/66841)
- `lag`/`lead` ウィンドウ関数の3番目の引数が、定数値に加えて列参照もサポートするようになりました。[#60209](https://github.com/StarRocks/starrocks/pull/60209)
- FULL OUTER JOIN USING は、SQL 標準のセマンティクスに従うようになりました。USING 列は出力に2回ではなく1回だけ表示されます。[#65122](https://github.com/StarRocks/starrocks/pull/65122)
- `query_queue_v2` がデフォルトで有効になりました。[#67462](https://github.com/StarRocks/starrocks/pull/67462)
- SQL トランザクションは、デフォルトではセッション変数 `enable_sql_transaction` によって制御されます。[#63535](https://github.com/StarRocks/starrocks/pull/63535)
