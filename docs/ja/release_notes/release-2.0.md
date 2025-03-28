---
displayed_sidebar: docs
---

# StarRocks version 2.0

## 2.0.9

リリース日: 2022年8月6日

### バグ修正

以下のバグが修正されました:

- Broker Load ジョブにおいて、ブローカーが高負荷の場合、内部ハートビートがタイムアウトし、データ損失が発生する可能性があります。 [#8282](https://github.com/StarRocks/starrocks/issues/8282)
- Broker Load ジョブにおいて、`COLUMNS FROM PATH AS` パラメータで指定されたカラムが存在しない場合、BEs が停止します。 [#5346](https://github.com/StarRocks/starrocks/issues/5346)
- 一部のクエリが Leader FE に転送され、`/api/query_detail` アクションが SHOW FRONTENDS などの SQL ステートメントに関する誤った実行情報を返すことがあります。 [#9185](https://github.com/StarRocks/starrocks/issues/9185)
- 同じ HDFS データファイルをロードするために複数の Broker Load ジョブが作成された場合、1つのジョブが例外に遭遇すると、他のジョブもデータを正しく読み取れず失敗する可能性があります。 [#9506](https://github.com/StarRocks/starrocks/issues/9506)

## 2.0.8

リリース日: 2022年7月15日

### バグ修正

以下のバグが修正されました:

- Leader FE ノードを繰り返し切り替えると、すべてのロードジョブがハングし、失敗する可能性があります。 [#7350](https://github.com/StarRocks/starrocks/issues/7350)
- MemTable のメモリ使用量推定が 4GB を超えると、ロード中のデータスキューにより一部のフィールドが大量のメモリリソースを占有するため、BE がクラッシュします。 [#7161](https://github.com/StarRocks/starrocks/issues/7161)
- FEs を再起動した後、大文字と小文字の誤った解析によりマテリアライズドビューのスキーマが変更されました。 [#7362](https://github.com/StarRocks/starrocks/issues/7362)
- Routine Load を使用して Kafka から StarRocks に JSON データをロードする際、JSON データに空行があると、その後のデータが失われます。 [#8534](https://github.com/StarRocks/starrocks/issues/8534)

## 2.0.7

リリース日: 2022年6月13日

### バグ修正

以下のバグが修正されました:

- コンパクション中のテーブルのカラムに重複値が 0x40000000 を超えると、コンパクションが中断されます。 [#6513](https://github.com/StarRocks/starrocks/issues/6513)
- FE が再起動した後、BDB JE v7.3.8 のいくつかの問題により高い I/O と異常なディスク使用量の増加に直面し、正常に戻る兆候がありません。BDB JE v7.3.7 にロールバックすると FE は正常に戻ります。 [#6634](https://github.com/StarRocks/starrocks/issues/6634)

## 2.0.6

リリース日: 2022年5月25日

### バグ修正

以下のバグが修正されました:

- 一部の GUI ツールが `set_sql_limit` 変数を自動的に設定します。その結果、SQL ステートメント ORDER BY LIMIT が無視され、クエリに対して誤った行数が返されます。 [#5966](https://github.com/StarRocks/starrocks/issues/5966)
- コロケーショングループ (CG) に多数のテーブルが含まれ、頻繁にデータがロードされる場合、CG は `stable` 状態を維持できない可能性があります。この場合、JOIN ステートメントは Colocate Join 操作をサポートしません。StarRocks はデータロード中に少し長く待つように最適化されました。これにより、データがロードされる tablet レプリカの整合性を最大化できます。
- 一部のレプリカが高負荷や高ネットワーク遅延などの理由でロードに失敗すると、これらのレプリカでクローンがトリガーされます。この場合、デッドロックが発生し、プロセスの負荷が低いが多数のリクエストがタイムアウトする状況が発生する可能性があります。 [#5646](https://github.com/StarRocks/starrocks/issues/5646) [#6290](https://github.com/StarRocks/starrocks/issues/6290)
- 主キーテーブルを使用するテーブルのスキーマが変更された後、そのテーブルにデータをロードすると「duplicate key xxx」エラーが発生する可能性があります。 [#5878](https://github.com/StarRocks/starrocks/issues/5878)
- DROP SCHEMA ステートメントがデータベースで実行されると、データベースが強制的に削除され、復元できなくなります。 [#6201](https://github.com/StarRocks/starrocks/issues/6201)

## 2.0.5

リリース日: 2022年5月13日

アップグレード推奨: このバージョンでは、保存されたデータやデータクエリの正確性に関連するいくつかの重大なバグが修正されています。できるだけ早く StarRocks クラスターをアップグレードすることをお勧めします。

### バグ修正

以下のバグが修正されました:

- [重大なバグ] BE の障害によりデータが失われる可能性があります。このバグは、特定のバージョンを複数の BEs に一度に公開するメカニズムを導入することで修正されました。 [#3140](https://github.com/StarRocks/starrocks/issues/3140)

- [重大なバグ] 特定のデータ取り込みフェーズで tablets が移行されると、データが元のディスクに書き込まれ続けます。その結果、データが失われ、クエリが正常に実行できなくなります。 [#5160](https://github.com/StarRocks/starrocks/issues/5160)

- [重大なバグ] 複数の DELETE 操作を実行した後にクエリを実行すると、クエリに対して低基数カラムの最適化が行われた場合、誤ったクエリ結果を得る可能性があります。 [#5712](https://github.com/StarRocks/starrocks/issues/5712)

- [重大なバグ] JOIN 句を含むクエリが DOUBLE 値のカラムと VARCHAR 値のカラムを組み合わせる場合、クエリ結果が誤っている可能性があります。 [#5809](https://github.com/StarRocks/starrocks/pull/5809)

- 特定のバージョンのレプリカが FEs によって有効とマークされる前に、StarRocks クラスターにデータをロードすると、StarRocks がデータを見つけられずエラーを報告します。 [#5153](https://github.com/StarRocks/starrocks/issues/5153)

- `SPLIT` 関数のパラメータが `NULL` に設定されている場合、StarRocks クラスターの BEs が停止する可能性があります。 [#4092](https://github.com/StarRocks/starrocks/issues/4092)

- Apache Doris 0.13 から StarRocks 1.19.x にクラスターをアップグレードし、しばらく稼働させた後、StarRocks 2.0.1 へのさらなるアップグレードが失敗する可能性があります。 [#5309](https://github.com/StarRocks/starrocks/issues/5309)

## 2.0.4

リリース日: 2022年4月18日

### バグ修正

以下のバグが修正されました:

- カラムを削除し、新しいパーティションを追加し、tablets をクローンした後、古い tablets と新しい tablets のカラムのユニーク ID が同じでない可能性があり、システムが共有 tablet スキーマを使用しているため、BE が停止する可能性があります。 [#4514](https://github.com/StarRocks/starrocks/issues/4514)
- StarRocks 外部テーブルにデータをロードする際、ターゲット StarRocks クラスターの設定された FE が Leader でない場合、FE が停止します。 [#4573](https://github.com/StarRocks/starrocks/issues/4573)
- Duplicate Key テーブルがスキーマ変更を行い、同時にマテリアライズドビューを作成すると、クエリ結果が誤っている可能性があります。 [#4839](https://github.com/StarRocks/starrocks/issues/4839)
- BE 障害によるデータ損失の可能性の問題（バッチ公開バージョンを使用して解決）。 [#3140](https://github.com/StarRocks/starrocks/issues/3140)

## 2.0.3

リリース日: 2022年3月14日

### バグ修正

以下のバグが修正されました:

- BE ノードがサスペンド状態のときにクエリが失敗します。
- 単一 tablet テーブルジョインに適切な実行プランがない場合、クエリが失敗します。 [#3854](https://github.com/StarRocks/starrocks/issues/3854)
- FE ノードが低基数最適化のためのグローバル辞書を構築するための情報を収集する際にデッドロック問題が発生する可能性があります。 [#3839](https://github.com/StarRocks/starrocks/issues/3839)

## 2.0.2

リリース日: 2022年3月2日

### 改善点

- メモリ使用量が最適化されました。ユーザーは `label_keep_max_num` パラメータを指定して、一定期間内に保持するロードジョブの最大数を制御できます。これにより、頻繁なデータロード中に FE の高いメモリ使用量によるフル GC を防ぎます。

### バグ修正

以下のバグが修正されました:

- カラムデコーダーが例外に遭遇した場合、BE ノードが失敗します。
- JSON データをロードするために使用されるコマンドで `jsonpaths` が指定されている場合、Auto __op マッピングが効果を発揮しません。
- Broker Load を使用したデータロード中にソースデータが変更されると、BE ノードが失敗します。
- マテリアライズドビューが作成された後、一部の SQL ステートメントがエラーを報告します。
- SQL 句に低基数最適化のためのグローバル辞書をサポートする述語とサポートしない述語が含まれている場合、クエリが失敗する可能性があります。

## 2.0.1

リリース日: 2022年1月21日

### 改善点

- StarRocks が外部テーブルを使用して Hive データをクエリする際に、Hive の implicit_cast 操作を読み取ることができます。 [#2829](https://github.com/StarRocks/starrocks/pull/2829)
- StarRocks CBO が統計を収集して高並列クエリをサポートする際の高い CPU 使用率を修正するために、読み書きロックが使用されます。 [#2901](https://github.com/StarRocks/starrocks/pull/2901)
- CBO 統計収集と UNION 演算子が最適化されました。

### バグ修正

- レプリカのグローバル辞書の不一致によって引き起こされるクエリエラーが修正されました。 [#2700](https://github.com/StarRocks/starrocks/pull/2700) [#2765](https://github.com/StarRocks/starrocks/pull/2765)
- データロード中に `exec_mem_limit` パラメータが効果を発揮しないエラーが修正されました。 [#2693](https://github.com/StarRocks/starrocks/pull/2693)
  > `exec_mem_limit` パラメータは、データロード中の各 BE ノードのメモリ制限を指定します。
- 主キーテーブルにデータがインポートされる際に発生する OOM エラーが修正されました。 [#2743](https://github.com/StarRocks/starrocks/pull/2743) [#2777](https://github.com/StarRocks/starrocks/pull/2777)
- StarRocks が外部テーブルを使用して大規模な MySQL テーブルをクエリする際に BE ノードが応答しなくなるエラーが修正されました。 [#2881](https://github.com/StarRocks/starrocks/pull/2881)

### 挙動の変更

StarRocks は外部テーブルを使用して Hive および AWS S3 ベースの外部テーブルにアクセスできます。ただし、S3 データにアクセスするために使用される jar ファイルは大きすぎるため、StarRocks のバイナリパッケージにはこの jar ファイルが含まれていません。この jar ファイルを使用したい場合は、[Hive_s3_lib](https://releases.starrocks.io/resources/hive_s3_jar.tar.gz) からダウンロードできます。

## 2.0.0

リリース日: 2022年1月5日

### 新機能

- 外部テーブル
  - [実験的機能] S3 上の Hive 外部テーブルのサポート
  - 外部テーブルの DecimalV3 サポート [#425](https://github.com/StarRocks/starrocks/pull/425)
- 複雑な式をストレージ層にプッシュダウンして計算を行い、パフォーマンスを向上させます
- 主キーが正式にリリースされ、Stream Load、Broker Load、Routine Load をサポートし、Flink-cdc に基づく MySQL データの秒単位の同期ツールも提供します

### 改善点

- 算術演算子の最適化
  - 低基数の辞書のパフォーマンスを最適化 [#791](https://github.com/StarRocks/starrocks/pull/791)
  - 単一テーブルの int のスキャンパフォーマンスを最適化 [#273](https://github.com/StarRocks/starrocks/issues/273)
  - 高基数の `count(distinct int)` のパフォーマンスを最適化 [#139](https://github.com/StarRocks/starrocks/pull/139) [#250](https://github.com/StarRocks/starrocks/pull/250) [#544](https://github.com/StarRocks/starrocks/pull/544)[#570](https://github.com/StarRocks/starrocks/pull/570)
  - 実装レベルで `Group by int` / `limit` / `case when` / `not equal` を最適化
- メモリ管理の最適化
  - メモリ統計と制御フレームワークをリファクタリングしてメモリ使用量を正確にカウントし、OOM を完全に解決
  - メタデータのメモリ使用量を最適化
  - 大量のメモリ解放が実行スレッドで長時間スタックする問題を解決
  - プロセスの優雅な終了メカニズムを追加し、メモリリークチェックをサポート [#1093](https://github.com/StarRocks/starrocks/pull/1093)

### バグ修正

- Hive 外部テーブルが大量のメタデータを取得する際にタイムアウトする問題を修正
- マテリアライズドビュー作成のエラーメッセージが不明瞭な問題を修正
- ベクトル化エンジンでの like の実装を修正 [#722](https://github.com/StarRocks/starrocks/pull/722)
- `alter table` の述語を解析する際のエラーを修正 [#725](https://github.com/StarRocks/starrocks/pull/725)
- `curdate` 関数が日付をフォーマットできない問題を修正