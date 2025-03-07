---
displayed_sidebar: docs
---

# データロードに関するよくある質問

## 1. "close index channel failed" または "too many tablet versions" エラーが発生した場合はどうすればよいですか？

ロードジョブを頻繁に実行しすぎたため、データがタイムリーにコンパクト化されませんでした。その結果、ロード中に生成されたデータバージョンの数が許可されている最大数（デフォルトでは1000）を超えています。この問題を解決するには、以下の方法のいずれかを使用してください：

- 各個別のジョブでロードするデータ量を増やし、ロード頻度を減らします。

- 各 BE の BE 設定ファイル **be.conf** のいくつかの設定項目を変更して、コンパクションを加速します：

  - 重複キーテーブル、集計テーブル、ユニークキーテーブルの場合、`cumulative_compaction_num_threads_per_disk`、`base_compaction_num_threads_per_disk`、および `cumulative_compaction_check_interval_seconds` の値を適切に増やすことができます。例：

    ```Plain
    cumulative_compaction_num_threads_per_disk = 4
    base_compaction_num_threads_per_disk = 2
    cumulative_compaction_check_interval_seconds = 2
    ```

  - 主キーテーブルの場合、`update_compaction_num_threads_per_disk` の値を適切に増やし、`update_compaction_per_tablet_min_interval_seconds` の値を減らすことができます。

  上記の設定項目を変更した後、メモリと I/O が正常であることを確認してください。

## 2. "Label Already Exists" エラーが発生した場合はどうすればよいですか？

このエラーは、同じ StarRocks データベース内で、他のロードジョブと同じラベルを持つロードジョブが既に成功して実行されているか、実行中であるために発生します。

Stream Load ジョブは HTTP に従って送信されます。一般的に、すべてのプログラム言語の HTTP クライアントにはリクエスト再試行ロジックが組み込まれています。StarRocks クラスターが HTTP クライアントからロードジョブリクエストを受け取ると、すぐにリクエストの処理を開始しますが、ジョブの結果をタイムリーに HTTP クライアントに返しません。その結果、HTTP クライアントは同じロードジョブリクエストを再送信します。しかし、StarRocks クラスターは既に最初のリクエストを処理しているため、2 番目のリクエストに対して `Label Already Exists` エラーを返します。

異なるロード方法を使用して送信されたロードジョブが同じラベルを持たず、繰り返し送信されていないことを確認するには、次の手順を実行します：

- FE ログを確認し、失敗したロードジョブのラベルが 2 回記録されているかどうかを確認します。ラベルが 2 回記録されている場合、クライアントはロードジョブリクエストを 2 回送信しています。

  > **NOTE**
  >
  > StarRocks クラスターは、ロード方法に基づいてロードジョブのラベルを区別しません。したがって、異なるロード方法を使用して送信されたロードジョブは、同じラベルを持つ可能性があります。

- SHOW LOAD WHERE LABEL = "xxx" を実行して、同じラベルを持ち、**FINISHED** 状態にあるロードジョブを確認します。

  > **NOTE**
  >
  > `xxx` は確認したいラベルです。

ロードジョブを送信する前に、データをロードするのに必要な時間をおおよそ計算し、それに応じてクライアント側のリクエストタイムアウト期間を調整することをお勧めします。これにより、クライアントがロードジョブリクエストを複数回送信するのを防ぐことができます。

## 3. "ETL_QUALITY_UNSATISFIED; msg:quality not good enough to cancel" エラーが発生した場合はどうすればよいですか？

[SHOW LOAD](../../sql-reference/sql-statements/loading_unloading/SHOW_LOAD.md) を実行し、返された実行結果のエラー URL を使用してエラーの詳細を確認します。

一般的なデータ品質エラーは次のとおりです：

- "convert csv string to INT failed."
  
  ソース列からの文字列が、対応する宛先列のデータ型に変換できませんでした。例えば、`abc` が数値に変換できませんでした。

- "the length of input is too long than schema."
  
  ソース列からの値の長さが、対応する宛先列でサポートされている長さを超えています。例えば、CHAR データ型のソース列の値が、テーブル作成時に指定された宛先列の最大長を超えているか、INT データ型のソース列の値が 4 バイトを超えています。

- "actual column number is less than schema column number."
  
  指定された列セパレーターに基づいてソース行が解析された後、取得された列の数が宛先テーブルの列数よりも少ないです。考えられる理由は、ロードコマンドまたはステートメントで指定された列セパレーターが、実際にその行で使用されている列セパレーターと異なることです。

- "actual column number is more than schema column number."
  
  指定された列セパレーターに基づいてソース行が解析された後、取得された列の数が宛先テーブルの列数よりも多いです。考えられる理由は、ロードコマンドまたはステートメントで指定された列セパレーターが、実際にその行で使用されている列セパレーターと異なることです。

- "the frac part length longer than schema scale."
  
  DECIMAL 型のソース列からの値の小数部分が、指定された長さを超えています。

- "the int part length longer than schema precision."
  
  DECIMAL 型のソース列からの値の整数部分が、指定された長さを超えています。

- "there is no corresponding partition for this key."
  
  ソース行のパーティション列の値が、パーティション範囲内にありません。

## 4. RPC がタイムアウトした場合はどうすればよいですか？

各 BE の BE 設定ファイル **be.conf** の `write_buffer_size` 設定項目を確認します。この設定項目は、BE 上のメモリブロックごとの最大サイズを制御するために使用されます。デフォルトの最大サイズは 100 MB です。最大サイズが非常に大きい場合、リモートプロシージャコール (RPC) がタイムアウトする可能性があります。この問題を解決するには、BE 設定ファイルの `write_buffer_size` および `tablet_writer_rpc_timeout_sec` 設定項目を調整します。詳細については、[BE configurations](../../loading/loading_introduction/loading_considerations.md#be-configurations) を参照してください。

## 5. "Value count does not match column count" エラーが発生した場合はどうすればよいですか？

ロードジョブが失敗した後、ジョブ結果で返されたエラー URL を使用してエラーの詳細を取得し、"Value count does not match column count" エラーが発生したことがわかりました。これは、ソースデータファイルの列数と宛先 StarRocks テーブルの列数が一致しないことを示しています：

```Java
Error: Value count does not match column count. Expect 3, but got 1. Row: 2023-01-01T18:29:00Z,cpu0,80.99
Error: Value count does not match column count. Expect 3, but got 1. Row: 2023-01-01T18:29:10Z,cpu1,75.23
Error: Value count does not match column count. Expect 3, but got 1. Row: 2023-01-01T18:29:20Z,cpu2,59.44
```

この問題の理由は次のとおりです：

ロードコマンドまたはステートメントで指定された列セパレーターが、ソースデータファイルで実際に使用されている列セパレーターと異なります。前述の例では、CSV 形式のデータファイルはコンマ（`,`）で区切られた 3 つの列で構成されています。しかし、ロードコマンドまたはステートメントでは `\t` が列セパレーターとして指定されています。その結果、ソースデータファイルの 3 つの列が誤って 1 つの列に解析されます。

ロードコマンドまたはステートメントでコンマ（`,`）を列セパレーターとして指定します。その後、ロードジョブを再送信してください。