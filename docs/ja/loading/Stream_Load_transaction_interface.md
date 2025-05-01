---
displayed_sidebar: docs
keywords: ['Stream Load']
---

# Stream Load トランザクションインターフェースを使用したデータのロード

v2.4以降、StarRocksはStream Loadトランザクションインターフェースを提供し、Apache Flink®やApache Kafka®などの外部システムからデータをロードするためのトランザクションに対して、2フェーズコミット（2PC）を実装します。Stream Loadトランザクションインターフェースは、高度に並行したストリームロードのパフォーマンスを向上させます。

このトピックでは、Stream Loadトランザクションインターフェースと、このインターフェースを使用してStarRocksにデータをロードする方法について説明します。

## 説明

Stream Loadトランザクションインターフェースは、HTTPプロトコル互換のツールや言語を使用してAPI操作を呼び出すことをサポートします。このトピックでは、curlを例にとってこのインターフェースの使用方法を説明します。このインターフェースは、トランザクション管理、データ書き込み、トランザクションの事前コミット、トランザクションの重複排除、トランザクションのタイムアウト管理など、さまざまな機能を提供します。

:::note
Stream LoadはCSVおよびJSONファイル形式をサポートしています。個々のサイズが10 GBを超えない少数のファイルからデータをロードしたい場合、この方法を推奨します。Stream LoadはParquetファイル形式をサポートしていません。Parquetファイルからデータをロードする必要がある場合は、[INSERT+files()](../loading/InsertInto.md#insert-data-directly-from-files-in-an-external-source-using-files) を使用してください。
:::

### トランザクション管理

Stream Loadトランザクションインターフェースは、トランザクションを管理するために使用される以下のAPI操作を提供します：

- `/api/transaction/begin`: 新しいトランザクションを開始します。

- `/api/transaction/commit`: 現在のトランザクションをコミットしてデータ変更を永続化します。

- `/api/transaction/rollback`: 現在のトランザクションをロールバックしてデータ変更を中止します。

### トランザクションの事前コミット

Stream Loadトランザクションインターフェースは、現在のトランザクションを事前コミットしてデータ変更を一時的に永続化するための`/api/transaction/prepare`操作を提供します。トランザクションを事前コミットした後、トランザクションをコミットまたはロールバックすることができます。トランザクションが事前コミットされた後にStarRocksクラスターがダウンした場合でも、クラスターが正常に復旧した後にトランザクションをコミットすることができます。

> **NOTE**
>
> トランザクションが事前コミットされた後は、そのトランザクションを使用してデータを書き続けないでください。トランザクションを使用してデータを書き続けると、書き込みリクエストがエラーを返します。

### データ書き込み

Stream Loadトランザクションインターフェースは、データを書き込むための`/api/transaction/load`操作を提供します。この操作は、1つのトランザクション内で複数回呼び出すことができます。

### トランザクションの重複排除

Stream Loadトランザクションインターフェースは、StarRocksのラベリングメカニズムを引き継いでいます。各トランザクションに一意のラベルをバインドすることで、トランザクションに対する最大1回の保証を実現できます。

### トランザクションのタイムアウト管理

各FEの設定ファイルで`stream_load_default_timeout_second`パラメータを使用して、そのFEのデフォルトのトランザクションタイムアウト期間を指定できます。

トランザクションを作成する際、HTTPリクエストヘッダーの`timeout`フィールドを使用して、トランザクションのタイムアウト期間を指定できます。

トランザクションを作成する際、HTTPリクエストヘッダーの`idle_transaction_timeout`フィールドを使用して、トランザクションがアイドル状態でいられるタイムアウト期間を指定することもできます。タイムアウト期間内にデータが書き込まれない場合、トランザクションは自動的にロールバックされます。

## 利点

Stream Loadトランザクションインターフェースは、次の利点をもたらします：

- **厳密な1回のセマンティクス**

  トランザクションは事前コミットとコミットの2つのフェーズに分割され、システム間でデータをロードしやすくします。たとえば、このインターフェースはFlinkからのデータロードに対して厳密な1回のセマンティクスを保証できます。

- **ロードパフォーマンスの向上**

  プログラムを使用してロードジョブを実行する場合、Stream Loadトランザクションインターフェースを使用すると、複数のミニバッチのデータをオンデマンドでマージし、1つのトランザクション内で一度にすべて送信することができます。これにより、ロードするデータバージョンが少なくなり、ロードパフォーマンスが向上します。

## 制限

Stream Loadトランザクションインターフェースには次の制限があります：

- **単一データベース単一テーブル**トランザクションのみがサポートされています。**複数データベース複数テーブル**トランザクションのサポートは開発中です。

- **1つのクライアントからの並行データ書き込み**のみがサポートされています。**複数のクライアントからの並行データ書き込み**のサポートは開発中です。

- `/api/transaction/load`操作は1つのトランザクション内で複数回呼び出すことができます。この場合、呼び出されるすべての`/api/transaction/load`操作に指定されたパラメータ設定は同じでなければなりません。

- Stream Loadトランザクションインターフェースを使用してCSV形式のデータをロードする場合、データファイル内の各データレコードが行区切り文字で終わるようにしてください。

## 注意事項

- 呼び出した`/api/transaction/begin`、`/api/transaction/load`、または`/api/transaction/prepare`操作がエラーを返した場合、トランザクションは失敗し、自動的にロールバックされます。
- 新しいトランザクションを開始するために`/api/transaction/begin`操作を呼び出す際、ラベルを指定する必要があります。なお、後続の`/api/transaction/load`、`/api/transaction/prepare`、および`/api/transaction/commit`操作は、`/api/transaction/begin`操作と同じラベルを使用する必要があります。
- 以前のトランザクションのラベルを使用して`/api/transaction/begin`操作を呼び出して新しいトランザクションを開始すると、以前のトランザクションは失敗し、ロールバックされます。
- StarRocksがCSV形式のデータに対してサポートするデフォルトの列区切り文字と行区切り文字は`\t`と`\n`です。データファイルがデフォルトの列区切り文字または行区切り文字を使用していない場合、`/api/transaction/load`操作を呼び出す際に、データファイルで実際に使用されている列区切り文字または行区切り文字を`"column_separator: <column_separator>"`または`"row_delimiter: <row_delimiter>"`を使用して指定する必要があります。

## 始める前に

### 権限の確認

StarRocksテーブルにデータをロードするには、これらのStarRocksテーブルに対してINSERT権限を持つユーザーとしてのみ可能です。INSERT権限を持っていない場合は、[GRANT](../sql-reference/sql-statements/account-management/GRANT.md) に従って、StarRocksクラスターに接続するために使用するユーザーにINSERT権限を付与してください。

#### ネットワーク設定の確認

ロードしたいデータが存在するマシンが、StarRocksクラスターのFEおよびBEノードに[`http_port`](../administration/Configuration.md)（デフォルト：`8030`）および[`be_http_port`](../administration/Configuration.md)（デフォルト：`8040`）を介してアクセスできることを確認してください。

## 基本操作

### サンプルデータの準備

このトピックでは、CSV形式のデータを例として使用します。

1. ローカルファイルシステムの`/home/disk1/`パスに、`example1.csv`という名前のCSVファイルを作成します。このファイルは、ユーザーID、ユーザー名、ユーザースコアを順に表す3つの列で構成されています。

   ```Plain
   1,Lily,23
   2,Rose,23
   3,Alice,24
   4,Julia,25
   ```

2. StarRocksデータベース`test_db`に、`table1`という名前の主キーテーブルを作成します。このテーブルは、`id`、`name`、`score`の3つの列で構成されており、`id`が主キーです。

   ```SQL
   CREATE TABLE `table1`
   (
       `id` int(11) NOT NULL COMMENT "user ID",
       `name` varchar(65533) NULL COMMENT "user name",
       `score` int(11) NOT NULL COMMENT "user score"
   )
   ENGINE=OLAP
   PRIMARY KEY(`id`)
   DISTRIBUTED BY HASH(`id`) BUCKETS 10;
   ```

### トランザクションを開始する

#### 構文

```Bash
curl --location-trusted -u <username>:<password> -H "label:<label_name>" \
    -H "Expect:100-continue" \
    -H "db:<database_name>" -H "table:<table_name>" \
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/begin
```

#### 例

```Bash
curl --location-trusted -u <jack>:<123456> -H "label:streamload_txn_example1_table1" \
    -H "Expect:100-continue" \
    -H "db:test_db" -H "table:table1" \
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/begin
```

> **NOTE**
>
> この例では、`streamload_txn_example1_table1`がトランザクションのラベルとして指定されています。

#### 戻り結果

- トランザクションが正常に開始された場合、次の結果が返されます：

  ```Bash
  {
      "Status": "OK",
      "Message": "",
      "Label": "streamload_txn_example1_table1",
      "TxnId": 9032,
      "BeginTxnTimeMs": 0
  }
  ```

- トランザクションが重複したラベルにバインドされている場合、次の結果が返されます：

  ```Bash
  {
      "Status": "LABEL_ALREADY_EXISTS",
      "ExistingJobStatus": "RUNNING",
      "Message": "Label [streamload_txn_example1_table1] has already been used."
  }
  ```

- 重複ラベル以外のエラーが発生した場合、次の結果が返されます：

  ```Bash
  {
      "Status": "FAILED",
      "Message": ""
  }
  ```

### データを書き込む

#### 構文

```Bash
curl --location-trusted -u <username>:<password> -H "label:<label_name>" \
    -H "Expect:100-continue" \
    -H "db:<database_name>" -H "table:<table_name>" \
    -T <file_path> \
    -XPUT http://<fe_host>:<fe_http_port>/api/transaction/load
```

> **NOTE**
>
> `/api/transaction/load`操作を呼び出す際、ロードしたいデータファイルの保存パスを`<file_path>`で指定する必要があります。

#### 例

```Bash
curl --location-trusted -u <jack>:<123456> -H "label:streamload_txn_example1_table1" \
    -H "Expect:100-continue" \
    -H "db:test_db" -H "table:table1" \
    -T /home/disk1/example1.csv \
    -H "column_separator: ," \
    -XPUT http://<fe_host>:<fe_http_port>/api/transaction/load
```

> **NOTE**
>
> この例では、データファイル`example1.csv`で使用されている列区切り文字はカンマ（`,`）であり、StarRocksのデフォルトの列区切り文字（`\t`）ではありません。そのため、`/api/transaction/load`操作を呼び出す際に、カンマ（`,`）を列区切り文字として指定する必要があります。

#### 戻り結果

- データ書き込みが成功した場合、次の結果が返されます：

  ```Bash
  {
      "TxnId": 1,
      "Seq": 0,
      "Label": "streamload_txn_example1_table1",
      "Status": "OK",
      "Message": "",
      "NumberTotalRows": 5265644,
      "NumberLoadedRows": 5265644,
      "NumberFilteredRows": 0,
      "NumberUnselectedRows": 0,
      "LoadBytes": 10737418067,
      "LoadTimeMs": 418778,
      "StreamLoadPutTimeMs": 68,
      "ReceivedDataTimeMs": 38964,
  }
  ```

- トランザクションが不明と見なされる場合、次の結果が返されます：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "TXN_NOT_EXISTS"
  }
  ```

- トランザクションが無効な状態と見なされる場合、次の結果が返されます：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "Transcation State Invalid"
  }
  ```

- 不明なトランザクションや無効な状態以外のエラーが発生した場合、次の結果が返されます：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": ""
  }
  ```

### トランザクションを事前コミットする

#### 構文

```Bash
curl --location-trusted -u <username>:<password> -H "label:<label_name>" \
    -H "Expect:100-continue" \
    -H "db:<database_name>" \
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/prepare
```

#### 例

```Bash
curl --location-trusted -u <jack>:<123456> -H "label:streamload_txn_example1_table1" \
    -H "Expect:100-continue" \
    -H "db:test_db" \
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/prepare
```

#### 戻り結果

- 事前コミットが成功した場合、次の結果が返されます：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "OK",
      "Message": "",
      "NumberTotalRows": 5265644,
      "NumberLoadedRows": 5265644,
      "NumberFilteredRows": 0,
      "NumberUnselectedRows": 0,
      "LoadBytes": 10737418067,
      "LoadTimeMs": 418778,
      "StreamLoadPutTimeMs": 68,
      "ReceivedDataTimeMs": 38964,
      "WriteDataTimeMs": 417851
      "CommitAndPublishTimeMs": 1393
  }
  ```

- トランザクションが存在しないと見なされる場合、次の結果が返されます：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "Transcation Not Exist"
  }
  ```

- 事前コミットがタイムアウトした場合、次の結果が返されます：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "commit timeout",
  }
  ```

- 存在しないトランザクションや事前コミットのタイムアウト以外のエラーが発生した場合、次の結果が返されます：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "publish timeout"
  }
  ```

### トランザクションをコミットする

#### 構文

```Bash
curl --location-trusted -u <username>:<password> -H "label:<label_name>" \
    -H "Expect:100-continue" \
    -H "db:<database_name>" \
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/commit
```

#### 例

```Bash
curl --location-trusted -u <jack>:<123456> -H "label:streamload_txn_example1_table1" \
    -H "Expect:100-continue" \
    -H "db:test_db" \
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/commit
```

#### 戻り結果

- コミットが成功した場合、次の結果が返されます：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "OK",
      "Message": "",
      "NumberTotalRows": 5265644,
      "NumberLoadedRows": 5265644,
      "NumberFilteredRows": 0,
      "NumberUnselectedRows": 0,
      "LoadBytes": 10737418067,
      "LoadTimeMs": 418778,
      "StreamLoadPutTimeMs": 68,
      "ReceivedDataTimeMs": 38964,
      "WriteDataTimeMs": 417851
      "CommitAndPublishTimeMs": 1393
  }
  ```

- トランザクションがすでにコミットされている場合、次の結果が返されます：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "OK",
      "Message": "Transaction already commited",
  }
  ```

- トランザクションが存在しないと見なされる場合、次の結果が返されます：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "Transcation Not Exist"
  }
  ```

- コミットがタイムアウトした場合、次の結果が返されます：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "commit timeout",
  }
  ```

- データの公開がタイムアウトした場合、次の結果が返されます：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "publish timeout",
      "CommitAndPublishTimeMs": 1393
  }
  ```

- 存在しないトランザクションやタイムアウト以外のエラーが発生した場合、次の結果が返されます：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": ""
  }
  ```

### トランザクションをロールバックする

#### 構文

```Bash
curl --location-trusted -u <username>:<password> -H "label:<label_name>" \
    -H "Expect:100-continue" \
    -H "db:<database_name>" \
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/rollback
```

#### 例

```Bash
curl --location-trusted -u <jack>:<123456> -H "label:streamload_txn_example1_table1" \
    -H "Expect:100-continue" \
    -H "db:test_db" \
    -XPOST http://<fe_host>:<fe_http_port>/api/transaction/rollback
```

#### 戻り結果

- ロールバックが成功した場合、次の結果が返されます：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "OK",
      "Message": ""
  }
  ```

- トランザクションが存在しないと見なされる場合、次の結果が返されます：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": "Transcation Not Exist"
  }
  ```

- 存在しないトランザクション以外のエラーが発生した場合、次の結果が返されます：

  ```Bash
  {
      "TxnId": 1,
      "Label": "streamload_txn_example1_table1",
      "Status": "FAILED",
      "Message": ""
  }
  ```

## 参考文献

Stream Loadの適切な適用シナリオとサポートされているデータファイル形式、およびStream Loadの動作についての情報は、[Loading from a local file system via Stream Load](../loading/StreamLoad.md#loading-from-a-local-file-system-via-stream-load) を参照してください。

Stream Loadジョブの作成に関する構文とパラメータについての情報は、[STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md) を参照してください。