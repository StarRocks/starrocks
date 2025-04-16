---
displayed_sidebar: docs
---

# Broker Load

## 1. Broker Load は、正常に実行されて FINISHED 状態にあるロードジョブを再実行することをサポートしていますか？

Broker Load は、正常に実行されて FINISHED 状態にあるロードジョブを再実行することをサポートしていません。また、データの損失や重複を防ぐために、正常に実行されたロードジョブのラベルを再利用することはできません。[SHOW LOAD](../../sql-reference/sql-statements/loading_unloading/SHOW_LOAD.md) を使用してロードジョブの履歴を表示し、再実行したいロードジョブを見つけることができます。その後、そのロードジョブの情報をコピーし、ラベルを除くジョブ情報を使用して別のロードジョブを作成できます。

## 2. Broker Load を使用して HDFS からデータをロードする際、宛先の StarRocks テーブルにロードされた日付と時刻の値が、ソースデータファイルの日付と時刻の値よりも 8 時間遅れている場合はどうすればよいですか？

宛先の StarRocks テーブルと Broker Load ジョブは、作成時に中国標準時 (CST) タイムゾーンを使用するようにコンパイルされています（`timezone` パラメータを使用して指定）。しかし、サーバーは協定世界時 (UTC) タイムゾーンに基づいて実行されるように設定されています。その結果、データロード中にソースデータファイルの日付と時刻の値に 8 時間が追加されます。この問題を防ぐには、宛先の StarRocks テーブルを作成する際に `timezone` パラメータを指定しないでください。

## 3. Broker Load を使用して ORC 形式のデータをロードする際に `ErrorMsg: type:ETL_RUN_FAIL; msg:Cannot cast '<slot 6>' from VARCHAR to ARRAY<VARCHAR(30)>` エラーが発生した場合はどうすればよいですか？

ソースデータファイルの列名が宛先の StarRocks テーブルの列名と異なります。この場合、ロード文の `SET` 句を使用してファイルとテーブルの間の列マッピングを指定する必要があります。`SET` 句を実行する際、StarRocks は型推論を行う必要がありますが、ソースデータを宛先データ型に変換するための [cast](../../sql-reference/sql-functions/cast.md) 関数の呼び出しに失敗します。この問題を解決するには、ソースデータファイルの列名が宛先の StarRocks テーブルの列名と同じであることを確認してください。これにより、`SET` 句は不要となり、StarRocks はデータ型変換を行うために cast 関数を呼び出す必要がなくなります。その後、Broker Load ジョブは正常に実行されます。

## 4. Broker Load ジョブがエラーを報告しないのに、ロードされたデータをクエリできないのはなぜですか？

Broker Load は非同期のロード方法です。ロード文がエラーを返さなくても、ロードジョブは失敗する可能性があります。Broker Load ジョブを実行した後、[SHOW LOAD](../../sql-reference/sql-statements/loading_unloading/SHOW_LOAD.md) を使用してロードジョブの結果と `errmsg` を確認できます。その後、ジョブの設定を変更して再試行できます。

## 5. "failed to send batch" または "TabletWriter add batch with unknown id" エラーが発生した場合はどうすればよいですか？

データの書き込みにかかる時間が上限を超え、タイムアウトエラーが発生しています。この問題を解決するには、[セッション変数](../../sql-reference/System_variable.md) `query_timeout` と [BE 設定項目](../../administration/management/BE_configuration.md#configure-be-static-parameters) `streaming_load_rpc_max_alive_time_sec` の設定をビジネス要件に基づいて変更します。

## 6. "LOAD-RUN-FAIL; msg:OrcScannerAdapter::init_include_columns. col name = xxx not found" エラーが発生した場合はどうすればよいですか？

Parquet または ORC 形式のデータをロードしている場合、ソースデータファイルの最初の行に保持されている列名が宛先の StarRocks テーブルの列名と同じであるかどうかを確認してください。

```SQL
(tmp_c1,tmp_c2)
SET
(
   id=tmp_c2,
   name=tmp_c1
)
```

上記の例では、ソースデータファイルの `tmp_c1` および `tmp_c2` 列を、それぞれ宛先の StarRocks テーブルの `name` および `id` 列にマッピングしています。`SET` 句を指定しない場合、`column_list` パラメータで指定された列名が列マッピングを宣言するために使用されます。詳細については、[BROKER LOAD](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) を参照してください。

> **注意**
>
> ソースデータファイルが Apache Hive™ によって生成された ORC 形式のファイルであり、ファイルの最初の行に `(_col0, _col1, _col2, ...)` が含まれている場合、"Invalid Column Name" エラーが発生する可能性があります。このエラーが発生した場合、`SET` 句を使用して列マッピングを指定する必要があります。

## 7. Broker Load ジョブが非常に長い時間実行される原因となるエラーをどのように処理しますか？

FE ログファイル **fe.log** を表示し、ジョブラベルに基づいてロードジョブの ID を検索します。その後、BE ログファイル **be.INFO** を表示し、ジョブ ID に基づいてロードジョブのログ記録を取得して、エラーの根本原因を特定します。

## 8. 高可用性 (HA) モードで実行される Apache HDFS クラスターをどのように構成しますか？

HDFS クラスターが高可用性 (HA) モードで実行されている場合、次のように構成します：

- `dfs.nameservices`: HDFS クラスターの名前、例: `"dfs.nameservices" = "my_ha"`。

- `dfs.ha.namenodes.xxx`: HDFS クラスター内の NameNode の名前。複数の NameNode 名を指定する場合は、カンマ (`,`) で区切ります。`xxx` は `dfs.nameservices` で指定した HDFS クラスター名です。例: `"dfs.ha.namenodes.my_ha" = "my_nn"`。

- `dfs.namenode.rpc-address.xxx.nn`: HDFS クラスター内の NameNode の RPC アドレス。`nn` は `dfs.ha.namenodes.xxx` で指定した NameNode 名です。例: `"dfs.namenode.rpc-address.my_ha.my_nn" = "host:port"`。

- `dfs.client.failover.proxy.provider`: クライアントが接続する NameNode のプロバイダー。デフォルト値: `org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider`。

例:

```SQL
(
    "dfs.nameservices" = "my-ha",
    "dfs.ha.namenodes.my-ha" = "my-namenode1, my-namenode2",
    "dfs.namenode.rpc-address.my-ha.my-namenode1" = "nn1-host:rpc_port",
    "dfs.namenode.rpc-address.my-ha.my-namenode2" = "nn2-host:rpc_port",
    "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
)
```

HA モードは、シンプル認証または Kerberos 認証と共に使用できます。例えば、HA モードで実行される HDFS クラスターにシンプル認証を使用してアクセスするには、次の設定を指定する必要があります：

```SQL
(
    "username"="user",
    "password"="passwd",
    "dfs.nameservices" = "my-ha",
    "dfs.ha.namenodes.my-ha" = "my_namenode1, my_namenode2",
    "dfs.namenode.rpc-address.my-ha.my-namenode1" = "nn1-host:rpc_port",
    "dfs.namenode.rpc-address.my-ha.my-namenode2" = "nn2-host:rpc_port",
    "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
)
```

HDFS クラスターの設定を **hdfs-site.xml** ファイルに追加できます。この方法では、HDFS クラスターからデータをロードするためにブローカーを使用する際に、ファイルパスと認証情報を指定するだけで済みます。

## 9. Hadoop ViewFS Federation をどのように構成しますか？

ViewFs 関連の設定ファイル `core-site.xml` と `hdfs-site.xml` を **broker/conf** ディレクトリにコピーします。

カスタムファイルシステムがある場合は、ファイルシステム関連の **.jar** ファイルも **broker/lib** ディレクトリにコピーする必要があります。

## 10. Kerberos 認証を必要とする HDFS クラスターにアクセスする際に "Can't get Kerberos realm" エラーが発生した場合はどうすればよいですか？

ブローカーが展開されているすべてのホストに **/etc/krb5.conf** ファイルが構成されていることを確認してください。

エラーが続く場合は、ブローカーの起動スクリプトの `JAVA_OPTS` 変数の末尾に `-Djava.security.krb5.conf:/etc/krb5.conf` を追加します。