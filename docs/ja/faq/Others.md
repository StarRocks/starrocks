---
displayed_sidebar: docs
---

# Other FAQ

このトピックでは、一般的な質問への回答を提供します。

## VARCHAR (32) と STRING は同じストレージスペースを占有しますか？

どちらも可変長データ型です。同じ長さのデータを保存する場合、VARCHAR (32) と STRING は同じストレージスペースを占有します。

## VARCHAR (32) と STRING はデータクエリにおいて同じパフォーマンスを発揮しますか？

はい。

## Oracle からインポートした TXT ファイルが、文字セットを UTF-8 に設定しても文字化けするのはなぜですか？

この問題を解決するには、以下の手順を実行してください。

1. 例えば、**original** という名前のファイルがあり、そのテキストが文字化けしています。このファイルの文字セットは ISO-8859-1 です。以下のコードを実行してファイルの文字セットを取得します。

    ```plaintext
    file --mime-encoding origin.txt
    origin.txt: iso-8859-1
    ```

2. `iconv` コマンドを実行して、このファイルの文字セットを UTF-8 に変換します。

    ```plaintext
    iconv -f iso-8859-1 -t utf-8 origin.txt > origin_utf-8.txt
    ```

3. 変換後もこのファイルのテキストが文字化けする場合は、このファイルの文字セットを GBK として再評価し、再度 UTF-8 に変換します。

    ```plaintext
    iconv -f gbk -t utf-8 origin.txt > origin_utf-8.txt
    ```

## MySQL で定義された STRING の長さは StarRocks で定義されたものと同じですか？

VARCHAR(n) に関しては、StarRocks は "n" をバイトで定義し、MySQL は "n" を文字で定義します。UTF-8 によれば、1 つの漢字は 3 バイトに相当します。StarRocks と MySQL が "n" を同じ数値で定義した場合、MySQL は StarRocks の 3 倍の文字を保存します。

## テーブルのパーティションフィールドのデータ型を FLOAT、DOUBLE、または DECIMAL にできますか？

いいえ、DATE、DATETIME、INT のみがサポートされています。

## テーブル内のデータが占有するストレージスペースを確認するにはどうすればよいですか？

SHOW DATA ステートメントを実行して、対応するストレージスペースを確認します。また、データ量、コピー数、行数も確認できます。

**注**: データ統計には時間遅延があります。

## StarRocks データベースのクォータ増加をリクエストするにはどうすればよいですか？

クォータ増加をリクエストするには、以下のコードを実行します。

```plaintext
ALTER DATABASE example_db SET DATA QUOTA 10T;
```

## UPSERT ステートメントを実行してテーブル内の特定のフィールドを更新することはできますか？

StarRocks 2.2 以降では、主キーテーブルを使用してテーブル内の特定のフィールドを更新することができます。StarRocks 1.9 以降では、主キーテーブルを使用してテーブル内のすべてのフィールドを更新することができます。詳細については、StarRocks 2.2 の [Primary Key table](../table_design/table_types/primary_key_table.md) を参照してください。

## 2 つのテーブルまたは 2 つのパーティション間でデータを入れ替えるにはどうすればよいですか？

SWAP WITH ステートメントを実行して、2 つのテーブルまたは 2 つのパーティション間でデータを入れ替えます。SWAP WITH ステートメントは、INSERT OVERWRITE ステートメントよりも安全です。データを入れ替える前に、データを確認し、入れ替え後のデータが入れ替え前のデータと一致しているかどうかを確認してください。

- 2 つのテーブルを入れ替える: 例えば、table 1 という名前のテーブルがあります。別のテーブルで table 1 を置き換えたい場合、以下の手順を実行します。

    1. table 2 という新しいテーブルを作成します。

        ```SQL
        create table2 like table1;
        ```

    2. Stream Load、Broker Load、または Insert Into を使用して、table 1 から table 2 にデータをロードします。

    3. table 1 を table 2 で置き換えます。

        ```SQL
        ALTER TABLE table1 SWAP WITH table2;
        ```

        これにより、データは正確に table 1 にロードされます。

- 2 つのパーティションを入れ替える: 例えば、table 1 という名前のテーブルがあります。table 1 のパーティションデータを置き換えたい場合、以下の手順を実行します。

    1. 一時パーティションを作成します。

        ```SQL
        ALTER TABLE table1

        ADD TEMPORARY PARTITION tp1

        VALUES LESS THAN("2020-02-01");
        ```

    2. table 1 から一時パーティションにパーティションデータをロードします。

    3. table 1 のパーティションを一時パーティションで置き換えます。

        ```SQL
        ALTER TABLE table1

        REPLACE PARTITION (p1) WITH TEMPORARY PARTITION (tp1);
        ```

## フロントエンド (FE) を再起動すると "error to open replicated environment, will exit" というエラーが発生します

このエラーは BDBJE のバグによるものです。この問題を解決するには、BDBJE のバージョンを 1.17 以上に更新してください。

## 新しい Apache Hive テーブルからデータをクエリすると "Broker list path exception" というエラーが発生します

### 問題の説明

```plaintext
msg:Broker list path exception

path=hdfs://172.31.3.136:9000/user/hive/warehouse/zltest.db/student_info/*, broker=TNetworkAddress(hostname:172.31.4.233, port:8000)
```

### 解決策

StarRocks の技術サポートに連絡し、namenode のアドレスとポートが正しいかどうか、また namenode のアドレスとポートにアクセスする権限があるかどうかを確認してください。

## 新しい Apache Hive テーブルからデータをクエリすると "get hive partition metadata failed" というエラーが発生します

### 問題の説明

```plaintext
msg:get hive partition meta data failed: java.net.UnknownHostException: emr-header-1.cluster-242
```

### 解決策

ネットワークが接続されていることを確認し、**host** ファイルを StarRocks クラスター内の各バックエンド (BE) にアップロードしてください。

## Apache Hive の ORC 外部テーブルにアクセスすると "do_open failed. reason = Invalid ORC postscript length" というエラーが発生します

### 問題の説明

Apache Hive のメタデータは FEs にキャッシュされています。しかし、StarRocks がメタデータを更新するには 2 時間のタイムラグがあります。StarRocks が更新を完了する前に、Apache Hive テーブルに新しいデータを挿入または更新すると、BEs によってスキャンされた HDFS のデータと FEs によって取得されたデータが異なるため、このエラーが発生します。

```plaintext
MySQL [bdp_dim]> select * from dim_page_func_s limit 1;

ERROR 1064 (HY000): HdfsOrcScanner::do_open failed. reason = Invalid ORC postscript length
```

### 解決策

この問題を解決するには、次のいずれかの操作を行います。

- 現在のバージョンを StarRocks 2.2 以上にアップグレードします。
- Apache Hive テーブルを手動でリフレッシュします。詳細については、[Metadata caching strategy](../data_source/External_table.md) を参照してください。

## MySQL の外部テーブルに接続すると "caching_sha2_password cannot be loaded" というエラーが発生します

### 問題の説明

MySQL 8.0 のデフォルトの認証プラグインは caching_sha2_password です。MySQL 5.7 のデフォルトの認証プラグインは mysql_native_password です。このエラーは、間違った認証プラグインを使用しているために発生します。

### 解決策

この問題を解決するには、次のいずれかの操作を行います。

- StarRocks に接続します。

```SQL
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'yourpassword';
```

- `my.cnf` ファイルを修正します。

```plaintext
vim my.cnf

[mysqld]

default_authentication_plugin=mysql_native_password
```

## テーブルを削除した後にディスクスペースを即座に解放するにはどうすればよいですか？

DROP TABLE ステートメントを実行してテーブルを削除すると、StarRocks は割り当てられたディスクスペースを解放するのに時間がかかります。割り当てられたディスクスペースを即座に解放するには、DROP TABLE FORCE ステートメントを実行してテーブルを削除します。DROP TABLE FORCE ステートメントを実行すると、StarRocks は未完了のイベントがあるかどうかを確認せずにテーブルを直接削除します。テーブルが削除されると復元できないため、DROP TABLE FORCE ステートメントは慎重に実行することをお勧めします。

## StarRocks の現在のバージョンを確認するにはどうすればよいですか？

`select current_version();` コマンドまたは CLI コマンド `./bin/show_fe_version.sh` を実行して、現在のバージョンを確認します。

## FE のメモリサイズを設定するにはどうすればよいですか？

メタデータは FE が使用するメモリに保存されます。以下の表に示すように、tablet の数に応じて FE のメモリサイズを設定できます。例えば、tablet の数が 100 万未満の場合、FE に最低 16 GB のメモリを割り当てる必要があります。**fe.conf** ファイルの **JAVA_OPTS** 設定項目で `-Xms` と `-Xmx` のパラメータの値を設定でき、`-Xms` と `-Xmx` のパラメータの値は一致している必要があります。すべての FEs で設定が同じである必要があります。なぜなら、どの FE も Leader に選出される可能性があるからです。

| Number of tablets    | Memory size of each FE |
| -------------- | ----------- |
| below 1 million      | 16 GB        |
| 1 ～ 2 million | 32 GB        |
| 2 ～ 5 million | 64 GB        |
| 5 ～ 10 million   | 128 GB       |

## StarRocks はどのようにクエリ時間を計算しますか？

StarRocks は複数のスレッドを使用してデータをクエリすることをサポートしています。クエリ時間とは、複数のスレッドがデータをクエリするのに使用する時間を指します。

## StarRocks はデータをローカルにエクスポートする際にパスを設定することをサポートしていますか？

いいえ。

## StarRocks の同時実行の上限はどれくらいですか？

実際のビジネスシナリオまたはシミュレートされたビジネスシナリオに基づいて同時実行制限をテストできます。一部のユーザーのフィードバックによれば、最大で 20,000 QPS または 30,000 QPS を達成できることがあります。

## StarRocks の初回の SSB テストのパフォーマンスが 2 回目よりも遅いのはなぜですか？

最初のクエリのディスク読み取り速度はディスクのパフォーマンスに関連しています。最初のクエリの後、ページキャッシュが生成されるため、後続のクエリは以前よりも速くなります。

## クラスターに最低限必要な BEs の数はどれくらいですか？

StarRocks は単一ノードのデプロイをサポートしているため、最低 1 つの BE を構成する必要があります。BEs は AVX2 で実行する必要があるため、8 コア 16GB 以上の構成のマシンに BEs をデプロイすることをお勧めします。

## Apache Superset を使用して StarRocks のデータを可視化する際にデータの権限を設定するにはどうすればよいですか？

新しいユーザーアカウントを作成し、テーブルクエリへの権限をユーザーに付与することでデータ権限を設定できます。

## `enable_profile` を `true` に設定した後にプロファイルが表示されないのはなぜですか？

レポートはアクセスのためにリーダー FE にのみ送信されます。

## StarRocks のテーブル内のフィールド注釈を確認するにはどうすればよいですか？

`show create table xxx` コマンドを実行します。

## テーブルを作成する際に NOW() 関数のデフォルト値を指定するにはどうすればよいですか？

StarRocks 2.1 以降のバージョンのみが関数のデフォルト値を指定することをサポートしています。StarRocks 2.1 より前のバージョンでは、関数に定数のみを指定できます。

## BE ノードのストレージスペースを解放するにはどうすればよいですか？

`rm -rf` コマンドを使用して `trash` ディレクトリを削除できます。スナップショットからデータを復元した場合は、`snapshot` ディレクトリを削除できます。

## BE ノードに追加のディスクを追加できますか？

はい。BE の設定項目 `storage_root_path` で指定されたディレクトリにディスクを追加できます。

## ローディングタスクとパーティション作成タスクの同時実行によるエクスプレッション・パーティションの競合を防ぐにはどうすればよいですか？

現在、式に基づくパーティション化を使用するテーブルでは、ロードタスクで作成されたパーティションと ALTER TABLE タスクで作成されたパーティションが競合します。ロードタスクが優先されるため、競合する ALTER タスクは失敗します。この問題を回避するには、以下の回避策を検討してください：

- 粗い時間ベースのパーティション（例えば、日または月によるパーティション）を使用する場合、ALTER 操作が時間の境界を越えるのを防ぐことができ、パーティション作成の失敗のリスクを減らすことができます。
- きめ細かい時間ベースのパーティションを使用する場合（例えば、時間単位のパーティション）、将来の時間範囲のパーティションを手動で作成し、ロードタスクによって作成された新しいパーティションによって ALTER 操作が中断されないようにすることができます。[EXPLAIN ANALYZE](../sql-reference/sql-statements/cluster-management/plan_profile/EXPLAIN_ANALYZE.md) 機能を使用して、トランザクションをコミットせずに INSERT ステートメントを実行してパーティション作成をトリガすることができます。これにより、実際のデータに影響を与えることなく、必要なパーティションを作成できます。次の例は、今後 8 時間分のパーティションを作成する方法を示しています：

```SQL
CREATE TABLE t(
    event_time DATETIME
)
PARTITION BY date_trunc('hour', event_time);

EXPLAIN ANALYZE
INSERT INTO t (event_time)
SELECT DATE_ADD(NOW(), INTERVAL d hour)
FROM table(generate_series(0, 8)) AS g(d);

SHOW PARTITION FROM t;
```
