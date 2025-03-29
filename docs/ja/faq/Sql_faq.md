---
displayed_sidebar: docs
---

# SQL クエリ

このトピックでは、SQL に関するよくある質問への回答を提供します。

## マテリアライズドビューを作成するときに "fail to allocate memory." というエラーが発生する

この問題を解決するには、**be.conf** ファイルの `memory_limitation_per_thread_for_schema_change` パラメータの値を増やしてください。このパラメータは、スキームを変更するために単一のタスクに割り当てられる最大ストレージを指します。最大ストレージのデフォルト値は 2 GB です。

## StarRocks はクエリ結果のキャッシュをサポートしていますか？

StarRocks は最終的なクエリ結果を直接キャッシュしません。v2.5 以降、StarRocks は Query Cache 機能を使用して、キャッシュ内の最初の段階の集約の中間結果を保存します。以前のクエリと意味的に同等の新しいクエリは、キャッシュされた計算結果を再利用して計算を高速化できます。Query Cache は BE メモリを使用します。詳細は [Query cache](../using_starrocks/caching/query_cache.md) を参照してください。

## 計算に `Null` が含まれる場合、ISNULL() 関数を除いて関数の計算結果は false になります

標準 SQL では、`NULL` 値を持つオペランドを含むすべての計算は `NULL` を返します。

## StarRocks は DECODE 関数をサポートしていますか？

StarRocks は Oracle データベースの DECODE 関数をサポートしていません。StarRocks は MySQL と互換性があるため、CASE WHEN ステートメントを使用できます。

## StarRocks の主キーテーブルにデータがロードされた直後に最新のデータをクエリできますか？

はい。StarRocks は Google Mesa を参考にしてデータをマージします。StarRocks では、BE がデータマージをトリガーし、データをマージするための 2 種類の Compaction を持っています。データマージが完了していない場合は、クエリ中に完了します。したがって、データロード後に最新のデータを読み取ることができます。

## StarRocks に保存された utf8mb4 文字が切り捨てられたり文字化けしたりしますか？

いいえ。

## `alter table` コマンドを実行すると "table's state is not normal" というエラーが発生する

このエラーは、前回の変更が完了していないために発生します。以下のコードを実行して、前回の変更の状態を確認できます。

```SQL
show tablet from lineitem where State="ALTER"; 
```

変更操作にかかる時間はデータ量に関連しています。一般的に、変更は数分で完了します。テーブルを変更している間は、データロードが変更の完了速度を低下させるため、StarRocks へのデータロードを停止することをお勧めします。

## Apache Hive の外部テーブルをクエリすると "get partition detail failed: org.apache.doris.common.DdlException: get hive partition meta data failed: java.net.UnknownHostException:hadooptest" というエラーが発生する

このエラーは、Apache Hive パーティションのメタデータを取得できない場合に発生します。この問題を解決するには、**core-sit.xml** と **hdfs-site.xml** を **fe.conf** ファイルと **be.conf** ファイルにコピーしてください。

## データをクエリすると "planner use long time 3000 remaining task num 1" というエラーが発生する

このエラーは通常、完全なガベジコレクション (full GC) によって発生し、バックエンドのモニタリングと **fe.gc** ログを使用して確認できます。この問題を解決するには、以下の操作のいずれかを実行します。

- SQL のクライアントが複数のフロントエンド (FEs) に同時にアクセスできるようにして、負荷を分散します。
- Java Virtual Machine (JVM) のヒープサイズを **fe.conf** ファイルで 8 GB から 16 GB に変更して、メモリを増やし、full GC の影響を軽減します。

## 列 A のカーディナリティが小さい場合、`select B from tbl order by A limit 10` のクエリ結果が毎回異なる

SQL は列 A の順序を保証することしかできず、列 B の順序が各クエリで同じであることを保証できません。MySQL はスタンドアロンデータベースであるため、列 A と列 B の順序を保証できます。

StarRocks は分散型データベースであり、基礎となるテーブルに保存されたデータはシャーディングパターンになっています。列 A のデータは複数のマシンに分散されているため、複数のマシンから返される列 B の順序は各クエリで異なる場合があり、結果として B の順序が毎回一致しません。この問題を解決するには、`select B from tbl order by A limit 10` を `select B from tbl order by A,B limit 10` に変更します。

## SELECT * と SELECT の間に列効率の大きなギャップがあるのはなぜですか？

この問題を解決するには、プロファイルを確認し、MERGE の詳細を確認してください。

- ストレージ層での集約に時間がかかりすぎていないか確認します。

- インジケータ列が多すぎるか確認します。もしそうなら、何百万行もの列を集約します。

```plaintext
MERGE:

    - aggr: 26s270ms

    - sort: 15s551ms
```

## DELETE はネストされた関数をサポートしていますか？

ネストされた関数はサポートされていません。例えば、`DELETE from test_new WHERE to_days(now())-to_days(publish_time) >7;` の `to_days(now())` などです。

## データベースに数百のテーブルがある場合、データベースの使用効率を向上させるにはどうすればよいですか？

効率を向上させるために、MySQL のクライアントサーバーに接続する際に `-A` パラメータを追加します: `mysql -uroot -h127.0.0.1 -P8867 -A`。MySQL のクライアントサーバーはデータベース情報を事前に読み込みません。

## BE ログと FE ログが占めるディスクスペースを削減するにはどうすればよいですか？

ログレベルと対応するパラメータを調整します。詳細は [Parameter Configuration](../administration/management/BE_configuration.md) を参照してください。

## レプリケーション数を変更しようとすると "table *** is colocate table, cannot change replicationNum" というエラーが発生する

コロケートテーブルを作成する際には、`group` プロパティを設定する必要があります。そのため、単一のテーブルのレプリケーション数を変更することはできません。グループ内のすべてのテーブルのレプリケーション数を変更するには、以下の手順を実行します。

1. グループ内のすべてのテーブルに対して `group_with` を `empty` に設定します。
2. グループ内のすべてのテーブルに対して適切な `replication_num` を設定します。
3. `group_with` を元の値に戻します。

## VARCHAR を最大値に設定するとストレージに影響がありますか？

VARCHAR は可変長データ型であり、実際のデータ長に基づいて変更できる指定された長さを持っています。テーブルを作成する際に異なる varchar 長を指定しても、同じデータに対するクエリパフォーマンスにはほとんど影響がありません。

## テーブルを切り捨てると "create partititon timeout" というエラーが発生する

テーブルを切り捨てるには、対応するパーティションを作成してからそれらをスワップする必要があります。作成する必要があるパーティションの数が多い場合、このエラーが発生します。さらに、多くのデータロードタスクがある場合、Compaction プロセス中にロックが長時間保持されます。そのため、テーブルを作成する際にロックを取得できません。データロードタスクが多すぎる場合は、**be.conf** ファイルで `tablet_map_shard_size` を `512` に設定してロック競合を減らします。

## Apache Hive の外部テーブルにアクセスすると "Failed to specify server's Kerberos principal name" というエラーが発生する

**fe.conf** ファイルと **be.conf** ファイルの **hdfs-site.xml** に次の情報を追加します。

```HTML
<property>

<name>dfs.namenode.kerberos.principal.pattern</name>

<value>*</value>

</property>
```

## "2021-10" は StarRocks の日付形式ですか？

いいえ。

## "2021-10" をパーティションフィールドとして使用できますか？

いいえ、関数を使用して "2021-10" を "2021-10-01" に変更し、"2021-10-01" をパーティションフィールドとして使用してください。

## StarRocks データベースまたはテーブルのサイズをどこでクエリできますか？

[SHOW DATA](../sql-reference/sql-statements/Database/SHOW_DATA.md) コマンドを使用できます。

`SHOW DATA;` は、現在のデータベース内のすべてのテーブルのデータサイズとレプリカを表示します。

`SHOW DATA FROM <db_name>.<table_name>;` は、指定されたデータベースの指定されたテーブルのデータサイズ、レプリカの数、および行数を表示します。