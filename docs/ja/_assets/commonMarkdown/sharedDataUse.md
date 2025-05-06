他のオブジェクトストレージ用のストレージボリュームを作成し、デフォルトのストレージボリュームを設定する方法については、 [CREATE STORAGE VOLUME](../../sql-reference/sql-statements/cluster-management/storage_volume/CREATE_STORAGE_VOLUME.md) および [SET DEFAULT STORAGE VOLUME](../../sql-reference/sql-statements/cluster-management/storage_volume/SET_DEFAULT_STORAGE_VOLUME.md) を参照してください。

### データベースとクラウドネイティブテーブルを作成する

デフォルトのストレージボリュームを作成した後、このストレージボリュームを使用してデータベースとクラウドネイティブテーブルを作成できます。

共有データ StarRocks クラスターは、すべての [StarRocks table types](../../table_design/table_types/table_types.md) をサポートしています。

次の例では、データベース `cloud_db` と重複キーテーブルタイプに基づいたテーブル `detail_demo` を作成し、ローカルディスクキャッシュを有効にし、ホットデータの有効期間を1か月に設定し、オブジェクトストレージへの非同期データ取り込みを無効にしています。

```SQL
CREATE DATABASE cloud_db;
USE cloud_db;
CREATE TABLE IF NOT EXISTS detail_demo (
    recruit_date  DATE           NOT NULL COMMENT "YYYY-MM-DD",
    region_num    TINYINT        COMMENT "range [-128, 127]",
    num_plate     SMALLINT       COMMENT "range [-32768, 32767] ",
    tel           INT            COMMENT "range [-2147483648, 2147483647]",
    id            BIGINT         COMMENT "range [-2^63 + 1 ~ 2^63 - 1]",
    password      LARGEINT       COMMENT "range [-2^127 + 1 ~ 2^127 - 1]",
    name          CHAR(20)       NOT NULL COMMENT "range char(m),m in (1-255) ",
    profile       VARCHAR(500)   NOT NULL COMMENT "upper limit value 65533 bytes",
    ispass        BOOLEAN        COMMENT "true/false")
DUPLICATE KEY(recruit_date, region_num)
DISTRIBUTED BY HASH(recruit_date, region_num)
PROPERTIES (
    "storage_volume" = "def_volume",
    "datacache.enable" = "true",
    "datacache.partition_duration" = "1 MONTH"
);
```

> **NOTE**
>
> 共有データ StarRocks クラスターでデータベースまたはクラウドネイティブテーブルを作成する際にストレージボリュームが指定されていない場合、デフォルトのストレージボリュームが使用されます。

通常のテーブル `PROPERTIES` に加えて、共有データ StarRocks クラスター用のテーブルを作成する際には、次の `PROPERTIES` を指定する必要があります。

#### datacache.enable

ローカルディスクキャッシュを有効にするかどうか。

- `true` (デフォルト) このプロパティが `true` に設定されている場合、ロードされるデータはオブジェクトストレージとローカルディスク（クエリアクセラレーションのキャッシュとして）に同時に書き込まれます。
- `false` このプロパティが `false` に設定されている場合、データはオブジェクトストレージにのみロードされます。

> **NOTE**
>
> バージョン 3.0 では、このプロパティは `enable_storage_cache` と呼ばれていました。
>
> ローカルディスクキャッシュを有効にするには、CN 設定項目 `storage_root_path` にディスクのディレクトリを指定する必要があります。

#### datacache.partition_duration

ホットデータの有効期間。ローカルディスクキャッシュが有効になっている場合、すべてのデータがキャッシュにロードされます。キャッシュがいっぱいになると、StarRocks はキャッシュから最近使用されていないデータを削除します。クエリが削除されたデータをスキャンする必要がある場合、StarRocks は現在の時点からの有効期間内かどうかを確認します。データが有効期間内であれば、StarRocks はデータを再度キャッシュにロードします。データが有効期間外であれば、StarRocks はそれをキャッシュにロードしません。このプロパティは文字列値で、次の単位で指定できます: `YEAR`、`MONTH`、`DAY`、および `HOUR`。例えば、`7 DAY` や `12 HOUR` です。指定されていない場合、すべてのデータがホットデータとしてキャッシュされます。

> **NOTE**
>
> バージョン 3.0 では、このプロパティは `storage_cache_ttl` と呼ばれていました。
>
> このプロパティは `datacache.enable` が `true` に設定されている場合にのみ利用可能です。

### テーブル情報を表示する

特定のデータベース内のテーブル情報を `SHOW PROC "/dbs/<db_id>"` を使用して表示できます。詳細は [SHOW PROC](../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_PROC.md) を参照してください。

例:

```Plain
mysql> SHOW PROC "/dbs/xxxxx";
+---------+-------------+----------+---------------------+--------------+--------+--------------+--------------------------+--------------+---------------+------------------------------+
| TableId | TableName   | IndexNum | PartitionColumnName | PartitionNum | State  | Type         | LastConsistencyCheckTime | ReplicaCount | PartitionType | StoragePath                  |
+---------+-------------+----------+---------------------+--------------+--------+--------------+--------------------------+--------------+---------------+------------------------------+
| 12003   | detail_demo | 1        | NULL                | 1            | NORMAL | CLOUD_NATIVE | NULL                     | 8            | UNPARTITIONED | s3://xxxxxxxxxxxxxx/1/12003/ |
+---------+-------------+----------+---------------------+--------------+--------+--------------+--------------------------+--------------+---------------+------------------------------+
```

共有データ StarRocks クラスター内のテーブルの `Type` は `CLOUD_NATIVE` です。`StoragePath` フィールドには、テーブルが保存されているオブジェクトストレージのディレクトリが返されます。

### 共有データ StarRocks クラスターにデータをロードする

共有データ StarRocks クラスターは、StarRocks が提供するすべてのロード方法をサポートしています。詳細は [Loading options](../../loading/Loading_intro.md) を参照してください。

### 共有データ StarRocks クラスターでのクエリ

共有データ StarRocks クラスター内のテーブルは、StarRocks が提供するすべてのクエリタイプをサポートしています。詳細は StarRocks [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) を参照してください。

> **NOTE**
>
> 共有データ StarRocks クラスターは [synchronous materialized views](../../using_starrocks/Materialized_view-single_table.md) をサポートしていません。