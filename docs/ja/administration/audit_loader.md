---
displayed_sidebar: docs
---

# AuditLoader を使用して StarRocks 内で監査ログを管理する

このトピックでは、プラグイン - AuditLoader を使用して、テーブル内で StarRocks の監査ログを管理する方法について説明します。

StarRocks は監査ログを内部データベースではなく、ローカルファイル **fe/log/fe.audit.log** に保存します。プラグイン AuditLoader を使用すると、クラスタ内で監査ログを直接管理できます。インストール後、AuditLoader はファイルからログを読み取り、HTTP PUT を介して StarRocks にロードします。その後、SQL ステートメントを使用して StarRocks 内の監査ログをクエリできます。

## 監査ログを保存するテーブルを作成する

StarRocks クラスタ内にデータベースとテーブルを作成し、監査ログを保存します。詳細な手順については、 [CREATE DATABASE](../sql-reference/sql-statements/data-definition/CREATE_DATABASE.md) と [CREATE TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md) を参照してください。

監査ログのフィールドは異なる StarRocks バージョン間で異なるため、以下の例から選択して、使用している StarRocks に互換性のあるテーブルを作成する必要があります。

> **注意**
>
> - 例のテーブルスキーマを変更しないでください。そうしないと、ログのロードが失敗します。
> - 監査ログのフィールドは異なる StarRocks バージョン間で異なるため、新しいバージョンの AuditLoader は、利用可能なすべての StarRocks バージョンから共通のフィールドを収集します。

```SQL
CREATE DATABASE starrocks_audit_db__;

CREATE TABLE starrocks_audit_db__.starrocks_audit_tbl__ (
  `queryId`           VARCHAR(64)                COMMENT "Unique query ID",
  `timestamp`         DATETIME         NOT NULL  COMMENT "Query start time",
  `queryType`         VARCHAR(12)                COMMENT "Query type (query, slow_query, connection）",
  `clientIp`          VARCHAR(32)                COMMENT "Client IP address",
  `user`              VARCHAR(64)                COMMENT "User who initiates the query",
  `authorizedUser`    VARCHAR(64)                COMMENT "user_identity",
  `resourceGroup`     VARCHAR(64)                COMMENT "Resource group name",
  `catalog`           VARCHAR(32)                COMMENT "Catalog name",
  `db`                VARCHAR(96)                COMMENT "Database that the query scans",
  `state`             VARCHAR(8)                 COMMENT "Query state (EOF, ERR, OK)",
  `errorCode`         VARCHAR(512)               COMMENT "Error code",
  `queryTime`         BIGINT                     COMMENT "Query latency in milliseconds",
  `scanBytes`         BIGINT                     COMMENT "Size of the scanned data in bytes",
  `scanRows`          BIGINT                     COMMENT "Row count of the scanned data",
  `returnRows`        BIGINT                     COMMENT "Row count of the result",
  `cpuCostNs`         BIGINT                     COMMENT "CPU resources consumption time for query in nanoseconds",
  `memCostBytes`      BIGINT                     COMMENT "Memory cost for query in bytes",
  `stmtId`            INT                        COMMENT "Incremental SQL statement ID",
  `isQuery`           TINYINT                    COMMENT "If the SQL is a query (0 and 1)",
  `feIp`              VARCHAR(128)               COMMENT "IP address of FE that executes the SQL",
  `stmt`              VARCHAR(1048576)           COMMENT "Original SQL statement",
  `digest`            VARCHAR(32)                COMMENT "Slow SQL fingerprint",
  `planCpuCosts`      DOUBLE                     COMMENT "CPU resources consumption time for planning in nanoseconds",
  `planMemCosts`      DOUBLE                     COMMENT "Memory cost for planning in bytes"
) ENGINE = OLAP
DUPLICATE KEY (`queryId`, `timestamp`, `queryType`)
COMMENT "Audit log table"
PARTITION BY RANGE (`timestamp`) ()
DISTRIBUTED BY HASH (`queryId`) BUCKETS 3 
PROPERTIES (
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.start" = "-30",       -- 最新の 30 日間の監査ログを保持します。ビジネスの需要に基づいてこの値を調整できます。
  "dynamic_partition.end" = "3",
  "dynamic_partition.prefix" = "p",
  "dynamic_partition.buckets" = "3",
  "dynamic_partition.enable" = "true",
  "replication_num" = "3"                 -- 監査ログの 3 つのレプリカを保持します。運用環境では 3 つのレプリカを保持することをお勧めします。
);
```

`starrocks_audit_tbl__` は動的パーティションで作成されます。デフォルトでは、テーブルが作成されてから 10 分後に最初の動的パーティションが作成されます。その後、監査ログをテーブルにロードできます。以下のステートメントを使用してテーブル内のパーティションを確認できます。

```SQL
SHOW PARTITIONS FROM starrocks_audit_db__.starrocks_audit_tbl__;
```

パーティションが作成されたら、次のステップに進むことができます。

## AuditLoader をダウンロードして設定する

1. [Download](https://releases.starrocks.io/resources/AuditLoader.zip) AuditLoader インストールパッケージをダウンロードします。このパッケージは、利用可能なすべての StarRocks バージョンと互換性があります。

2. インストールパッケージを解凍します。

    ```shell
    unzip auditloader.zip
    ```

    次のファイルが展開されます。

    - **auditloader.jar**: AuditLoader の JAR ファイル。
    - **plugin.properties**: AuditLoader のプロパティファイル。このファイルを変更する必要はありません。
    - **plugin.conf**: AuditLoader の設定ファイル。通常は、このファイルの `user` と `password` フィールドを変更するだけで済みます。

3. **plugin.conf** を変更して AuditLoader を設定します。AuditLoader が正常に動作するために、次の項目を設定する必要があります。

    - `frontend_host_port`: FE の IP アドレスと HTTP ポート。形式は `<fe_ip>:<fe_http_port>` です。デフォルト値は `127.0.0.1:8030` です。
    - `database`: 監査ログをホストするために作成したデータベースの名前。
    - `table`: 監査ログをホストするために作成したテーブルの名前。
    - `user`: クラスタのユーザー名。このテーブルにデータをロードする権限 (LOAD_PRIV) を持っている必要があります。
    - `password`: ユーザーのパスワード。

4. ファイルを再びパッケージに圧縮します。

    ```shell
    zip -q -m -r auditloader.zip auditloader.jar plugin.conf plugin.properties
    ```

5. パッケージを FE ノードをホストするすべてのマシンに配布します。すべてのパッケージが同一のパスに保存されていることを確認してください。そうでないと、インストールが失敗します。パッケージを配布した後、パッケージの絶対パスをコピーすることを忘れないでください。

## AuditLoader をインストールする

AuditLoader を StarRocks のプラグインとしてインストールするには、コピーしたパスとともに次のステートメントを実行します。

```SQL
INSTALL PLUGIN FROM "<absolute_path_to_package>";
```

詳細な手順については、 [INSTALL PLUGIN](../sql-reference/sql-statements/Administration/INSTALL_PLUGIN.md) を参照してください。

## インストールを確認し、監査ログをクエリする

1. [SHOW PLUGINS](../sql-reference/sql-statements/Administration/SHOW_PLUGINS.md) を使用して、インストールが成功したかどうかを確認できます。

    次の例では、プラグイン `AuditLoader` の `Status` が `INSTALLED` であることを示しており、インストールが成功したことを意味します。

    ```Plain
    mysql> SHOW PLUGINS\G
    *************************** 1. row ***************************
        Name: __builtin_AuditLogBuilder
        Type: AUDIT
    Description: builtin audit logger
        Version: 0.12.0
    JavaVersion: 1.8.31
    ClassName: com.starrocks.qe.AuditLogBuilder
        SoName: NULL
        Sources: Builtin
        Status: INSTALLED
    Properties: {}
    *************************** 2. row ***************************
        Name: AuditLoader
        Type: AUDIT
    Description: Available for versions 2.3+. Load audit log to starrocks, and user can view the statistic of queries.
        Version: 4.0.0
    JavaVersion: 1.8.0
    ClassName: com.starrocks.plugin.audit.AuditLoaderPlugin
        SoName: NULL
        Sources: /x/xx/xxx/xxxxx/auditloader.zip
        Status: INSTALLED
    Properties: {}
    2 rows in set (0.01 sec)
    ```

2. いくつかのランダムな SQL を実行して監査ログを生成し、AuditLoader が監査ログを StarRocks にロードするのを許可するために 60 秒（または AuditLoader を設定する際に指定した `max_batch_interval_sec` の時間）待ちます。

3. テーブルをクエリして監査ログを確認します。

    ```SQL
    SELECT * FROM starrocks_audit_db__.starrocks_audit_tbl__;
    ```

    次の例は、監査ログがテーブルに正常にロードされた場合を示しています。

    ```Plain
    mysql> SELECT * FROM starrocks_audit_db__.starrocks_audit_tbl__\G
    *************************** 1. row ***************************
         queryId: 84a69010-d47e-11ee-9647-024228044898
       timestamp: 2024-02-26 16:10:35
       queryType: query
        clientIp: xxx.xx.xxx.xx:65283
            user: root
    authorizedUser: 'root'@'%'
    resourceGroup: default_wg
         catalog: default_catalog
              db: 
           state: EOF
       errorCode:
       queryTime: 3
       scanBytes: 0
        scanRows: 0
      returnRows: 1
       cpuCostNs: 33711
    memCostBytes: 4200
          stmtId: 102
         isQuery: 1
            feIp: xxx.xx.xxx.xx
            stmt: SELECT * FROM starrocks_audit_db__.starrocks_audit_tbl__
          digest:
    planCpuCosts: 0
    planMemCosts: 0
    1 row in set (0.01 sec)
    ```

## トラブルシューティング

動的パーティションが作成され、プラグインがインストールされた後もテーブルに監査ログがロードされない場合は、**plugin.conf** が正しく設定されているかどうかを確認できます。これを変更するには、まずプラグインをアンインストールする必要があります。

```SQL
UNINSTALL PLUGIN AuditLoader;
```

すべての設定が正しく行われたら、上記の手順に従って再度 AuditLoader をインストールできます。