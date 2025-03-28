---
displayed_sidebar: docs
---

# AuditLoader を使用して StarRocks 内の監査ログを管理する

このトピックでは、プラグイン - AuditLoader を使用して、StarRocks 内のテーブルで監査ログを管理する方法について説明します。

StarRocks は監査ログを内部データベースではなく、ローカルファイル **fe/log/fe.audit.log** に保存します。プラグイン AuditLoader を使用すると、クラスター内で監査ログを直接管理できます。インストール後、AuditLoader はファイルからログを読み取り、HTTP PUT を介して StarRocks にロードします。その後、SQL ステートメントを使用して StarRocks 内の監査ログをクエリできます。

## 監査ログを保存するテーブルを作成する

StarRocks クラスター内にデータベースとテーブルを作成し、監査ログを保存します。詳細な手順については、[CREATE DATABASE](../../sql-reference/sql-statements/Database/CREATE_DATABASE.md) および [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) を参照してください。

監査ログのフィールドは異なる StarRocks バージョン間で異なるため、以下の例から選択して、使用している StarRocks に互換性のあるテーブルを作成する必要があります。

> **注意**
>
> - 例のテーブルスキーマを変更しないでください。変更すると、ログのロードが失敗します。
> - 監査ログのフィールドは異なる StarRocks バージョン間で異なるため、新しいバージョンの AuditLoader は、利用可能なすべての StarRocks バージョンから共通のフィールドを収集します。

```SQL
CREATE DATABASE starrocks_audit_db__;

CREATE TABLE starrocks_audit_db__.starrocks_audit_tbl__ (
  `queryId`           VARCHAR(64)                COMMENT "一意のクエリ ID",
  `timestamp`         DATETIME         NOT NULL  COMMENT "クエリ開始時間",
  `queryType`         VARCHAR(12)                COMMENT "クエリタイプ (query, slow_query, connection）",
  `clientIp`          VARCHAR(32)                COMMENT "クライアント IP アドレス",
  `user`              VARCHAR(64)                COMMENT "クエリを開始したユーザー",
  `authorizedUser`    VARCHAR(64)                COMMENT "user_identity",
  `resourceGroup`     VARCHAR(64)                COMMENT "リソースグループ名",
  `catalog`           VARCHAR(32)                COMMENT "カタログ名",
  `db`                VARCHAR(96)                COMMENT "クエリがスキャンするデータベース",
  `state`             VARCHAR(8)                 COMMENT "クエリ状態 (EOF, ERR, OK)",
  `errorCode`         VARCHAR(512)               COMMENT "エラーコード",
  `queryTime`         BIGINT                     COMMENT "クエリの遅延時間 (ミリ秒単位)",
  `scanBytes`         BIGINT                     COMMENT "スキャンされたデータのサイズ (バイト単位)",
  `scanRows`          BIGINT                     COMMENT "スキャンされたデータの行数",
  `returnRows`        BIGINT                     COMMENT "結果の行数",
  `cpuCostNs`         BIGINT                     COMMENT "クエリの CPU リソース消費時間 (ナノ秒単位)",
  `memCostBytes`      BIGINT                     COMMENT "クエリのメモリコスト (バイト単位)",
  `stmtId`            INT                        COMMENT "インクリメンタル SQL ステートメント ID",
  `isQuery`           TINYINT                    COMMENT "SQL がクエリかどうか (0 または 1)",
  `feIp`              VARCHAR(128)               COMMENT "SQL を実行する FE の IP アドレス",
  `stmt`              VARCHAR(1048576)           COMMENT "元の SQL ステートメント",
  `digest`            VARCHAR(32)                COMMENT "スロー SQL フィンガープリント",
  `planCpuCosts`      DOUBLE                     COMMENT "プランニングの CPU リソース消費時間 (ナノ秒単位)",
  `planMemCosts`      DOUBLE                     COMMENT "プランニングのメモリコスト (バイト単位)",
  `warehouse`         VARCHAR(128)               COMMENT "ウェアハウス名"
) ENGINE = OLAP
DUPLICATE KEY (`queryId`, `timestamp`, `queryType`)
COMMENT "監査ログテーブル"
PARTITION BY RANGE (`timestamp`) ()
DISTRIBUTED BY HASH (`queryId`) BUCKETS 3 
PROPERTIES (
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.start" = "-30",       -- 最新の 30 日間の監査ログを保持します。ビジネスの需要に応じてこの値を調整できます。
  "dynamic_partition.end" = "3",
  "dynamic_partition.prefix" = "p",
  "dynamic_partition.buckets" = "3",
  "dynamic_partition.enable" = "true",
  "replication_num" = "3"                 -- 監査ログの 3 つのレプリカを保持します。運用環境では 3 つのレプリカを保持することをお勧めします。
);
```

`starrocks_audit_tbl__` は動的パーティションで作成されます。デフォルトでは、テーブル作成後 10 分で最初の動的パーティションが作成されます。その後、監査ログをテーブルにロードできます。次のステートメントを使用してテーブル内のパーティションを確認できます。

```SQL
SHOW PARTITIONS FROM starrocks_audit_db__.starrocks_audit_tbl__;
```

パーティションが作成されたら、次のステップに進むことができます。

## AuditLoader のダウンロードと設定

1. [AuditLoader インストールパッケージ](https://releases.starrocks.io/resources/AuditLoader.zip) をダウンロードします。このパッケージは、利用可能なすべての StarRocks バージョンと互換性があります。

2. インストールパッケージを解凍します。

    ```shell
    unzip auditloader.zip
    ```

    次のファイルが展開されます。

    - **auditloader.jar**: AuditLoader の JAR ファイル。
    - **plugin.properties**: AuditLoader のプロパティファイル。このファイルを変更する必要はありません。
    - **plugin.conf**: AuditLoader の設定ファイル。通常、ファイル内の `user` と `password` フィールドを変更するだけで済みます。

3. **plugin.conf** を変更して AuditLoader を設定します。AuditLoader が正しく動作するために、次の項目を設定する必要があります。

    - `frontend_host_port`: FE の IP アドレスと HTTP ポート、形式は `<fe_ip>:<fe_http_port>`。デフォルト値 `127.0.0.1:8030` に設定することをお勧めします。StarRocks の各 FE は独立して監査ログを管理し、プラグインをインストールすると、各 FE は独自のバックグラウンドスレッドを開始して監査ログを取得し保存し、Stream Load を介して書き込みます。`frontend_host_port` 設定項目は、プラグインのバックグラウンド Stream Load タスクのための HTTP プロトコルの IP とポートを提供するために使用され、このパラメータは複数の値をサポートしていません。パラメータの IP 部分にはクラスター内の任意の FE の IP を使用できますが、対応する FE がクラッシュした場合、他の FE のバックグラウンドでの監査ログ書き込みタスクも通信の失敗により失敗するため、お勧めしません。デフォルト値 `127.0.0.1:8030` に設定することで、各 FE は独自の HTTP ポートを使用して通信し、他の FE の例外が発生した場合の通信への影響を回避できます（すべての書き込みタスクは最終的に FE Leader ノードに転送されて実行されます）。
    - `database`: 監査ログをホストするために作成したデータベースの名前。
    - `table`: 監査ログをホストするために作成したテーブルの名前。
    - `user`: クラスターのユーザー名。このユーザーはテーブルにデータをロードする権限 (LOAD_PRIV) を持っている必要があります。
    - `password`: ユーザーパスワード。
    - `secret_key`: パスワードを暗号化するために使用されるキー（文字列、16 バイトを超えてはならない）。このパラメータが設定されていない場合、**plugin.conf** 内のパスワードは暗号化されず、`password` に平文のパスワードを指定するだけで済みます。このパラメータが指定されている場合、パスワードはこのキーで暗号化されていることを示し、`password` に暗号化された文字列を指定する必要があります。暗号化されたパスワードは、StarRocks で `AES_ENCRYPT` 関数を使用して生成できます：`SELECT TO_BASE64(AES_ENCRYPT('password','secret_key'));`。
    - `enable_compute_all_query_digest`: すべてのクエリに対して Hash SQL フィンガープリントを生成するかどうか（StarRocks はデフォルトでスロークエリに対してのみ SQL フィンガープリントを有効にしています）。プラグインでのフィンガープリント計算は FE のものとは異なり、[SQL ステートメントを正規化](../Query_planning.md#sql-fingerprint) しますが、プラグインはしません。この機能を有効にすると、フィンガープリント計算は追加の計算リソースを消費します。
    - `filter`: 監査ログロードのフィルター条件。このパラメータは Stream Load の [WHERE パラメータ](../../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md#opt_properties) に基づいており、`-H “where: <condition>”` として指定され、デフォルトは空の文字列です。例：`filter=isQuery=1 and clientIp like '127.0.0.1%' and user='root'`。

4. ファイルを再度パッケージに圧縮します。

    ```shell
    zip -q -m -r auditloader.zip auditloader.jar plugin.conf plugin.properties
    ```

5. パッケージを FE ノードをホストするすべてのマシンに配布します。すべてのパッケージが同一のパスに保存されていることを確認してください。そうでない場合、インストールは失敗します。パッケージを配布した後、パッケージの絶対パスをコピーすることを忘れないでください。

  > **注意**
  >
  > **auditloader.zip** をすべての FE がアクセス可能な HTTP サービス（たとえば、`httpd` や `nginx`）に配布し、ネットワークを使用してインストールすることもできます。どちらの場合も、インストールが実行された後、**auditloader.zip** がパスに永続化され、インストール後にソースファイルを削除しないように注意してください。

## AuditLoader のインストール

AuditLoader を StarRocks のプラグインとしてインストールするために、コピーしたパスと共に次のステートメントを実行します。

```SQL
INSTALL PLUGIN FROM "<absolute_path_to_package>";
```

ローカルパッケージからのインストール例：

```SQL
INSTALL PLUGIN FROM "<absolute_path_to_package>";
```

ネットワークパスを介してプラグインをインストールする場合、INSTALL ステートメントのプロパティにパッケージの md5 を提供する必要があります。

例：

```sql
INSTALL PLUGIN FROM "http://xx.xx.xxx.xxx/extra/auditloader.zip" PROPERTIES("md5sum" = "3975F7B880C9490FE95F42E2B2A28E2D");
```

詳細な手順については、[INSTALL PLUGIN](../../sql-reference/sql-statements/cluster-management/plugin/INSTALL_PLUGIN.md) を参照してください。

## インストールの確認と監査ログのクエリ

1. [SHOW PLUGINS](../../sql-reference/sql-statements/cluster-management/plugin/SHOW_PLUGINS.md) を使用して、インストールが成功したかどうかを確認できます。

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
    Description: Available for versions 2.5+. Load audit log to starrocks, and user can view the statistic of queries
        Version: 4.2.1
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
       warehouse: default_warehouse
    1 row in set (0.01 sec)
    ```

## トラブルシューティング

動的パーティションが作成され、プラグインがインストールされた後もテーブルに監査ログがロードされない場合、**plugin.conf** が正しく設定されているかどうかを確認できます。これを変更するには、まずプラグインをアンインストールする必要があります。

```SQL
UNINSTALL PLUGIN AuditLoader;
```

AuditLoader のログは各 FE の **fe.log** に出力されます。**fe.log** 内で `audit` というキーワードを検索することで取得できます。すべての設定が正しく行われた後、上記の手順に従って再度 AuditLoader をインストールできます。