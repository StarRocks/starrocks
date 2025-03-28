---
displayed_sidebar: docs
---

# CREATE STORAGE VOLUME

## 説明

リモートストレージシステム用のストレージボリュームを作成します。この機能は v3.1 からサポートされています。

ストレージボリュームは、リモートデータストレージのプロパティと認証情報で構成されます。共有データ StarRocks クラスターでデータベースやクラウドネイティブテーブルを作成する際に、ストレージボリュームを参照できます。

> **注意**
>
> SYSTEM レベルで CREATE STORAGE VOLUME 権限を持つユーザーのみがこの操作を実行できます。

## 構文

```SQL
CREATE STORAGE VOLUME [IF NOT EXISTS] <storage_volume_name>
TYPE = { S3 | HDFS | AZBLOB }
LOCATIONS = ('<remote_storage_path>')
[ COMMENT '<comment_string>' ]
PROPERTIES
("key" = "value",...)
```

## パラメータ

| **パラメータ**       | **説明**                                              |
| ------------------- | ------------------------------------------------------------ |
| storage_volume_name | ストレージボリュームの名前です。`builtin_storage_volume` という名前のストレージボリュームは作成できません。これは組み込みストレージボリュームの作成に使用されるためです。命名規則については、[System limits](../../../System_limit.md) を参照してください。|
| TYPE                | リモートストレージシステムのタイプです。有効な値は `S3`、`HDFS`、`AZBLOB` です。`S3` は AWS S3 または S3 互換ストレージシステムを示します。`AZBLOB` は Azure Blob Storage を示します（v3.1.1 以降でサポート）。`ADLS2` は Azure Data Lake Storage Gen2 を示します（v3.4.1 以降でサポート）。`HDFS` は HDFS クラスターを示します。 |
| LOCATIONS           | ストレージの場所です。形式は次のとおりです：<ul><li>AWS S3 または S3 プロトコル互換ストレージシステムの場合：`s3://<s3_path>`。`<s3_path>` は絶対パスでなければなりません。例：`s3://testbucket/subpath`。ストレージボリュームに [Partitioned Prefix](#partitioned-prefix) 機能を有効にしたい場合、バケット名のみを指定する必要があり、サブパスの指定は許可されません。</li><li>Azure Blob Storage の場合：`azblob://<azblob_path>`。`<azblob_path>` は絶対パスでなければなりません。例：`azblob://testcontainer/subpath`。</li><li>Azure Data Lake Storage Gen2 の場合：`adls2://<file_system_name>/<dir_name>`。例：`adls2://testfilesystem/starrocks`。</li><li>HDFS の場合：`hdfs://<host>:<port>/<hdfs_path>`。`<hdfs_path>` は絶対パスでなければなりません。例：`hdfs://127.0.0.1:9000/user/xxx/starrocks`。</li><li>WebHDFS の場合：`webhdfs://<host>:<http_port>/<hdfs_path>`。`<http_port>` は NameNode の HTTP ポートです。`<hdfs_path>` は絶対パスでなければなりません。例：`webhdfs://127.0.0.1:50070/user/xxx/starrocks`。</li><li>ViewFS の場合：`viewfs://<ViewFS_cluster>/<viewfs_path>`。`<ViewFS_cluster>` は ViewFS クラスター名です。`<viewfs_path>` は絶対パスでなければなりません。例：`viewfs://myviewfscluster/user/xxx/starrocks`。</li></ul> |
| COMMENT             | ストレージボリュームに関するコメントです。                           |
| PROPERTIES          | リモートストレージシステムにアクセスするためのプロパティと認証情報を指定するための `"key" = "value"` ペアのパラメータです。詳細は [PROPERTIES](#properties) を参照してください。 |

### PROPERTIES

以下の表は、ストレージボリュームのすべての利用可能なプロパティを示しています。表の後には、[認証情報](#credential-information) と [機能](#features) の観点から異なるシナリオに基づいたこれらのプロパティの使用方法が示されています。

| **プロパティ**                        | **説明**                                              |
| ----------------------------------- | ------------------------------------------------------------ |
| enabled                             | このストレージボリュームを有効にするかどうか。デフォルトは `false` です。無効なストレージボリュームは参照できません。 |
| aws.s3.region                       | S3 バケットが存在するリージョンです。例：`us-west-2`。 |
| aws.s3.endpoint                     | S3 バケットにアクセスするためのエンドポイント URL です。例：`https://s3.us-west-2.amazonaws.com`。[プレビュー] v3.3.0 以降、Amazon S3 Express One Zone ストレージクラスがサポートされています。例：`https://s3express.us-west-2.amazonaws.com`。   |
| aws.s3.use_aws_sdk_default_behavior | AWS SDK のデフォルト認証情報を使用するかどうか。有効な値は `true` と `false`（デフォルト）です。 |
| aws.s3.use_instance_profile         | S3 にアクセスするための認証方法としてインスタンスプロファイルとアサインドロールを使用するかどうか。有効な値は `true` と `false`（デフォルト）です。<ul><li>IAM ユーザー認証（アクセスキーとシークレットキー）を使用して S3 にアクセスする場合、この項目を `false` に設定し、`aws.s3.access_key` と `aws.s3.secret_key` を指定する必要があります。</li><li>インスタンスプロファイルを使用して S3 にアクセスする場合、この項目を `true` に設定する必要があります。</li><li>アサインドロールを使用して S3 にアクセスする場合、この項目を `true` に設定し、`aws.s3.iam_role_arn` を指定する必要があります。</li><li>外部 AWS アカウントを使用する場合、この項目を `true` に設定し、`aws.s3.iam_role_arn` と `aws.s3.external_id` を指定する必要があります。</li></ul> |
| aws.s3.access_key                   | S3 バケットにアクセスするためのアクセスキー ID です。             |
| aws.s3.secret_key                   | S3 バケットにアクセスするためのシークレットアクセスキーです。         |
| aws.s3.iam_role_arn                 | データファイルが保存されている S3 バケットに対して権限を持つ IAM ロールの ARN です。 |
| aws.s3.external_id                  | S3 バケットへのクロスアカウントアクセスに使用される AWS アカウントの外部 ID です。 |
| azure.blob.endpoint                 | Azure Blob Storage アカウントのエンドポイントです。例：`https://test.blob.core.windows.net`。 |
| azure.blob.shared_key               | Azure Blob Storage へのリクエストを承認するために使用される共有キーです。 |
| azure.blob.sas_token                | Azure Blob Storage へのリクエストを承認するために使用される共有アクセス署名 (SAS) です。 |
| azure.adls2.endpoint                 | Azure Data Lake Storage Gen2 アカウントのエンドポイントです。例：`https://test.dfs.core.windows.net`。 |
| azure.adls2.shared_key               | Azure Data Lake Storage Gen2 へのリクエストを承認するために使用される共有キーです。 |
| azure.adls2.sas_token                | Azure Data Lake Storage Gen2 へのリクエストを承認するために使用される共有アクセス署名 (SAS) です。 |
| hadoop.security.authentication      | 認証方法です。有効な値は `simple`（デフォルト）と `kerberos` です。`simple` はシンプル認証、つまりユーザー名を示します。`kerberos` は Kerberos 認証を示します。 |
| username                            | HDFS クラスターの NameNode にアクセスするためのユーザー名です。                      |
| hadoop.security.kerberos.ticket.cache.path | kinit で生成されたチケットキャッシュを保存するパスです。                   |
| dfs.nameservices                    | HDFS クラスターの名前です。                                        |
| dfs.ha.namenodes.`<ha_cluster_name>`                   | NameNode の名前です。複数の名前はカンマ (,) で区切る必要があります。ダブルクォート内にスペースは許可されません。`<ha_cluster_name>` は `dfs.nameservices` で指定された HDFS サービスの名前です。 |
| dfs.namenode.rpc-address.`<ha_cluster_name>`.`<NameNode>` | NameNode の RPC アドレス情報です。`<NameNode>` は `dfs.ha.namenodes.<ha_cluster_name>` で指定された NameNode の名前です。 |
| dfs.client.failover.proxy.provider                    | クライアント接続のための NameNode のプロバイダーです。デフォルト値は `org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider` です。 |
| fs.viewfs.mounttable.`<ViewFS_cluster>`.link./`<viewfs_path>` | マウントする ViewFS クラスターへのパスです。複数のパスはカンマ (,) で区切る必要があります。`<ViewFS_cluster>` は `LOCATIONS` で指定された ViewFS クラスター名です。 |
| aws.s3.enable_partitioned_prefix    | ストレージボリュームに対して Partitioned Prefix 機能を有効にするかどうか。デフォルトは `false` です。この機能の詳細については、[Partitioned Prefix](#partitioned-prefix) を参照してください。 |
| aws.s3.num_partitioned_prefix       | ストレージボリュームに対して作成されるプレフィックスの数です。デフォルトは `256` です。有効範囲は [4, 1024] です。|

#### 認証情報

##### AWS S3

- AWS SDK のデフォルト認証情報を使用して S3 にアクセスする場合、次のプロパティを設定します：

  ```SQL
  "enabled" = "{ true | false }",
  "aws.s3.region" = "<region>",
  "aws.s3.endpoint" = "<endpoint_url>",
  "aws.s3.use_aws_sdk_default_behavior" = "true"
  ```

- IAM ユーザー認証（アクセスキーとシークレットキー）を使用して S3 にアクセスする場合、次のプロパティを設定します：

  ```SQL
  "enabled" = "{ true | false }",
  "aws.s3.region" = "<region>",
  "aws.s3.endpoint" = "<endpoint_url>",
  "aws.s3.use_aws_sdk_default_behavior" = "false",
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.access_key" = "<access_key>",
  "aws.s3.secret_key" = "<secrete_key>"
  ```

- インスタンスプロファイルを使用して S3 にアクセスする場合、次のプロパティを設定します：

  ```SQL
  "enabled" = "{ true | false }",
  "aws.s3.region" = "<region>",
  "aws.s3.endpoint" = "<endpoint_url>",
  "aws.s3.use_aws_sdk_default_behavior" = "false",
  "aws.s3.use_instance_profile" = "true"
  ```

- アサインドロールを使用して S3 にアクセスする場合、次のプロパティを設定します：

  ```SQL
  "enabled" = "{ true | false }",
  "aws.s3.region" = "<region>",
  "aws.s3.endpoint" = "<endpoint_url>",
  "aws.s3.use_aws_sdk_default_behavior" = "false",
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<role_arn>"
  ```

- 外部 AWS アカウントからアサインドロールを使用して S3 にアクセスする場合、次のプロパティを設定します：

  ```SQL
  "enabled" = "{ true | false }",
  "aws.s3.region" = "<region>",
  "aws.s3.endpoint" = "<endpoint_url>",
  "aws.s3.use_aws_sdk_default_behavior" = "false",
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<role_arn>",
  "aws.s3.external_id" = "<external_id>"
  ```

##### GCS

GCP Cloud Storage を使用する場合、次のプロパティを設定します：

```SQL
"enabled" = "{ true | false }",

-- 例：us-east-1
"aws.s3.region" = "<region>",

-- 例：https://storage.googleapis.com
"aws.s3.endpoint" = "<endpoint_url>",

"aws.s3.access_key" = "<access_key>",
"aws.s3.secret_key" = "<secrete_key>"
```

##### MinIO

MinIO を使用する場合、次のプロパティを設定します：

```SQL
"enabled" = "{ true | false }",

-- 例：us-east-1
"aws.s3.region" = "<region>",

-- 例：http://172.26.xx.xxx:39000
"aws.s3.endpoint" = "<endpoint_url>",

"aws.s3.access_key" = "<access_key>",
"aws.s3.secret_key" = "<secrete_key>"
```

##### Azure Blob Storage

Azure Blob Storage でのストレージボリュームの作成は v3.1.1 以降でサポートされています。

- Shared Key を使用して Azure Blob Storage にアクセスする場合、次のプロパティを設定します：

  ```SQL
  "enabled" = "{ true | false }",
  "azure.blob.endpoint" = "<endpoint_url>",
  "azure.blob.shared_key" = "<shared_key>"
  ```

- 共有アクセス署名 (SAS) を使用して Azure Blob Storage にアクセスする場合、次のプロパティを設定します：

  ```SQL
  "enabled" = "{ true | false }",
  "azure.blob.endpoint" = "<endpoint_url>",
  "azure.blob.sas_token" = "<sas_token>"
  ```

:::note
Azure Blob Storage アカウントを作成する際には、階層型名前空間を無効にする必要があります。
:::

##### Azure Data Lake Storage Gen2

Azure Data Lake Storage Gen2 でのストレージボリュームの作成は v3.4.1 以降でサポートされています。

- Shared Key を使用して Azure Blob Storage にアクセスする場合、次のプロパティを設定します：

  ```SQL
  "enabled" = "{ true | false }",
  "azure.adls2.endpoint" = "<endpoint_url>",
  "azure.adls2.shared_key" = "<shared_key>"
  ```

- 共有アクセス署名 (SAS) を使用して Azure Blob Storage にアクセスする場合、次のプロパティを設定します：

  ```SQL
  "enabled" = "{ true | false }",
  "azure.adls2.endpoint" = "<endpoint_url>",
  "azure.adls2.sas_token" = "<sas_token>"
  ```

:::note
Azure Data Lake Storage Gen1 はサポートされていません。
:::

##### HDFS

- 認証を使用せずに HDFS にアクセスする場合、次のプロパティを設定します：

  ```SQL
  "enabled" = "{ true | false }"
  ```

- シンプル認証（v3.2 からサポート）を使用して HDFS にアクセスする場合、次のプロパティを設定します：

  ```SQL
  "enabled" = "{ true | false }",
  "hadoop.security.authentication" = "simple",
  "username" = "<hdfs_username>"
  ```

- Kerberos チケットキャッシュ認証（v3.2 からサポート）を使用して HDFS にアクセスする場合、次のプロパティを設定します：

  ```SQL
  "enabled" = "{ true | false }",
  "hadoop.security.authentication" = "kerberos",
  "hadoop.security.kerberos.ticket.cache.path" = "<ticket_cache_path>"
  ```

  > **注意**
  >
  > - この設定は、システムが Kerberos 経由で HDFS にアクセスするために KeyTab を使用することを強制するだけです。各 BE または CN ノードが KeyTab ファイルにアクセスできることを確認してください。また、**/etc/krb5.conf** ファイルが正しく設定されていることを確認してください。
  > - チケットキャッシュは外部の kinit ツールによって生成されます。チケットを更新するために crontab または類似の定期的なタスクを設定してください。

- HDFS クラスターが NameNode HA 構成を有効にしている場合（v3.2 からサポート）、さらに次のプロパティを設定します：

  ```SQL
  "dfs.nameservices" = "<ha_cluster_name>",
  "dfs.ha.namenodes.<ha_cluster_name>" = "<NameNode1>,<NameNode2> [, ...]",
  "dfs.namenode.rpc-address.<ha_cluster_name>.<NameNode1>" = "<hdfs_host>:<hdfs_port>",
  "dfs.namenode.rpc-address.<ha_cluster_name>.<NameNode2>" = "<hdfs_host>:<hdfs_port>",
  [...]
  "dfs.client.failover.proxy.provider.<ha_cluster_name>" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
  ```

  詳細については、[HDFS HA Documentation](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithNFS.html) を参照してください。

  - WebHDFS を使用している場合（v3.2 からサポート）、次のプロパティを設定します：

  ```SQL
  "enabled" = "{ true | false }"
  ```

  詳細については、[WebHDFS Documentation](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html) を参照してください。

- Hadoop ViewFS を使用している場合（v3.2 からサポート）、次のプロパティを設定します：

  ```SQL
  -- <ViewFS_cluster> を ViewFS クラスターの名前に置き換えます。
  "fs.viewfs.mounttable.<ViewFS_cluster>.link./<viewfs_path_1>" = "hdfs://<hdfs_host_1>:<hdfs_port_1>/<hdfs_path_1>",
  "fs.viewfs.mounttable.<ViewFS_cluster>.link./<viewfs_path_2>" = "hdfs://<hdfs_host_2>:<hdfs_port_2>/<hdfs_path_2>",
  [, ...]
  ```

  詳細については、[ViewFS Documentation](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/ViewFs.html) を参照してください。

#### 機能

##### Partitioned Prefix

v3.2.4 から、StarRocks は S3 互換オブジェクトストレージシステムに対して Partitioned Prefix 機能を持つストレージボリュームの作成をサポートしています。この機能が有効になると、StarRocks はバケット内のデータを複数の均一にプレフィックスされたパーティション（サブパス）に保存します。これにより、バケットに保存されたデータファイルの読み書き性能が大幅に向上します。なぜなら、バケットの QPS またはスループット制限はパーティションごとに設定されるからです。

この機能を有効にするには、以下のプロパティを上記の認証関連パラメータに加えて設定します：

```SQL
"aws.s3.enable_partitioned_prefix" = "{ true | false }",
"aws.s3.num_partitioned_prefix" = "<INT>"
```

:::note
- Partitioned Prefix 機能は S3 互換オブジェクトストレージシステムにのみ対応しています。つまり、ストレージボリュームの `TYPE` は `S3` でなければなりません。
- ストレージボリュームの `LOCATIONS` にはバケット名のみを含める必要があります。例：`s3://testbucket`。バケット名の後にサブパスを指定することはできません。
- 両方のプロパティは、ストレージボリュームが作成された後は変更できません。
- FE 設定ファイル **fe.conf** を使用してストレージボリュームを作成する際にこの機能を有効にすることはできません。
:::

## 例

例 1: IAM ユーザー認証（アクセスキーとシークレットキー）を使用して S3 にアクセスし、有効化された AWS S3 バケット `defaultbucket` 用のストレージボリューム `my_s3_volume` を作成します。

```SQL
CREATE STORAGE VOLUME my_s3_volume
TYPE = S3
LOCATIONS = ("s3://defaultbucket/test/")
PROPERTIES
(
    "aws.s3.region" = "us-west-2",
    "aws.s3.endpoint" = "https://s3.us-west-2.amazonaws.com",
    "aws.s3.use_aws_sdk_default_behavior" = "false",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.access_key" = "xxxxxxxxxx",
    "aws.s3.secret_key" = "yyyyyyyyyy"
);
```

例 2: HDFS 用のストレージボリューム `my_hdfs_volume` を作成し、有効化します。

```SQL
CREATE STORAGE VOLUME my_hdfs_volume
TYPE = HDFS
LOCATIONS = ("hdfs://127.0.0.1:9000/sr/test/")
PROPERTIES
(
    "enabled" = "true"
);
```

例 3: シンプル認証を使用して HDFS 用のストレージボリューム `hdfsvolumehadoop` を作成します。

```sql
CREATE STORAGE VOLUME hdfsvolumehadoop
TYPE = HDFS
LOCATIONS = ("hdfs://127.0.0.1:9000/sr/test/")
PROPERTIES(
    "hadoop.security.authentication" = "simple",
    "username" = "starrocks"
);
```

例 4: Kerberos チケットキャッシュ認証を使用して HDFS にアクセスし、ストレージボリューム `hdfsvolkerberos` を作成します。

```sql
CREATE STORAGE VOLUME hdfsvolkerberos
TYPE = HDFS
LOCATIONS = ("hdfs://127.0.0.1:9000/sr/test/")
PROPERTIES(
    "hadoop.security.authentication" = "kerberos",
    "hadoop.security.kerberos.ticket.cache.path" = "/path/to/ticket/cache/path"
);
```

例 5: NameNode HA 構成が有効な HDFS クラスター用のストレージボリューム `hdfsvolha` を作成します。

```sql
CREATE STORAGE VOLUME hdfsvolha
TYPE = HDFS
LOCATIONS = ("hdfs://myhacluster/data/sr")
PROPERTIES(
    "dfs.nameservices" = "myhacluster",
    "dfs.ha.namenodes.myhacluster" = "nn1,nn2,nn3",
    "dfs.namenode.rpc-address.myhacluster.nn1" = "machine1.example.com:8020",
    "dfs.namenode.rpc-address.myhacluster.nn2" = "machine2.example.com:8020",
    "dfs.namenode.rpc-address.myhacluster.nn3" = "machine3.example.com:8020",
    "dfs.namenode.http-address.myhacluster.nn1" = "machine1.example.com:9870",
    "dfs.namenode.http-address.myhacluster.nn2" = "machine2.example.com:9870",
    "dfs.namenode.http-address.myhacluster.nn3" = "machine3.example.com:9870",
    "dfs.client.failover.proxy.provider.myhacluster" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
);
```

例 6: WebHDFS 用のストレージボリューム `webhdfsvol` を作成します。

```sql
CREATE STORAGE VOLUME webhdfsvol
TYPE = HDFS
LOCATIONS = ("webhdfs://namenode:9870/data/sr");
```

例 7: Hadoop ViewFS を使用してストレージボリューム `viewfsvol` を作成します。

```sql
CREATE STORAGE VOLUME viewfsvol
TYPE = HDFS
LOCATIONS = ("viewfs://clusterX/data/sr")
PROPERTIES(
    "fs.viewfs.mounttable.clusterX.link./data" = "hdfs://nn1-clusterx.example.com:8020/data",
    "fs.viewfs.mounttable.clusterX.link./project" = "hdfs://nn2-clusterx.example.com:8020/project"
);
```

例 8: Azure Data Lake Storage Gen2 を使用してストレージボリューム `adls2` を作成します。

```SQL
CREATE STORAGE VOLUME adls2
    TYPE = ADLS2
    LOCATIONS = ("adls2://testfilesystem/starrocks")
    PROPERTIES (
        "azure.adls2.endpoint" = "https://test.dfs.core.windows.net",
        "azure.adls2.sas_token" = "xxx"
    );
```

## 関連する SQL ステートメント

- [ALTER STORAGE VOLUME](ALTER_STORAGE_VOLUME.md)
- [DROP STORAGE VOLUME](DROP_STORAGE_VOLUME.md)
- [SET DEFAULT STORAGE VOLUME](SET_DEFAULT_STORAGE_VOLUME.md)
- [DESC STORAGE VOLUME](DESC_STORAGE_VOLUME.md)
- [SHOW STORAGE VOLUMES](SHOW_STORAGE_VOLUMES.md)