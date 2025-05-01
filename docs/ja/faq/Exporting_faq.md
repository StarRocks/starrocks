---
displayed_sidebar: docs
---

# データエクスポート

## Alibaba cloud OSS バックアップとリストア

StarRocks は、alicloud OSS / AWS S3（または S3 プロトコルと互換性のあるオブジェクトストレージ）へのデータのバックアップをサポートしています。DB1 クラスターと DB2 クラスターという 2 つの StarRocks クラスターがあるとします。DB1 のデータを alicloud OSS にバックアップし、必要に応じて DB2 にリストアします。バックアップとリカバリーの一般的なプロセスは次のとおりです。

### クラウドリポジトリの作成

DB1 と DB2 でそれぞれ SQL を実行します:

```sql
CREATE REPOSITORY `repository name`
WITH BROKER `broker_name`
ON LOCATION "oss://bucket name/path"
PROPERTIES
(
"fs.oss.accessKeyId" = "xxx",
"fs.oss.accessKeySecret" = "yyy",
"fs.oss.endpoint" = "oss-cn-beijing.aliyuncs.com"
);
```

a. DB1 と DB2 の両方で作成する必要があり、作成された REPOSITORY 名は同じである必要があります。リポジトリを表示します:

```sql
SHOW REPOSITORIES;
```

b. broker_name はクラスター内のブローカー名を入力する必要があります。BrokerName を表示します:

```sql
SHOW BROKER;
```

c. fs.oss.endpoint の後のパスにはバケット名を含める必要はありません。

### データテーブルのバックアップ

バックアップするテーブルを DB1 のクラウドリポジトリに BACKUP します。DB1 で SQL を実行します:

```sql
BACKUP SNAPSHOT [db_name].{snapshot_name}
TO `repository_name`
ON (
`table_name` [PARTITION (`p1`, ...)],
...
)
PROPERTIES ("key"="value", ...);
```

```plain text
PROPERTIES は現在、以下のプロパティをサポートしています:
"type" = "full": これはフルアップデートであることを示します（デフォルト）。
"timeout" = "3600": タスクのタイムアウト。デフォルトは 1 日です。単位は秒です。
```

StarRocks は現時点でフルデータベースバックアップをサポートしていません。バックアップするテーブルまたはパーティションを ON (...) で指定する必要があり、これらのテーブルまたはパーティションは並行してバックアップされます。

進行中のバックアップタスクを表示します（同時に実行できるバックアップタスクは 1 つだけです）:

```sql
SHOW BACKUP FROM db_name;
```

バックアップが完了したら、OSS にバックアップデータが既に存在するかどうかを確認できます（不要なバックアップは OSS で削除する必要があります）:

```sql
SHOW SNAPSHOT ON OSS repository name; 
```

### データリストア

DB2 でのデータリストアには、DB2 にリストアするテーブル構造を作成する必要はありません。リストア操作中に自動的に作成されます。リストア SQL を実行します:

```sql
RESTORE SNAPSHOT [db_name].{snapshot_name}
FROM `repository_name`
ON (
    'table_name' [PARTITION ('p1', ...)] [AS 'tbl_alias'],
    ...
)
PROPERTIES ("key"="value", ...);
```

リストアの進行状況を表示します:

```sql
SHOW RESTORE;
```