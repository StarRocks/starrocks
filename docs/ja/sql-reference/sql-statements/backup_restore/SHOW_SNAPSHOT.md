---
displayed_sidebar: docs
description: "指定されたリポジトリ内のデータスナップショットを表示します。"
---

# SHOW SNAPSHOT

指定されたリポジトリ内のデータスナップショットを表示します。詳細については、[データのバックアップと復元](../../../administration/management/Backup_and_restore.md)を参照してください。

## 構文

```SQL
SHOW SNAPSHOT ON <repo_name>
[WHERE SNAPSHOT = <snapshot_name> [AND TIMESTAMP = <backup_timestamp>]]
```

## パラメーター

| **パラメーター** | **説明**                                      |
| ---------------- | --------------------------------------------- |
| repo_name        | スナップショットが属するリポジトリの名前。    |
| snapshot_name    | スナップショットの名前。                      |
| backup_timestamp | スナップショットのバックアップタイムスタンプ。|

## 戻り値

| **戻り値** | **説明**                                                        |
| ---------- | --------------------------------------------------------------- |
| Snapshot   | スナップショットの名前。                                        |
| Timestamp  | スナップショットのバックアップタイムスタンプ。                  |
| Status     | スナップショットが正常な場合は `OK` を表示します。正常でない場合はエラーメッセージを表示します。 |
| Database   | スナップショットが属するデータベースの名前。                    |
| Details    | スナップショットのディレクトリと構造を JSON 形式で表示します。  |
| ClusterId  | スナップショットを作成したクラスタの ID。v4.2.0 以降でサポートされます。 |
| FinishTime | バックアップが完了した時刻。v4.2.0 以降でサポートされます。       |
| TTL        | スナップショットの保持期間。[BACKUP](./BACKUP.md) の `ttl` プロパティで指定します。v4.2.0 以降でサポートされます。 |
| ExpireTime | スナップショットの有効期限。これを過ぎると自動クリーンアップの対象になります。v4.2.0 以降でサポートされます。 |

`ClusterId`、`FinishTime`、`TTL`、`ExpireTime` の 4 列はスナップショット自身から読み取られます。スナップショットのメタデータを読み取れない場合、4 列とも `NULL` になります。v4.2.0 より前に作成されたスナップショット、実行中のバックアップ、中断されたバックアップがこれにあたります。永久に保持されるスナップショットでは `TTL` と `ExpireTime` も `NULL` になります。

`Timestamp` はバックアップを開始した時刻であり、スナップショットの名前の由来でもあります。`FinishTime` はバックアップが完了した時刻で、`ExpireTime` はここから算出されます。

## 例

例 1: リポジトリ `example_repo` 内のスナップショットを表示します。

```SQL
SHOW SNAPSHOT ON example_repo;
```

例 2: リポジトリ `example_repo` 内のスナップショット `backup1` を表示します。

```SQL
SHOW SNAPSHOT ON example_repo
WHERE SNAPSHOT = "backup1";
```

例 3: リポジトリ `example_repo` 内のスナップショット `backup1` とバックアップタイムスタンプ `2018-05-05-15-34-26` を表示します。

```SQL
SHOW SNAPSHOT ON example_repo 
WHERE SNAPSHOT = "backup1" AND TIMESTAMP = "2018-05-05-15-34-26";
```