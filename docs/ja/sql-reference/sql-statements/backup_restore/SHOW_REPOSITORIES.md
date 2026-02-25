---
displayed_sidebar: docs
---

# SHOW REPOSITORIES

StarRocks で作成されたリポジトリを表示します。

## Syntax

```SQL
SHOW REPOSITORIES
```

## Return

| **Return** | **Description**                                      |
| ---------- | ---------------------------------------------------- |
| RepoId     | リポジトリ ID。                                       |
| RepoName   | リポジトリ名。                                        |
| CreateTime | リポジトリの作成時間。                                |
| IsReadOnly | リポジトリが読み取り専用かどうか。                    |
| Location   | リモートストレージシステム内のリポジトリの場所。      |
| Broker     | リポジトリの作成に使用された Broker。                 |
| ErrMsg     | リポジトリへの接続中のエラーメッセージ。             |

## Examples

例 1: StarRocks で作成されたリポジトリを表示します。

```SQL
SHOW REPOSITORIES;
```