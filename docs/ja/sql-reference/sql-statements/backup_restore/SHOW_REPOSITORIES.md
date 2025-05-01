---
displayed_sidebar: docs
---

# SHOW REPOSITORIES

## 説明

StarRocks で作成されたリポジトリを表示します。

## 構文

```SQL
SHOW REPOSITORIES
```

## 戻り値

| **戻り値** | **説明**                                           |
| ---------- | -------------------------------------------------- |
| RepoId     | リポジトリ ID。                                    |
| RepoName   | リポジトリ名。                                     |
| CreateTime | リポジトリの作成時間。                             |
| IsReadOnly | リポジトリが読み取り専用かどうか。                 |
| Location   | リモートストレージシステム内のリポジトリの場所。   |
| Broker     | リポジトリの作成に使用された Broker。              |
| ErrMsg     | リポジトリへの接続中のエラーメッセージ。           |

## 例

例 1: StarRocks で作成されたリポジトリを表示します。

```SQL
SHOW REPOSITORIES;
```