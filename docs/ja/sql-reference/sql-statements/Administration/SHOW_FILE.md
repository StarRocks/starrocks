---
displayed_sidebar: docs
---

# SHOW FILE

SHOW FILE ステートメントを実行して、データベースに保存されているファイルの情報を表示できます。

## Syntax

```SQL
SHOW FILE [FROM database]
```

このステートメントによって返されるファイル情報は次のとおりです:

- `FileId`: ファイルのグローバルに一意なID。

- `DbName`: ファイルが属するデータベース。

- `Catalog`: ファイルが属するカテゴリ。

- `FileName`: ファイルの名前。

- `FileSize`: ファイルのサイズ。単位はバイトです。

- `MD5`: ファイルをチェックするために使用されるメッセージダイジェストアルゴリズム。

## Examples

`my_database` に保存されているファイルを表示します。

```SQL
SHOW FILE FROM my_database;
```