---
displayed_sidebar: docs
---

# DROP FILE

DROP FILE ステートメントを実行してファイルを削除できます。このステートメントを使用してファイルを削除すると、ファイルは frontend (FE) メモリと Berkeley DB Java Edition (BDBJE) の両方から削除されます。

## Syntax

```SQL
DROP FILE "file_name" [FROM database]
[properties]
```

## Parameters

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| file_name     | Yes          | ファイルの名前。                                             |
| database      | No           | ファイルが属するデータベース。                               |
| properties    | Yes          | ファイルのプロパティ。以下の表は、properties の構成項目を説明しています。 |

**`properties` の構成項目**

| **Configuration items** | **Required** | **Description**                       |
| ----------------------- | ------------ | ------------------------------------- |
| catalog                 | Yes          | ファイルが属するカテゴリ。            |

## Examples

**ca.pem** という名前のファイルを削除します。

```SQL
DROP FILE "ca.pem" properties("catalog" = "kafka");
```