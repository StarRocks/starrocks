---
displayed_sidebar: docs
---

# DROP FILE

## Description

DROP FILE ステートメントを実行してファイルを削除できます。このステートメントを使用してファイルを削除すると、ファイルは frontend (FE) メモリと Berkeley DB Java Edition (BDBJE) の両方から削除されます。

:::tip

この操作には SYSTEM レベルの FILE 権限が必要です。この権限を付与するには、[GRANT](../../account-management/GRANT.md) の指示に従ってください。

:::

## Syntax

```SQL
DROP FILE "file_name" [FROM database]
[properties]
```

## Parameters

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| file_name     | Yes          | ファイルの名前です。                                         |
| database      | No           | ファイルが属するデータベースです。                           |
| properties    | Yes          | ファイルのプロパティです。以下の表はプロパティの設定項目を説明しています。 |

**`properties` の設定項目**

| **Configuration items** | **Required** | **Description**                       |
| ----------------------- | ------------ | ------------------------------------- |
| catalog                 | Yes          | ファイルが属するカテゴリです。        |

## Examples

**ca.pem** という名前のファイルを削除します。

```SQL
DROP FILE "ca.pem" properties("catalog" = "kafka");
```