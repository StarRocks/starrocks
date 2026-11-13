---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "入力パラメータの中で最初に NULL でない式を返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# coalesce

入力パラメータの中で最初にNULLでない式を返します。NULLでない式が見つからない場合はNULLを返します。

## Syntax

```plaintext
coalesce(expr1,...);
```

## Parameters

`expr1`: 入力式で、互換性のあるデータ型に評価される必要があります。

## Return value

戻り値は`expr1`と同じ型です。

## Examples

```Plain Text
mysql> select coalesce(3,NULL,1,1);
+-------------------------+
| coalesce(3, NULL, 1, 1) |
+-------------------------+
|                       3 |
+-------------------------+
1 row in set (0.00 sec)
```