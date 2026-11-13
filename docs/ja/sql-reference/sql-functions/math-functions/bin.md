---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "入力をバイナリに変換します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# bin

入力 `arg` をバイナリに変換します。

## 構文

```Shell
bin(arg)
```

## パラメータ

`arg`: バイナリに変換したい入力です。BIGINT データ型をサポートします。

## 戻り値

VARCHAR データ型の値を返します。

## 例

```Plain
mysql> select bin(3);
+--------+
| bin(3) |
+--------+
| 11     |
+--------+
1 row in set (0.02 sec)
```