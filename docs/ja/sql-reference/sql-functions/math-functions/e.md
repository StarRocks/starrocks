---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "自然対数の底である定数 e を返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# e

自然対数の底である定数 e を返します。

## 構文

```SQL
E();
```

## パラメータ

この関数はパラメータを取りません。

## 戻り値

定数 e を DOUBLE 型で返します。

## 例

```Plaintext
mysql> select e();
+-------------------+
| e()               |
+-------------------+
| 2.718281828459045 |
+-------------------+
1 row in set (0.01 sec)
```