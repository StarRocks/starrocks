---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "数値式のビット単位の否定を返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# bitnot

数値式のビット単位の否定を返します。

## 構文

```plaintext
BITNOT(x);
```

## パラメータ

`x`: この式は、TINYINT、SMALLINT、INT、BIGINT、LARGEINT のいずれかのデータ型に評価される必要があります。

## 戻り値

戻り値は `x` と同じ型です。いずれかの値が NULL の場合、結果は NULL です。

## 例

```Plain Text
mysql> select bitnot(3);
+-----------+
| bitnot(3) |
+-----------+
|        -4 |
+-----------+
```