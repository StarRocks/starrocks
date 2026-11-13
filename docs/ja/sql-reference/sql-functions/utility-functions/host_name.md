---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "計算が実行されているノードのホスト名を取得します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# host_name

計算が実行されているノードのホスト名を取得します。

## 構文

```plaintext
host_name();
```

## パラメータ

なし

## 戻り値

VARCHAR 値を返します。

## 例

```Plaintext
select host_name();
+-------------+
| host_name() |
+-------------+
| sandbox-sql |
+-------------+
1 row in set (0.01 sec)
```