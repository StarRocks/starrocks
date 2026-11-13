---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "現在のユーザーに対してアクティブ化されているロールをクエリします。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# current_role

現在のユーザーに対してアクティブ化されているロールをクエリします。

## 構文

```plaintext
current_role();
current_role;
```

## パラメータ

なし。

## 戻り値

VARCHAR 値を返します。

## 例

```Plain
mysql> select current_role();
+----------------+
| current_role() |
+----------------+
| db_admin       |
+----------------+
```