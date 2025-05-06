---
displayed_sidebar: docs
---

# current_role

## 説明

現在のユーザーに対してアクティブになっているロールをクエリします。

## 構文

```Haskell
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