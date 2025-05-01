---
displayed_sidebar: docs
---

# current_version

## 説明

StarRocks の現在のバージョンを返します。異なるクライアントとの互換性のために、2 つの構文が提供されています。

## 構文

```Haskell
current_version();

@@version_comment;
```

## パラメータ

なし

## 戻り値

VARCHAR 型の値を返します。

## 例

```Plain Text
mysql> select current_version();
+-------------------+
| current_version() |
+-------------------+
| 2.1.2 0782ad7     |
+-------------------+
1 row in set (0.00 sec)

mysql> select @@version_comment;
+-------------------------+
| @@version_comment       |
+-------------------------+
| StarRocks version 2.1.2 |
+-------------------------+
1 row in set (0.01 sec)
```