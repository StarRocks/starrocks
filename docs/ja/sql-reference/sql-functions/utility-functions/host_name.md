---
displayed_sidebar: docs
---

# host_name

## 説明

計算が実行されるノードのホスト名を取得します。

## 構文

```Haskell
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