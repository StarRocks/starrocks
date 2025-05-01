---
displayed_sidebar: docs
---

# space

## 説明

指定された数のスペースからなる文字列を返します。

## 構文

```Haskell
space(x);
```

## パラメータ

`x`: 返すスペースの数。サポートされているデータ型は INT です。

## 戻り値

VARCHAR 型の値を返します。

## 例

```Plain Text
mysql> select space(6);
+----------+
| space(6) |
+----------+
|          |
+----------+
1 row in set (0.00 sec)
```

## キーワード

SPACE