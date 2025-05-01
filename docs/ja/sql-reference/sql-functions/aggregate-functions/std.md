---
displayed_sidebar: docs
---

# std

## 説明

式の標準偏差を返します。バージョン v2.5.10 以降、この関数はウィンドウ関数としても使用できます。

## 構文

```Haskell
STD(expr)
```

## パラメータ

`expr`: 式です。テーブルのカラムである場合、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、または DECIMAL に評価されなければなりません。

## 戻り値

DOUBLE 値を返します。

## 例

サンプルデータセット

```plaintext
MySQL > select * from std_test;
+------+------+
| col0 | col1 |
+------+------+
|    0 |    0 |
|    1 |    2 |
|    2 |    4 |
|    3 |    6 |
|    4 |    8 |
+------+------+
```

`col0` と `col1` の標準偏差を計算します。

```plaintext
MySQL > select std(col0) as std_of_col0, std(col1) as std_of_col1 from std_test;
+--------------------+--------------------+
| std_of_col0        | std_of_col1        |
+--------------------+--------------------+
| 1.4142135623730951 | 2.8284271247461903 |
+--------------------+--------------------+
```

## キーワード

STD