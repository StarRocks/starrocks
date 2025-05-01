---
displayed_sidebar: docs
---

# stddev,stddev_pop,std

## 説明

`expr` 式の母集団標準偏差を返します。バージョン 2.5.10 以降、この関数はウィンドウ関数としても使用できます。

## 構文

```Haskell
STDDEV(expr)
```

## パラメータ

`expr`: 式です。テーブルの列である場合、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、または DECIMAL に評価される必要があります。

## 戻り値

DOUBLE 値を返します。式は以下の通りで、`n` はテーブルの行数を表します。

![image](../../../_assets/stddevpop_formula.png)

## 例

```plaintext
mysql> SELECT stddev(lo_quantity), stddev_pop(lo_quantity) from lineorder;
+---------------------+-------------------------+
| stddev(lo_quantity) | stddev_pop(lo_quantity) |
+---------------------+-------------------------+
|   14.43100708360797 |       14.43100708360797 |
+---------------------+-------------------------+
```

## 関連項目

[stddev_samp](./stddev_samp.md)

## キーワード

STDDEV,STDDEV_POP,POP