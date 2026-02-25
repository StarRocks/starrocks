---
displayed_sidebar: docs
---

# stddev_samp

式の標本標準偏差を返します。バージョン 2.5.10 以降、この関数はウィンドウ関数としても使用できます。

## 構文

```Haskell
STDDEV_SAMP(expr)
```

## パラメータ

`expr`: 式。テーブルの列である場合、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、または DECIMAL に評価される必要があります。

## 戻り値

DOUBLE 値を返します。式は以下の通りで、`n` はテーブルの行数を表します。

![image](../../../_assets/stddevsamp_formula.png)

## 例

```plain text
MySQL > select stddev_samp(scan_rows)
from log_statis
group by datetime;
+--------------------------+
| stddev_samp(`scan_rows`) |
+--------------------------+
|        2.372044195280762 |
+--------------------------+
```

## 関連項目

[stddev](./stddev.md)

## キーワード

STDDEV_SAMP,STDDEV,SAMP