---
displayed_sidebar: docs
---

# var_samp,variance_samp

## Description

式の標本分散を返します。バージョン 2.5.10 以降、この関数はウィンドウ関数としても使用できます。

## Syntax

```Haskell
VAR_SAMP(expr)
```

## Parameters

`expr`: 式です。テーブルの列である場合、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、または DECIMAL に評価される必要があります。

## Return value

DOUBLE 値を返します。

## Examples

```plaintext
MySQL > select var_samp(scan_rows)
from log_statis
group by datetime;
+-----------------------+
| var_samp(`scan_rows`) |
+-----------------------+
|    5.6227132145741789 |
+-----------------------+
```

## keyword

VAR_SAMP,VARIANCE_SAMP,VAR,SAMP,VARIANCE