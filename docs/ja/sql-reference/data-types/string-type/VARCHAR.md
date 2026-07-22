---
displayed_sidebar: docs
description: "可変長文字列型で長さを指定可能な文字列データ型。"
---

# VARCHAR

## 説明

VARCHAR(M)

可変長の文字列です。`M` は文字列の長さを示します。デフォルト値は `1` です。単位: バイト。

- StarRocks 2.1 より前のバージョンでは、`M` の値の範囲は [1, 65533] です。
- StarRocks 2.1 から 4.1 までのバージョンでは、`M` の値の範囲は [1, 1048576] です。
- StarRocks 4.2 以降では、`M` の値の範囲は [1, 2147482624]（2 GiB から 1 KiB を引いた値）です。

## 例

テーブルを作成し、カラムタイプを VARCHAR と指定します。

```SQL
CREATE TABLE varcharDemo (
    pk INT COMMENT "range [-2147483648, 2147483647]",
    pd_type VARCHAR(20) COMMENT "variable-length string"
) ENGINE=OLAP 
DUPLICATE KEY(pk)
COMMENT "OLAP"
DISTRIBUTED BY HASH(pk)
```