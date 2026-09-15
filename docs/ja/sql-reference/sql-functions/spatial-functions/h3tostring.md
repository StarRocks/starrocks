---
displayed_sidebar: docs
description: "H3セルインデックスをBIGINT整数表現から16進数文字列に変換します。"
---

# h3ToString

H3セルインデックスをBIGINT整数表現から小文字の16進数文字列に変換します。これはH3ライブラリと外部ツールで使用される標準的な文字列形式です。

## Syntax

```Haskell
VARCHAR h3ToString(BIGINT h3index)
```

## パラメータ

- `h3index`: H3セルインデックス。サポートされるデータ型: BIGINT。

## 戻り値

H3インデックスの小文字16進数文字列表現をVARCHARで返します。引数がNULLの場合はNULLを返します。

## 例

```sql
SELECT h3ToString(617420388352917503);
+-----------------------------------+
| h3ToString(617420388352917503)    |
+-----------------------------------+
| 89184926cdbffff                   |
+-----------------------------------+
```

## キーワード

H3TOSTRING,H3,SPATIAL
