---
displayed_sidebar: docs
description: "16進数H3文字列を解析して対応するBIGINTインデックスを返します。"
---

# stringToH3

16進数H3文字列を解析して対応するBIGINTインデックスを返します。これは`h3ToString`の逆操作です。入力は有効なH3 16進数文字列（`0x`プレフィックスなし）でなければなりません。

## Syntax

```Haskell
BIGINT stringToH3(VARCHAR h3string)
```

## パラメータ

- `h3string`: 16進数H3インデックス文字列。サポートされるデータ型: VARCHAR。

## 戻り値

H3セルインデックスをBIGINTで返します。入力がNULL、または有効なH3 16進数文字列でない場合はNULLを返します。

## 例

```sql
SELECT stringToH3('89184926cc3ffff');
+--------------------------------+
| stringToH3('89184926cc3ffff') |
+--------------------------------+
| 617420388351344639             |
+--------------------------------+
```

## キーワード

STRINGTOH3,H3,SPATIAL
