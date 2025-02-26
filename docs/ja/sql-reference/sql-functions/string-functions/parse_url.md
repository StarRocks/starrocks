---
displayed_sidebar: docs
---

# parse_url

URL を解析し、この URL からコンポーネントを抽出します。

## 構文

```Haskell
parse_url(expr1,expr2);
```

## パラメータ

`expr1`: URL。サポートされているデータ型は VARCHAR です。

`expr2`: この URL から抽出するコンポーネント。サポートされているデータ型は VARCHAR です。有効な値は以下の通りです:

- PROTOCOL
- HOST
- PATH
- REF
- AUTHORITY
- FILE
- USERINFO
- QUERY。QUERY 内のパラメータは返されません。特定のパラメータを返したい場合は、[trim](trim.md) と共に parse_url を使用してこの実装を達成してください。詳細は[例](#examples)を参照してください。

`expr2` は **大文字小文字を区別** します。

## 戻り値

VARCHAR 型の値を返します。URL が無効な場合はエラーが返されます。要求された情報が見つからない場合は、NULL が返されます。

## 例

```Plain Text
select parse_url('http://facebook.com/path/p1.php?query=1', 'HOST');
+--------------------------------------------------------------+
| parse_url('http://facebook.com/path/p1.php?query=1', 'HOST') |
+--------------------------------------------------------------+
| facebook.com                                                 |
+--------------------------------------------------------------+

select parse_url('http://facebook.com/path/p1.php?query=1', 'AUTHORITY');
+-------------------------------------------------------------------+
| parse_url('http://facebook.com/path/p1.php?query=1', 'AUTHORITY') |
+-------------------------------------------------------------------+
| facebook.com                                                      |
+-------------------------------------------------------------------+

select parse_url('http://facebook.com/path/p1.php?query=1', 'QUERY');
+---------------------------------------------------------------+
| parse_url('http://facebook.com/path/p1.php?query=1', 'QUERY') |
+---------------------------------------------------------------+
| query=1                                                       |
+---------------------------------------------------------------+

select trim(parse_url('http://facebook.com/path/p1.php?query=1', 'QUERY'),'query='); 
+-------------------------------------------------------------------------------+
| trim(parse_url('http://facebook.com/path/p1.php?query=1', 'QUERY'), 'query=') |
+-------------------------------------------------------------------------------+
| 1                                                                             |
+-------------------------------------------------------------------------------+
```