# parse_url

## Description

Parses a URL and extracts a component from this URL.

## Syntax

```Haskell
parse_url(expr1,expr2);
```

## Parameters

`expr1`: the URL. The supported data type is VARCHAR.

`expr2`: the component to extract from this URL. The supported data type is VARCHAR. Valid values:

- PROTOCOL
- HOST
- PATH
- REF
- AUTHORITY
- FILE
- USERINFO
- QUERY. Parameters in QUERY cannot be returned. If you want to return specific parameters, use parse_url with [trim](trim.md) to achieve this implementation. For details, see [Examples](#examples).

`expr2` is **case-sensitive**.

## Return value

Returns a value of the VARCHAR type. If the URL is invalid, an error is returned. If the requested information cannot be find, NULL is returned.

## Examples

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
