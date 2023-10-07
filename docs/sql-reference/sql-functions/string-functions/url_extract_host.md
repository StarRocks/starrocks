# url_extract_host

## Description

Parses a URL HOST from this URL.

## Syntax

```Haskell
url_extract_host(expr);
```

## Parameters

`expr`: the URL. The supported data type is VARCHAR.

## Return value

Returns a value of the VARCHAR type. If the URL is invalid, an error is returned. If the requested information cannot be find, NULL is returned.

## Examples

```Plain Text
select url_extract_host('http://starrocks.com/path/p1.php?query=1');
+--------------------------------------------------------------+
| url_extract_host('http://starrocks.com/path/p1.php?query=1') |
+--------------------------------------------------------------+
| starrocks.com                                                 |
+--------------------------------------------------------------+
```
