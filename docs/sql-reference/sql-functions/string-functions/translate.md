## Description
The function `TRANSLATE()` is used to substitute characters within a string. It works by taking a string as input, along with a set of characters to be replaced, and the corresponding characters to replace them with. TRANSLATE() then performs the specified substitutions.

## Syntax

```Haskell
TRANSLATE( <expr>, <from_string>, <to_string> )
```

in which
- `expr`: the expression to be translated. When a character in the expr is not found in the from_string, it is simply included in the result string.
- `from_string`: Each character in the from_string is either replaced by its corresponding character in the to_string, or if there is no corresponding character (i.e. if the to_string has fewer characters than the from_string, the character is excluded from the resulting string.)
- `to_string`: A string with all characters that are used to replace characters from the from_string.
If to_string is longer than from_string, need to report an error The third parameter is too long and would be truncated.

## Examples

```SQL
MySQL > select translate('s1m1a1rrfcks','mf1','to') as test;

+-----------+
| test      |
+-----------+
| starrocks |
+-----------+
```

## Keywords

TRANSLATE