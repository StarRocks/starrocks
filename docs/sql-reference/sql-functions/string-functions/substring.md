# substring

## description

## Syntax

```Haskell
VARCHAR SUBSTRING(VARCHAR, start, length)
```

The SUBSTRING() function extracts some characters from a string.

## Parameter Values

VARCHAR	Required. The string to extract from
start	Required. The start position. The first position in string is 1
length	Required. The number of characters to extract. Must be a positive number

## example

```Plain Text
MySQL > select substring("starrockscluster", 1, 9);
+----------------------------+
| substring("starrockscluster", 1, 9) |
+----------------------------+
| starrocks                      |
+----------------------------+
```

## keyword

substring,string,sub