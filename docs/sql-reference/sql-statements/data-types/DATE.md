# DATE

## description

### Syntax

DATE function

DATE(expr)

Convert input type to DATE type

Date type

Date type, the current range of values is ['0000-01-01','9999-12-31'], and the default print form is 'YYYYY-MM-DD'.

## example

```sql
mysql> SELECT DATE('2003-12-31 01:02:03');
-> '2003-12-31'
```
