# DATE

## description

### Syntax

DATE函数

DATE(expr)

将输入的类型转化为DATE类型

DATE类型

日期类型，目前的取值范围是['0000-01-01', '9999-12-31'], 默认的打印形式是'YYYY-MM-DD'

## example

```sql
mysql> SELECT DATE('2003-12-31 01:02:03');
-> '2003-12-31'
```

## keyword

DATE
