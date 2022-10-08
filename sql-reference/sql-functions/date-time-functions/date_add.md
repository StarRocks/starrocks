# date_add

## 功能

向日期添加指定的时间间隔。

## 语法

```Haskell
DATETIME DATE_ADD(DATETIME date,INTERVAL expr type)
```

## 参数说明

`date`：必须是合法的日期表达式。

`expr`：需要添加的时间间隔。

`type`：可以是下列值：YEAR, MONTH, DAY, HOUR, MINUTE, SECOND。

## 示例

```Plain Text
select date_add('2010-11-30 23:59:59', INTERVAL 2 DAY);
+-------------------------------------------------+
| date_add('2010-11-30 23:59:59', INTERVAL 2 DAY) |
+-------------------------------------------------+
| 2010-12-02 23:59:59                             |
+-------------------------------------------------+
```
