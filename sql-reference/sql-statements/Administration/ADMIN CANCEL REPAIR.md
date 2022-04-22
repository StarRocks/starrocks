# ADMIN CANCEL REPAIR

## 功能

该语句用于取消以高优先级修复指定表或分区。

## 语法

```sql
ADMIN CANCEL REPAIR TABLE table_name[ PARTITION (p1,...)];
```

注：方括号 [] 中内容可省略不写。

说明：

1. 该语句仅表示系统不再以高优先级修复指定表或分区的分片副本。系统仍会以默认调度方式修复副本。

## 示例

1. 取消高优先级修复。

    ```sql
    ADMIN CANCEL REPAIR TABLE tbl PARTITION(p1);
    ```

## 关键字(keywords)

ADMIN，CANCEL， REPAIR
