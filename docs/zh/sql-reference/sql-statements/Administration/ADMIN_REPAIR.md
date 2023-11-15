# ADMIN REPAIR

## 功能

该语句用于尝试优先修复指定的表或分区。

## 语法

```sql
ADMIN REPAIR TABLE table_name[ PARTITION (p1,...)];
```

注：方括号 [] 中内容可省略不写。

说明：

1. 该语句仅表示让系统尝试以高优先级修复指定表或分区的分片副本，并不保证能够修复成功。用户可以通过 `ADMIN SHOW REPLICA STATUS;` 命令查看修复情况。
2. 默认的 timeout 是 14400 秒(4 小时)。超时意味着系统将不再以高优先级修复指定表或分区的分片副本。需要重新使用该命令设置。

## 示例

1. 尝试修复指定表

    ```sql
    ADMIN REPAIR TABLE tbl1;
    ```

2. 尝试修复指定分区

    ```sql
    ADMIN REPAIR TABLE tbl1 PARTITION (p1, p2);
    ```

## 关键字(keywords)

ADMIN, REPAIR
