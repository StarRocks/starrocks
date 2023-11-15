# 数据删除恢复

StarRocks支持对误删除的数据库/表/分区进行数据恢复，在drop table或者 drop database之后，StarRocks不会立刻对数据进行物理删除，而是在Trash中保留一段时间（默认1天），管理员可以通过RECOVER命令对误删除的数据进行恢复

## 相关命令

语法：

~~~sql
-- 1) 恢复 database
RECOVER DATABASE db_name;
-- 2) 恢复 table
RECOVER TABLE [db_name.]table_name;
-- 3) 恢复 partition
RECOVER PARTITION partition_name FROM [db_name.]table_name;
~~~

## 重点说明

1. 该操作仅能恢复之前一段时间内删除的元信息。默认为 1 天。（可通过fe.conf中catalog_trash_expire_second参数配置）
2. 如果删除元信息后新建立了同名同类型的元信息，则之前删除的元信息不能被恢复

## 样例

1. 恢复名为 example_db 的 database

    ~~~sql
    RECOVER DATABASE example_db;
    ~~~

2. 恢复名为 example_tbl 的 table

    ~~~sql
    RECOVER TABLE example_db.example_tbl;
    ~~~

3. 恢复表 example_tbl 中名为 p1 的 partition

    ~~~sql
    RECOVER PARTITION p1 FROM example_tbl;
    ~~~
