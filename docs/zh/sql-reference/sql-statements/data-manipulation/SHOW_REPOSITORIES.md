# SHOW REPOSITORIES

## description

该语句用于查看当前已创建的仓库。

<<<<<<< HEAD
语法：
=======
备份数据需要创建仓库，详细的备份恢复操作说明请参考: [备份恢复](../../../administration/Backup_and_restore.md)章节。

## 语法

注：方括号 [] 中内容可省略不写。
>>>>>>> a83aa885d ([Doc] fix links in 2.2 (#35221))

```sql
SHOW REPOSITORIES;
```

```plain text
说明：
1. 各列含义如下：
RepoId：     唯一的仓库ID
RepoName：   仓库名称
CreateTime： 第一次创建该仓库的时间
IsReadOnly： 是否为只读仓库
Location：   仓库中用于备份数据的根目录
Broker：     依赖的 Broker
ErrMsg：     StarRocks 会定期检查仓库的连通性，如果出现问题，这里会显示错误信息
```

## example

1. 查看已创建的仓库：

    ```sql
    SHOW REPOSITORIES;
    ```

## keyword

SHOW, REPOSITORY, REPOSITORIES
