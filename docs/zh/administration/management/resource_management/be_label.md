---
displayed_sidebar: "Chinese"
---

# 使用标签管理 BE 节点

自 3.2.8 版本起，StarRocks 支持使用标签对 BE 节点进行分组。如果您在建表时指定标签，则相同副本均衡分布在各个标签中，同时相同副本也均衡分布在同一标签内的 BE 节点中。该特性可以提高数据高可用和隔离资源等。

## 使用方式

### 为 BE 节点添加标签

假设 StarRocks 集群中存在六个 BE 节点，平均分布在三个机架中，则您可以按 BE 节点所在机架，为 BE 节点添加标签。

```SQL
ALTER SYSTEM MODIFY BACKEND "172.xx.xx.46:9050" SET ("labels.location" = "rack:rack1");
ALTER SYSTEM MODIFY BACKEND "172.xx.xx.47:9050" SET ("labels.location" = "rack:rack1");
ALTER SYSTEM MODIFY BACKEND "172.xx.xx.48:9050" SET ("labels.location" = "rack:rack2");
ALTER SYSTEM MODIFY BACKEND "172.xx.xx.49:9050" SET ("labels.location" = "rack:rack2");
ALTER SYSTEM MODIFY BACKEND "172.xx.xx.50:9050" SET ("labels.location" = "rack:rack3");
ALTER SYSTEM MODIFY BACKEND "172.xx.xx.51:9050" SET ("labels.location" = "rack:rack3");
```

添加标签后，可以执行 `SHOW BACKENDS;`，在返回结果的 `Location` 字段中查看 BE 节点的标签。

如果需要修改  BE 节点的标签，可以执行  `ALTER SYSTEM MODIFY BACKEND ``"172.xx.xx.48:9050" SET ("labels.location" = "rack:xxx");`。

### 为表添加标签

如果您需要指定表的数据分布的位置，比如分布在两个机架中 rack1 和 rack2，则您可以为表添加标签。

添加标签后，表中相同 Tablet 的副本按 Round Robin 的方式选取所在的标签。并且同一标签中如果同一 Tablet 的副本存在多个，则这些同一 Tablet 的多个副本会尽可能均匀分布在该标签内的不同的 BE 节点上。

:::note

- 表所在标签中的全部 BE 节点数必须大于副本数，否则会报错 `Table replication num should be less than of equal to the number of available BE nodes`.
- 为表添加的标签必须已经存在，否则会报错  `Getting analyzing error. Detail message: Cannot find any backend with location: rack:xxx`.

:::

#### 建表时

建表时指定表的数据分布在 rack 1 和 rack 2，则可以执行如下语句：

```SQL
CREATE TABLE example_table (
    order_id bigint NOT NULL,
    dt date NOT NULL,
    user_id INT NOT NULL,
    good_id INT NOT NULL,
    cnt int NOT NULL,
    revenue int NOT NULL
)
PROPERTIES
("labels.location" = "rack:rack1,rack:rack2");
```

对于新建的表，表属性 `labels.location` 默认为 `*` ，表示副本在所有标签中均匀分布。

如果新建的表的数据分布无需感知集群中服务器的地理位置信息，可以手动设置表属性 `"labels.location" ``= ``""`。

#### 建表后

建表后如果需要修改表的数据分布位置，例如修改为 rack 1、rack 2 和 rack 3，则可以执行如下语句：

```SQL
ALTER TABLE example_table
    SET ("labels.location" = "rack:rack1,rack:rack2,rack:rack3");
```

:::note

如果您升级 StarRocks 至 3.2.8 或者以后版本，对于升级前已经创建的历史表，默认不使用标签分布数据。如果需要按照标签分布历史表数据，则可以执行如下语句，为历史表添加标签：

```SQL
ALTER TABLE example_table1
    SET ("labels.location" = "rack:rack1,rack:rack2");
```

:::
