---
displayed_sidebar: "Chinese"
---

# 使用标签管理 BE 节点

自 3.3.0 版本起，StarRocks 支持基于 BE 节点所在机架、数据中心等信息，使用标签对 BE 节点进行分组，这样可以保证数据可以按机架、数据中心等信息均匀分布，应对某些机架断电、或数据中心故障情况下的灾备需求。

## 功能简介

高可靠数据存储的核心：**所有相同副本不在同一个位置，尽可能均匀分布，以避免单个位置出现故障导致相同副本记录的那部分数据丢失。**

目前 StarRocks 中 Tablet 以多副本 (Replica) 的形式分布在 BE 节点时，只保证所有相同的副本不放置在同一个 BE 中，如此确实避免一个 BE 节点的异常影响服务的可用性。然而在实际场景中集群范围可能覆盖多个机架或者多个数据中心，如果副本分布没有考虑整体集群部署范围中的 BE 节点所在机架或者数据中心等位置情况，可能导致所有相同的副本放置在同一个机架或者数据中心中，则某个机架断电，或某个数据中心出现故障，则会导致这些相同副本记录的那部分数据丢失。

为了进一步提高数据可靠性，自 3.3.0 起，StarRocks 支持基于 BE 节点所在机架、数据中心等信息，使用标签对 BE 节点进行分组，这样 StarRocks 可以感知到 BE 节点的地理位置情况。StarRocks 通过在副本分布时确保相同副本均衡分布在各个标签中，同时相同副本也均衡分布在同一标签内的 BE 节点中。可以保证数据可以按机架、数据中心等信息均匀分布，以避免区域性故障影响服务的可用性。

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

如果表是升级至 3.3.0 版本之前已经创建的历史表，默认不使用标签分布数据。如果需要按照标签分布历史表数据，则可以执行如下语句，为历史表添加标签：

```SQL
ALTER TABLE example_table1
    SET ("labels.location" = "rack:rack1,rack:rack2");
```

:::
