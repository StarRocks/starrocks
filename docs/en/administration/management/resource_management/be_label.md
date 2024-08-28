---
displayed_sidebar: docs
---

# Add labels on BEs

Since v3.2.8, StarRocks supports adding labels on BEs. When creating a table or an asynchronous materialized view, you can specify the label of a certain group of BE nodes. This ensures that data replicas are distributed only on the BE nodes associated with that label. Data replicas will be evenly distributed among nodes with the same label, enhancing data high availability and resource isolation.

## Usage

### Add labels on BEs

Suppose that one StarRocks cluster includes six BEs which are distributed evenly across three racks. You can add labels on BEs based on the racks where the BEs are located.

```SQL
ALTER SYSTEM MODIFY BACKEND "172.xx.xx.46:9050" SET ("labels.location" = "rack:rack1");
ALTER SYSTEM MODIFY BACKEND "172.xx.xx.47:9050" SET ("labels.location" = "rack:rack1");
ALTER SYSTEM MODIFY BACKEND "172.xx.xx.48:9050" SET ("labels.location" = "rack:rack2");
ALTER SYSTEM MODIFY BACKEND "172.xx.xx.49:9050" SET ("labels.location" = "rack:rack2");
ALTER SYSTEM MODIFY BACKEND "172.xx.xx.50:9050" SET ("labels.location" = "rack:rack3");
ALTER SYSTEM MODIFY BACKEND "172.xx.xx.51:9050" SET ("labels.location" = "rack:rack3");
```

After adding labels, you can execute `SHOW BACKENDS;` and view the labels of BEs in the `Location` field of the returned result.

If you need to modify the labels of BEs, you can execute `ALTER SYSTEM MODIFY BACKEND "172.xx.xx.48:9050" SET ("labels.location" = "rack:xxx");`.

### Use labels to specify table data distribution on BE nodes

If you need to specify the locations to which a table's data is distributed, for example, distributing a table's data across two racks, rack1 and rack2, you can add labels to the table.

After labels are added, all the replicas of the same tablet in the table are distributed across labels in a Round-Robin approach. Moreover, if multiple replicas of the same tablet exist within the same label, these replicas will be distributed as evenly as possible across different BEs in that label.

:::note

- The total number of BE nodes associated with the labels must be greater than the number of replicas. Otherwise, an error `Table replication num should be less than or equal to the number of available BE nodes` will occur.
- The label to be associated with a table must already exist. Otherwise, an error `Getting analyzing error. Detail message: Cannot find any backend with location: rack:xxx` will occur.

:::

#### At table creation

You can use the property `"labels.location"` to distribute the table's data across rack 1 and rack 2 at table creation:

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

For newly created tables, the default value of the table property `labels.location` is `*`, indicating that replicas are evenly distributed across all labels. If the data distribution of a newly created table does not need to be aware of the geographical locations of servers in the cluster, you can manually set the table property `"labels.location" = ""`.

#### After table creation

If you need to modify the data distribution location of the table after table creation, for example, modify the location to rack 1, rack 2, and rack 3, you can execute the following statement:

```SQL
ALTER TABLE example_table
    SET ("labels.location" = "rack:rack1,rack:rack2,rack:rack3");
```

:::note

If you have upgraded StarRocks to version 3.2.8 or later, for historical tables created before the upgrade, data is not distributed based on labels by default.  If you need to distribute data of a historical table based on labels, you can execute the following statement to add labels to the historical table:

```SQL
ALTER TABLE example_table1
    SET ("labels.location" = "rack:rack1,rack:rack2");
```

:::

### Use labels to specify materialized view data distribution on BE nodes

If you need to specify the locations to which an asynchronous materialized view's data is distributed, for example, distributing data across two racks, rack1 and rack2, you can add labels to the materialized view.

After labels are added, all the replicas of the same tablet in the materialized view are distributed across labels in a Round-Robin approach. Moreover, if multiple replicas of the same tablet exist within the same label, these replicas will be distributed as evenly as possible across different BEs in that label.

:::note

- The total number of BE nodes associated with the labels must be greater than the number of replicas. Otherwise, an error `Table replication num should be less than or equal to the number of available BE nodes` will occur.
- The labels to be associated with the materialized view must already exist. Otherwise, an error `Getting analyzing error. Detail message: Cannot find any backend with location: rack:xxx` will occur.

:::

#### At materialized view creation

If you want to distribute the materialized view's data across rack 1 and rack 2 while creating it, you can execute the following statement:

```SQL
CREATE MATERIALIZED VIEW mv_example_mv
DISTRIBUTED BY RANDOM
PROPERTIES (
"labels.location" = "rack:rack1,rack:rack2")
as 
select order_id, dt from example_table;
```

For newly created materialized view, the default value of the property `labels.location` is `*`, indicating that replicas are evenly distributed across all labels. If the data distribution of a newly created materialized view does not need to be aware of the geographical locations of servers in the cluster, you can manually set the property `"labels.location" = ""`.

#### After materialized view creation

If you need to modify the data distribution location of the materialized view after it is created, for example, modify the location to rack 1, rack 2, and rack 3, you can execute the following statement:

```SQL
ALTER MATERIALIZED VIEW mv_example_mv
    SET ("labels.location" = "rack:rack1,rack:rack2,rack:rack3");
```

:::note

If you have upgraded StarRocks to version 3.2.8 or later, for existing materialized views created before the upgrade, data is not distributed based on labels by default.  If you need to distribute data of an existing based on labels, you can execute the following statement to add labels to the materialized view:

```SQL
ALTER TABLE example_mv1
    SET ("labels.location" = "rack:rack1,rack:rack2");
```

:::

