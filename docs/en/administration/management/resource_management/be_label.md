---
displayed_sidebar: "English"
---

# Add labels on BEs

Since v3.3.0, StarRocks supports adding labels on BEs based on information such as the racks and data centers where BEs are located. It ensures that data can be evenly distributed based on racks, data centers, etc., to address disaster recovery requirements in case that certain racks lose power or data centers encounter failures.

## Overview

The core of high-reliability data storage lies in that **all the identical replicas are not** **located in the same location and are distributed as evenly as possible to avoid data loss in case of failures in a single location.**

Currently, in StarRocks, tablets are distributed across BEs in the form of multiple replicas, which only ensure that all the identical replicas are not placed in the same BE. This indeed can avoid the abnormality of a BE to affect the availability of the service. However, in real-world scenarios, the deployment scope of one cluster may span across multiple racks or data centers. When distributing replicas, you do not think about the overall deployment scope of BEs, such as the racks or data centers they are in. It may result in all identical replicas being placed within the same rack or data center. In the event of a rack power outage or data center failure, data of these replicas may be lost.

To further enhance data reliability, since v3.3.0, StarRocks supports adding labels on BEs based on information such as the racks and data centers where the BEs are located. This allows StarRocks to be aware of the geographical locations of BEs. StarRocks ensures balanced distribution of identical replicas across different labels during replica distribution, while also ensuring that identical replicas are evenly distributed within the BEs of the same label. This ensures that data can be evenly distributed based on rack, data center, etc., to mitigate the impact of regional failures on service availability.

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

### Add labels on tables

If you need to specify the locations to which a table's data is distributed, for example, distributing a table's data across two racks, rack1 and rack2, you can add labels to the table.

After labels are added, all the replicas of the same tablet in the table are distributed across labels in a Round-Robin approach. Moreover, if multiple replicas of the same tablet exist within the same label, these replicas will be distributed as evenly as possible across different BEs in that label.

:::note

- The total number of BEs in the labels where the table resides must be greater than the number of replicas. Otherwise, an error `Table replication num should be less than or equal to the number of available BE nodes` will occur.
- The labels added to the table must already exist. Otherwise, an error `Getting analyzing error. Detail message: Cannot find any backend with location: rack:xxx` will occur.

:::

#### At table creation

If you want to distribute the table's data across rack 1 and rack 2 at table creation, you can execute the following statement:

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

For newly created tables, the default value of the table property `labels.location` is `*`, indicating that replicas are evenly distributed across all labels. If the data distribution of a newly created table does not need to be aware of the geographical locations of servers in the cluster, you can manually set the table property `"labels.location" ``= ``""`.

#### After table creation

If you need to modify the data distribution location of the table after table creation, for example, modify the location to rack 1, rack 2, and rack 3, you can execute the following statement:

```SQL
ALTER TABLE example_table
    SET ("labels.location" = "rack:rack1,rack:rack2,rack:rack3");
```

:::note

For historical tables that are created before v3.3.0, data of these historical tables is not distributed based on labels by default. If you need to distribute data of a historical table based on labels, you can execute the following statement to add labels to the historical table:

```SQL
ALTER TABLE example_table1
    SET ("labels.location" = "rack:rack1,rack:rack2");
```

:::
