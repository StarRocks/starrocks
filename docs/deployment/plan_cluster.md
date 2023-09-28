# Plan StarRocks cluster

This topic describes how to plan resources for your StarRocks cluster in production from the perspectives of node count, CPU core count, memory size, and storage size.

## Node count

StarRocks mainly consists of two types of components: FE nodes and BE nodes. Each node must be deployed separately on a physical or virtual machine.

### FE node count

FE nodes are mainly responsible for metadata management, client connection management, query planning, and query scheduling.

In production, we recommend you deploy at least **THREE** Follower FE nodes in your StarRocks cluster to prevent single points of failure (SPOFs).

StarRocks uses the BDB JE protocol to manage the metadata across FE nodes. StarRocks elects a Leader FE node from all Follower FE nodes. Only the Leader FE node can write metadata. The other Follower FE nodes only update their metadata based on the logs from the Leader FE node. Each time the Leader FE node fails, StarRocks re-elects a new Leader FE node as long as more than half of the Follower FE nodes are alive.

If your application generates highly concurrent query requests, you can add Observer FE nodes to your cluster. Observer FE nodes only process the query requests and do not participate in the election for the Leader FE node.

### BE node count

BE nodes are responsible for data storage and SQL execution.

In production, we recommend you deploy at least **THREE** BE nodes in your StarRocks cluster to ensure high data reliability and service availability. A high-availability cluster of BEs is automatically formed when at least three BE nodes are deployed and added to your StarRocks cluster. The failure of one BE node will not affect the overall availability of the BE services.

You can increase the number of BE nodes to enable your StarRocks cluster to process highly concurrent queries.

### CN node count

CN nodes are optional components of StarRocks, and are only responsible for SQL execution.

You can increase the number of CN nodes to elastically scale compute resources without changing the data distribution in your StarRocks cluster.

## CPU and memory

Usually, the FE service does not consume a lot of CPU and memory resources. We recommend allocating 8 CPU cores and 16 GB RAM to each FE node.

Unlike the FE service, the BE service can be significantly CPU- and memory-intensive if your application works with highly concurrent or complex queries on a large dataset. Therefore, we recommend allocating 16 CPU cores and 64 GB RAM to each BE node.

## Storage capacity

### FE storage

Because FE nodes only maintain StarRocks' metadata in their storage, 100 GB of HDD storage is enough for each FE node in most scenarios.

### BE storage

#### Estimate initial storage space for BE

The total storage space that your StarRocks cluster will need is simultaneously influenced by the size of your raw data, the data replica count, and the compression ratio of the data compression algorithm you use.

With the following formula, you can estimate the total storage space you will need for all BE nodes:

```Plain
Total BE storage space = Raw data size * Replica count/Compression ratio

Raw data size = Sum of the space taken up by all fields in a row * Row count
```

In StarRocks, data in a table is first divided into multiple partitions and then into multiple tablets. Tablets are the basic logical units of data management in StarRocks. To ensure high data reliability, you can maintain multiple replicas of each tablet and store them across different BEs. By default, StarRocks maintains three replicas.

Currently, StarRocks supports four data compression algorithms, which are listed in order from higher to lower compression ratio: zlib, Zstandard (or zstd), LZ4, and Snappy. They can provide a compression ratio from 3:1 to 5:1.

After determining the total storage space, you can simply divide it by the number of BE nodes in your cluster to estimate the average storage space per BE node.

#### Add extra storage as you go

If the BE storage space runs out as your raw data grows, you can supplement it by scaling your cluster vertically or horizontally, or simply scaling up your cloud storage.

- Add new BE nodes to your StarRocks cluster

  You can add new BE nodes to your StarRocks cluster so that the data can be re-distributed evenly to more nodes. For detailed instructions, see [Scale your StarRocks cluster - Scale BE out](../administration/Scale_up_down.md).

  After new BE nodes are added, StarRocks automatically re-balances the data among all BE nodes. Such auto-balancing is supported on all table types.

- Add extra storage volumes to your BE nodes

  You can also add extra storage volumes to existing BE nodes. For detailed instructions, see [Scale your StarRocks cluster - Scale BE up](../administration/Scale_up_down.md).

  After extra storage volumes are added, StarRocks automatically re-balances the data in all tables.

- Add cloud storage

  If your StarRocks cluster is deployed on cloud, you can scale up your cloud storage on demand. For detailed instructions, contact your cloud provider.
