# 手动修改指南

如果补丁应用失败，请手动修改以下 4 个文件：

## 文件 1: fe/fe-core/src/main/java/com/starrocks/catalog/EsTable.java

在第 109 行，将：
```java
    private EsTablePartitions esTablePartitions;
```

改为：
```java
    @SerializedName(value = "etp")
    private EsTablePartitions esTablePartitions;
```

## 文件 2: fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/EsTablePartitions.java

### 添加 import 语句（在第 37 行后添加）
```java
import com.google.gson.annotations.SerializedName;
```

### 修改字段声明（第 62-65 行）
将：
```java
    private PartitionInfo partitionInfo;
    private Map<Long, String> partitionIdToIndices;
    private Map<String, EsShardPartitions> partitionedIndexStates;
    private Map<String, EsShardPartitions> unPartitionedIndexStates;
```

改为：
```java
    @SerializedName(value = "pi")
    private PartitionInfo partitionInfo;
    @SerializedName(value = "piti")
    private Map<Long, String> partitionIdToIndices;
    @SerializedName(value = "pis")
    private Map<String, EsShardPartitions> partitionedIndexStates;
    @SerializedName(value = "upis")
    private Map<String, EsShardPartitions> unPartitionedIndexStates;
```

## 文件 3: fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/EsShardPartitions.java

### 添加 import 语句（在第 38 行后添加）
```java
import com.google.gson.annotations.SerializedName;
```

### 修改字段声明（第 56-61 行）
将：
```java
    private final String indexName;
    // shardid -> host1, host2, host3
    private Map<Integer, List<EsShardRouting>> shardRoutings;
    private SingleRangePartitionDesc partitionDesc;
    private PartitionKey partitionKey;
    private long partitionId = -1;
```

改为：
```java
    @SerializedName(value = "in")
    private final String indexName;
    // shardid -> host1, host2, host3
    @SerializedName(value = "sr")
    private Map<Integer, List<EsShardRouting>> shardRoutings;
    @SerializedName(value = "pd")
    private SingleRangePartitionDesc partitionDesc;
    @SerializedName(value = "pk")
    private PartitionKey partitionKey;
    @SerializedName(value = "pid")
    private long partitionId = -1;
```

## 文件 4: fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/EsShardRouting.java

### 添加 import 语句（在第 18 行后添加）
```java
import com.google.gson.annotations.SerializedName;
```

### 修改字段声明（第 26-32 行）
将：
```java
    private final String indexName;
    private final int shardId;
    private final boolean isPrimary;
    private final TNetworkAddress address;

    private TNetworkAddress httpAddress;
    private final String nodeId;
```

改为：
```java
    @SerializedName(value = "in")
    private final String indexName;
    @SerializedName(value = "sid")
    private final int shardId;
    @SerializedName(value = "ip")
    private final boolean isPrimary;
    @SerializedName(value = "addr")
    private final TNetworkAddress address;

    @SerializedName(value = "ha")
    private TNetworkAddress httpAddress;
    @SerializedName(value = "nid")
    private final String nodeId;
```

## 编译命令

```bash
# 完整编译
cd /path/to/starrocks
./build.sh --fe

# 或快速编译（仅 fe-core）
cd fe/fe-core
mvn compile -DskipTests
```
