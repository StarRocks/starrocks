# Phase 1 修复补丁 - 序列化问题

## 问题描述

Phase 1 的 OpenSearch connector 在查询时返回 0 行数据，根本原因是 `EsTablePartitions` 对象没有被正确序列化传递给优化器。

### 根本原因

在 `LogicalEsScanOperator` 构造函数中：
```java
this.esTablePartitions = ((EsTable) table).getEsTablePartitions();
```

但 `EsTable.esTablePartitions` 字段**没有 `@SerializedName` 注解**，因此它**不会被 Gson 序列化**。当查询执行时，`esTablePartitions` 是 `null`，导致无法获取分片信息，查询返回 0 行。

## 修复内容

需要给以下类和字段添加 `@SerializedName` 注解：

### 1. EsTable.java
- `esTablePartitions` 字段添加 `@SerializedName(value = "etp")`

### 2. EsTablePartitions.java
- 添加 Gson 导入：`import com.google.gson.annotations.SerializedName;`
- `partitionInfo` 添加 `@SerializedName(value = "pi")`
- `partitionIdToIndices` 添加 `@SerializedName(value = "piti")`
- `partitionedIndexStates` 添加 `@SerializedName(value = "pis")`
- `unPartitionedIndexStates` 添加 `@SerializedName(value = "upis")`

### 3. EsShardPartitions.java
- 添加 Gson 导入：`import com.google.gson.annotations.SerializedName;`
- `indexName` 添加 `@SerializedName(value = "in")`
- `shardRoutings` 添加 `@SerializedName(value = "sr")`
- `partitionDesc` 添加 `@SerializedName(value = "pd")`
- `partitionKey` 添加 `@SerializedName(value = "pk")`
- `partitionId` 添加 `@SerializedName(value = "pid")`

### 4. EsShardRouting.java
- 添加 Gson 导入：`import com.google.gson.annotations.SerializedName;`
- `indexName` 添加 `@SerializedName(value = "in")`
- `shardId` 添加 `@SerializedName(value = "sid")`
- `isPrimary` 添加 `@SerializedName(value = "ip")`
- `address` 添加 `@SerializedName(value = "addr")`
- `httpAddress` 添加 `@SerializedName(value = "ha")`
- `nodeId` 添加 `@SerializedName(value = "nid")`

## 应用补丁步骤

### 步骤 1: 复制补丁到远程环境

```bash
# 在本地
scp fix_serialization.patch <user>@<remote-host>:/path/to/starrocks/
```

### 步骤 2: 应用补丁

```bash
# 在远程环境
cd /path/to/starrocks

# 检查补丁是否可以应用
git apply --check fix_serialization.patch

# 应用补丁
git apply fix_serialization.patch
```

### 步骤 3: 重新编译

```bash
# 重新编译 FE
cd /path/to/starrocks
./build.sh --fe

# 或者只编译 fe-core 模块
cd fe
cd fe-core
mvn compile -DskipTests
```

### 步骤 4: 部署新的 JAR 文件

```bash
# 复制新的 starrocks-fe.jar 到容器
docker cp output/fe/lib/starrocks-fe.jar starrocks-40:/data/deploy/starrocks/fe/lib/

# 或者如果 OpenSearch connector 是单独的 jar
docker cp output/fe/lib/starrocks-fe.jar starrocks-40:/data/deploy/starrocks/fe/lib/
```

### 步骤 5: 重启 FE 服务

```bash
# 重启 FE
docker exec starrocks-40 supervisorctl restart feservice

# 等待 FE 启动完成
sleep 30

# 检查健康状态
docker exec starrocks-40 curl -s http://localhost:8030/api/health
```

### 步骤 6: 验证修复

```bash
# 连接到 StarRocks
docker exec -it starrocks-40 mysql -P9030 -h127.0.0.1

# 执行测试查询
USE opensearch_test.default_db;
SELECT * FROM products;

# 应该返回 3 行数据
```

## 预期结果

修复后，查询应该能正常返回数据：

```
+----+-----------+-------------+------+-------------+----------+
| _id | name      | price       | ... | category    | in_stock |
+----+-----------+-------------+------+-------------+----------+
| 1  | iPhone 15 | 999.99      | ... | Electronics | 1        |
| 2  | MacBook...| 2499.99     | ... | Electronics | 0        |
| 3  | AirPods...| 249.99      | ... | Electronics | 1        |
+----+-----------+-------------+------+-------------+----------+
```

## 回滚方法

如果补丁导致问题，可以回滚：

```bash
cd /path/to/starrocks
git checkout -- fe/fe-core/src/main/java/com/starrocks/catalog/EsTable.java
git checkout -- fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/EsTablePartitions.java
git checkout -- fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/EsShardPartitions.java
git checkout -- fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/EsShardRouting.java
```
