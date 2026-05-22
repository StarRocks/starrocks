# StarRocks + OpenSearch 集群测试环境

## 🚀 环境概览

| 组件 | 地址 | 状态 |
|------|------|------|
| StarRocks FE | 127.0.0.1:9030 | ✅ 运行中 |
| StarRocks BE | 127.0.0.1:9060 | ✅ 运行中 |
| StarRocks WebUI | http://127.0.0.1:8030 | ✅ 可访问 |
| OpenSearch | http://localhost:9200 | ✅ 运行中 |

## 📦 Docker 容器

```bash
# 查看所有容器
docker ps | grep -E "starrocks|opensearch"

# 容器列表
starrocks-allin1    # StarRocks 集群 (FE+BE)
starrocks-build     # 编译环境
opensearch-test     # OpenSearch 服务
```

## 🔧 快速开始

### 1. 编译 OpenSearch 连接器

```bash
# 进入开发容器
docker exec -it starrocks-build bash
cd /starrocks

# 编译 FE
./build.sh --fe
```

### 2. 部署并测试

```bash
# 使用测试脚本
chmod +x opensearch_test_commands.sh
./opensearch_test_commands.sh all
```

### 3. 手动测试步骤

```bash
# Step 1: 编译完成后，复制 jar 到 StarRocks
docker cp starrocks-build:/starrocks/fe/fe-core/target/starrocks-fe.jar \
        starrocks-allin1:/opt/starrocks/fe/lib/

# Step 2: 重启 StarRocks
docker restart starrocks-allin1

# Step 3: 等待就绪
sleep 30

# Step 4: 创建 Catalog
docker exec -i starrocks-allin1 mysql -P9030 -h127.0.0.1 << 'EOF'
CREATE EXTERNAL CATALOG opensearch_catalog
PROPERTIES (
    "type" = "opensearch",
    "hosts" = "http://host.docker.internal:9200",
    "opensearch.wan.only" = "true"
);
EOF

# Step 5: 查询数据
docker exec starrocks-allin1 mysql -P9030 -h127.0.0.1 -e "
    SELECT * FROM opensearch_catalog.default.test_products LIMIT 10;
"
```

## 📊 测试数据

OpenSearch 中已创建 `test_products` 索引，包含 5 条测试数据：

```json
{
  "id": 1,
  "name": "Product 1",
  "category": "Category 2",
  "price": 15.99,
  "stock": 90,
  "created_at": "2024-02-15T10:00:00Z",
  "is_available": true
}
```

## 🧪 测试命令

### 基本查询

```sql
-- 查看所有 Catalogs
SHOW CATALOGS;

-- 查看 OpenSearch 数据库
SHOW DATABASES FROM opensearch_catalog;

-- 查看表
SHOW TABLES FROM opensearch_catalog.default;

-- 查询数据
SELECT * FROM opensearch_catalog.default.test_products LIMIT 10;
```

### 聚合查询

```sql
-- 按分类统计
SELECT 
    category,
    COUNT(*) as cnt,
    AVG(price) as avg_price
FROM opensearch_catalog.default.test_products
GROUP BY category;
```

### 条件查询

```sql
-- 价格大于 20 的产品
SELECT * FROM opensearch_catalog.default.test_products
WHERE price > 20.0
ORDER BY price DESC;
```

## 🛠️ 常用命令

### 容器管理

```bash
# 查看日志
docker logs -f starrocks-allin1
docker logs -f opensearch-test

# 进入容器
docker exec -it starrocks-allin1 bash
docker exec -it starrocks-build bash

# 停止服务
docker stop starrocks-allin1 opensearch-test

# 启动服务
docker start starrocks-allin1 opensearch-test

# 删除容器
docker rm -f starrocks-allin1 opensearch-test starrocks-build
```

### OpenSearch 管理

```bash
# 查看索引
curl http://localhost:9200/_cat/indices?v

# 查看映射
curl http://localhost:9200/test_products/_mapping

# 搜索数据
curl http://localhost:9200/test_products/_search

# 添加新数据
curl -X POST http://localhost:9200/test_products/_doc/6 -H 'Content-Type: application/json' -d'{
    "id": 6,
    "name": "New Product",
    "category": "Category 1",
    "price": 99.99,
    "stock": 50,
    "created_at": "2024-06-20T10:00:00Z",
    "is_available": true
}'
```

### StarRocks 管理

```bash
# 连接 MySQL 客户端
docker exec -it starrocks-allin1 mysql -P9030 -h127.0.0.1

# 执行 SQL 文件
docker exec -i starrocks-allin1 mysql -P9030 -h127.0.0.1 < your_sql_file.sql

# 查看 FE 状态
docker exec starrocks-allin1 mysql -P9030 -h127.0.0.1 -e "SHOW FRONTENDS;"

# 查看 BE 状态
docker exec starrocks-allin1 mysql -P9030 -h127.0.0.1 -e "SHOW BACKENDS;"
```

## 🔍 故障排查

### 无法连接 OpenSearch

```bash
# 检查 OpenSearch 状态
curl http://localhost:9200

# 从 StarRocks 容器测试连接
docker exec starrocks-allin1 curl http://host.docker.internal:9200
```

### 无法创建 Catalog

```bash
# 检查 OpenSearch 连接器是否已部署
docker exec starrocks-allin1 ls /opt/starrocks/fe/lib/ | grep starrocks

# 查看 FE 日志
docker logs starrocks-allin1 2>&1 | grep -i opensearch
```

### 编译失败

```bash
# 进入开发容器检查
docker exec -it starrocks-build bash
cd /starrocks/fe && mvn clean install -DskipTests
```

## 📝 注意事项

1. **Phase 1 限制**: 当前仅支持 HTTP，不支持 HTTPS 和认证
2. **数据类型**: 使用 EsTable 作为底层表类型
3. **性能**: 首次查询可能需要预热
4. **网络**: StarRocks 容器通过 `host.docker.internal` 访问主机上的 OpenSearch

## 🎯 下一步

1. ✅ 运行测试脚本验证连接器功能
2. ✅ 测试各种查询场景
3. 🔄 实现 HTTPS 支持 (Phase 2)
4. 🔄 实现认证支持 (Phase 3)
