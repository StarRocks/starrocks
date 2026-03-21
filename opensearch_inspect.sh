#!/bin/bash
# OpenSearch 索引查看工具

OPENSEARCH_HOST="http://localhost:9200"
INDEX_NAME="${1:-products}"

echo "=================================="
echo "OpenSearch 索引查看工具"
echo "索引: $INDEX_NAME"
echo "=================================="
echo ""

# 1. 索引基本信息
echo "📋 1. 索引基本信息"
echo "----------------------------------"
curl -s "$OPENSEARCH_HOST/$INDEX_NAME" | jq -r '.[].settings.index | {number_of_shards, number_of_replicas, creation_date}' 2>/dev/null || echo "无法获取索引设置"
echo ""

# 2. 索引映射（字段结构）
echo "📊 2. 索引映射（字段结构）"
echo "----------------------------------"
curl -s "$OPENSEARCH_HOST/$INDEX_NAME/_mapping" | jq -r '.[].mappings.properties | to_entries[] | "  - \(.key): \(.value.type)"' 2>/dev/null || echo "无法获取映射"
echo ""

# 3. 文档统计
echo "📈 3. 文档统计"
echo "----------------------------------"
curl -s "$OPENSEARCH_HOST/$INDEX_NAME/_count" | jq -r '{total_docs: .count}' 2>/dev/null || echo "无法获取文档数"
echo ""

# 4. 索引健康状态
echo "❤️  4. 索引健康状态"
echo "----------------------------------"
curl -s "$OPENSEARCH_HOST/_cluster/health/$INDEX_NAME" | jq -r '{status, active_shards, unassigned_shards}' 2>/dev/null || echo "无法获取健康状态"
echo ""

# 5. 示例数据（前5条）
echo "📄 5. 示例数据（前5条）"
echo "----------------------------------"
curl -s "$OPENSEARCH_HOST/$INDEX_NAME/_search?size=5" | jq -r '.hits.hits[] | "  ID: \(._id) | 名称: \(._source.name) | 价格: $\(.source.price) | 分类: \(._source.category)"' 2>/dev/null || echo "无法获取数据"
echo ""

# 6. 按分类统计
echo "📊 6. 按分类统计"
echo "----------------------------------"
curl -s -X POST "$OPENSEARCH_HOST/$INDEX_NAME/_search" \
  -H 'Content-Type: application/json' \
  -d'{"size":0,"aggs":{"by_category":{"terms":{"field":"category"}}}}' 2>/dev/null | \
  jq -r '.aggregations.by_category.buckets[] | "  \(.key): \(.doc_count) 件"' 2>/dev/null || echo "无法获取统计"
echo ""

# 7. 库存状态统计
echo "📦 7. 库存状态统计"
echo "----------------------------------"
curl -s -X POST "$OPENSEARCH_HOST/$INDEX_NAME/_search" \
  -H 'Content-Type: application/json' \
  -d'{"size":0,"aggs":{"by_stock":{"terms":{"field":"in_stock"}}}}' 2>/dev/null | \
  jq -r '.aggregations.by_stock.buckets[] | "  \(.key): \(.doc_count) 件"' 2>/dev/null || echo "无法获取库存统计"
echo ""

# 8. 价格统计
echo "💰 8. 价格统计"
echo "----------------------------------"
curl -s -X POST "$OPENSEARCH_HOST/$INDEX_NAME/_search" \
  -H 'Content-Type: application/json' \
  -d'{"size":0,"aggs":{"price_stats":{"stats":{"field":"price"}}}}' 2>/dev/null | \
  jq -r '.aggregations.price_stats | "  平均: $\(.avg | round(2)) | 最小: $\(.min) | 最大: $\(.max) | 总和: $\(.sum)"' 2>/dev/null || echo "无法获取价格统计"
echo ""

echo "=================================="
echo "查看完成"
echo "=================================="
