// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "column/column.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/descriptors.pb.h"
#include "runtime/descriptors.h"
#include "storage/tablet_schema.h"
#include "util/random.h"

namespace starrocks {

class MemPool;
class RuntimeState;

struct OlapTableColumnParam {
    std::vector<TabletColumn*> columns;
    std::vector<int32_t> sort_key_uid;
    int32_t short_key_column_count;

    void to_protobuf(POlapTableColumnParam* pcolumn) const;
};

struct OlapTableIndexSchema {
    int64_t index_id;
    std::vector<SlotDescriptor*> slots;
    int64_t schema_id;
    int32_t schema_hash;
    OlapTableColumnParam* column_param;
    ExprContext* where_clause = nullptr;

    void to_protobuf(POlapTableIndexSchema* pindex) const;
};

class OlapTableSchemaParam {
public:
    OlapTableSchemaParam() = default;
    ~OlapTableSchemaParam() noexcept = default;

    Status init(const TOlapTableSchemaParam& tschema, RuntimeState* state = nullptr);
    Status init(const POlapTableSchemaParam& pschema);

    int64_t db_id() const { return _db_id; }
    int64_t table_id() const { return _table_id; }
    int64_t version() const { return _version; }

    TupleDescriptor* tuple_desc() const { return _tuple_desc; }
    const std::vector<OlapTableIndexSchema*>& indexes() const { return _indexes; }

    void to_protobuf(POlapTableSchemaParam* pschema) const;

    // NOTE: this function is not thread-safe.
    POlapTableSchemaParam* to_protobuf() const {
        if (_proto_schema == nullptr) {
            _proto_schema = _obj_pool.add(new POlapTableSchemaParam());
            to_protobuf(_proto_schema);
        }
        return _proto_schema;
    }

    std::string debug_string() const;

private:
    int64_t _db_id = 0;
    int64_t _table_id = 0;
    int64_t _version = 0;

    TupleDescriptor* _tuple_desc = nullptr;
    mutable POlapTableSchemaParam* _proto_schema = nullptr;
    std::vector<OlapTableIndexSchema*> _indexes;
    mutable ObjectPool _obj_pool;
};

using OlapTableIndexTablets = TOlapTableIndexTablets;

using TabletLocation = TTabletLocation;

class OlapTableLocationParam {
public:
    explicit OlapTableLocationParam(const TOlapTableLocationParam& t_param) {
        for (auto& location : t_param.tablets) {
            _tablets.emplace(location.tablet_id, std::move(location));
        }
    }

    TabletLocation* find_tablet(int64_t tablet_id) {
        auto it = _tablets.find(tablet_id);
        if (it != std::end(_tablets)) {
            return &it->second;
        }
        return nullptr;
    }

    void add_locations(std::vector<TTabletLocation>& locations) {
        for (auto& location : locations) {
            if (_tablets.count(location.tablet_id) == 0) {
                _tablets.emplace(location.tablet_id, std::move(location));
                VLOG(2) << "add location " << location;
            }
        }
    }

private:
    std::unordered_map<int64_t, TabletLocation> _tablets;
};

struct NodeInfo {
    int64_t id;
    int64_t option;
    std::string host;
    int32_t brpc_port;

    explicit NodeInfo(const TNodeInfo& tnode)
            : id(tnode.id), option(tnode.option), host(tnode.host), brpc_port(tnode.async_internal_port) {}
};

class StarRocksNodesInfo {
public:
    explicit StarRocksNodesInfo(const TNodesInfo& t_nodes) {
        for (auto& node : t_nodes.nodes) {
            _nodes.emplace(node.id, node);
        }
    }
    const NodeInfo* find_node(int64_t id) const {
        auto it = _nodes.find(id);
        if (it != std::end(_nodes)) {
            return &it->second;
        }
        return nullptr;
    }

    void add_nodes(const std::vector<TNodeInfo>& t_nodes) {
        for (const auto& node : t_nodes) {
            auto node_info = find_node(node.id);
            if (node_info == nullptr) {
                _nodes.emplace(node.id, node);
            }
        }
    }

private:
    std::unordered_map<int64_t, NodeInfo> _nodes;
};

struct ChunkRow {
    ChunkRow() = default;
    ChunkRow(Columns* columns_, uint32_t index_) : columns(columns_), index(index_) {}

    std::string debug_string();

    Columns* columns = nullptr;
    uint32_t index = 0;
};

struct OlapTablePartition {
    int64_t id = 0;
    ChunkRow start_key;
    ChunkRow end_key;
    std::vector<ChunkRow> in_keys;
    int64_t num_buckets = 0;
    std::vector<OlapTableIndexTablets> indexes;
};

struct PartionKeyComparator {
    // return true if lhs < rhs
    // 'nullptr' is max value, but 'null' is min value
    bool operator()(const ChunkRow* lhs, const ChunkRow* rhs) const {
        if (lhs->columns == nullptr) {
            return false;
        } else if (rhs->columns == nullptr) {
            return true;
        }
        DCHECK_EQ(lhs->columns->size(), rhs->columns->size());

        for (size_t i = 0; i < lhs->columns->size(); ++i) {
            int cmp = (*lhs->columns)[i]->compare_at(lhs->index, rhs->index, *(*rhs->columns)[i], -1);
            if (cmp != 0) {
                return cmp < 0;
            }
        }
        // equal, return false
        return false;
    }
};

// store an olap table's tablet information
class OlapTablePartitionParam {
public:
    OlapTablePartitionParam(std::shared_ptr<OlapTableSchemaParam> schema, const TOlapTablePartitionParam& param);
    ~OlapTablePartitionParam();

    Status init(RuntimeState* state);

    Status prepare(RuntimeState* state);
    Status open(RuntimeState* state);
    void close(RuntimeState* state);

    int64_t db_id() const { return _t_param.db_id; }
    int64_t table_id() const { return _t_param.table_id; }
    int64_t version() const { return _t_param.version; }

    bool enable_automatic_partition() const { return _t_param.enable_automatic_partition; }

    // `invalid_row_index` stores index that chunk[index]
    // has been filtered out for not being able to find tablet.
    // it could be any row, becauset it's just for outputing error message for user to diagnose.
    Status find_tablets(Chunk* chunk, std::vector<OlapTablePartition*>* partitions, std::vector<uint32_t>* indexes,
                        std::vector<uint8_t>* selection, std::vector<int>* invalid_row_indexs, int64_t txn_id,
                        std::vector<std::vector<std::string>>* partition_not_exist_row_values);

    const std::map<int64_t, OlapTablePartition*>& get_partitions() const { return _partitions; }

    Status add_partitions(const std::vector<TOlapTablePartition>& partitions);

    Status remove_partitions(const std::vector<int64_t>& partition_ids);

    bool is_un_partitioned() const { return _partition_columns.empty(); }

private:
    Status _create_partition_keys(const std::vector<TExprNode>& t_exprs, ChunkRow* part_key);

    void _compute_hashes(Chunk* chunk, std::vector<uint32_t>* indexes);

    // check if this partition contain this key
    bool _part_contains(OlapTablePartition* part, ChunkRow* key) const {
        if (part->start_key.columns == nullptr) {
            // start_key is nullptr means the lower bound is boundless
            return true;
        }
        return !PartionKeyComparator()(key, &part->start_key);
    }

private:
    std::shared_ptr<OlapTableSchemaParam> _schema;
    TOlapTablePartitionParam _t_param;

    std::vector<SlotDescriptor*> _partition_slot_descs;
    std::vector<SlotDescriptor*> _distributed_slot_descs;
    Columns _partition_columns;
    std::vector<Column*> _distributed_columns;
    std::vector<ExprContext*> _partitions_expr_ctxs;

    ObjectPool _obj_pool;
    std::map<int64_t, OlapTablePartition*> _partitions;
    // one partition have multi sub partition
    std::map<ChunkRow*, std::vector<int64_t>, PartionKeyComparator> _partitions_map;

    Random _rand{(uint32_t)time(nullptr)};
};

} // namespace starrocks
