// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/column.h"
#include "exec/tablet_info.h"

namespace starrocks {
class RuntimeState;
namespace vectorized {
struct ChunkRow {
    ChunkRow() = default;
    ChunkRow(Columns* columns_, uint32_t index_) : columns(columns_), index(index_) {}

    Columns* columns = nullptr;
    uint32_t index = 0;
};

struct OlapTablePartition {
    int64_t id = 0;
    ChunkRow start_key;
    ChunkRow end_key;
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

    Status init();

    Status prepare(RuntimeState* state);
    Status open(RuntimeState* state);
    void close(RuntimeState* state);

    int64_t db_id() const { return _t_param.db_id; }
    int64_t table_id() const { return _t_param.table_id; }
    int64_t version() const { return _t_param.version; }

    // `invalid_row_index` stores index that chunk[index]
    // has been filtered out for not being able to find tablet.
    // it could be any row, becauset it's just for outputing error message for user to diagnose.
    Status find_tablets(Chunk* chunk, std::vector<OlapTablePartition*>* partitions, std::vector<uint32_t>* indexes,
                        std::vector<uint8_t>* selection, std::vector<int>* invalid_row_indexs);

    const std::vector<OlapTablePartition*>& get_partitions() const { return _partitions; }

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
    std::vector<OlapTablePartition*> _partitions;
    std::map<ChunkRow*, OlapTablePartition*, PartionKeyComparator> _partitions_map;
};
} // namespace vectorized
} // namespace starrocks
