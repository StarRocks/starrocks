// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/vectorized/tablet_info.h"

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "exprs/expr.h"
#include "runtime/mem_pool.h"
#include "types/constexpr.h"
#include "util/string_parser.hpp"

namespace starrocks {

class RuntimeState;

namespace vectorized {

OlapTablePartitionParam::OlapTablePartitionParam(std::shared_ptr<OlapTableSchemaParam> schema,
                                                 const TOlapTablePartitionParam& t_param)
        : _schema(std::move(schema)), _t_param(t_param) {}

OlapTablePartitionParam::~OlapTablePartitionParam() = default;

Status OlapTablePartitionParam::init() {
    std::map<std::string, SlotDescriptor*> slots_map;
    for (auto slot_desc : _schema->tuple_desc()->slots()) {
        slots_map.emplace(slot_desc->col_name(), slot_desc);
    }

    for (auto& part_col : _t_param.partition_columns) {
        auto it = slots_map.find(part_col);
        if (it == std::end(slots_map)) {
            std::stringstream ss;
            ss << "partition column not found, column=" << part_col;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        _partition_slot_descs.push_back(it->second);
    }
    _partition_columns.resize(_partition_slot_descs.size());

    if (_t_param.__isset.distributed_columns) {
        for (auto& col : _t_param.distributed_columns) {
            auto it = slots_map.find(col);
            if (it == std::end(slots_map)) {
                std::stringstream ss;
                ss << "distributed column not found, columns=" << col;
                return Status::InternalError(ss.str());
            }
            _distributed_slot_descs.emplace_back(it->second);
        }
    }
    _distributed_columns.resize(_distributed_slot_descs.size());

    if (_t_param.__isset.partition_exprs) {
        RETURN_IF_ERROR(Expr::create_expr_trees(&_obj_pool, _t_param.partition_exprs, &_partitions_expr_ctxs));
    }

    // initial partitions
    for (auto& t_part : _t_param.partitions) {
        OlapTablePartition* part = _obj_pool.add(new OlapTablePartition());
        part->id = t_part.id;
        if (t_part.__isset.start_keys) {
            RETURN_IF_ERROR_WITH_WARN(_create_partition_keys(t_part.start_keys, &part->start_key), "start_keys");
        }

        if (t_part.__isset.end_keys) {
            RETURN_IF_ERROR_WITH_WARN(_create_partition_keys(t_part.end_keys, &part->end_key), "end keys");
        }

        part->num_buckets = t_part.num_buckets;
        auto num_indexes = _schema->indexes().size();
        if (t_part.indexes.size() != num_indexes) {
            std::stringstream ss;
            ss << "number of partition's index is not equal with schema's"
               << ", num_part_indexes=" << t_part.indexes.size() << ", num_schema_indexes=" << num_indexes;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        part->indexes = t_part.indexes;
        std::sort(part->indexes.begin(), part->indexes.end(),
                  [](const OlapTableIndexTablets& lhs, const OlapTableIndexTablets& rhs) {
                      return lhs.index_id < rhs.index_id;
                  });
        // check index
        for (int j = 0; j < num_indexes; ++j) {
            if (part->indexes[j].index_id != _schema->indexes()[j]->index_id) {
                std::stringstream ss;
                ss << "partition's index is not equal with schema's"
                   << ", part_index=" << part->indexes[j].index_id
                   << ", schema_index=" << _schema->indexes()[j]->index_id;
                LOG(WARNING) << ss.str();
                return Status::InternalError(ss.str());
            }
        }
        _partitions.emplace_back(part);
        _partitions_map.emplace(&part->end_key, part);
    }

    return Status::OK();
}

Status OlapTablePartitionParam::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Expr::prepare(_partitions_expr_ctxs, state));
    return Status::OK();
}

Status OlapTablePartitionParam::open(RuntimeState* state) {
    RETURN_IF_ERROR(Expr::open(_partitions_expr_ctxs, state));
    return Status::OK();
}

void OlapTablePartitionParam::close(RuntimeState* state) {
    Expr::close(_partitions_expr_ctxs, state);
}

Status OlapTablePartitionParam::_create_partition_keys(const std::vector<TExprNode>& t_exprs, ChunkRow* part_key) {
    for (int i = 0; i < t_exprs.size(); i++) {
        const TExprNode& t_expr = t_exprs[i];
        const auto& type_desc = TypeDescriptor::from_thrift(t_expr.type);
        const auto type = type_desc.type;
        if (_partition_columns[i] == nullptr) {
            _partition_columns[i] = ColumnHelper::create_column(type_desc, false);
        }

        switch (type) {
        case TYPE_DATE: {
            DateValue v;
            if (v.from_string(t_expr.date_literal.value.c_str(), t_expr.date_literal.value.size())) {
                DateColumn* column = down_cast<DateColumn*>(_partition_columns[i].get());
                column->get_data().emplace_back(v);
            } else {
                std::stringstream ss;
                ss << "invalid date literal in partition column, date=" << t_expr.date_literal;
                return Status::InternalError(ss.str());
            }
            break;
        }
        case TYPE_DATETIME: {
            TimestampValue v;
            if (v.from_string(t_expr.date_literal.value.c_str(), t_expr.date_literal.value.size())) {
                TimestampColumn* column = down_cast<TimestampColumn*>(_partition_columns[i].get());
                column->get_data().emplace_back(v);
            } else {
                std::stringstream ss;
                ss << "invalid date literal in partition column, date=" << t_expr.date_literal;
                return Status::InternalError(ss.str());
            }
            break;
        }
        case TYPE_TINYINT: {
            Int8Column* column = down_cast<Int8Column*>(_partition_columns[i].get());
            column->get_data().emplace_back(t_expr.int_literal.value);
            break;
        }
        case TYPE_SMALLINT: {
            Int16Column* column = down_cast<Int16Column*>(_partition_columns[i].get());
            column->get_data().emplace_back(t_expr.int_literal.value);
            break;
        }
        case TYPE_INT: {
            Int32Column* column = down_cast<Int32Column*>(_partition_columns[i].get());
            column->get_data().emplace_back(t_expr.int_literal.value);
            break;
        }
        case TYPE_BIGINT: {
            Int64Column* column = down_cast<Int64Column*>(_partition_columns[i].get());
            column->get_data().emplace_back(t_expr.int_literal.value);
            break;
        }
        case TYPE_LARGEINT: {
            StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
            __int128 val = StringParser::string_to_int<__int128>(t_expr.large_int_literal.value.c_str(),
                                                                 t_expr.large_int_literal.value.size(), &parse_result);
            if (parse_result != StringParser::PARSE_SUCCESS) {
                val = MAX_INT128;
            }
            Int128Column* column = down_cast<Int128Column*>(_partition_columns[i].get());
            column->get_data().emplace_back(val);
            break;
        }
        default: {
            std::stringstream ss;
            ss << "unsupported partition column node type, type=" << t_expr.node_type;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        }
    }

    part_key->columns = &_partition_columns;
    part_key->index = _partition_columns[0]->size() - 1;
    return Status::OK();
}

Status OlapTablePartitionParam::find_tablets(Chunk* chunk, std::vector<OlapTablePartition*>* partitions,
                                             std::vector<uint32_t>* indexes, std::vector<uint8_t>* selection,
                                             int* invalid_row_index) {
    size_t num_rows = chunk->num_rows();
    partitions->resize(num_rows);

    _compute_hashes(chunk, indexes);

    if (!_partition_columns.empty()) {
        Columns partition_columns(_partition_slot_descs.size());
        if (!_partitions_expr_ctxs.empty()) {
            for (size_t i = 0; i < partition_columns.size(); ++i) {
                ASSIGN_OR_RETURN(partition_columns[i], _partitions_expr_ctxs[i]->evaluate(chunk));
            }
        } else {
            for (size_t i = 0; i < partition_columns.size(); ++i) {
                partition_columns[i] = chunk->get_column_by_slot_id(_partition_slot_descs[i]->id());
                DCHECK(partition_columns[i] != nullptr);
            }
        }

        ChunkRow row;
        row.columns = &partition_columns;
        row.index = 0;
        for (size_t i = 0; i < num_rows; ++i) {
            if ((*selection)[i]) {
                row.index = i;
                auto it = _partitions_map.upper_bound(&row);
                if (UNLIKELY(it == _partitions_map.end())) {
                    (*partitions)[i] = nullptr;
                    (*selection)[i] = 0;
                    if (invalid_row_index != nullptr) {
                        *invalid_row_index = i;
                    }
                } else if (LIKELY(_part_contains(it->second, &row))) {
                    (*partitions)[i] = it->second;
                    (*indexes)[i] = (*indexes)[i] % it->second->num_buckets;
                } else {
                    (*partitions)[i] = nullptr;
                    (*selection)[i] = 0;
                    if (invalid_row_index != nullptr) {
                        *invalid_row_index = i;
                    }
                }
            }
        }
    } else {
        OlapTablePartition* partition = _partitions_map.begin()->second;
        int64_t num_bucket = partition->num_buckets;
        for (size_t i = 0; i < num_rows; ++i) {
            if ((*selection)[i]) {
                (*partitions)[i] = partition;
                (*indexes)[i] = (*indexes)[i] % num_bucket;
            }
        }
    }
    return Status::OK();
}

void OlapTablePartitionParam::_compute_hashes(Chunk* chunk, std::vector<uint32_t>* indexes) {
    size_t num_rows = chunk->num_rows();
    indexes->assign(num_rows, 0);

    for (size_t i = 0; i < _distributed_slot_descs.size(); ++i) {
        _distributed_columns[i] = chunk->get_column_by_slot_id(_distributed_slot_descs[i]->id()).get();
        _distributed_columns[i]->crc32_hash(&(*indexes)[0], 0, num_rows);
    }
}

} // namespace vectorized
} // namespace starrocks
