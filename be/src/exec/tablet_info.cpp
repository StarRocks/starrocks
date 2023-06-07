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

#include "exec/tablet_info.h"

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "exprs/expr.h"
#include "runtime/mem_pool.h"
#include "types/constexpr.h"
#include "util/string_parser.hpp"

namespace starrocks {

static const std::string LOAD_OP_COLUMN = "__op";

std::string ChunkRow::debug_string() {
    std::stringstream os;
    os << "index " << index << " [";
    if (columns && columns->size() > 0) {
        for (size_t col = 0; col < columns->size() - 1; ++col) {
            os << (*columns)[col]->debug_item(index);
            os << ", ";
        }
        os << (*columns)[columns->size() - 1]->debug_item(index);
    }
    os << "]";
    return os.str();
}

void OlapTableIndexSchema::to_protobuf(POlapTableIndexSchema* pindex) const {
    pindex->set_id(index_id);
    pindex->set_schema_hash(schema_hash);
    for (auto slot : slots) {
        pindex->add_columns(slot->col_name());
    }
}

Status OlapTableSchemaParam::init(const POlapTableSchemaParam& pschema) {
    _db_id = pschema.db_id();
    _table_id = pschema.table_id();
    _version = pschema.version();
    std::map<std::string, SlotDescriptor*> slots_map;
    _tuple_desc = _obj_pool.add(new TupleDescriptor(pschema.tuple_desc()));
    for (auto& p_slot_desc : pschema.slot_descs()) {
        auto slot_desc = _obj_pool.add(new SlotDescriptor(p_slot_desc));
        _tuple_desc->add_slot(slot_desc);
        slots_map.emplace(slot_desc->col_name(), slot_desc);
    }
    for (auto& p_index : pschema.indexes()) {
        auto index = _obj_pool.add(new OlapTableIndexSchema());
        index->index_id = p_index.id();
        index->schema_hash = p_index.schema_hash();
        for (auto& col : p_index.columns()) {
            auto it = slots_map.find(col);
            if (it != std::end(slots_map)) {
                index->slots.emplace_back(it->second);
            }
        }
        _indexes.emplace_back(index);
    }

    std::sort(_indexes.begin(), _indexes.end(), [](const OlapTableIndexSchema* lhs, const OlapTableIndexSchema* rhs) {
        return lhs->index_id < rhs->index_id;
    });
    return Status::OK();
}

Status OlapTableSchemaParam::init(const TOlapTableSchemaParam& tschema) {
    _db_id = tschema.db_id;
    _table_id = tschema.table_id;
    _version = tschema.version;
    std::map<std::string, SlotDescriptor*> slots_map;
    _tuple_desc = _obj_pool.add(new TupleDescriptor(tschema.tuple_desc));
    for (auto& t_slot_desc : tschema.slot_descs) {
        auto slot_desc = _obj_pool.add(new SlotDescriptor(t_slot_desc));
        _tuple_desc->add_slot(slot_desc);
        slots_map.emplace(slot_desc->col_name(), slot_desc);
    }
    for (auto& t_index : tschema.indexes) {
        auto index = _obj_pool.add(new OlapTableIndexSchema());
        index->index_id = t_index.id;
        index->schema_hash = t_index.schema_hash;
        for (auto& col : t_index.columns) {
            auto it = slots_map.find(col);
            if (it != std::end(slots_map)) {
                index->slots.emplace_back(it->second);
            }
        }
        _indexes.emplace_back(index);
    }

    std::sort(_indexes.begin(), _indexes.end(), [](const OlapTableIndexSchema* lhs, const OlapTableIndexSchema* rhs) {
        return lhs->index_id < rhs->index_id;
    });
    return Status::OK();
}

void OlapTableSchemaParam::to_protobuf(POlapTableSchemaParam* pschema) const {
    pschema->set_db_id(_db_id);
    pschema->set_table_id(_table_id);
    pschema->set_version(_version);
    _tuple_desc->to_protobuf(pschema->mutable_tuple_desc());
    for (auto slot : _tuple_desc->slots()) {
        slot->to_protobuf(pschema->add_slot_descs());
    }
    for (auto index : _indexes) {
        index->to_protobuf(pschema->add_indexes());
    }
}

std::string OlapTableSchemaParam::debug_string() const {
    std::stringstream ss;
    ss << "tuple_desc=" << _tuple_desc->debug_string();
    return ss.str();
}

OlapTablePartitionParam::OlapTablePartitionParam(std::shared_ptr<OlapTableSchemaParam> schema,
                                                 const TOlapTablePartitionParam& t_param)
        : _schema(std::move(schema)), _t_param(t_param) {}

OlapTablePartitionParam::~OlapTablePartitionParam() = default;

Status OlapTablePartitionParam::init(RuntimeState* state) {
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

    if (_t_param.__isset.partition_exprs && _t_param.partition_exprs.size() > 0) {
        if (state == nullptr) {
            return Status::InternalError("state is null when partition_exprs is not empty");
        }
        RETURN_IF_ERROR(Expr::create_expr_trees(&_obj_pool, _t_param.partition_exprs, &_partitions_expr_ctxs, state));
    }

    // initial partitions
    for (auto& t_part : _t_param.partitions) {
        OlapTablePartition* part = _obj_pool.add(new OlapTablePartition());
        part->id = t_part.id;
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
        _partitions.emplace(part->id, part);

        if (t_part.is_shadow_partition) {
            VLOG(1) << "add shadow partition:" << part->id;
            continue;
        }

        if (t_part.__isset.start_keys) {
            RETURN_IF_ERROR_WITH_WARN(_create_partition_keys(t_part.start_keys, &part->start_key), "start_keys");
        }

        if (t_part.__isset.end_keys) {
            RETURN_IF_ERROR_WITH_WARN(_create_partition_keys(t_part.end_keys, &part->end_key), "end_keys");
        }

        if (t_part.__isset.in_keys) {
            part->in_keys.resize(t_part.in_keys.size());
            for (int i = 0; i < t_part.in_keys.size(); i++) {
                RETURN_IF_ERROR_WITH_WARN(_create_partition_keys(t_part.in_keys[i], &part->in_keys[i]), "in_keys");
            }
        }

        if (t_part.__isset.in_keys) {
            for (auto& in_key : part->in_keys) {
                _partitions_map.emplace(&in_key, part);
            }
        } else {
            _partitions_map.emplace(&part->end_key, part);
            VLOG(1) << "add partition:" << part->id << " start " << part->start_key.debug_string() << " end "
                    << part->end_key.debug_string();
        }
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
    if (t_exprs.size() != _partition_columns.size()) {
        return Status::InternalError(fmt::format("partition expr size {} not equal partition column size {}",
                                                 t_exprs.size(), _partition_columns.size()));
    }

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
                auto* column = down_cast<DateColumn*>(_partition_columns[i].get());
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
                auto* column = down_cast<TimestampColumn*>(_partition_columns[i].get());
                column->get_data().emplace_back(v);
            } else {
                std::stringstream ss;
                ss << "invalid date literal in partition column, date=" << t_expr.date_literal;
                return Status::InternalError(ss.str());
            }
            break;
        }
        case TYPE_TINYINT: {
            auto* column = down_cast<Int8Column*>(_partition_columns[i].get());
            column->get_data().emplace_back(t_expr.int_literal.value);
            break;
        }
        case TYPE_SMALLINT: {
            auto* column = down_cast<Int16Column*>(_partition_columns[i].get());
            column->get_data().emplace_back(t_expr.int_literal.value);
            break;
        }
        case TYPE_INT: {
            auto* column = down_cast<Int32Column*>(_partition_columns[i].get());
            column->get_data().emplace_back(t_expr.int_literal.value);
            break;
        }
        case TYPE_BIGINT: {
            auto* column = down_cast<Int64Column*>(_partition_columns[i].get());
            column->get_data().emplace_back(t_expr.int_literal.value);
            break;
        }
        case TYPE_LARGEINT: {
            StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
            auto val = StringParser::string_to_int<__int128>(t_expr.large_int_literal.value.c_str(),
                                                             t_expr.large_int_literal.value.size(), &parse_result);
            if (parse_result != StringParser::PARSE_SUCCESS) {
                val = MAX_INT128;
            }
            auto* column = down_cast<Int128Column*>(_partition_columns[i].get());
            column->get_data().emplace_back(val);
            break;
        }
        case TYPE_VARCHAR: {
            int len = t_expr.string_literal.value.size();
            const char* str_val = t_expr.string_literal.value.c_str();
            Slice value(str_val, len);
            auto* column = down_cast<BinaryColumn*>(_partition_columns[i].get());
            column->append(value);
            break;
        }
        case TYPE_BOOLEAN: {
            auto* column = down_cast<BooleanColumn*>(_partition_columns[i].get());
            column->get_data().emplace_back(t_expr.bool_literal.value);
            break;
        }
        default: {
            std::stringstream ss;
            ss << "unsupported partition column node type, type=" << t_expr.node_type << ", logic type=" << type;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        }
    }

    part_key->columns = &_partition_columns;
    part_key->index = _partition_columns[0]->size() - 1;
    return Status::OK();
}

Status OlapTablePartitionParam::add_partitions(const std::vector<TOlapTablePartition>& partitions) {
    for (auto& t_part : partitions) {
        if (_partitions.count(t_part.id) != 0) {
            continue;
        }

        OlapTablePartition* part = _obj_pool.add(new OlapTablePartition());
        part->id = t_part.id;
        if (t_part.__isset.start_keys) {
            RETURN_IF_ERROR_WITH_WARN(_create_partition_keys(t_part.start_keys, &part->start_key), "start_keys");
        }
        if (t_part.__isset.end_keys) {
            RETURN_IF_ERROR_WITH_WARN(_create_partition_keys(t_part.end_keys, &part->end_key), "end keys");
        }

        if (t_part.__isset.in_keys) {
            part->in_keys.resize(t_part.in_keys.size());
            for (int i = 0; i < t_part.in_keys.size(); i++) {
                RETURN_IF_ERROR_WITH_WARN(_create_partition_keys(t_part.in_keys[i], &part->in_keys[i]), "in_keys");
            }
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
        _partitions.emplace(part->id, part);
        if (t_part.__isset.in_keys) {
            for (auto& in_key : part->in_keys) {
                _partitions_map.emplace(&in_key, part);
            }
        } else {
            _partitions_map.emplace(&part->end_key, part);
            VLOG(1) << "add automatic partition:" << part->id << " start " << part->start_key.debug_string() << " end "
                    << part->end_key.debug_string();
        }
    }

    return Status::OK();
}

Status OlapTablePartitionParam::find_tablets(Chunk* chunk, std::vector<OlapTablePartition*>* partitions,
                                             std::vector<uint32_t>* indexes, std::vector<uint8_t>* selection,
                                             std::vector<int>* invalid_row_indexs, int64_t txn_id,
                                             std::vector<std::vector<std::string>>* partition_not_exist_row_values) {
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
        bool is_list_partition = _t_param.partitions[0].__isset.in_keys;
        for (size_t i = 0; i < num_rows; ++i) {
            if ((*selection)[i]) {
                row.index = i;
                if (is_list_partition) {
                    // list partition
                    auto it = _partitions_map.find(&row);
                    if (it != _partitions_map.end() && _part_contains(it->second, &row)) {
                        (*partitions)[i] = it->second;
                        (*indexes)[i] = (*indexes)[i] % it->second->num_buckets;
                    } else {
                        if (partition_not_exist_row_values) {
                            auto partition_value_items = std::make_unique<std::vector<std::string>>();
                            for (auto& column : *row.columns) {
                                VLOG(3) << "partition not exist chunk row:" << chunk->debug_row(i) << " partition row "
                                        << row.debug_string();
                                partition_value_items->emplace_back(column->raw_item_value(i));
                            }
                            (*partition_not_exist_row_values).emplace_back(*partition_value_items);
                        } else {
                            VLOG(3) << "partition not exist chunk row:" << chunk->debug_row(i) << " partition row "
                                    << row.debug_string();
                            (*partitions)[i] = nullptr;
                            (*selection)[i] = 0;
                            if (invalid_row_indexs != nullptr) {
                                invalid_row_indexs->emplace_back(i);
                            }
                        }
                    }
                } else {
                    // range partition
                    auto it = _partitions_map.upper_bound(&row);
                    if (it != _partitions_map.end() && _part_contains(it->second, &row)) {
                        (*partitions)[i] = it->second;
                        (*indexes)[i] = (*indexes)[i] % it->second->num_buckets;
                    } else {
                        if (partition_not_exist_row_values) {
                            // only support single column partition for range partition now
                            if (partition_columns.size() != 1) {
                                return Status::InternalError(
                                        "automatic partition only support single column partition.");
                            }
                            auto partition_value_items = std::make_unique<std::vector<std::string>>();
                            for (auto& column : *row.columns) {
                                VLOG(3) << "partition not exist chunk row:" << chunk->debug_row(i) << " partition row "
                                        << row.debug_string();
                                partition_value_items->emplace_back(column->raw_item_value(i));
                                (*partition_not_exist_row_values).emplace_back(*partition_value_items);
                            }
                        } else {
                            VLOG(3) << "partition not exist chunk row:" << chunk->debug_row(i) << " partition row "
                                    << row.debug_string();
                            (*partitions)[i] = nullptr;
                            (*selection)[i] = 0;
                            if (invalid_row_indexs != nullptr) {
                                invalid_row_indexs->emplace_back(i);
                            }
                        }
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

    // if no distributed columns, use random distribution
    if (_distributed_slot_descs.size() == 0) {
        uint32_t r = _rand.Next();
        for (auto i = 0; i < num_rows; ++i) {
            (*indexes)[i] = r++;
        }
    }
}

} // namespace starrocks
