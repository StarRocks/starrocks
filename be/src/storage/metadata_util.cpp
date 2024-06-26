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

#include "storage/metadata_util.h"

#include <boost/algorithm/string.hpp>

#include "common/config.h"
#include "gen_cpp/AgentService_types.h"
#include "gutil/strings/substitute.h"
#include "storage/aggregate_type.h"
#include "storage/olap_common.h"
#include "storage/tablet_schema.h"
#include "util/json_util.h"
#include "utils.h"

namespace starrocks {

// Old version StarRocks use `TColumnType` to save type info, convert it into `TTypeDesc`.
// NOTE: This is only used for some legacy UT
void convert_to_new_version(TColumn* tcolumn) {
    if (!tcolumn->__isset.type_desc) {
        tcolumn->__set_index_len(tcolumn->column_type.index_len);

        TScalarType scalar_type;
        scalar_type.__set_type(tcolumn->column_type.type);
        scalar_type.__set_len(tcolumn->column_type.len);
        scalar_type.__set_precision(tcolumn->column_type.precision);
        scalar_type.__set_scale(tcolumn->column_type.scale);

        tcolumn->type_desc.types.resize(1);
        tcolumn->type_desc.types.back().__set_type(TTypeNodeType::SCALAR);
        tcolumn->type_desc.types.back().__set_scalar_type(scalar_type);
        tcolumn->__isset.type_desc = true;
    }
}
static StorageAggregateType t_aggregation_type_to_field_aggregation_method(TAggregationType::type agg_type) {
    switch (agg_type) {
    case TAggregationType::NONE:
        return STORAGE_AGGREGATE_NONE;
    case TAggregationType::MAX:
        return STORAGE_AGGREGATE_MAX;
    case TAggregationType::MIN:
        return STORAGE_AGGREGATE_MIN;
    case TAggregationType::REPLACE:
        return STORAGE_AGGREGATE_REPLACE;
    case TAggregationType::REPLACE_IF_NOT_NULL:
        return STORAGE_AGGREGATE_REPLACE_IF_NOT_NULL;
    case TAggregationType::BITMAP_UNION:
        return STORAGE_AGGREGATE_BITMAP_UNION;
    case TAggregationType::HLL_UNION:
        return STORAGE_AGGREGATE_HLL_UNION;
    case TAggregationType::SUM:
        return STORAGE_AGGREGATE_SUM;
    case TAggregationType::PERCENTILE_UNION:
        return STORAGE_AGGREGATE_PERCENTILE_UNION;
    }
    return STORAGE_AGGREGATE_NONE;
}

static Status validate_tablet_schema(const TabletSchemaPB& schema_pb) {
#if !defined(NDEBUG) || defined(BE_TEST)
    std::unordered_set<std::string> column_names;
    std::unordered_set<int64_t> column_ids;
    for (const auto& col : schema_pb.column()) {
        DCHECK(col.has_unique_id()) << col.DebugString();
        if (auto [it, ok] = column_ids.insert(col.unique_id()); !ok) {
            LOG(ERROR) << "Duplicate column unique id: " << col.unique_id() << " schema: " << schema_pb.DebugString();
            return Status::InvalidArgument("Duplicate column id found in tablet schema");
        }
        if (auto [it, ok] = column_names.insert(col.name()); !ok) {
            LOG(ERROR) << "Duplicate column name: " << col.name() << " schema: " << schema_pb.DebugString();
            return Status::InvalidArgument("Duplicate column name found in tablet schema");
        }
    }
    return Status::OK();
#else
    return Status::OK();
#endif
}

// This function is used to initialize ColumnPB for subfield like element of Array.
static void init_column_pb_for_sub_field(ColumnPB* field) {
    const int32_t kFakeUniqueId = -1;

    field->set_unique_id(kFakeUniqueId);
    field->set_is_key(false);
    field->set_is_nullable(true);
    field->set_aggregation(get_string_by_aggregation_type(STORAGE_AGGREGATE_NONE));
}

// Because thrift doesn't support nested definition. So we use list to flatten the nested.
// In thrift, types and index will be used together to present one type.
// After this function returned, index will point to the next position to be parsed.
static Status type_desc_to_pb(const std::vector<TTypeNode>& types, int* index, ColumnPB* column_pb) {
    const TTypeNode& curr_type_node = types[*index];
    ++(*index);
    switch (curr_type_node.type) {
    case TTypeNodeType::SCALAR: {
        auto& scalar = curr_type_node.scalar_type;

        LogicalType field_type = thrift_to_type(scalar.type);
        column_pb->set_type(logical_type_to_string(field_type));
        column_pb->set_length(TabletColumn::get_field_length_by_type(field_type, scalar.len));
        column_pb->set_index_length(column_pb->length());
        column_pb->set_frac(curr_type_node.scalar_type.scale);
        column_pb->set_precision(curr_type_node.scalar_type.precision);
        return Status::OK();
    }
    case TTypeNodeType::ARRAY: {
        column_pb->set_type(logical_type_to_string(TYPE_ARRAY));

        // FIXME: I'm not sure if these fields are necessary, just keep it to be safe
        column_pb->set_length(TabletColumn::get_field_length_by_type(TYPE_ARRAY, sizeof(Collection)));
        column_pb->set_index_length(column_pb->length());

        // Currently, All array element is nullable
        auto field_pb = column_pb->add_children_columns();
        init_column_pb_for_sub_field(field_pb);
        RETURN_IF_ERROR(type_desc_to_pb(types, index, field_pb));
        field_pb->set_name("element");
        return Status::OK();
    }
    case TTypeNodeType::STRUCT: {
        column_pb->set_type(logical_type_to_string(TYPE_STRUCT));

        // FIXME: I'm not sure if these fields are necessary, just keep it to be safe
        column_pb->set_length(TabletColumn::get_field_length_by_type(TYPE_STRUCT, sizeof(Collection)));
        column_pb->set_index_length(column_pb->length());

        auto& fields = curr_type_node.struct_fields;
        for (const auto& field : fields) {
            auto field_pb = column_pb->add_children_columns();
            init_column_pb_for_sub_field(field_pb);
            // All struct fields all nullable now
            RETURN_IF_ERROR(type_desc_to_pb(types, index, field_pb));
            field_pb->set_name(field.name);
            if (field.__isset.id && field.id >= 0) {
                field_pb->set_unique_id(field.id);
            }
        }
        return Status::OK();
    }
    case TTypeNodeType::MAP: {
        column_pb->set_type(logical_type_to_string(TYPE_MAP));

        // FIXME: I'm not sure if these fields are necessary, just keep it to be safe
        column_pb->set_length(TabletColumn::get_field_length_by_type(TYPE_MAP, sizeof(Collection)));
        column_pb->set_index_length(column_pb->length());

        {
            auto key_pb = column_pb->add_children_columns();
            init_column_pb_for_sub_field(key_pb);
            RETURN_IF_ERROR(type_desc_to_pb(types, index, key_pb));
            key_pb->set_name("key");
        }
        {
            auto value_pb = column_pb->add_children_columns();
            init_column_pb_for_sub_field(value_pb);
            RETURN_IF_ERROR(type_desc_to_pb(types, index, value_pb));
            value_pb->set_name("value");
        }
        return Status::OK();
    }
    }
    return Status::InternalError("Unreachable path");
}

Status t_column_to_pb_column(int32_t unique_id, const TColumn& t_column, ColumnPB* column_pb) {
    DCHECK(t_column.__isset.type_desc);
    const std::vector<TTypeNode>& types = t_column.type_desc.types;
    int index = 0;
    RETURN_IF_ERROR(type_desc_to_pb(types, &index, column_pb));
    if (index != types.size()) {
        LOG(WARNING) << "Schema not match, size:" << types.size() << ", index=" << index;
        return Status::InternalError("Failed to parse type, number of schema elements not match");
    }
    column_pb->set_unique_id(unique_id);
    column_pb->set_name(t_column.column_name);
    column_pb->set_is_key(t_column.is_key);
    column_pb->set_is_nullable(t_column.is_allow_null);
    column_pb->set_has_bitmap_index(t_column.has_bitmap_index);
    column_pb->set_is_auto_increment(t_column.is_auto_increment);
    if (t_column.is_key) {
        auto agg_method = STORAGE_AGGREGATE_NONE;
        column_pb->set_aggregation(get_string_by_aggregation_type(agg_method));
    } else {
        auto agg_method = t_aggregation_type_to_field_aggregation_method(t_column.aggregation_type);
        column_pb->set_aggregation(get_string_by_aggregation_type(agg_method));
    }

    if (types[0].type == TTypeNodeType::SCALAR && types[0].scalar_type.type == TPrimitiveType::VARCHAR) {
        int32_t index_len = t_column.__isset.index_len ? t_column.index_len : 10;
        column_pb->set_index_length(index_len);
    }
    // Default value
    if (t_column.__isset.default_value) {
        column_pb->set_default_value(t_column.default_value);
    }
    if (t_column.__isset.is_bloom_filter_column) {
        column_pb->set_is_bf_column(t_column.is_bloom_filter_column);
    }

    return Status::OK();
}

Status convert_t_schema_to_pb_schema(const TTabletSchema& tablet_schema, uint32_t next_unique_id,
                                     const std::unordered_map<uint32_t, uint32_t>& col_ordinal_to_unique_id,
                                     TabletSchemaPB* schema, TCompressionType::type compression_type) {
    if (tablet_schema.__isset.id) {
        schema->set_id(tablet_schema.id);
    }
    if (tablet_schema.__isset.schema_version) {
        schema->set_schema_version(tablet_schema.schema_version);
    }
    schema->set_num_short_key_columns(tablet_schema.short_key_column_count);
    schema->set_num_rows_per_row_block(config::default_num_rows_per_column_file_block);
    switch (tablet_schema.keys_type) {
    case TKeysType::DUP_KEYS:
        schema->set_keys_type(KeysType::DUP_KEYS);
        break;
    case TKeysType::UNIQUE_KEYS:
        schema->set_keys_type(KeysType::UNIQUE_KEYS);
        break;
    case TKeysType::AGG_KEYS:
        schema->set_keys_type(KeysType::AGG_KEYS);
        break;
    case TKeysType::PRIMARY_KEYS:
        schema->set_keys_type(KeysType::PRIMARY_KEYS);
        break;
    }

    switch (compression_type) {
    case TCompressionType::LZ4_FRAME:
    case TCompressionType::LZ4:
        schema->set_compression_type(LZ4_FRAME);
        break;
    case TCompressionType::ZLIB:
        schema->set_compression_type(ZLIB);
        break;
    case TCompressionType::ZSTD:
        schema->set_compression_type(ZSTD);
        break;
    case TCompressionType::SNAPPY:
        schema->set_compression_type(SNAPPY);
        break;
    default:
        LOG(WARNING) << "Unexpected compression type" << compression_type;
        return Status::InternalError("Unexpected compression type");
    }

    // set column information
    uint32_t col_ordinal = 0;
    bool has_bf_columns = false;
    std::unordered_map<std::string, ColumnPB*> column_map;

    uint32_t max_col_unique_id = 0;
    for (TColumn tcolumn : tablet_schema.columns) {
        convert_to_new_version(&tcolumn);
        uint32_t col_unique_id;
        if (tcolumn.col_unique_id >= 0) {
            col_unique_id = tcolumn.col_unique_id;
        } else {
            col_unique_id = col_ordinal_to_unique_id.at(col_ordinal);
        }
        max_col_unique_id = col_unique_id > max_col_unique_id ? col_unique_id : max_col_unique_id;
        col_ordinal++;
        ColumnPB* column = schema->add_column();

        RETURN_IF_ERROR(t_column_to_pb_column(col_unique_id, tcolumn, column));

        has_bf_columns |= column->is_bf_column();

        column_map.emplace(boost::to_lower_copy(column->name()), column);
    }

    if (tablet_schema.__isset.indexes) {
        for (auto& index : tablet_schema.indexes) {
            TabletIndexPB* index_pb = schema->add_table_indices();
            index_pb->set_index_id(index.index_id);
            index_pb->set_index_name(index.index_name);
            if (index.index_type == TIndexType::type::BITMAP) {
                RETURN_IF(index.columns.size() != 1,
                          Status::Cancelled("BITMAP index " + index.index_name +
                                            " do not support to build with more than one column"));

                index_pb->set_index_type(IndexType::BITMAP);
                const auto& mit = column_map.find(boost::to_lower_copy(index.columns[0]));

                // TODO: Fix abnormal scenes when index column can not be found
                if (mit != column_map.end()) {
                    mit->second->set_has_bitmap_index(true);
                } else {
                    LOG(WARNING) << "index column (" << index.columns[0] << ") can not be found in table columns";
                }
            } else if (index.index_type == TIndexType::type::GIN) {
                RETURN_IF(index.columns.size() != 1,
                          Status::Cancelled("GIN index " + index.index_name +
                                            " do not support to build with more than one column"));
                index_pb->set_index_type(IndexType::GIN);

                const auto& index_col_name = index.columns[0];
                const auto& mit = column_map.find(boost::to_lower_copy(index_col_name));

                // TODO: Fix abnormal scenes when index column can not be found
                if (mit != column_map.end()) {
                    index_pb->add_col_unique_id(mit->second->unique_id());
                } else {
                    LOG(WARNING) << "index " << index_col_name << " can not be found in table columns";
                }

                std::map<std::string, std::map<std::string, std::string>> properties_map;
                properties_map.emplace(COMMON_PROPERTIES, index.common_properties);
                properties_map.emplace(INDEX_PROPERTIES, index.index_properties);
                properties_map.emplace(SEARCH_PROPERTIES, index.search_properties);
                properties_map.emplace(EXTRA_PROPERTIES, index.extra_properties);
                index_pb->set_index_properties(to_json(properties_map));
            } else if (index.index_type == TIndexType::type::NGRAMBF) {
                RETURN_IF(index.columns.size() != 1,
                          Status::Cancelled("NGRAM_BF index " + index.index_name +
                                            " do not support to build with more than one column"));

                index_pb->set_index_type(IndexType::NGRAMBF);
                const auto& index_col_name = index.columns[0];
                const auto& mit = column_map.find(boost::to_lower_copy(index_col_name));

                if (mit != column_map.end()) {
                    mit->second->set_is_bf_column(true);
                    index_pb->add_col_unique_id(mit->second->unique_id());
                } else {
                    return Status::Cancelled(
                            strings::Substitute("index column $0 can not be found in table columns", index.columns[0]));
                }
                std::map<std::string, std::map<std::string, std::string>> properties_map;
                properties_map.emplace(INDEX_PROPERTIES, index.index_properties);
                std::string str = to_json(properties_map);
                index_pb->set_index_properties(str);
            } else {
                std::string index_type;
                EnumToString(TIndexType, index.index_type, index_type);
                return Status::Cancelled(strings::Substitute("Not supported index type $0", index_type));
            }
        }
    }
    for (const auto idx : tablet_schema.sort_key_idxes) {
        schema->add_sort_key_idxes(idx);
    }
    for (const auto uid : tablet_schema.sort_key_unique_ids) {
        schema->add_sort_key_unique_ids(uid);
    }

    if (max_col_unique_id + 1 > next_unique_id) {
        schema->set_next_column_unique_id(max_col_unique_id + 1);
    } else {
        schema->set_next_column_unique_id(next_unique_id);
    }

    if (has_bf_columns && tablet_schema.__isset.bloom_filter_fpp) {
        schema->set_bf_fpp(tablet_schema.bloom_filter_fpp);
    }
    return validate_tablet_schema(*schema);
}

Status convert_t_schema_to_pb_schema(const TTabletSchema& t_schema, TCompressionType::type compression_type,
                                     TabletSchemaPB* out_schema) {
    auto col_idx_to_unique_id = std::unordered_map<uint32_t, uint32_t>{};
    auto next_unique_id = t_schema.columns.size();
    for (auto col_idx = uint32_t{0}; col_idx < next_unique_id; ++col_idx) {
        col_idx_to_unique_id[col_idx] = col_idx;
    }
    return convert_t_schema_to_pb_schema(t_schema, next_unique_id, col_idx_to_unique_id, out_schema, compression_type);
}
} // namespace starrocks
