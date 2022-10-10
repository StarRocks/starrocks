// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/tablet_meta.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/metadata_util.h"

#include <boost/algorithm/string.hpp>

#include "common/config.h"
#include "gen_cpp/AgentService_types.h"
#include "gutil/strings/substitute.h"
#include "storage/olap_common.h"
#include "storage/tablet_schema.h"

namespace starrocks {

enum class FieldTypeVersion {
    kV1,
    kV2,
};

// Old version StarRocks use `TColumnType` to save type info, convert it into `TTypeDesc`.
static void convert_to_new_version(TColumn* tcolumn) {
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

static FieldAggregationMethod t_aggregation_type_to_field_aggregation_method(TAggregationType::type agg_type) {
    switch (agg_type) {
    case TAggregationType::NONE:
        return OLAP_FIELD_AGGREGATION_NONE;
    case TAggregationType::MAX:
        return OLAP_FIELD_AGGREGATION_MAX;
    case TAggregationType::MIN:
        return OLAP_FIELD_AGGREGATION_MIN;
    case TAggregationType::REPLACE:
        return OLAP_FIELD_AGGREGATION_REPLACE;
    case TAggregationType::REPLACE_IF_NOT_NULL:
        return OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL;
    case TAggregationType::BITMAP_UNION:
        return OLAP_FIELD_AGGREGATION_BITMAP_UNION;
    case TAggregationType::HLL_UNION:
        return OLAP_FIELD_AGGREGATION_HLL_UNION;
    case TAggregationType::SUM:
        return OLAP_FIELD_AGGREGATION_SUM;
    case TAggregationType::PERCENTILE_UNION:
        return OLAP_FIELD_AGGREGATION_PERCENTILE_UNION;
    }
    return OLAP_FIELD_AGGREGATION_NONE;
}

static FieldType t_primitive_type_to_field_type(TPrimitiveType::type primitive_type, FieldTypeVersion v) {
    switch (primitive_type) {
    case TPrimitiveType::INVALID_TYPE:
    case TPrimitiveType::NULL_TYPE:
    case TPrimitiveType::BINARY:
    case TPrimitiveType::TIME:
        return OLAP_FIELD_TYPE_UNKNOWN;
    case TPrimitiveType::BOOLEAN:
        return OLAP_FIELD_TYPE_BOOL;
    case TPrimitiveType::TINYINT:
        return OLAP_FIELD_TYPE_TINYINT;
    case TPrimitiveType::SMALLINT:
        return OLAP_FIELD_TYPE_SMALLINT;
    case TPrimitiveType::INT:
        return OLAP_FIELD_TYPE_INT;
    case TPrimitiveType::BIGINT:
        return OLAP_FIELD_TYPE_BIGINT;
    case TPrimitiveType::FLOAT:
        return OLAP_FIELD_TYPE_FLOAT;
    case TPrimitiveType::DOUBLE:
        return OLAP_FIELD_TYPE_DOUBLE;
    case TPrimitiveType::DATE:
        return v == FieldTypeVersion::kV1 ? OLAP_FIELD_TYPE_DATE : OLAP_FIELD_TYPE_DATE_V2;
    case TPrimitiveType::DATETIME:
        return v == FieldTypeVersion::kV1 ? OLAP_FIELD_TYPE_DATETIME : OLAP_FIELD_TYPE_TIMESTAMP;
    case TPrimitiveType::CHAR:
        return OLAP_FIELD_TYPE_CHAR;
    case TPrimitiveType::LARGEINT:
        return OLAP_FIELD_TYPE_LARGEINT;
    case TPrimitiveType::VARCHAR:
        return OLAP_FIELD_TYPE_VARCHAR;
    case TPrimitiveType::HLL:
        return OLAP_FIELD_TYPE_HLL;
    case TPrimitiveType::DECIMAL:
    case TPrimitiveType::DECIMALV2:
        return v == FieldTypeVersion::kV1 ? OLAP_FIELD_TYPE_DECIMAL : OLAP_FIELD_TYPE_DECIMAL_V2;
    case TPrimitiveType::DECIMAL32:
        return OLAP_FIELD_TYPE_DECIMAL32;
    case TPrimitiveType::DECIMAL64:
        return OLAP_FIELD_TYPE_DECIMAL64;
    case TPrimitiveType::DECIMAL128:
        return OLAP_FIELD_TYPE_DECIMAL128;
    case TPrimitiveType::OBJECT:
        return OLAP_FIELD_TYPE_OBJECT;
    case TPrimitiveType::PERCENTILE:
        return OLAP_FIELD_TYPE_PERCENTILE;
    case TPrimitiveType::JSON:
        return OLAP_FIELD_TYPE_JSON;
    case TPrimitiveType::FUNCTION:
        return OLAP_FIELD_TYPE_UNKNOWN;
    }
    return OLAP_FIELD_TYPE_UNKNOWN;
}

static Status t_column_to_pb_column(int32_t unique_id, const TColumn& t_column, FieldTypeVersion v, ColumnPB* column_pb,
                                    size_t depth = 0) {
    const int32_t kFakeUniqueId = -1;

    const std::vector<TTypeNode>& types = t_column.type_desc.types;
    if (depth == types.size()) {
        return Status::InvalidArgument("type nodes must ended with scalar type");
    }

    // No names provided for child columns, assign them a fake name.
    auto c_name = depth == 0 ? t_column.column_name : strings::Substitute("_$0_$1", t_column.column_name, depth);

    // A child column cannot be a key column.
    auto is_key = depth == 0 && t_column.is_key;
    bool is_nullable = depth > 0 || t_column.is_allow_null;

    column_pb->set_unique_id(unique_id);
    column_pb->set_name(c_name);
    column_pb->set_is_key(is_key);
    column_pb->set_is_nullable(is_nullable);
    if (depth > 0 || is_key) {
        auto agg_method = OLAP_FIELD_AGGREGATION_NONE;
        column_pb->set_aggregation(TabletColumn::get_string_by_aggregation_type(agg_method));
    } else {
        auto agg_method = t_aggregation_type_to_field_aggregation_method(t_column.aggregation_type);
        column_pb->set_aggregation(TabletColumn::get_string_by_aggregation_type(agg_method));
    }

    const TTypeNode& curr_type_node = types[depth];
    switch (curr_type_node.type) {
    case TTypeNodeType::SCALAR: {
        if (depth + 1 != types.size()) {
            return Status::InvalidArgument("scalar type cannot have child node");
        }
        TScalarType scalar = curr_type_node.scalar_type;

        FieldType field_type = t_primitive_type_to_field_type(scalar.type, v);
        column_pb->set_type(TabletColumn::get_string_by_field_type(field_type));
        column_pb->set_length(TabletColumn::get_field_length_by_type(field_type, scalar.len));
        column_pb->set_index_length(column_pb->length());
        column_pb->set_frac(curr_type_node.scalar_type.scale);
        column_pb->set_precision(curr_type_node.scalar_type.precision);
        if (field_type == OLAP_FIELD_TYPE_VARCHAR) {
            int32_t index_len = depth == 0 && t_column.__isset.index_len ? t_column.index_len : 10;
            column_pb->set_index_length(index_len);
        }
        if (depth == 0 && t_column.__isset.default_value) {
            column_pb->set_default_value(t_column.default_value);
        }
        if (depth == 0 && t_column.__isset.is_bloom_filter_column) {
            column_pb->set_is_bf_column(t_column.is_bloom_filter_column);
        }
        return Status::OK();
    }
    case TTypeNodeType::ARRAY:
        column_pb->set_type(TabletColumn::get_string_by_field_type(OLAP_FIELD_TYPE_ARRAY));
        column_pb->set_length(TabletColumn::get_field_length_by_type(OLAP_FIELD_TYPE_ARRAY, sizeof(Collection)));
        column_pb->set_index_length(column_pb->length());
        return t_column_to_pb_column(kFakeUniqueId, t_column, v, column_pb->add_children_columns(), depth + 1);
    case TTypeNodeType::STRUCT:
        return Status::NotSupported("struct not supported yet");
    case TTypeNodeType::MAP:
        return Status::NotSupported("map not supported yet");
    }
    return Status::InternalError("Unreachable path");
}

Status convert_t_schema_to_pb_schema(const TTabletSchema& tablet_schema, uint32_t next_unique_id,
                                     const std::unordered_map<uint32_t, uint32_t>& col_ordinal_to_unique_id,
                                     TabletSchemaPB* schema, TCompressionType::type compression_type) {
    if (tablet_schema.__isset.id) {
        schema->set_id(tablet_schema.id);
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
    default:
        CHECK(false) << "unsupported keys type " << tablet_schema.keys_type;
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
    default:
        LOG(WARNING) << "Unexpected compression type" << compression_type;
        return Status::InternalError("Unexpected compression type");
    }

    FieldTypeVersion field_version = FieldTypeVersion::kV1;
    if (config::storage_format_version == 2) {
        field_version = FieldTypeVersion::kV2;
    }

    // set column information
    uint32_t col_ordinal = 0;
    uint32_t key_count = 0;
    bool has_bf_columns = false;
    for (TColumn tcolumn : tablet_schema.columns) {
        convert_to_new_version(&tcolumn);
        uint32_t col_unique_id = col_ordinal_to_unique_id.at(col_ordinal++);
        ColumnPB* column = schema->add_column();

        RETURN_IF_ERROR(t_column_to_pb_column(col_unique_id, tcolumn, field_version, column));

        key_count += column->is_key();
        has_bf_columns |= column->is_bf_column();

        if (tablet_schema.__isset.indexes) {
            for (auto& index : tablet_schema.indexes) {
                if (index.index_type == TIndexType::type::BITMAP) {
                    DCHECK_EQ(index.columns.size(), 1);
                    if (boost::iequals(tcolumn.column_name, index.columns[0])) {
                        column->set_has_bitmap_index(true);
                        break;
                    }
                }
            }
        }
    }

    schema->set_next_column_unique_id(next_unique_id);
    if (has_bf_columns && tablet_schema.__isset.bloom_filter_fpp) {
        schema->set_bf_fpp(tablet_schema.bloom_filter_fpp);
    }
    return Status::OK();
}

} // namespace starrocks
