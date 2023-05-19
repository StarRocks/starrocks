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

#include "connector/mysql_connector.h"

#include "column/chunk.h"
#include "exprs/expr.h"
#include "exprs/in_const_predicate.hpp"
#include "storage/chunk_helper.h"

namespace starrocks::connector {
#define APPLY_FOR_NUMERICAL_TYPE(M, APPEND_TO_SQL) \
    M(TYPE_TINYINT, APPEND_TO_SQL)                 \
    M(TYPE_BOOLEAN, APPEND_TO_SQL)                 \
    M(TYPE_SMALLINT, APPEND_TO_SQL)                \
    M(TYPE_INT, APPEND_TO_SQL)                     \
    M(TYPE_BIGINT, APPEND_TO_SQL)

#define APPLY_FOR_VARCHAR_DATE_TYPE(M, APPEND_TO_SQL) \
    M(TYPE_DATE, APPEND_TO_SQL)                       \
    M(TYPE_DATETIME, APPEND_TO_SQL)                   \
    M(TYPE_CHAR, APPEND_TO_SQL)                       \
    M(TYPE_VARCHAR, APPEND_TO_SQL)

// ================================

DataSourceProviderPtr MySQLConnector::create_data_source_provider(ConnectorScanNode* scan_node,
                                                                  const TPlanNode& plan_node) const {
    return std::make_unique<MySQLDataSourceProvider>(scan_node, plan_node);
}

// ================================

MySQLDataSourceProvider::MySQLDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node)
        : _scan_node(scan_node), _mysql_scan_node(plan_node.mysql_scan_node) {}

DataSourcePtr MySQLDataSourceProvider::create_data_source(const TScanRange& scan_range) {
    return std::make_unique<MySQLDataSource>(this, scan_range);
}

const TupleDescriptor* MySQLDataSourceProvider::tuple_descriptor(RuntimeState* state) const {
    return state->desc_tbl().get_tuple_descriptor(_mysql_scan_node.tuple_id);
}

// ================================

MySQLDataSource::MySQLDataSource(const MySQLDataSourceProvider* provider, const TScanRange& scan_range)
        : _provider(provider) {}

Status MySQLDataSource::_init_params(RuntimeState* state) {
    VLOG(1) << "MySQLDataSource::init mysql scan params";

    DCHECK(state != nullptr);

    _columns = _provider->_mysql_scan_node.columns;
    _filters = _provider->_mysql_scan_node.filters;
    _temporal_clause = _provider->_mysql_scan_node.temporal_clause;

    _table_name = _provider->_mysql_scan_node.table_name;

    // get tuple desc
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_provider->_mysql_scan_node.tuple_id);
    DCHECK(_tuple_desc != nullptr);

    _slot_num = _tuple_desc->slots().size();
    // get mysql info
    const auto* mysql_table = dynamic_cast<const MySQLTableDescriptor*>(_tuple_desc->table_desc());
    DCHECK(mysql_table != nullptr);

    _my_param.host = mysql_table->host();
    _my_param.port = mysql_table->port();
    _my_param.user = mysql_table->user();
    _my_param.passwd = mysql_table->passwd();
    _my_param.db = mysql_table->mysql_db();
    // new one scanner
    _mysql_scanner = std::make_unique<MysqlScanner>(_my_param);
    DCHECK(_mysql_scanner != nullptr);

    return Status::OK();
}

Status MySQLDataSource::open(RuntimeState* state) {
    _init_params(state);
    DCHECK(state != nullptr);
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(_mysql_scanner->open());

    // Get [slot_id, slot] map
    std::unordered_map<SlotId, SlotDescriptor*> slot_by_id;
    for (SlotDescriptor* slot : _tuple_desc->slots()) {
        slot_by_id[slot->id()] = slot;
    }

    std::unordered_map<std::string, std::vector<std::string>> filters_in;
    std::unordered_map<std::string, bool> filters_null_in_set;

    // In Filter have been put into _conjunct_ctxs,
    // so we iterate all ExprContext to use it.
    for (auto ctx : _conjunct_ctxs) {
        const Expr* root_expr = ctx->root();
        if (root_expr == nullptr) {
            continue;
        }

        std::vector<SlotId> slot_ids;

        // In Filter must has only one slot_id.
        if (root_expr->get_slot_ids(&slot_ids) != 1) {
            continue;
        }

        SlotId slot_id = slot_ids[0];
        auto iter = slot_by_id.find(slot_id);
        if (iter != slot_by_id.end()) {
            LogicalType type = iter->second->type().type;
            // dipatch to process,
            // we support numerical type, char type and date type.
            switch (type) {
                // In Filter is must handle by VectorizedInConstPredicate type.
#define READ_CONST_PREDICATE(TYPE, APPEND_TO_SQL)                                             \
    case TYPE: {                                                                              \
        if (typeid(*root_expr) == typeid(VectorizedInConstPredicate<TYPE>)) {                 \
            const auto* pred = down_cast<const VectorizedInConstPredicate<TYPE>*>(root_expr); \
            const auto& hash_set = pred->hash_set();                                          \
            if (pred->is_not_in()) {                                                          \
                continue;                                                                     \
            }                                                                                 \
            auto& field_name = iter->second->col_name();                                      \
            filters_null_in_set[field_name] = pred->null_in_set();                            \
            std::vector<std::string> vector_values;                                           \
            vector_values.reserve(1024);                                                      \
            for (const auto& value : hash_set) {                                              \
                APPEND_TO_SQL                                                                 \
            }                                                                                 \
            filters_in.emplace(field_name, vector_values);                                    \
        }                                                                                     \
        break;                                                                                \
    }

#define DIRECT_APPEND_TO_SQL vector_values.emplace_back(std::to_string(value));
                APPLY_FOR_NUMERICAL_TYPE(READ_CONST_PREDICATE, DIRECT_APPEND_TO_SQL)
#undef APPLY_FOR_NUMERICAL_TYPE
#undef DIRECT_APPEND_TO_SQL

#define CONVERT_APPEND_TO_SQL vector_values.emplace_back(_mysql_scanner->escape(value.to_string()).to_string());
                APPLY_FOR_VARCHAR_DATE_TYPE(READ_CONST_PREDICATE, CONVERT_APPEND_TO_SQL)
#undef APPLY_FOR_VARCHAR_DATE_TYPE
#undef CONVERT_APPEND_TO_SQL

            case TYPE_UNKNOWN:
            case TYPE_NULL:
            case TYPE_BINARY:
            case TYPE_DECIMAL:
            case TYPE_STRUCT:
            case TYPE_ARRAY:
            case TYPE_MAP:
            case TYPE_HLL:
            case TYPE_TIME:
            case TYPE_OBJECT:
            case TYPE_PERCENTILE:
            case TYPE_LARGEINT:
            case TYPE_DECIMAL128:
            case TYPE_DECIMALV2:
            case TYPE_DECIMAL32:
            case TYPE_DECIMAL64:
            case TYPE_DOUBLE:
            case TYPE_FLOAT:
            case TYPE_JSON:
            case TYPE_FUNCTION:
            case TYPE_VARBINARY:
            case TYPE_UNSIGNED_TINYINT:
            case TYPE_UNSIGNED_SMALLINT:
            case TYPE_UNSIGNED_INT:
            case TYPE_UNSIGNED_BIGINT:
            case TYPE_DISCRETE_DOUBLE:
            case TYPE_DATE_V1:
            case TYPE_DATETIME_V1:
            case TYPE_NONE:
            case TYPE_MAX_VALUE:
                break;
            }
        }
    }

    RETURN_IF_ERROR(_mysql_scanner->query(_table_name, _columns, _filters, filters_in, filters_null_in_set, _read_limit,
                                          _temporal_clause));
    // check materialize slot num
    int materialize_num = 0;

    for (const auto& slot : _tuple_desc->slots()) {
        if (slot->is_materialized()) {
            materialize_num++;
        }
    }

    if (_mysql_scanner->field_num() != materialize_num) {
        return Status::InternalError("input and output not equal.");
    }

    return Status::OK();
}

Status MySQLDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    VLOG(1) << "MySQLDataSource::GetNext";

    DCHECK(state != nullptr && chunk != nullptr);

    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    if (_is_finished) {
        return Status::EndOfFile("finished!");
    }

    _init_chunk(chunk, 0);
    // indicates whether there are more rows to process. Set in _hbase_scanner.next().
    bool mysql_eos = false;
    int row_num = 0;

    while (true) {
        RETURN_IF_CANCELLED(state);

        if (row_num >= state->chunk_size()) {
            return Status::OK();
        }

        // read mysql
        char** data = nullptr;
        size_t* length = nullptr;
        RETURN_IF_ERROR(_mysql_scanner->get_next_row(&data, &length, &mysql_eos));
        if (mysql_eos) {
            _is_finished = true;
            return Status::OK();
        }

        ++row_num;
        RETURN_IF_ERROR(fill_chunk(chunk, data, length));
        ++_rows_read;
        _bytes_read += (*chunk)->bytes_usage();
    }
}

int64_t MySQLDataSource::raw_rows_read() const {
    return _rows_read;
}

int64_t MySQLDataSource::num_rows_read() const {
    return _rows_read;
}

int64_t MySQLDataSource::num_bytes_read() const {
    return _bytes_read;
}

int64_t MySQLDataSource::cpu_time_spent() const {
    return _cpu_time_ns;
}

void MySQLDataSource::close(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
}

Status MySQLDataSource::fill_chunk(ChunkPtr* chunk, char** data, size_t* length) {
    SCOPED_RAW_TIMER(&_cpu_time_ns);

    int materialized_col_idx = -1;
    for (size_t col_idx = 0; col_idx < _slot_num; ++col_idx) {
        SlotDescriptor* slot_desc = _tuple_desc->slots()[col_idx];
        ColumnPtr column = (*chunk)->get_column_by_slot_id(slot_desc->id());

        // because the fe planner filter the non_materialize column
        if (!slot_desc->is_materialized()) {
            continue;
        }

        ++materialized_col_idx;

        if (data[materialized_col_idx] == nullptr) {
            if (slot_desc->is_nullable()) {
                column->append_nulls(1);
            } else {
                std::stringstream ss;
                ss << "nonnull column contains NULL. table=" << _table_name << ", column=" << slot_desc->col_name();
                return Status::InternalError(ss.str());
            }
        } else {
            RETURN_IF_ERROR(append_text_to_column(data[materialized_col_idx], length[materialized_col_idx], slot_desc,
                                                  column.get()));
        }
    }
    return Status::OK();
}

Status MySQLDataSource::append_text_to_column(const char* data, const int& len, const SlotDescriptor* slot_desc,
                                              Column* column) {
    // only \N will be treated as NULL
    if (slot_desc->is_nullable()) {
        if (len == 2 && data[0] == '\\' && data[1] == 'N') {
            column->append_nulls(1);
            return Status::OK();
        }
    }

    Column* data_column = column;
    if (data_column->is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(data_column);
        data_column = nullable_column->data_column().get();
    }

    bool parse_success = true;

    // Parse the raw-text data. Translate the text string to internal format.
    switch (slot_desc->type().type) {
    case TYPE_JSON: {
        ASSIGN_OR_RETURN(auto value, JsonValue::parse(Slice(data, len)));
        reinterpret_cast<JsonColumn*>(data_column)->append(std::move(value));
        break;
    }
    case TYPE_VARCHAR:
    case TYPE_CHAR: {
        Slice value(data, len);
        reinterpret_cast<BinaryColumn*>(data_column)->append(value);
        break;
    }
    case TYPE_BOOLEAN: {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        auto value = StringParser::string_to_bool(data, len, &parse_result);
        if (parse_result == StringParser::PARSE_SUCCESS)
            append_value_to_column<TYPE_BOOLEAN>(data_column, value);
        else
            parse_success = false;
        break;
    }
    case TYPE_TINYINT: {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        auto value = StringParser::string_to_int<int8_t>(data, len, &parse_result);
        if (parse_result == StringParser::PARSE_SUCCESS)
            append_value_to_column<TYPE_TINYINT>(data_column, value);
        else
            parse_success = false;
        break;
    }
    case TYPE_SMALLINT: {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        auto value = StringParser::string_to_int<int16_t>(data, len, &parse_result);
        if (parse_result == StringParser::PARSE_SUCCESS)
            append_value_to_column<TYPE_SMALLINT>(data_column, value);
        else
            parse_success = false;
        break;
    }
    case TYPE_INT: {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        auto value = StringParser::string_to_int<int32_t>(data, len, &parse_result);
        if (parse_result == StringParser::PARSE_SUCCESS)
            append_value_to_column<TYPE_INT>(data_column, value);
        else
            parse_success = false;
        break;
    }
    case TYPE_BIGINT: {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        auto value = StringParser::string_to_int<int64_t>(data, len, &parse_result);
        if (parse_result == StringParser::PARSE_SUCCESS)
            append_value_to_column<TYPE_BIGINT>(data_column, value);
        else
            parse_success = false;
        break;
    }
    case TYPE_LARGEINT: {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        auto value = StringParser::string_to_int<__int128>(data, len, &parse_result);
        if (parse_result == StringParser::PARSE_SUCCESS)
            append_value_to_column<TYPE_LARGEINT>(data_column, value);
        else
            parse_success = false;
        break;
    }
    case TYPE_FLOAT: {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        auto value = StringParser::string_to_float<float>(data, len, &parse_result);
        if (parse_result == StringParser::PARSE_SUCCESS)
            append_value_to_column<TYPE_FLOAT>(data_column, value);
        else
            parse_success = false;
        break;
    }
    case TYPE_DOUBLE: {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        auto value = StringParser::string_to_float<double>(data, len, &parse_result);
        if (parse_result == StringParser::PARSE_SUCCESS)
            append_value_to_column<TYPE_DOUBLE>(data_column, value);
        else
            parse_success = false;
        break;
    }
    case TYPE_DATE: {
        DateValue value{};
        if (value.from_string(data, len))
            append_value_to_column<TYPE_DATE>(data_column, value);
        else
            parse_success = false;
        break;
    }
    case TYPE_DATETIME: {
        TimestampValue value{};
        if (value.from_string(data, len))
            append_value_to_column<TYPE_DATETIME>(data_column, value);
        else
            parse_success = false;
        break;
    }
    case TYPE_DECIMALV2: {
        DecimalV2Value value;
        if (value.parse_from_str(data, len) == DecimalError::E_DEC_OK)
            append_value_to_column<TYPE_DECIMALV2>(data_column, value);
        else
            parse_success = false;
        break;
    }
    case TYPE_DECIMAL32: {
        int32_t value;
        if (!DecimalV3Cast::from_string<int32_t>(&value, slot_desc->type().precision, slot_desc->type().scale, data,
                                                 len))
            append_value_to_column<TYPE_DECIMAL32>(data_column, value);
        else
            parse_success = false;
        break;
    }
    case TYPE_DECIMAL64: {
        int64_t value;
        if (!DecimalV3Cast::from_string<int64_t>(&value, slot_desc->type().precision, slot_desc->type().scale, data,
                                                 len))
            append_value_to_column<TYPE_DECIMAL64>(data_column, value);
        else
            parse_success = false;
        break;
    }
    case TYPE_DECIMAL128: {
        int128_t value;
        if (!DecimalV3Cast::from_string<int128_t>(&value, slot_desc->type().precision, slot_desc->type().scale, data,
                                                  len))
            append_value_to_column<TYPE_DECIMAL128>(data_column, value);
        else
            parse_success = false;
        break;
    }
    default:
        parse_success = false;
        DCHECK(false) << "bad column type: " << slot_desc->type();
        break;
    }

    if (column->is_nullable()) {
        if (parse_success) {
            // if parse success, data_column has been appended through 'append_value_to_column'
            // and not we should append flag value into null_column
            auto* nullable_column = down_cast<NullableColumn*>(column);
            NullData& null_data = nullable_column->null_column_data();
            null_data.push_back(0);
            return Status::OK();
        } else {
            // if column is nullable, just append null to it when parse error
            column->append_nulls(1);
            return Status::OK();
        }
    }

    if (!parse_success) {
        std::stringstream ss;
        ss << "mysql row data parse error, column_data_type=" << slot_desc->type().type << std::endl;
        return Status::InternalError(ss.str());
    }

    return Status::OK();
}

template <LogicalType LT, typename CppType>
void MySQLDataSource::append_value_to_column(Column* column, CppType& value) {
    using ColumnType = typename starrocks::RunTimeColumnType<LT>;

    auto* runtime_column = down_cast<ColumnType*>(column);
    runtime_column->append(value);
}

} // namespace starrocks::connector
