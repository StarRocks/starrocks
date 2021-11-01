// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "mysql_scan_node.h"

#include <sstream>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column_base.h"
#include "column/nullable_column.h"
#include "common/config.h"
#include "exec/text_converter.hpp"
#include "exprs/slot_ref.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/date_value.hpp"
#include "runtime/decimalv2_value.h"
#include "runtime/decimalv3.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "util/runtime_profile.h"

namespace starrocks::vectorized {

MysqlScanNode::MysqlScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs),
          _is_init(false),
          _table_name(tnode.mysql_scan_node.table_name),
          _tuple_id(tnode.mysql_scan_node.tuple_id),
          _columns(tnode.mysql_scan_node.columns),
          _filters(tnode.mysql_scan_node.filters),
          _tuple_desc(nullptr) {}

Status MysqlScanNode::prepare(RuntimeState* state) {
    VLOG(1) << "MysqlScanNode::Prepare";

    if (_is_init) {
        return Status::OK();
    }

    DCHECK(state != nullptr);

    RETURN_IF_ERROR(ScanNode::prepare(state));

    // get tuple desc
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
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
    _mysql_scanner.reset(new (std::nothrow) MysqlScanner(_my_param));
    DCHECK(_mysql_scanner != nullptr);

    _tuple_pool.reset(new (std::nothrow) MemPool());
    DCHECK(_tuple_pool != nullptr);

    _is_init = true;

    return Status::OK();
}

Status MysqlScanNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));
    VLOG(1) << "MysqlScanNode::Open";

    DCHECK(state != nullptr);
    DCHECK(_is_init);

    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(_mysql_scanner->open());
    RETURN_IF_ERROR(_mysql_scanner->query(_table_name, _columns, _filters));
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

Status MysqlScanNode::append_text_to_column(const char* data, const int& len, const SlotDescriptor* slot_desc,
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
    case TYPE_VARCHAR:
    case TYPE_CHAR: {
        Slice value(data, len);
        dynamic_cast<BinaryColumn*>(data_column)->append(value);
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

template <PrimitiveType PT, typename CppType>
void MysqlScanNode::append_value_to_column(Column* column, CppType& value) {
    using ColumnType = typename vectorized::RunTimeColumnType<PT>;

    ColumnType* runtime_column = down_cast<ColumnType*>(column);
    runtime_column->append(value);
}

Status MysqlScanNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    VLOG(1) << "MysqlScanNode::GetNext";

    DCHECK(state != nullptr && chunk != nullptr && eos != nullptr);
    DCHECK(_is_init);

    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_TIMER(materialize_tuple_timer());

    if (reached_limit() || _is_finished) {
        *eos = true;
        return Status::OK();
    }

    *chunk = std::make_shared<Chunk>();
    std::vector<SlotDescriptor*> slot_descs = _tuple_desc->slots();
    // init column information
    for (auto& slot_desc : slot_descs) {
        ColumnPtr column = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
        (*chunk)->append_column(std::move(column), slot_desc->id());
    }

    // indicates whether there are more rows to process. Set in _hbase_scanner.next().
    bool mysql_eos = false;
    int row_num = 0;

    while (true) {
        RETURN_IF_CANCELLED(state);

        if (reached_limit()) {
            _is_finished = true;
            // if row_num is greater than 0, in this call, eos = false, and eos will be set to true
            // in the next call
            if (row_num == 0) {
                *eos = true;
            }
            return Status::OK();
        }

        if (row_num >= config::vector_chunk_size) {
            return Status::OK();
        }

        // read mysql
        char** data = nullptr;
        size_t* length = nullptr;
        RETURN_IF_ERROR(_mysql_scanner->get_next_row(&data, &length, &mysql_eos));
        if (mysql_eos) {
            _is_finished = true;
            // if row_num is greater than 0, in this call, eos = false, and eos will be set to true
            // in the next call
            if (row_num == 0) {
                *eos = true;
            }
            return Status::OK();
        }

        ++row_num;

        int materialized_col_idx = -1;
        for (size_t col_idx = 0; col_idx < _slot_num; ++col_idx) {
            SlotDescriptor* slot_desc = slot_descs[col_idx];
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
                RETURN_IF_ERROR(append_text_to_column(data[materialized_col_idx], length[materialized_col_idx],
                                                      slot_desc, column.get()));
            }
        }

        ++_num_rows_returned;
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    }
}

Status MysqlScanNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::InternalError("Not support");
}

Status MysqlScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::CLOSE));
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    _tuple_pool.reset();

    return ScanNode::close(state);
}

void MysqlScanNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(static_cast<size_t>(indentation_level) * 2, ' ');
    *out << "MysqlScanNode(tupleid=" << _tuple_id << " table=" << _table_name;
    *out << ")" << std::endl;

    for (const auto& child : _children) {
        child->debug_string(indentation_level + 1, out);
    }
}

Status MysqlScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    return Status::OK();
}

} // namespace starrocks::vectorized
