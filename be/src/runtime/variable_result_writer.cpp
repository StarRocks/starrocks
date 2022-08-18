// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "runtime/variable_result_writer.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "exprs/expr.h"
#include "gen_cpp/Data_types.h"
#include "runtime/buffer_control_block.h"
#include "runtime/primitive_type.h"
#include "util/thrift_util.h"

namespace starrocks::vectorized {

VariableResultWriter::VariableResultWriter(BufferControlBlock* sinker,
                                           const std::vector<ExprContext*>& output_expr_ctxs,
                                           starrocks::RuntimeProfile* parent_profile)
        : _sinker(sinker), _output_expr_ctxs(output_expr_ctxs), _parent_profile(parent_profile) {}

VariableResultWriter::~VariableResultWriter() = default;

Status VariableResultWriter::init(RuntimeState* state) {
    _init_profile();
    if (nullptr == _sinker) {
        return Status::InternalError("sinker is nullptr.");
    }
    return Status::OK();
}

void VariableResultWriter::_init_profile() {
    _total_timer = ADD_TIMER(_parent_profile, "TotalSendTime");
    _serialize_timer = ADD_CHILD_TIMER(_parent_profile, "SerializeTime", "TotalSendTime");
    _sent_rows_counter = ADD_COUNTER(_parent_profile, "NumSentRows", TUnit::UNIT);
}

Status VariableResultWriter::append_chunk(vectorized::Chunk* chunk) {
    SCOPED_TIMER(_total_timer);
    if (nullptr == chunk || 0 == chunk->num_rows()) {
        return Status::OK();
    }

    int num_columns = _output_expr_ctxs.size();
    vectorized::Columns result_columns;
    result_columns.reserve(num_columns);
    for (int i = 0; i < num_columns; ++i) {
        ASSIGN_OR_RETURN(auto col, _output_expr_ctxs[i]->evaluate(chunk));
        result_columns.emplace_back(std::move(col));
    }

    std::unique_ptr<TFetchDataResult> result(new (std::nothrow) TFetchDataResult());
    if (!result) {
        return Status::MemoryAllocFailed("memory allocate failed");
    }

    BinaryColumn* variable = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(result_columns[0].get()));
    std::vector<TVariableData> var_list;

    int num_rows = chunk->num_rows();
    var_list.resize(num_rows);
    if (!result_columns[0]->is_null(0)) {
        var_list[0].__set_isNull(false);
        var_list[0].__set_result(variable->get_slice(0).to_string());
    } else {
        var_list[0].__set_isNull(true);
    }
    result->result_batch.rows.resize(num_rows);

    ThriftSerializer serializer(true, chunk->memory_usage());
    for (int i = 0; i < num_rows; ++i) {
        RETURN_IF_ERROR(serializer.serialize(&var_list[i], &result->result_batch.rows[i]));
    }

    Status status = _sinker->add_batch(result);

    if (status.ok()) {
        _written_rows += num_rows;
        return status;
    }

    LOG(WARNING) << "Append user variable result to sink failed, status : " << status.to_string();
    return status;
}

Status VariableResultWriter::close() {
    COUNTER_SET(_sent_rows_counter, _written_rows);
    return Status::OK();
}

} // namespace starrocks::vectorized
