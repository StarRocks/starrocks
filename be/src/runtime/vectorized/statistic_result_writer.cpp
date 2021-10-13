// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "runtime/vectorized/statistic_result_writer.h"

#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "exprs/expr.h"
#include "gen_cpp/Data_types.h"
#include "runtime/buffer_control_block.h"
#include "runtime/primitive_type.h"
#include "runtime/row_batch.h"
#include "util/thrift_util.h"
#include "util/types.h"

namespace starrocks::vectorized {

const int STATISTIC_DATA_VERSION1 = 1;
const int DICT_STATISTIC_DATA_VERSION = 101;

StatisticResultWriter::StatisticResultWriter(BufferControlBlock* sinker,
                                             const std::vector<ExprContext*>& output_expr_ctxs,
                                             starrocks::RuntimeProfile* parent_profile)
        : _sinker(sinker), _output_expr_ctxs(output_expr_ctxs), _parent_profile(parent_profile) {}

StatisticResultWriter::~StatisticResultWriter() = default;

Status StatisticResultWriter::init(RuntimeState* state) {
    _init_profile();
    if (nullptr == _sinker) {
        return Status::InternalError("sinker is nullptr.");
    }
    return Status::OK();
}

void StatisticResultWriter::_init_profile() {
    _total_timer = ADD_TIMER(_parent_profile, "TotalSendTime");
    _serialize_timer = ADD_CHILD_TIMER(_parent_profile, "SerializeTime", "TotalSendTime");
    _sent_rows_counter = ADD_COUNTER(_parent_profile, "NumSentRows", TUnit::UNIT);
}

Status StatisticResultWriter::append_row_batch(const RowBatch* batch) {
    return Status::NotSupported("Statistic result writer not support None-vectorized");
}

Status StatisticResultWriter::append_chunk(vectorized::Chunk* chunk) {
    SCOPED_TIMER(_total_timer);
    if (nullptr == chunk || 0 == chunk->num_rows()) {
        return Status::OK();
    }

    // Step 1: compute expr
    int num_columns = _output_expr_ctxs.size();

    vectorized::Columns result_columns;
    result_columns.reserve(num_columns);

    for (int i = 0; i < num_columns; ++i) {
        result_columns.emplace_back(_output_expr_ctxs[i]->evaluate(chunk));
    }

    // Step 2: fill version magic_num, first column must be version(const int)
    DCHECK(!result_columns.empty());
    DCHECK(!result_columns[0]->empty());
    DCHECK(!result_columns[0]->is_null(0));

    int version = down_cast<Int32Column*>(ColumnHelper::get_data_column(result_columns[0].get()))->get_data()[0];

    auto* result = new (std::nothrow) TFetchDataResult();

    // Step 3: fill statistic data
    if (version == STATISTIC_DATA_VERSION1) {
        _fill_statistic_data_v1(version, result_columns, chunk, result);
    } else if (version == DICT_STATISTIC_DATA_VERSION) {
        _fill_dict_statistic_data(version, result_columns, chunk, result);
    }

    // Step 4: send
    size_t num_rows = result->result_batch.rows.size();
    Status status = _sinker->add_batch(result);

    if (status.ok()) {
        _written_rows += num_rows;
        return status;
    }

    LOG(WARNING) << "Append statistic result to sink failed.";
    delete result;
    return status;
}

void StatisticResultWriter::_fill_dict_statistic_data(int version, const vectorized::Columns& columns,
                                                      const vectorized::Chunk* chunk, TFetchDataResult* result) {
    SCOPED_TIMER(_serialize_timer);
    DCHECK(columns.size() == 3);
    auto versioncolumn = ColumnHelper::cast_to_raw<TYPE_BIGINT>(columns[1]);
    auto dictColumnViewer = ColumnViewer<TYPE_VARCHAR>(columns[2]);

    std::vector<TStatisticData> data_list;
    int num_rows = chunk->num_rows();
    data_list.resize(num_rows);

    for (int i = 0; i < num_rows; ++i) {
        data_list[i].__set_meta_version(versioncolumn->get_data()[i]);
        if (!dictColumnViewer.is_null(i)) {
            data_list[i].__set_dict(from_json_string<TGlobalDict>(
                    std::string(dictColumnViewer.value(i).data, dictColumnViewer.value(i).size)));
        }
    }

    result->result_batch.rows.resize(num_rows);
    result->result_batch.__set_statistic_version(version);

    ThriftSerializer serializer(true, chunk->memory_usage());
    for (int i = 0; i < num_rows; ++i) {
        serializer.serialize(&data_list[i], &result->result_batch.rows[i]);
    }
}

void StatisticResultWriter::_fill_statistic_data_v1(int version, const vectorized::Columns& columns,
                                                    const vectorized::Chunk* chunk, TFetchDataResult* result) {
    SCOPED_TIMER(_serialize_timer);

    // mapping with Data.thrift.TStatisticData
    DCHECK(columns.size() == 11);

    // skip read version
    auto& updateTimes = ColumnHelper::cast_to_raw<TYPE_DATETIME>(columns[1])->get_data();
    auto& dbIds = ColumnHelper::cast_to_raw<TYPE_BIGINT>(columns[2])->get_data();
    auto& tableIds = ColumnHelper::cast_to_raw<TYPE_BIGINT>(columns[3])->get_data();
    BinaryColumn* nameColumn = ColumnHelper::cast_to_raw<TYPE_VARCHAR>(columns[4]);
    auto& rowCounts = ColumnHelper::cast_to_raw<TYPE_BIGINT>(columns[5])->get_data();
    auto& dataSizes = ColumnHelper::cast_to_raw<TYPE_BIGINT>(columns[6])->get_data();

    auto& countDistincts = ColumnHelper::cast_to_raw<TYPE_BIGINT>(columns[7])->get_data();
    auto& nullCounts = ColumnHelper::cast_to_raw<TYPE_BIGINT>(columns[8])->get_data();
    BinaryColumn* maxColumn = ColumnHelper::cast_to_raw<TYPE_VARCHAR>(columns[9]);
    BinaryColumn* minColumn = ColumnHelper::cast_to_raw<TYPE_VARCHAR>(columns[10]);

    std::vector<TStatisticData> data_list;
    int num_rows = chunk->num_rows();

    data_list.resize(num_rows);
    for (int i = 0; i < num_rows; ++i) {
        data_list[i].__set_updateTime(updateTimes[i].to_string());
        data_list[i].__set_dbId(dbIds[i]);
        data_list[i].__set_tableId(tableIds[i]);
        data_list[i].__set_columnName(nameColumn->get_slice(i).to_string());
        data_list[i].__set_rowCount(rowCounts[i]);
        data_list[i].__set_dataSize(dataSizes[i]);
        data_list[i].__set_countDistinct(countDistincts[i]);
        data_list[i].__set_nullCount(nullCounts[i]);
        data_list[i].__set_max(maxColumn->get_slice(i).to_string());
        data_list[i].__set_min(minColumn->get_slice(i).to_string());
    }

    result->result_batch.rows.resize(num_rows);
    result->result_batch.__set_statistic_version(version);

    ThriftSerializer serializer(true, chunk->memory_usage());
    for (int i = 0; i < num_rows; ++i) {
        serializer.serialize(&data_list[i], &result->result_batch.rows[i]);
    }
}

Status StatisticResultWriter::close() {
    COUNTER_SET(_sent_rows_counter, _written_rows);
    return Status::OK();
}

} // namespace starrocks::vectorized
