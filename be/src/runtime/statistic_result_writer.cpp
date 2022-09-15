// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "runtime/statistic_result_writer.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "exprs/expr.h"
#include "runtime/buffer_control_block.h"
#include "runtime/primitive_type.h"
#include "util/thrift_util.h"

namespace starrocks::vectorized {

const int STATISTIC_DATA_VERSION1 = 1;
const int STATISTIC_HISTOGRAM_VERSION = 2;
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

Status StatisticResultWriter::append_chunk(vectorized::Chunk* chunk) {
    SCOPED_TIMER(_total_timer);
    auto process_status = _process_chunk(chunk);
    if (!process_status.ok() || process_status.value() == nullptr) {
        return process_status.status();
    }
    auto result = std::move(process_status.value());

    size_t num_rows = result->result_batch.rows.size();
    Status status = _sinker->add_batch(result);

    if (status.ok()) {
        _written_rows += num_rows;
        return status;
    }

    LOG(WARNING) << "Append statistic result to sink failed.";
    return status;
}

StatusOr<TFetchDataResultPtrs> StatisticResultWriter::process_chunk(vectorized::Chunk* chunk) {
    SCOPED_TIMER(_total_timer);
    TFetchDataResultPtrs results;
    auto process_status = _process_chunk(chunk);
    if (!process_status.ok()) {
        return process_status.status();
    }
    if (process_status.value() != nullptr) {
        results.push_back(std::move(process_status.value()));
    }
    return results;
}

StatusOr<bool> StatisticResultWriter::try_add_batch(TFetchDataResultPtrs& results) {
    size_t num_rows = 0;
    for (auto& result : results) {
        num_rows += result->result_batch.rows.size();
    }

    auto status = _sinker->try_add_batch(results);

    if (status.ok()) {
        if (status.value()) {
            _written_rows += num_rows;
            results.clear();
        }
    } else {
        results.clear();
        LOG(WARNING) << "Append statistic result to sink failed.";
    }
    return status;
}

StatusOr<TFetchDataResultPtr> StatisticResultWriter::_process_chunk(vectorized::Chunk* chunk) {
    if (nullptr == chunk || 0 == chunk->num_rows()) {
        return nullptr;
    }

    // Step 1: compute expr
    int num_columns = _output_expr_ctxs.size();

    vectorized::Columns result_columns;
    result_columns.reserve(num_columns);

    for (int i = 0; i < num_columns; ++i) {
        ASSIGN_OR_RETURN(auto col, _output_expr_ctxs[i]->evaluate(chunk));
        result_columns.emplace_back(std::move(col));
    }

    // Step 2: fill version magic_num, first column must be version(const int)
    DCHECK(!result_columns.empty());
    DCHECK(!result_columns[0]->empty());
    DCHECK(!result_columns[0]->is_null(0));

    int version = down_cast<Int32Column*>(ColumnHelper::get_data_column(result_columns[0].get()))->get_data()[0];

    std::unique_ptr<TFetchDataResult> result(new (std::nothrow) TFetchDataResult());
    if (!result) {
        return Status::MemoryAllocFailed("memory allocate failed");
    }

    // Step 3: fill statistic data
    if (version == STATISTIC_DATA_VERSION1) {
        RETURN_IF_ERROR_WITH_WARN(_fill_statistic_data_v1(version, result_columns, chunk, result.get()),
                                  "Fill statistic data failed");
    } else if (version == DICT_STATISTIC_DATA_VERSION) {
        RETURN_IF_ERROR_WITH_WARN(_fill_dict_statistic_data(version, result_columns, chunk, result.get()),
                                  "Fill dict statistic data failed");
    } else if (version == STATISTIC_HISTOGRAM_VERSION) {
        RETURN_IF_ERROR_WITH_WARN(_fill_statistic_histogram(version, result_columns, chunk, result.get()),
                                  "Fill histogram statistic data failed");
    }
    return result;
}

Status StatisticResultWriter::_fill_dict_statistic_data(int version, const vectorized::Columns& columns,
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
        RETURN_IF_ERROR(serializer.serialize(&data_list[i], &result->result_batch.rows[i]));
    }
    return Status::OK();
}

Status StatisticResultWriter::_fill_statistic_data_v1(int version, const vectorized::Columns& columns,
                                                      const vectorized::Chunk* chunk, TFetchDataResult* result) {
    SCOPED_TIMER(_serialize_timer);

    // mapping with Data.thrift.TStatisticData
    DCHECK(columns.size() == 11);

    // skip read version
    auto& updateTimes = ColumnHelper::cast_to_raw<TYPE_DATETIME>(columns[1])->get_data();
    auto& dbIds = ColumnHelper::cast_to_raw<TYPE_BIGINT>(columns[2])->get_data();
    auto& tableIds = ColumnHelper::cast_to_raw<TYPE_BIGINT>(columns[3])->get_data();
    BinaryColumn* nameColumn = ColumnHelper::cast_to_raw<TYPE_VARCHAR>(columns[4]);
    Int64Column* rowCounts = down_cast<Int64Column*>(ColumnHelper::get_data_column(columns[5].get()));
    Int64Column* dataSizes = down_cast<Int64Column*>(ColumnHelper::get_data_column(columns[6].get()));
    Int64Column* countDistincts = down_cast<Int64Column*>(ColumnHelper::get_data_column(columns[7].get()));
    Int64Column* nullCounts = down_cast<Int64Column*>(ColumnHelper::get_data_column(columns[8].get()));
    BinaryColumn* maxColumn = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(columns[9].get()));
    BinaryColumn* minColumn = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(columns[10].get()));

    std::vector<TStatisticData> data_list;
    int num_rows = chunk->num_rows();

    data_list.resize(num_rows);
    for (int i = 0; i < num_rows; ++i) {
        data_list[i].__set_updateTime(updateTimes[i].to_string());
        data_list[i].__set_dbId(dbIds[i]);
        data_list[i].__set_tableId(tableIds[i]);
        data_list[i].__set_columnName(nameColumn->get_slice(i).to_string());
        data_list[i].__set_rowCount(rowCounts->get(i).get_int64());
        data_list[i].__set_dataSize(dataSizes->get(i).get_int64());
        data_list[i].__set_countDistinct(countDistincts->get(i).get_int64());
        data_list[i].__set_nullCount(nullCounts->get(i).get_int64());
        data_list[i].__set_max(maxColumn->get_slice(i).to_string());
        data_list[i].__set_min(minColumn->get_slice(i).to_string());
    }

    result->result_batch.rows.resize(num_rows);
    result->result_batch.__set_statistic_version(version);

    ThriftSerializer serializer(true, chunk->memory_usage());
    for (int i = 0; i < num_rows; ++i) {
        RETURN_IF_ERROR(serializer.serialize(&data_list[i], &result->result_batch.rows[i]));
    }
    return Status::OK();
}

Status StatisticResultWriter::_fill_statistic_histogram(int version, const vectorized::Columns& columns,
                                                        const vectorized::Chunk* chunk, TFetchDataResult* result) {
    SCOPED_TIMER(_serialize_timer);
    DCHECK(columns.size() == 5);

    Int64Column* dbIds = down_cast<Int64Column*>(ColumnHelper::get_data_column(columns[1].get()));
    Int64Column* tableIds = down_cast<Int64Column*>(ColumnHelper::get_data_column(columns[2].get()));
    BinaryColumn* nameColumn = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(columns[3].get()));
    BinaryColumn* histogramColumn = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(columns[4].get()));

    std::vector<TStatisticData> data_list;
    int num_rows = chunk->num_rows();

    data_list.resize(num_rows);
    for (int i = 0; i < num_rows; ++i) {
        data_list[i].__set_dbId(dbIds->get(i).get_int64());
        data_list[i].__set_tableId(tableIds->get(i).get_int64());
        data_list[i].__set_columnName(nameColumn->get_slice(i).to_string());
        data_list[i].__set_histogram(histogramColumn->get_slice(i).to_string());
    }

    result->result_batch.rows.resize(num_rows);
    result->result_batch.__set_statistic_version(version);

    ThriftSerializer serializer(true, chunk->memory_usage());
    for (int i = 0; i < num_rows; ++i) {
        RETURN_IF_ERROR(serializer.serialize(&data_list[i], &result->result_batch.rows[i]));
    }
    return Status::OK();
}

Status StatisticResultWriter::close() {
    COUNTER_SET(_sent_rows_counter, _written_rows);
    return Status::OK();
}

} // namespace starrocks::vectorized
