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

#include "runtime/statistic_result_writer.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "exprs/expr.h"
#include "runtime/buffer_control_block.h"
#include "types/logical_type.h"
#include "util/thrift_util.h"

namespace starrocks {

const int STATISTIC_DATA_VERSION1 = 1;
const int STATISTIC_DATA_VERSION2 = 10;
const int STATISTIC_HISTOGRAM_VERSION = 2;
const int DICT_STATISTIC_DATA_VERSION = 101;
const int STATISTIC_TABLE_VERSION = 3;
const int STATISTIC_BATCH_VERSION = 4;
const int STATISTIC_PARTITION_VERSION = 11;
const int STATISTIC_EXTERNAL_VERSION = 5;
const int STATISTIC_EXTERNAL_QUERY_VERSION = 6;
const int STATISTIC_EXTERNAL_HISTOGRAM_VERSION = 7;
const int STATISTIC_EXTERNAL_QUERY_VERSION_V2 = 8;
const int STATISTIC_BATCH_V5_VERSION = 9;

StatisticResultWriter::StatisticResultWriter(BufferControlBlock* sinker,
                                             const std::vector<ExprContext*>& output_expr_ctxs,
                                             starrocks::RuntimeProfile* parent_profile)
        : BufferControlResultWriter(sinker, parent_profile), _output_expr_ctxs(output_expr_ctxs) {}

StatisticResultWriter::~StatisticResultWriter() = default;

Status StatisticResultWriter::init(RuntimeState* state) {
    _init_profile();
    if (nullptr == _sinker) {
        return Status::InternalError("sinker is nullptr.");
    }
    return Status::OK();
}

void StatisticResultWriter::_init_profile() {
    BufferControlResultWriter::_init_profile();
    _serialize_timer = ADD_TIMER(_parent_profile, "SerializeTime");
}

Status StatisticResultWriter::append_chunk(Chunk* chunk) {
    SCOPED_TIMER(_append_chunk_timer);
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

StatusOr<TFetchDataResultPtrs> StatisticResultWriter::process_chunk(Chunk* chunk) {
    SCOPED_TIMER(_append_chunk_timer);
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

StatusOr<TFetchDataResultPtr> StatisticResultWriter::_process_chunk(Chunk* chunk) {
    if (nullptr == chunk || 0 == chunk->num_rows()) {
        return nullptr;
    }

    // Step 1: compute expr
    int num_columns = _output_expr_ctxs.size();

    Columns result_columns;
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
    } else if (version == STATISTIC_DATA_VERSION2) {
        RETURN_IF_ERROR_WITH_WARN(_fill_statistic_data_v2(version, result_columns, chunk, result.get()),
                                  "Fill statistic data failed");
    } else if (version == DICT_STATISTIC_DATA_VERSION) {
        RETURN_IF_ERROR_WITH_WARN(_fill_dict_statistic_data(version, result_columns, chunk, result.get()),
                                  "Fill dict statistic data failed");
    } else if (version == STATISTIC_HISTOGRAM_VERSION) {
        RETURN_IF_ERROR_WITH_WARN(_fill_statistic_histogram(version, result_columns, chunk, result.get()),
                                  "Fill histogram statistic data failed");
    } else if (version == STATISTIC_TABLE_VERSION) {
        RETURN_IF_ERROR_WITH_WARN(_fill_table_statistic_data(version, result_columns, chunk, result.get()),
                                  "Fill table statistic data failed");
    } else if (version == STATISTIC_PARTITION_VERSION) {
        RETURN_IF_ERROR_WITH_WARN(_fill_partition_statistic_data(version, result_columns, chunk, result.get()),
                                  "Fill partition statistic data failed");
    } else if (version == STATISTIC_BATCH_VERSION) {
        RETURN_IF_ERROR_WITH_WARN(_fill_full_statistic_data_v4(version, result_columns, chunk, result.get()),
                                  "Fill table statistic data failed");
    } else if (version == STATISTIC_BATCH_V5_VERSION) {
        RETURN_IF_ERROR_WITH_WARN(_fill_full_statistic_data_v5(version, result_columns, chunk, result.get()),
                                  "Fill table statistic data failed");
    } else if (version == STATISTIC_EXTERNAL_VERSION) {
        RETURN_IF_ERROR_WITH_WARN(_fill_full_statistic_data_external(version, result_columns, chunk, result.get()),
                                  "Fill table statistic data failed");
    } else if (version == STATISTIC_EXTERNAL_QUERY_VERSION) {
        RETURN_IF_ERROR_WITH_WARN(_fill_full_statistic_query_external(version, result_columns, chunk, result.get()),
                                  "Fill table statistic data failed");
    } else if (version == STATISTIC_EXTERNAL_HISTOGRAM_VERSION) {
        RETURN_IF_ERROR_WITH_WARN(_fill_statistic_histogram_external(version, result_columns, chunk, result.get()),
                                  "Fill table statistic data failed");
    } else if (version == STATISTIC_EXTERNAL_QUERY_VERSION_V2) {
        RETURN_IF_ERROR_WITH_WARN(_fill_full_statistic_query_external_v2(version, result_columns, chunk, result.get()),
                                  "Fill table statistic data failed");
    }
    return result;
}

Status StatisticResultWriter::_fill_dict_statistic_data(int version, const Columns& columns, const Chunk* chunk,
                                                        TFetchDataResult* result) {
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
            TGlobalDict dict;
            thrift_from_json_string(&dict, std::string(dictColumnViewer.value(i).data, dictColumnViewer.value(i).size));
            data_list[i].__set_dict(dict);
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

Status StatisticResultWriter::_fill_statistic_data_v1(int version, const Columns& columns, const Chunk* chunk,
                                                      TFetchDataResult* result) {
    SCOPED_TIMER(_serialize_timer);

    // mapping with Data.thrift.TStatisticData
    DCHECK(columns.size() == 11);

    // skip read version
    auto& updateTimes = ColumnHelper::cast_to_raw<TYPE_DATETIME>(columns[1])->get_data();
    auto& dbIds = ColumnHelper::cast_to_raw<TYPE_BIGINT>(columns[2])->get_data();
    auto& tableIds = ColumnHelper::cast_to_raw<TYPE_BIGINT>(columns[3])->get_data();
    BinaryColumn* nameColumn = ColumnHelper::cast_to_raw<TYPE_VARCHAR>(columns[4]);
    const auto* rowCounts = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[5].get()));
    const auto* dataSizes = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[6].get()));
    const auto* countDistincts = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[7].get()));
    const auto* nullCounts = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[8].get()));
    const auto* maxColumn = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[9].get()));
    const auto* minColumn = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[10].get()));

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

// Added collection size compared to `_fill_statistic_data_v1`
Status StatisticResultWriter::_fill_statistic_data_v2(int version, const Columns& columns, const Chunk* chunk,
                                                      TFetchDataResult* result) {
    SCOPED_TIMER(_serialize_timer);

    // mapping with Data.thrift.TStatisticData
    DCHECK(columns.size() == 12);

    // skip read version
    auto& updateTimes = ColumnHelper::cast_to_raw<TYPE_DATETIME>(columns[1])->get_data();
    auto& dbIds = ColumnHelper::cast_to_raw<TYPE_BIGINT>(columns[2])->get_data();
    auto& tableIds = ColumnHelper::cast_to_raw<TYPE_BIGINT>(columns[3])->get_data();
    BinaryColumn* nameColumn = ColumnHelper::cast_to_raw<TYPE_VARCHAR>(columns[4]);
    auto* rowCounts = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[5].get()));
    auto* dataSizes = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[6].get()));
    auto* countDistincts = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[7].get()));
    auto* nullCounts = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[8].get()));
    auto* maxColumn = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[9].get()));
    auto* minColumn = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[10].get()));
    auto* collectionSize = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[11].get()));

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
        data_list[i].__set_collectionSize(collectionSize->get(i).get_int64());
    }

    result->result_batch.rows.resize(num_rows);
    result->result_batch.__set_statistic_version(version);

    ThriftSerializer serializer(true, chunk->memory_usage());
    for (int i = 0; i < num_rows; ++i) {
        RETURN_IF_ERROR(serializer.serialize(&data_list[i], &result->result_batch.rows[i]));
    }
    return Status::OK();
}

Status StatisticResultWriter::_fill_statistic_histogram(int version, const Columns& columns, const Chunk* chunk,
                                                        TFetchDataResult* result) {
    SCOPED_TIMER(_serialize_timer);
    DCHECK(columns.size() == 5);

    auto* dbIds = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[1].get()));
    auto* tableIds = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[2].get()));
    auto* nameColumn = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[3].get()));
    auto* histogramColumn = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[4].get()));

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

Status StatisticResultWriter::_fill_statistic_histogram_external(int version, const Columns& columns,
                                                                 const Chunk* chunk, TFetchDataResult* result) {
    SCOPED_TIMER(_serialize_timer);
    DCHECK(columns.size() == 3);

    auto* columnName = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[1].get()));
    auto* histogramColumn = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[2].get()));

    std::vector<TStatisticData> data_list;
    int num_rows = chunk->num_rows();

    data_list.resize(num_rows);
    for (int i = 0; i < num_rows; ++i) {
        data_list[i].__set_columnName(columnName->get_slice(i).to_string());
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

Status StatisticResultWriter::_fill_table_statistic_data(int version, const Columns& columns, const Chunk* chunk,
                                                         TFetchDataResult* result) {
    SCOPED_TIMER(_serialize_timer);
    DCHECK(columns.size() == 3);

    auto* partitionId = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[1].get()));
    auto* rowCounts = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[2].get()));

    std::vector<TStatisticData> data_list;
    int num_rows = chunk->num_rows();

    data_list.resize(num_rows);
    for (int i = 0; i < num_rows; ++i) {
        data_list[i].__set_partitionId(partitionId->get(i).get_int64());
        data_list[i].__set_rowCount(rowCounts->get(i).get_int64());
    }

    result->result_batch.rows.resize(num_rows);
    result->result_batch.__set_statistic_version(version);

    ThriftSerializer serializer(true, chunk->memory_usage());
    for (int i = 0; i < num_rows; ++i) {
        RETURN_IF_ERROR(serializer.serialize(&data_list[i], &result->result_batch.rows[i]));
    }
    return Status::OK();
}

/*
FE SQL:    
    SELECT cast(4 as INT), 
         cast(7164015 as BIGINT), 
         'k1', 
         cast(COUNT(1) as BIGINT), 
         cast(COUNT(1) * 4 as BIGINT), 
         hll_serialize(IFNULL(hll_raw(`k1`), hll_empty())), 
         cast(COUNT(1) - COUNT(`k1`) as BIGINT), 
         IFNULL(MAX(`k1`), ''), 
         IFNULL(MIN(`k1`), '')
    FROM xx
*/
Status StatisticResultWriter::_fill_full_statistic_data_v4(int version, const Columns& columns, const Chunk* chunk,
                                                           TFetchDataResult* result) {
    SCOPED_TIMER(_serialize_timer);

    // mapping with Data.thrift.TStatisticData
    DCHECK(columns.size() == 9);

    // skip read version
    auto pid = ColumnViewer<TYPE_BIGINT>(columns[1]);
    auto name = ColumnViewer<TYPE_VARCHAR>(columns[2]);
    auto rowCounts = ColumnViewer<TYPE_BIGINT>(columns[3]);
    auto dataSizes = ColumnViewer<TYPE_BIGINT>(columns[4]);
    auto hll = ColumnViewer<TYPE_VARCHAR>(columns[5]);
    auto nullCounts = ColumnViewer<TYPE_BIGINT>(columns[6]);
    auto max = ColumnViewer<TYPE_VARCHAR>(columns[7]);
    auto min = ColumnViewer<TYPE_VARCHAR>(columns[8]);

    std::vector<TStatisticData> data_list;
    int num_rows = chunk->num_rows();

    data_list.resize(num_rows);
    for (int i = 0; i < num_rows; ++i) {
        data_list[i].__set_partitionId(pid.value(i));
        data_list[i].__set_columnName(name.value(i).to_string());
        data_list[i].__set_rowCount(rowCounts.value(i));
        data_list[i].__set_dataSize(dataSizes.value(i));
        data_list[i].__set_hll(hll.value(i).to_string());
        data_list[i].__set_nullCount(nullCounts.value(i));
        data_list[i].__set_max(max.value(i).to_string());
        data_list[i].__set_min(min.value(i).to_string());
    }

    result->result_batch.rows.resize(num_rows);
    result->result_batch.__set_statistic_version(version);

    ThriftSerializer serializer(true, chunk->memory_usage());
    for (int i = 0; i < num_rows; ++i) {
        RETURN_IF_ERROR(serializer.serialize(&data_list[i], &result->result_batch.rows[i]));
    }
    return Status::OK();
}

Status StatisticResultWriter::_fill_full_statistic_data_v5(int version, const Columns& columns, const Chunk* chunk,
                                                           TFetchDataResult* result) {
    SCOPED_TIMER(_serialize_timer);

    // mapping with Data.thrift.TStatisticData
    DCHECK(columns.size() == 10);

    // skip read version
    auto pid = ColumnViewer<TYPE_BIGINT>(columns[1]);
    auto name = ColumnViewer<TYPE_VARCHAR>(columns[2]);
    auto rowCounts = ColumnViewer<TYPE_BIGINT>(columns[3]);
    auto dataSizes = ColumnViewer<TYPE_BIGINT>(columns[4]);
    auto hll = ColumnViewer<TYPE_VARCHAR>(columns[5]);
    auto nullCounts = ColumnViewer<TYPE_BIGINT>(columns[6]);
    auto max = ColumnViewer<TYPE_VARCHAR>(columns[7]);
    auto min = ColumnViewer<TYPE_VARCHAR>(columns[8]);
    auto collection_size = ColumnViewer<TYPE_BIGINT>(columns[9]);

    std::vector<TStatisticData> data_list;
    int num_rows = chunk->num_rows();

    data_list.resize(num_rows);
    for (int i = 0; i < num_rows; ++i) {
        data_list[i].__set_partitionId(pid.value(i));
        data_list[i].__set_columnName(name.value(i).to_string());
        data_list[i].__set_rowCount(rowCounts.value(i));
        data_list[i].__set_dataSize(dataSizes.value(i));
        data_list[i].__set_hll(hll.value(i).to_string());
        data_list[i].__set_nullCount(nullCounts.value(i));
        data_list[i].__set_max(max.value(i).to_string());
        data_list[i].__set_min(min.value(i).to_string());
        data_list[i].__set_collectionSize(collection_size.value(i));
    }

    result->result_batch.rows.resize(num_rows);
    result->result_batch.__set_statistic_version(version);

    ThriftSerializer serializer(true, chunk->memory_usage());
    for (int i = 0; i < num_rows; ++i) {
        RETURN_IF_ERROR(serializer.serialize(&data_list[i], &result->result_batch.rows[i]));
    }
    return Status::OK();
}

Status StatisticResultWriter::_fill_partition_statistic_data(int version, const Columns& columns, const Chunk* chunk,
                                                             TFetchDataResult* result) {
    /*
    SQL:
    SELECT cast(" + STATISTIC_PARTITION_VERSION + " as INT), +
     `partition_id`, 
    `column_name`, 
    hll_cardinality(hll_union(`ndv`)) as distinct_count
    */

    SCOPED_TIMER(_serialize_timer);

    // mapping with Data.thrift.TStatisticData
    DCHECK(columns.size() == 4);

    // skip read version
    auto partition_id = ColumnViewer<TYPE_BIGINT>(columns[1]);
    auto column_name = ColumnViewer<TYPE_VARCHAR>(columns[2]);
    auto distinct_count = ColumnViewer<TYPE_BIGINT>(columns[3]);
    std::vector<TStatisticData> data_list;
    int num_rows = chunk->num_rows();

    data_list.resize(num_rows);
    for (int i = 0; i < num_rows; ++i) {
        data_list[i].__set_partitionId(partition_id.value(i));
        data_list[i].__set_columnName(column_name.value(i).to_string());
        data_list[i].__set_countDistinct(distinct_count.value(i));
    }

    result->result_batch.rows.resize(num_rows);
    result->result_batch.__set_statistic_version(version);

    ThriftSerializer serializer(true, chunk->memory_usage());
    for (int i = 0; i < num_rows; ++i) {
        RETURN_IF_ERROR(serializer.serialize(&data_list[i], &result->result_batch.rows[i]));
    }
    return Status::OK();
}

/*
FE SQL:
    SELECT cast(5 as INT),
        'partitionName',
        'columnName',
        cast(COUNT(1) as BIGINT),
        cast($dataSize as BIGINT),
        $hllFunction,
        cast($countNullFunction as BIGINT),
        $maxFunction,
        $minFunction
    FROM xxx
*/
Status StatisticResultWriter::_fill_full_statistic_data_external(int version, const Columns& columns,
                                                                 const Chunk* chunk, TFetchDataResult* result) {
    SCOPED_TIMER(_serialize_timer);

    // mapping with Data.thrift.TStatisticData
    DCHECK(columns.size() == 9);

    // skip read version
    auto partitionName = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    auto name = ColumnViewer<TYPE_VARCHAR>(columns[2]);
    auto rowCounts = ColumnViewer<TYPE_BIGINT>(columns[3]);
    auto dataSizes = ColumnViewer<TYPE_BIGINT>(columns[4]);
    auto hll = ColumnViewer<TYPE_VARCHAR>(columns[5]);
    auto nullCounts = ColumnViewer<TYPE_BIGINT>(columns[6]);
    auto max = ColumnViewer<TYPE_VARCHAR>(columns[7]);
    auto min = ColumnViewer<TYPE_VARCHAR>(columns[8]);

    std::vector<TStatisticData> data_list;
    int num_rows = chunk->num_rows();

    data_list.resize(num_rows);
    for (int i = 0; i < num_rows; ++i) {
        data_list[i].__set_partitionName(partitionName.value(i).to_string());
        data_list[i].__set_columnName(name.value(i).to_string());
        data_list[i].__set_rowCount(rowCounts.value(i));
        data_list[i].__set_dataSize(dataSizes.value(i));
        data_list[i].__set_hll(hll.value(i).to_string());
        data_list[i].__set_nullCount(nullCounts.value(i));
        data_list[i].__set_max(max.value(i).to_string());
        data_list[i].__set_min(min.value(i).to_string());
    }

    result->result_batch.rows.resize(num_rows);
    result->result_batch.__set_statistic_version(version);

    ThriftSerializer serializer(true, chunk->memory_usage());
    for (int i = 0; i < num_rows; ++i) {
        RETURN_IF_ERROR(serializer.serialize(&data_list[i], &result->result_batch.rows[i]));
    }
    return Status::OK();
}

Status StatisticResultWriter::_fill_full_statistic_query_external(int version, const Columns& columns,
                                                                  const Chunk* chunk, TFetchDataResult* result) {
    SCOPED_TIMER(_serialize_timer);

    // mapping with Data.thrift.TStatisticData
    DCHECK(columns.size() == 8);

    auto columnName = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    auto rowCounts = ColumnViewer<TYPE_BIGINT>(columns[2]);
    auto dataSizes = ColumnViewer<TYPE_BIGINT>(columns[3]);
    auto countDistincts = ColumnViewer<TYPE_BIGINT>(columns[4]);
    auto nullCounts = ColumnViewer<TYPE_BIGINT>(columns[5]);
    auto maxColumn = ColumnViewer<TYPE_VARCHAR>(columns[6]);
    auto minColumn = ColumnViewer<TYPE_VARCHAR>(columns[7]);

    std::vector<TStatisticData> data_list;
    int num_rows = chunk->num_rows();

    data_list.resize(num_rows);
    for (int i = 0; i < num_rows; ++i) {
        data_list[i].__set_columnName(columnName.value(i).to_string());
        data_list[i].__set_rowCount(rowCounts.value(i));
        data_list[i].__set_dataSize(dataSizes.value(i));
        data_list[i].__set_countDistinct(countDistincts.value(i));
        data_list[i].__set_nullCount(nullCounts.value(i));
        data_list[i].__set_max(maxColumn.value(i).to_string());
        data_list[i].__set_min(minColumn.value(i).to_string());
    }

    result->result_batch.rows.resize(num_rows);
    result->result_batch.__set_statistic_version(version);

    ThriftSerializer serializer(true, chunk->memory_usage());
    for (int i = 0; i < num_rows; ++i) {
        RETURN_IF_ERROR(serializer.serialize(&data_list[i], &result->result_batch.rows[i]));
    }
    return Status::OK();
}

Status StatisticResultWriter::_fill_full_statistic_query_external_v2(int version, const Columns& columns,
                                                                     const Chunk* chunk, TFetchDataResult* result) {
    SCOPED_TIMER(_serialize_timer);

    // mapping with Data.thrift.TStatisticData
    DCHECK(columns.size() == 9);

    auto columnName = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    auto rowCounts = ColumnViewer<TYPE_BIGINT>(columns[2]);
    auto dataSizes = ColumnViewer<TYPE_BIGINT>(columns[3]);
    auto countDistincts = ColumnViewer<TYPE_BIGINT>(columns[4]);
    auto nullCounts = ColumnViewer<TYPE_BIGINT>(columns[5]);
    auto maxColumn = ColumnViewer<TYPE_VARCHAR>(columns[6]);
    auto minColumn = ColumnViewer<TYPE_VARCHAR>(columns[7]);
    auto updateTime = ColumnViewer<TYPE_DATETIME>(columns[8]);

    std::vector<TStatisticData> data_list;
    int num_rows = chunk->num_rows();

    data_list.resize(num_rows);
    for (int i = 0; i < num_rows; ++i) {
        data_list[i].__set_columnName(columnName.value(i).to_string());
        data_list[i].__set_rowCount(rowCounts.value(i));
        data_list[i].__set_dataSize(dataSizes.value(i));
        data_list[i].__set_countDistinct(countDistincts.value(i));
        data_list[i].__set_nullCount(nullCounts.value(i));
        data_list[i].__set_max(maxColumn.value(i).to_string());
        data_list[i].__set_min(minColumn.value(i).to_string());
        data_list[i].__set_updateTime(updateTime.value(i).to_string());
    }

    result->result_batch.rows.resize(num_rows);
    result->result_batch.__set_statistic_version(version);

    ThriftSerializer serializer(true, chunk->memory_usage());
    for (int i = 0; i < num_rows; ++i) {
        RETURN_IF_ERROR(serializer.serialize(&data_list[i], &result->result_batch.rows[i]));
    }
    return Status::OK();
}

} // namespace starrocks
