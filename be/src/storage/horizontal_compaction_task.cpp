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

#include "storage/horizontal_compaction_task.h"

#include <vector>

#include "column/schema.h"
#include "common/statusor.h"
#include "runtime/current_thread.h"
#include "storage/chunk_helper.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/tablet.h"
#include "storage/tablet_reader.h"
#include "storage/tablet_reader_params.h"
#include "util/time.h"
#include "util/trace.h"

namespace starrocks {

Status HorizontalCompactionTask::run_impl() {
    Statistics statistics;
    RETURN_IF_ERROR(_shortcut_compact(&statistics));
    RETURN_IF_ERROR(_horizontal_compact_data(&statistics));

    TRACE_COUNTER_INCREMENT("merged_rows", statistics.merged_rows);
    TRACE_COUNTER_INCREMENT("filtered_rows", statistics.filtered_rows);
    TRACE_COUNTER_INCREMENT("output_rows", statistics.output_rows);

    RETURN_IF_ERROR(_validate_compaction(statistics));
    TRACE("[Compaction] horizontal compaction validated");

    RETURN_IF_ERROR(_commit_compaction());
    TRACE("[Compaction] horizontal compaction committed");

    return Status::OK();
}

Status HorizontalCompactionTask::_horizontal_compact_data(Statistics* statistics) {
    if (_output_rowset != nullptr) {
        return Status::OK();
    }
    TRACE("[Compaction] start horizontal comapction data");
    // 1: init
    int64_t max_rows_per_segment = CompactionUtils::get_segment_max_rows(
            config::max_segment_file_size, _task_info.input_rows_num, _task_info.input_rowsets_size);

    std::unique_ptr<RowsetWriter> output_rs_writer;
    RETURN_IF_ERROR(CompactionUtils::construct_output_rowset_writer(_tablet.get(), max_rows_per_segment,
                                                                    _task_info.algorithm, _task_info.output_version,
                                                                    &output_rs_writer, _tablet_schema));

    Schema schema = ChunkHelper::convert_schema(_tablet_schema);
    TabletReader reader(std::static_pointer_cast<Tablet>(_tablet->shared_from_this()), output_rs_writer->version(),
                        schema, _tablet_schema);
    TabletReaderParams reader_params;
    DCHECK(compaction_type() == BASE_COMPACTION || compaction_type() == CUMULATIVE_COMPACTION);
    reader_params.reader_type =
            compaction_type() == BASE_COMPACTION ? READER_BASE_COMPACTION : READER_CUMULATIVE_COMPACTION;
    reader_params.profile = _runtime_profile.create_child("merge_rowsets");
    reader_params.column_access_paths = &_column_access_paths;

    int32_t chunk_size = CompactionUtils::get_read_chunk_size(
            config::compaction_memory_limit_per_worker, config::vector_chunk_size, _task_info.input_rows_num,
            _task_info.input_rowsets_size, _task_info.segment_iterator_num);
    VLOG(1) << "compaction task_id:" << _task_info.task_id << ", tablet=" << _tablet->tablet_id()
            << ", reader chunk size=" << chunk_size;
    reader_params.chunk_size = chunk_size;
    RETURN_IF_ERROR(reader.prepare());
    RETURN_IF_ERROR(reader.open(reader_params));
    TRACE("[Compaction] compaction prepare finished, max_rows_per_segment:$0, chunk_size:$1", max_rows_per_segment,
          chunk_size);

    // 2: read data and output compacted data
    StatusOr<size_t> res = _compact_data(chunk_size, reader, schema, output_rs_writer.get());
    if (!res.ok()) {
        return res.status();
    }
    TRACE("[Compaction] data compacted");

    // 3: generate new rowset
    RETURN_IF_ERROR(output_rs_writer->flush());

    TRACE("[Compaction] writer flushed");

    StatusOr<RowsetSharedPtr> build_res = output_rs_writer->build();
    if (!build_res.ok()) {
        LOG(WARNING) << "rowset writer build failed. compaction task_id:" << _task_info.task_id
                     << ", tablet:" << _task_info.tablet_id << " output_version=" << _task_info.output_version;
        return build_res.status();
    } else {
        _output_rowset = build_res.value();
    }
    _task_info.output_segments_num = _output_rowset->num_segments();
    _task_info.output_rowset_size = _output_rowset->data_disk_size();
    TRACE_COUNTER_INCREMENT("output_rowset_data_size", _output_rowset->data_disk_size());
    TRACE_COUNTER_INCREMENT("output_segments_num", _output_rowset->num_segments());
    TRACE("[Compaction] output rowset built");

    // collect statistics if necessary
    if (statistics) {
        statistics->output_rows = res.value();
        statistics->merged_rows = reader.merged_rows();
        statistics->filtered_rows = reader.stats().rows_del_filtered;
    }

    if (config::enable_rowset_verify) {
        RETURN_IF_ERROR(_output_rowset->verify());
    }

    return Status::OK();
}

StatusOr<size_t> HorizontalCompactionTask::_compact_data(int32_t chunk_size, TabletReader& reader, const Schema& schema,
                                                         RowsetWriter* output_rs_writer) {
    TRACE("[Compaction] start to compact data");
    auto status = Status::OK();
    size_t output_rows = 0;
    auto char_field_indexes = ChunkHelper::get_char_field_indexes(schema);
    auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
    while (LIKELY(!should_stop())) {
#ifndef BE_TEST
        status = tls_thread_status.mem_tracker()->check_mem_limit("Compaction");
        if (!status.ok()) {
            LOG(WARNING) << "fail to execute compaction: " << status.message() << std::endl;
            return status;
        }
#endif

        chunk->reset();
        status = reader.get_next(chunk.get());
        if (!status.ok()) {
            if (status.is_end_of_file()) {
                break;
            } else {
                LOG(WARNING) << "compaction reader get next error. tablet=" << _tablet->tablet_id()
                             << ", err=" << status.to_string();
                return Status::InternalError(fmt::format("reader get_next error: {}", status.to_string()));
            }
        }

        ChunkHelper::padding_char_columns(char_field_indexes, schema, _tablet_schema, chunk.get());

        RETURN_IF_ERROR(output_rs_writer->add_chunk(*chunk));
        output_rows += chunk->num_rows();
        _task_info.output_num_rows = output_rows;
        _task_info.filtered_rows = reader.stats().rows_del_filtered;
        _task_info.merged_rows = reader.merged_rows();
    }
    TRACE("[Compaction] data compacted");

    if (should_stop()) {
        LOG(INFO) << "horizontal compaction task_id:" << _task_info.task_id << ", tablet:" << _task_info.tablet_id
                  << " is stopped.";
        return Status::Cancelled("horizontal compaction task is stopped.");
    }
    return output_rows;
}

} // namespace starrocks
