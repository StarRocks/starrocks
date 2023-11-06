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

#include "compaction.h"

#include <utility>

#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/tablet_reader.h"
#include "util/defer_op.h"
#include "util/time.h"
#include "util/trace.h"

namespace starrocks {

Semaphore Compaction::_concurrency_sem;

Compaction::Compaction(MemTracker* mem_tracker, TabletSharedPtr tablet)
        : _mem_tracker(mem_tracker),
          _tablet(std::move(tablet)),
          _input_rowsets_size(0),
          _input_row_num(0),
          _state(CompactionState::INITED),
          _runtime_profile("compaction") {}

Compaction::~Compaction() = default;

Status Compaction::init(int concurrency) {
    _concurrency_sem.set_count(concurrency);
    return Status::OK();
}

Status Compaction::do_compaction() {
    _concurrency_sem.wait();
    TRACE("got concurrency lock and start to do compaction");
    Status st = do_compaction_impl();
    _concurrency_sem.signal();
    return st;
}

Status Compaction::do_compaction_impl() {
    OlapStopWatch watch;

    int64_t segments_num = 0;
    for (auto& rowset : _input_rowsets) {
        _input_rowsets_size += rowset->data_disk_size();
        _input_row_num += rowset->num_rows();
        segments_num += rowset->num_segments();
    }

    TRACE_COUNTER_INCREMENT("input_rowsets_data_size", _input_rowsets_size);
    TRACE_COUNTER_INCREMENT("input_row_num", _input_row_num);
    TRACE_COUNTER_INCREMENT("input_segments_num", segments_num);

    _output_version = Version(_input_rowsets.front()->start_version(), _input_rowsets.back()->end_version());

    // choose vertical or horizontal compaction algorithm
    auto iterator_num_res = Rowset::get_segment_num(_input_rowsets);
    if (!iterator_num_res.ok()) {
        LOG(WARNING) << "fail to get segment iterator num. tablet=" << _tablet->tablet_id()
                     << ", err=" << iterator_num_res.status().to_string();
        return iterator_num_res.status();
    }

    auto cur_tablet_schema = CompactionUtils::rowset_with_max_schema_version(_input_rowsets)->schema();
    size_t segment_iterator_num = iterator_num_res.value();
    CompactionAlgorithm algorithm = CompactionUtils::choose_compaction_algorithm(
            cur_tablet_schema->num_columns(), config::vertical_compaction_max_columns_per_group, segment_iterator_num);
    if (algorithm == VERTICAL_COMPACTION) {
        CompactionUtils::split_column_into_groups(cur_tablet_schema->num_columns(), cur_tablet_schema->sort_key_idxes(),
                                                  config::vertical_compaction_max_columns_per_group, &_column_groups);
    }

    uint32_t max_rows_per_segment =
            CompactionUtils::get_segment_max_rows(config::max_segment_file_size, _input_row_num, _input_rowsets_size);

    LOG(INFO) << "start " << compaction_name() << ". tablet=" << _tablet->tablet_id()
              << ", output version is=" << _output_version.first << "-" << _output_version.second
              << ", max rows per segment=" << max_rows_per_segment << ", segment iterator num=" << segment_iterator_num
              << ", algorithm=" << CompactionUtils::compaction_algorithm_to_string(algorithm)
              << ", column group size=" << _column_groups.size()
              << ", columns per group=" << config::vertical_compaction_max_columns_per_group;

    RETURN_IF_ERROR(CompactionUtils::construct_output_rowset_writer(
            _tablet.get(), max_rows_per_segment, algorithm, _output_version, &_output_rs_writer, cur_tablet_schema));
    TRACE("prepare finished");

    Statistics stats;
    Status st;
    if (algorithm == VERTICAL_COMPACTION) {
        st = _merge_rowsets_vertically(segment_iterator_num, &stats, cur_tablet_schema);
    } else {
        st = _merge_rowsets_horizontally(segment_iterator_num, &stats, cur_tablet_schema);
    }
    if (!st.ok()) {
        LOG(WARNING) << "fail to do " << compaction_name() << ". res=" << st << ", tablet=" << _tablet->tablet_id()
                     << ", output_version=" << _output_version.first << "-" << _output_version.second;
        return st;
    }
    TRACE("merge rowsets finished");
    TRACE_COUNTER_INCREMENT("merged_rows", stats.merged_rows);
    TRACE_COUNTER_INCREMENT("filtered_rows", stats.filtered_rows);
    TRACE_COUNTER_INCREMENT("output_rows", stats.output_rows);

    auto res = _output_rs_writer->build();
    if (!res.ok()) return res.status();
    _output_rowset = std::move(res).value();
    TRACE_COUNTER_INCREMENT("output_rowset_data_size", _output_rowset->data_disk_size());
    TRACE_COUNTER_INCREMENT("output_row_num", _output_rowset->num_rows());
    TRACE_COUNTER_INCREMENT("output_segments_num", _output_rowset->num_segments());
    TRACE("output rowset built");

    RETURN_IF_ERROR(check_correctness(stats));
    TRACE("check correctness finished");

    RETURN_IF_ERROR(modify_rowsets());
    TRACE("modify rowsets finished");

    int64_t now = UnixMillis();
    if (compaction_type() == ReaderType::READER_CUMULATIVE_COMPACTION) {
        _tablet->set_last_cumu_compaction_success_time(now);
    } else {
        _tablet->set_last_base_compaction_success_time(now);
    }

    LOG(INFO) << "succeed to do " << compaction_name() << ". tablet=" << _tablet->tablet_id()
              << ", output_version=" << _output_version.first << "-" << _output_version.second
              << ", input infos [segments=" << segments_num << ", rows=" << _input_row_num
              << ", disk size=" << _input_rowsets_size << "]"
              << ", output infos [segments=" << _output_rowset->num_segments()
              << ", rows=" << _output_rowset->num_rows() << ", disk size=" << _output_rowset->data_disk_size() << "]"
              << ". elapsed time=" << watch.get_elapse_second() << "s.";

    st = _output_rowset->load();
    LOG_IF(WARNING, !st.ok()) << "ignore load rowset error tablet:" << _tablet->tablet_id()
                              << " rowset:" << _output_rowset->rowset_id() << " " << st;
    return Status::OK();
}

Status Compaction::_merge_rowsets_horizontally(size_t segment_iterator_num, Statistics* stats_output,
                                               const TabletSchemaCSPtr& tablet_schema) {
    TRACE_COUNTER_SCOPE_LATENCY_US("merge_rowsets_latency_us");
    Schema schema = ChunkHelper::convert_schema(tablet_schema);
    auto merge_tablet_schema = std::shared_ptr<TabletSchema>(TabletSchema::copy(tablet_schema));
    TabletReader reader(_tablet, _output_rs_writer->version(), merge_tablet_schema, schema);
    TabletReaderParams reader_params;
    reader_params.reader_type = compaction_type();
    reader_params.profile = _runtime_profile.create_child("merge_rowsets");

    int64_t total_num_rows = 0;
    int64_t total_mem_footprint = 0;
    for (auto& rowset : _input_rowsets) {
        total_num_rows += rowset->num_rows();
        total_mem_footprint += rowset->total_row_size();
    }
    int32_t chunk_size =
            CompactionUtils::get_read_chunk_size(config::compaction_memory_limit_per_worker, config::vector_chunk_size,
                                                 total_num_rows, total_mem_footprint, segment_iterator_num);
    VLOG(1) << "tablet=" << _tablet->tablet_id() << ", reader chunk size=" << chunk_size;
    reader_params.chunk_size = chunk_size;
    RETURN_IF_ERROR(reader.prepare());
    RETURN_IF_ERROR(reader.open(reader_params));

    int64_t output_rows = 0;
    auto chunk = ChunkHelper::new_chunk(schema, reader_params.chunk_size);
    auto char_field_indexes = ChunkHelper::get_char_field_indexes(schema);

    Status status;
    while (!StorageEngine::instance()->bg_worker_stopped()) {
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
                LOG(WARNING) << "reader get next error. tablet=" << _tablet->tablet_id()
                             << ", err=" << status.to_string();
                return Status::InternalError(fmt::format("reader get_next error: {}", status.to_string()));
            }
        }

        ChunkHelper::padding_char_columns(char_field_indexes, schema, tablet_schema, chunk.get());

        if (auto st = _output_rs_writer->add_chunk(*chunk); !st.ok()) {
            LOG(WARNING) << "writer add_chunk error: " << st;
            return st;
        }
        output_rows += chunk->num_rows();
    }

    if (StorageEngine::instance()->bg_worker_stopped()) {
        return Status::InternalError("Process is going to quit. The compaction will stop.");
    }

    if (stats_output != nullptr) {
        stats_output->output_rows = output_rows;
        stats_output->merged_rows = reader.merged_rows();
        stats_output->filtered_rows = reader.stats().rows_del_filtered;
    }

    if (auto st = _output_rs_writer->flush(); !st.ok()) {
        LOG(WARNING) << "failed to flush rowset when merging rowsets of tablet " << _tablet->tablet_id()
                     << ", err=" << st;
        return st;
    }
    return Status::OK();
}

Status Compaction::_merge_rowsets_vertically(size_t segment_iterator_num, Statistics* stats_output,
                                             const TabletSchemaCSPtr& tablet_schema) {
    TRACE_COUNTER_SCOPE_LATENCY_US("merge_rowsets_latency_us");
    auto mask_buffer = std::make_unique<RowSourceMaskBuffer>(_tablet->tablet_id(), _tablet->data_dir()->path());
    auto source_masks = std::make_unique<std::vector<RowSourceMask>>();
    for (size_t i = 0; i < _column_groups.size(); ++i) {
        bool is_key = (i == 0);
        if (!is_key) {
            // read mask buffer from the beginning
            RETURN_IF_ERROR(mask_buffer->flip_to_read());
        }

        Schema schema = ChunkHelper::convert_schema(tablet_schema, _column_groups[i]);
        TabletReader reader(_tablet, _output_rs_writer->version(), schema, is_key, mask_buffer.get());
        RETURN_IF_ERROR(reader.prepare());
        TabletReaderParams reader_params;
        reader_params.reader_type = compaction_type();
        reader_params.profile = _runtime_profile.create_child("merge_rowsets");

        int64_t total_num_rows = 0;
        int64_t total_mem_footprint = 0;
        for (auto& rowset : _input_rowsets) {
            total_num_rows += rowset->num_rows();
            for (auto& segment : rowset->segments()) {
                for (uint32_t column_index : _column_groups[i]) {
                    auto uid = tablet_schema->column(column_index).unique_id();
                    if (!segment->is_valid_column(uid)) {
                        continue;
                    }
                    const auto* column_reader = segment->column_with_uid(uid);
                    if (column_reader == nullptr) {
                        continue;
                    }
                    total_mem_footprint += column_reader->total_mem_footprint();
                }
            }
        }
        int32_t chunk_size = CompactionUtils::get_read_chunk_size(config::compaction_memory_limit_per_worker,
                                                                  config::vector_chunk_size, total_num_rows,
                                                                  total_mem_footprint, segment_iterator_num);
        VLOG(1) << "tablet=" << _tablet->tablet_id() << ", column group=" << i << ", reader chunk size=" << chunk_size;
        reader_params.chunk_size = chunk_size;
        RETURN_IF_ERROR(reader.open(reader_params));

        int64_t output_rows = 0;
        auto chunk = ChunkHelper::new_chunk(schema, reader_params.chunk_size);
        auto char_field_indexes = ChunkHelper::get_char_field_indexes(schema);

        Status status;
        while (!StorageEngine::instance()->bg_worker_stopped()) {
#ifndef BE_TEST
            status = tls_thread_status.mem_tracker()->check_mem_limit("Compaction");
            if (!status.ok()) {
                LOG(WARNING) << "fail to execute compaction: " << status.message() << std::endl;
                return status;
            }
#endif

            chunk->reset();
            status = reader.get_next(chunk.get(), source_masks.get());
            if (!status.ok()) {
                if (status.is_end_of_file()) {
                    break;
                } else {
                    LOG(WARNING) << "reader get next error. tablet=" << _tablet->tablet_id()
                                 << ", err=" << status.to_string();
                    return Status::InternalError(fmt::format("reader get_next error: {}", status.to_string()));
                }
            }

            ChunkHelper::padding_char_columns(char_field_indexes, schema, tablet_schema, chunk.get());

            if (auto st = _output_rs_writer->add_columns(*chunk, _column_groups[i], is_key); !st.ok()) {
                LOG(WARNING) << "writer add chunk by columns error. tablet=" << _tablet->tablet_id() << ", err=" << st;
                return st;
            }

            if (is_key) {
                output_rows += chunk->num_rows();
                if (!source_masks->empty()) {
                    RETURN_IF_ERROR(mask_buffer->write(*source_masks));
                }
            }
            if (!source_masks->empty()) {
                source_masks->clear();
            }
        }

        if (StorageEngine::instance()->bg_worker_stopped()) {
            return Status::InternalError("Process is going to quit. The compaction will stop.");
        }

        if (is_key && stats_output != nullptr) {
            stats_output->output_rows = output_rows;
            stats_output->merged_rows = reader.merged_rows();
            stats_output->filtered_rows = reader.stats().rows_del_filtered;
        }

        if (auto st = _output_rs_writer->flush_columns(); !st.ok()) {
            LOG(WARNING) << "failed to flush column group when merging rowsets of tablet " << _tablet->tablet_id()
                         << ", err=" << st;
            return st;
        }

        if (is_key) {
            RETURN_IF_ERROR(mask_buffer->flush());
        }
    }

    if (auto st = _output_rs_writer->final_flush(); !st.ok()) {
        LOG(WARNING) << "failed to final flush rowset when merging rowsets of tablet " << _tablet->tablet_id()
                     << ", err=" << st;
        return st;
    }

    return Status::OK();
}

Status Compaction::modify_rowsets() {
    if (StorageEngine::instance()->bg_worker_stopped()) {
        return Status::InternalError("Process is going to quit. The compaction will stop.");
    }

    std::vector<RowsetSharedPtr> to_replace;
    std::unique_lock wrlock(_tablet->get_header_lock());
    _tablet->modify_rowsets({_output_rowset}, _input_rowsets, &to_replace);
    _tablet->save_meta();
    Rowset::close_rowsets(_input_rowsets);
    for (auto& rs : to_replace) {
        StorageEngine::instance()->add_unused_rowset(rs);
    }

    return Status::OK();
}

Status Compaction::check_version_continuity(const std::vector<RowsetSharedPtr>& rowsets) {
    RowsetSharedPtr prev_rowset = rowsets.front();
    for (size_t i = 1; i < rowsets.size(); ++i) {
        const RowsetSharedPtr& rowset = rowsets[i];
        if (rowset->start_version() != prev_rowset->end_version() + 1) {
            LOG(WARNING) << "There are missed versions among rowsets. "
                         << "prev_rowset version=" << prev_rowset->start_version() << "-" << prev_rowset->end_version()
                         << ", rowset version=" << rowset->start_version() << "-" << rowset->end_version();
            return Status::InternalError("cumulative compaction miss version error.");
        }
        prev_rowset = rowset;
    }

    return Status::OK();
}

Status Compaction::check_correctness(const Statistics& stats) {
    // check row number
    if (_input_row_num != _output_rowset->num_rows() + stats.merged_rows + stats.filtered_rows) {
        LOG(WARNING) << "row_num does not match between cumulative input and output! "
                     << "input_row_num=" << _input_row_num << ", merged_row_num=" << stats.merged_rows
                     << ", filtered_row_num=" << stats.filtered_rows
                     << ", output_row_num=" << _output_rowset->num_rows();

        return Status::InternalError("cumulative compaction check lines error.");
    }
    return Status::OK();
}

} // namespace starrocks
