// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/vectorized/compaction.h"

#include <utility>

#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/vectorized/chunk_helper.h"
#include "storage/vectorized/tablet_reader.h"
#include "util/defer_op.h"
#include "util/time.h"
#include "util/trace.h"

namespace starrocks::vectorized {

const char* compaction_algorithm_to_string(CompactionAlgorithm algorithm) {
    switch (algorithm) {
    case kHorizontal:
        return "horizontal";
    case kVertical:
        return "vertical";
    }
    return "unknown";
}

Semaphore Compaction::_concurrency_sem;

Compaction::Compaction(MemTracker* mem_tracker, TabletSharedPtr tablet)
        : _mem_tracker(mem_tracker),
          _tablet(std::move(tablet)),
          _input_rowsets_size(0),
          _input_row_num(0),
          _state(CompactionState::INITED),
          _runtime_profile("compaction") {}

Compaction::~Compaction() = default;

Status Compaction::init(int concurreny) {
    _concurrency_sem.set_count(concurreny);
    return Status::OK();
}

CompactionAlgorithm Compaction::choose_compaction_algorithm(size_t num_columns, int64_t max_columns_per_group,
                                                            size_t source_num) {
    // if the number of columns in the schema is less than or equal to max_columns_per_group, use kHorizontal.
    if (num_columns <= max_columns_per_group) {
        return kHorizontal;
    }

    // if source_num is less than or equal to 1, heap merge iterator is not used in compaction,
    // and row source mask is not created.
    // if source_num is more than MAX_SOURCES, mask in RowSourceMask may overflow.
    if (source_num <= 1 || source_num > RowSourceMask::MAX_SOURCES) {
        return kHorizontal;
    }

    return kVertical;
}

uint32_t Compaction::get_segment_max_rows(int64_t max_segment_file_size, int64_t input_row_num, int64_t input_size) {
    // The range of config::max_segments_file_size is between [1, INT64_MAX]
    // If the configuration is set wrong, the config::max_segments_file_size will be a negtive value.
    // Using division instead multiplication can avoid the overflow
    int64_t max_segment_rows = max_segment_file_size / (input_size / (input_row_num + 1) + 1);
    if (max_segment_rows > INT32_MAX || max_segment_rows <= 0) {
        max_segment_rows = INT32_MAX;
    }
    return max_segment_rows;
}

int32_t Compaction::get_read_chunk_size(int64_t mem_limit, int32_t config_chunk_size, int64_t total_num_rows,
                                        int64_t total_mem_footprint, size_t source_num) {
    uint64_t chunk_size = config_chunk_size;
    if (mem_limit > 0) {
        int64_t avg_row_size = (total_mem_footprint + 1) / (total_num_rows + 1);
        // The result of the division operation be zero, so added one
        chunk_size = 1 + mem_limit / (source_num * avg_row_size + 1);
    }

    if (chunk_size > config_chunk_size) {
        chunk_size = config_chunk_size;
    }
    return chunk_size;
}

void Compaction::split_column_into_groups(size_t num_columns, size_t num_key_columns, int64_t max_columns_per_group,
                                          std::vector<std::vector<uint32_t>>* column_groups) {
    std::vector<uint32_t> key_columns;
    for (size_t i = 0; i < num_key_columns; ++i) {
        key_columns.emplace_back(i);
    }
    column_groups->emplace_back(std::move(key_columns));

    for (size_t i = num_key_columns; i < num_columns; ++i) {
        if ((i - num_key_columns) % max_columns_per_group == 0) {
            column_groups->emplace_back();
        }
        column_groups->back().emplace_back(i);
    }
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

    // 1. prepare input and output parameters
    int64_t segments_num = 0;
    int64_t total_row_size = 0;
    for (auto& rowset : _input_rowsets) {
        _input_rowsets_size += rowset->data_disk_size();
        _input_row_num += rowset->num_rows();
        segments_num += rowset->num_segments();
        total_row_size += rowset->total_row_size();
    }

    TRACE_COUNTER_INCREMENT("input_rowsets_data_size", _input_rowsets_size);
    TRACE_COUNTER_INCREMENT("input_row_num", _input_row_num);
    TRACE_COUNTER_INCREMENT("input_segments_num", segments_num);

    _output_version = Version(_input_rowsets.front()->start_version(), _input_rowsets.back()->end_version());

    // choose vertical or horizontal compaction algorithm
    auto iterator_num_res = _get_segment_iterator_num();
    if (!iterator_num_res.ok()) {
        LOG(WARNING) << "fail to get segment iterator num. tablet=" << _tablet->tablet_id()
                     << ", err=" << iterator_num_res.status().to_string();
        return iterator_num_res.status();
    }
    size_t segment_iterator_num = iterator_num_res.value();
    int64_t max_columns_per_group = config::vertical_compaction_max_columns_per_group;
    size_t num_columns = _tablet->num_columns();
    CompactionAlgorithm algorithm =
            choose_compaction_algorithm(num_columns, max_columns_per_group, segment_iterator_num);
    if (algorithm == kVertical) {
        split_column_into_groups(_tablet->num_columns(), _tablet->num_key_columns(), max_columns_per_group,
                                 &_column_groups);
    }

    // max rows per segment
    int64_t max_rows_per_segment =
            get_segment_max_rows(config::max_segment_file_size, _input_row_num, _input_rowsets_size);

    LOG(INFO) << "start " << compaction_name() << ". tablet=" << _tablet->tablet_id()
              << ", output version is=" << _output_version.first << "-" << _output_version.second
              << ", max rows per segment=" << max_rows_per_segment << ", segment iterator num=" << segment_iterator_num
              << ", algorithm=" << compaction_algorithm_to_string(algorithm)
              << ", column group size=" << _column_groups.size() << ", columns per group=" << max_columns_per_group;

    // create rowset writer
    RETURN_IF_ERROR(construct_output_rowset_writer(max_rows_per_segment, algorithm));
    TRACE("prepare finished");

    // 2. write combined rows to output rowset
    Statistics stats;
    Status st;
    if (algorithm == kVertical) {
        st = _merge_rowsets_vertically(segment_iterator_num, &stats);
    } else {
        st = _merge_rowsets_horizontally(segment_iterator_num, &stats);
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

    // 3. check correctness, commented for this moment.
    RETURN_IF_ERROR(check_correctness(stats));
    TRACE("check correctness finished");

    // 4. modify rowsets in memory
    RETURN_IF_ERROR(modify_rowsets());
    TRACE("modify rowsets finished");

    // 5. update last success compaction time
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

    // warm-up this rowset
    st = _output_rowset->load();
    // only log load failure
    LOG_IF(WARNING, !st.ok()) << "ignore load rowset error tablet:" << _tablet->tablet_id()
                              << " rowset:" << _output_rowset->rowset_id() << " " << st;

    return Status::OK();
}

StatusOr<size_t> Compaction::_get_segment_iterator_num() {
    Schema schema = ChunkHelper::convert_schema_to_format_v2(_tablet->tablet_schema());
    TabletReader reader(_tablet, _output_version, schema);
    TabletReaderParams reader_params;
    reader_params.reader_type = compaction_type();
    RETURN_IF_ERROR(reader.prepare());
    std::vector<ChunkIteratorPtr> seg_iters;
    RETURN_IF_ERROR(reader.get_segment_iterators(reader_params, &seg_iters));
    return seg_iters.size();
}

Status Compaction::construct_output_rowset_writer(uint32_t max_rows_per_segment, CompactionAlgorithm algorithm) {
    RowsetWriterContext context(kDataFormatV2, config::storage_format_version);
    context.rowset_id = StorageEngine::instance()->next_rowset_id();
    context.tablet_uid = _tablet->tablet_uid();
    context.tablet_id = _tablet->tablet_id();
    context.partition_id = _tablet->partition_id();
    context.tablet_schema_hash = _tablet->schema_hash();
    context.rowset_type = BETA_ROWSET;
    context.rowset_path_prefix = _tablet->schema_hash_path();
    context.tablet_schema = &(_tablet->tablet_schema());
    context.rowset_state = VISIBLE;
    context.version = _output_version;
    context.segments_overlap = NONOVERLAPPING;
    context.max_rows_per_segment = max_rows_per_segment;
    context.writer_type = (algorithm == kVertical ? RowsetWriterType::kVertical : RowsetWriterType::kHorizontal);
    Status st = RowsetFactory::create_rowset_writer(context, &_output_rs_writer);
    if (!st.ok()) {
        std::stringstream ss;
        ss << "Fail to create rowset writer. tablet_id=" << context.tablet_id << " err=" << st;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

Status Compaction::_merge_rowsets_horizontally(size_t segment_iterator_num, Statistics* stats_output) {
    TRACE_COUNTER_SCOPE_LATENCY_US("merge_rowsets_latency_us");
    Schema schema = ChunkHelper::convert_schema_to_format_v2(_tablet->tablet_schema());
    TabletReader reader(_tablet, _output_rs_writer->version(), schema);
    TabletReaderParams reader_params;
    reader_params.reader_type = compaction_type();
    reader_params.profile = _runtime_profile.create_child("merge_rowsets");

    int64_t total_num_rows = 0;
    int64_t total_mem_footprint = 0;
    for (auto& rowset : _input_rowsets) {
        total_num_rows += rowset->num_rows();
        total_mem_footprint += rowset->total_row_size();
    }
    int32_t chunk_size = get_read_chunk_size(config::compaction_memory_limit_per_worker, config::vector_chunk_size,
                                             total_num_rows, total_mem_footprint, segment_iterator_num);
    VLOG(1) << "tablet=" << _tablet->tablet_id() << ", reader chunk size=" << chunk_size;
    reader_params.chunk_size = chunk_size;
    RETURN_IF_ERROR(reader.prepare());
    RETURN_IF_ERROR(reader.open(reader_params));

    int64_t output_rows = 0;

    auto chunk = ChunkHelper::new_chunk(schema, reader_params.chunk_size);

    auto char_field_indexes = ChunkHelper::get_char_field_indexes(schema);

    Status status;
    while (!ExecEnv::GetInstance()->storage_engine()->bg_worker_stopped()) {
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

        ChunkHelper::padding_char_columns(char_field_indexes, schema, _tablet->tablet_schema(), chunk.get());

        if (auto st = _output_rs_writer->add_chunk(*chunk); !st.ok()) {
            LOG(WARNING) << "writer add_chunk error: " << st;
            return st;
        }
        output_rows += chunk->num_rows();
    }

    if (ExecEnv::GetInstance()->storage_engine()->bg_worker_stopped()) {
        return Status::InternalError("Process is going to quit. The compaction should be stopped as soon as possible.");
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

Status Compaction::_merge_rowsets_vertically(size_t segment_iterator_num, Statistics* stats_output) {
    TRACE_COUNTER_SCOPE_LATENCY_US("merge_rowsets_latency_us");
    auto mask_buffer = std::make_unique<RowSourceMaskBuffer>(_tablet->tablet_id(), _tablet->data_dir()->path());
    auto source_masks = std::make_unique<std::vector<RowSourceMask>>();
    for (size_t i = 0; i < _column_groups.size(); ++i) {
        bool is_key = (i == 0);
        if (!is_key) {
            // read mask buffer from the beginning
            mask_buffer->flip_to_read();
        }

        Schema schema = ChunkHelper::convert_schema_to_format_v2(_tablet->tablet_schema(), _column_groups[i]);
        TabletReader reader(_tablet, _output_rs_writer->version(), schema, is_key, mask_buffer.get());
        RETURN_IF_ERROR(reader.prepare());
        TabletReaderParams reader_params;
        reader_params.reader_type = compaction_type();
        reader_params.profile = _runtime_profile.create_child("merge_rowsets");

        int64_t total_num_rows = 0;
        int64_t total_mem_footprint = 0;
        for (auto& rowset : _input_rowsets) {
            if (rowset->rowset_meta()->rowset_type() != BETA_ROWSET) {
                continue;
            }

            total_num_rows += rowset->num_rows();
            auto* beta_rowset = down_cast<BetaRowset*>(rowset.get());
            for (auto& segment : beta_rowset->segments()) {
                for (uint32_t column_index : _column_groups[i]) {
                    const auto* column_reader = segment->column(column_index);
                    if (column_reader == nullptr) {
                        continue;
                    }
                    total_mem_footprint += column_reader->total_mem_footprint();
                }
            }
        }
        int32_t chunk_size = get_read_chunk_size(config::compaction_memory_limit_per_worker, config::vector_chunk_size,
                                                 total_num_rows, total_mem_footprint, segment_iterator_num);
        VLOG(1) << "tablet=" << _tablet->tablet_id() << ", column group=" << i << ", reader chunk size=" << chunk_size;
        reader_params.chunk_size = chunk_size;
        RETURN_IF_ERROR(reader.open(reader_params));

        int64_t output_rows = 0;
        auto chunk = ChunkHelper::new_chunk(schema, reader_params.chunk_size);
        auto char_field_indexes = ChunkHelper::get_char_field_indexes(schema);

        Status status;
        while (!ExecEnv::GetInstance()->storage_engine()->bg_worker_stopped()) {
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

            ChunkHelper::padding_char_columns(char_field_indexes, schema, _tablet->tablet_schema(), chunk.get());

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

        if (ExecEnv::GetInstance()->storage_engine()->bg_worker_stopped()) {
            return Status::InternalError(
                    "Process is going to quit. The compaction should be stopped as soon as possible.");
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
    bool bg_worker_stopped = ExecEnv::GetInstance()->storage_engine()->bg_worker_stopped();
    if (bg_worker_stopped) {
        return Status::InternalError("Process is going to quit. The compaction should be stopped as soon as possible.");
    }

    std::vector<RowsetSharedPtr> output_rowsets;
    output_rowsets.push_back(_output_rowset);

    std::unique_lock wrlock(_tablet->get_header_lock());
    _tablet->modify_rowsets(output_rowsets, _input_rowsets);
    _tablet->save_meta();
    Rowset::close_rowsets(_input_rowsets);

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

} // namespace starrocks::vectorized
