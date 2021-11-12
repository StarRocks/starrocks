// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/compaction.h"

#include <utility>

#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/vectorized/chunk_helper.h"
#include "storage/vectorized/tablet_reader.h"
#include "util/defer_op.h"
#include "util/time.h"
#include "util/trace.h"

using std::vector;

namespace starrocks::vectorized {

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
    for (auto& rowset : _input_rowsets) {
        _input_rowsets_size += rowset->data_disk_size();
        _input_row_num += rowset->num_rows();
        segments_num += rowset->num_segments();
    }
    TRACE_COUNTER_INCREMENT("input_rowsets_data_size", _input_rowsets_size);
    TRACE_COUNTER_INCREMENT("input_row_num", _input_row_num);
    TRACE_COUNTER_INCREMENT("input_segments_num", segments_num);

    _output_version = Version(_input_rowsets.front()->start_version(), _input_rowsets.back()->end_version());
    _tablet->compute_version_hash_from_rowsets(_input_rowsets, &_output_version_hash);

    LOG(INFO) << "start " << compaction_name() << ". tablet=" << _tablet->full_name()
              << ", output version is=" << _output_version.first << "-" << _output_version.second;

    RETURN_IF_ERROR(construct_output_rowset_writer());
    TRACE("prepare finished");

    // 2. write combined rows to output rowset
    Statistics stats;
    auto res = merge_rowsets(_mem_tracker->limit(), &stats);

    if (!res.ok()) {
        LOG(WARNING) << "fail to do " << compaction_name() << ". res=" << res.to_string()
                     << ", tablet=" << _tablet->full_name() << ", output_version=" << _output_version.first << "-"
                     << _output_version.second;
        return res;
    }
    TRACE("merge rowsets finished");
    TRACE_COUNTER_INCREMENT("merged_rows", stats.merged_rows);
    TRACE_COUNTER_INCREMENT("filtered_rows", stats.filtered_rows);
    TRACE_COUNTER_INCREMENT("output_rows", stats.output_rows);

    _output_rowset = _output_rs_writer->build();
    if (_output_rowset == nullptr) {
        LOG(WARNING) << "rowset writer build failed. writer version:"
                     << ", output_version=" << _output_version.first << "-" << _output_version.second;
        return Status::MemoryAllocFailed("compaction malloc error.");
    }
    TRACE_COUNTER_INCREMENT("output_rowset_data_size", _output_rowset->data_disk_size());
    TRACE_COUNTER_INCREMENT("output_row_num", _output_rowset->num_rows());
    TRACE_COUNTER_INCREMENT("output_segments_num", _output_rowset->num_segments());
    TRACE("output rowset built");

    // 3. check correctness, commented for this moment.
    RETURN_IF_ERROR(check_correctness(stats));
    TRACE("check correctness finished");

    // 4. modify rowsets in memory
    modify_rowsets();
    TRACE("modify rowsets finished");

    // 5. update last success compaction time
    int64_t now = UnixMillis();
    if (compaction_type() == ReaderType::READER_CUMULATIVE_COMPACTION) {
        _tablet->set_last_cumu_compaction_success_time(now);
    } else {
        _tablet->set_last_base_compaction_success_time(now);
    }

    LOG(INFO) << "succeed to do " << compaction_name() << ". tablet=" << _tablet->full_name()
              << ", output_version=" << _output_version.first << "-" << _output_version.second
              << ", segments=" << segments_num << ". elapsed time=" << watch.get_elapse_second() << "s.";

    // warm-up this rowset
    auto st = _output_rowset->load();
    if (!st.ok()) {
        // only log load failure
        LOG(WARNING) << "ignore load rowset error tablet:" << _tablet->tablet_id()
                     << " rowset:" << _output_rowset->rowset_id() << " " << st;
    }

    return Status::OK();
}

Status Compaction::construct_output_rowset_writer() {
    RowsetWriterContext context(kDataFormatV2, config::storage_format_version);
    context.rowset_id = StorageEngine::instance()->next_rowset_id();
    context.tablet_uid = _tablet->tablet_uid();
    context.tablet_id = _tablet->tablet_id();
    context.partition_id = _tablet->partition_id();
    context.tablet_schema_hash = _tablet->schema_hash();
    context.rowset_type = BETA_ROWSET;
    context.rowset_path_prefix = _tablet->tablet_path();
    context.tablet_schema = &(_tablet->tablet_schema());
    context.rowset_state = VISIBLE;
    context.version = _output_version;
    context.version_hash = _output_version_hash;
    context.segments_overlap = NONOVERLAPPING;
    Status st = RowsetFactory::create_rowset_writer(context, &_output_rs_writer);
    if (!st.ok()) {
        std::stringstream ss;
        ss << "Fail to create rowset writer. tablet_id=" << context.tablet_id << " err=" << st;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

Status Compaction::merge_rowsets(int64_t mem_limit, Statistics* stats_output) {
    TRACE_COUNTER_SCOPE_LATENCY_US("merge_rowsets_latency_us");
    Schema schema = ChunkHelper::convert_schema_to_format_v2(_tablet->tablet_schema());
    TabletReader reader(_tablet, _output_rs_writer->version(), schema);
    TabletReaderParams reader_params;
    reader_params.reader_type = compaction_type();
    reader_params.profile = _runtime_profile.create_child("merge_rowsets");

    int64_t num_rows = 0;
    int64_t total_row_size = 0;
    uint64_t chunk_size = DEFAULT_CHUNK_SIZE;
    if (mem_limit > 0) {
        for (auto& rowset : _input_rowsets) {
            num_rows += rowset->num_rows();
            total_row_size += rowset->total_row_size();
        }
        int64_t avg_row_size = (total_row_size + 1) / (num_rows + 1);
        // The result of thie division operation be zero, so added one
        chunk_size = 1 + mem_limit / (_input_rowsets.size() * avg_row_size + 1);
    }
    if (chunk_size > config::vector_chunk_size) {
        chunk_size = config::vector_chunk_size;
    }
    reader_params.chunk_size = chunk_size;
    RETURN_IF_ERROR(reader.prepare());
    RETURN_IF_ERROR(reader.open(reader_params));

    int64_t output_rows = 0;

    auto chunk = ChunkHelper::new_chunk(schema, reader_params.chunk_size);

    auto char_field_indexes = ChunkHelper::get_char_field_indexes(schema);

    while (true) {
#ifndef BE_TEST
        Status st = tls_thread_status.mem_tracker()->check_mem_limit("Compaction");
        if (!st.ok()) {
            LOG(WARNING) << "fail to execute compaction: " << st.message() << std::endl;
            return Status::MemoryLimitExceeded(st.message());
        }
#endif

        chunk->reset();
        Status status = reader.get_next(chunk.get());
        if (!status.ok()) {
            if (status.is_end_of_file()) {
                break;
            } else {
                return Status::InternalError("reader get_next error.");
            }
        }

        ChunkHelper::padding_char_columns(char_field_indexes, schema, _tablet->tablet_schema(), chunk.get());

        OLAPStatus olap_status = _output_rs_writer->add_chunk(*chunk);
        if (olap_status != OLAP_SUCCESS) {
            LOG(WARNING) << "writer add_chunk error, err=" << olap_status;
            return Status::InternalError("writer add_chunk error.");
        }
        output_rows += chunk->num_rows();
    }

    if (stats_output != nullptr) {
        stats_output->output_rows = output_rows;
        stats_output->merged_rows = reader.merged_rows();
        stats_output->filtered_rows = reader.stats().rows_del_filtered;
    }

    OLAPStatus olap_status = _output_rs_writer->flush();
    if (olap_status != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to flush rowset when merging rowsets of tablet " + _tablet->full_name()
                     << ", err=" << olap_status;
        return Status::InternalError("failed to flush rowset when merging rowsets of tablet error.");
    }

    return Status::OK();
}

void Compaction::modify_rowsets() {
    std::vector<RowsetSharedPtr> output_rowsets;
    output_rowsets.push_back(_output_rowset);

    std::unique_lock wrlock(_tablet->get_header_lock());
    _tablet->modify_rowsets(output_rowsets, _input_rowsets);
    _tablet->save_meta();
}

Status Compaction::check_version_continuity(const std::vector<RowsetSharedPtr>& rowsets) {
    RowsetSharedPtr prev_rowset = rowsets.front();
    for (size_t i = 1; i < rowsets.size(); ++i) {
        const RowsetSharedPtr& rowset = rowsets[i];
        if (rowset->start_version() != prev_rowset->end_version() + 1) {
            LOG(WARNING) << "There are missed versions among rowsets. "
                         << "prev_rowset verison=" << prev_rowset->start_version() << "-" << prev_rowset->end_version()
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
                     << ", filted_row_num=" << stats.filtered_rows << ", output_row_num=" << _output_rowset->num_rows();

        return Status::InternalError("cumulative compaction check lines error.");
    }
    return Status::OK();
}

} // namespace starrocks::vectorized
