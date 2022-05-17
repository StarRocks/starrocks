// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/update_compaction_state.h"

#include "storage/chunk_helper.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/storage_engine.h"
#include "storage/update_manager.h"
#include "util/stack_util.h"

namespace starrocks::vectorized {

CompactionState::CompactionState() = default;

CompactionState::~CompactionState() {
    StorageEngine::instance()->update_manager()->compaction_state_mem_tracker()->release(_memory_usage);
    if (!_status.ok()) {
        LOG(WARNING) << "bad CompactionState, status:" << _status;
    }
}

Status CompactionState::load(Rowset* rowset) {
    if (UNLIKELY(!_status.ok())) {
        return _status;
    }
    std::call_once(_load_once_flag, [&] {
        _status = _do_load(rowset);
        if (!_status.ok()) {
            LOG(WARNING) << "load CompactionState error: " << _status
                         << " tablet:" << rowset->rowset_meta()->tablet_id() << " stack:\n"
                         << get_stack_trace();
            if (_status.is_mem_limit_exceeded()) {
                LOG(WARNING) << CurrentThread::mem_tracker()->debug_string();
            }
        }
    });
    return _status;
}

static const size_t large_compaction_memory_threshold = 1000000000;

Status CompactionState::_do_load(Rowset* rowset) {
    auto& schema = rowset->schema();
    vector<uint32_t> pk_columns;
    for (size_t i = 0; i < schema.num_key_columns(); i++) {
        pk_columns.push_back(static_cast<uint32_t>(i));
    }

    vectorized::Schema pkey_schema = ChunkHelper::convert_schema_to_format_v2(schema, pk_columns);

    std::unique_ptr<vectorized::Column> pk_column;
    if (!PrimaryKeyEncoder::create_column(pkey_schema, &pk_column).ok()) {
        CHECK(false) << "create column for primary key encoder failed";
    }

    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(rowset->rowset_path()));
    auto update_manager = StorageEngine::instance()->update_manager();
    auto tracker = update_manager->compaction_state_mem_tracker();
    segment_states.resize(rowset->num_segments());
    for (auto i = 0; i < rowset->num_segments(); i++) {
        std::string rssid_file = BetaRowset::segment_srcrssid_file_path(rowset->rowset_path(), rowset->rowset_id(), i);
        ASSIGN_OR_RETURN(auto read_file, fs->new_random_access_file(rssid_file));
        ASSIGN_OR_RETURN(auto file_size, read_file->get_size());
        std::vector<uint32_t>& src_rssids = segment_states[i].src_rssids;
        src_rssids.resize(file_size / sizeof(uint32_t));
        _memory_usage += file_size;
        tracker->consume(file_size);
        if (tracker->any_limit_exceeded()) {
            // currently we can only log error here, and allow memory over usage
            LOG(ERROR) << " memory limit exceeded when loading compaction state rssid tablet_id:"
                       << rowset->rowset_meta()->tablet_id() << " rowset #rows:" << rowset->num_rows()
                       << " size:" << rowset->data_disk_size() << " seg:" << i << " memory:" << _memory_usage
                       << " stats:" << update_manager->memory_stats();
        }
        RETURN_IF_ERROR(read_file->read_at_fully(0, src_rssids.data(), file_size));
    }

    RowsetReleaseGuard guard(rowset->shared_from_this());
    OlapReaderStatistics stats;
    auto beta_rowset = down_cast<BetaRowset*>(rowset);
    auto res = beta_rowset->get_segment_iterators2(pkey_schema, nullptr, 0, &stats);
    if (!res.ok()) {
        return res.status();
    }

    auto& itrs = res.value();
    CHECK(itrs.size() == rowset->num_segments()) << "itrs.size != num_segments";

    // only hold pkey, so can use larger chunk size
    auto chunk_shared_ptr = ChunkHelper::new_chunk(pkey_schema, config::vector_chunk_size);
    auto chunk = chunk_shared_ptr.get();

    for (size_t i = 0; i < itrs.size(); i++) {
        auto itr = itrs[i].get();
        if (itr == nullptr) {
            continue;
        }
        auto& dest = segment_states[i].pkeys;
        auto col = pk_column->clone();
        auto num_rows = beta_rowset->segments()[i]->num_rows();
        col->reserve(num_rows);
        while (true) {
            chunk->reset();
            auto st = itr->get_next(chunk);
            if (st.is_end_of_file()) {
                break;
            } else if (!st.ok()) {
                return st;
            } else {
                PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, chunk->num_rows(), col.get());
            }
        }
        itr->close();
        CHECK(col->size() == num_rows) << "read segment: iter rows != num rows";
        dest = std::move(col);
        _memory_usage += dest->memory_usage();
        tracker->consume(dest->memory_usage());
        if (tracker->any_limit_exceeded()) {
            // currently we can only log error here, and allow memory over usage
            LOG(ERROR) << " memory limit exceeded when loading compaction state pk tablet_id:"
                       << rowset->rowset_meta()->tablet_id() << " rowset #rows:" << rowset->num_rows()
                       << " size:" << rowset->data_disk_size() << " seg:" << i << " memory:" << _memory_usage
                       << " stats:" << update_manager->memory_stats();
        }
    }

    if (_memory_usage > large_compaction_memory_threshold) {
        LOG(INFO) << " loading large compaction state tablet_id:" << rowset->rowset_meta()->tablet_id()
                  << " rowset #rows:" << rowset->num_rows() << " size:" << rowset->data_disk_size()
                  << " memory:" << _memory_usage << " stats:" << update_manager->memory_stats();
    }
    return Status::OK();
}

} // namespace starrocks::vectorized
