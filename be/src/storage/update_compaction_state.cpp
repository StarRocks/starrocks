// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/update_compaction_state.h"

#include "storage/chunk_helper.h"
#include "storage/primary_key_encoder.h"
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

Status CompactionState::load_segments(Rowset* rowset, uint32_t segment_id) {
    if (segment_id >= pk_cols.size() && pk_cols.size() != 0) {
        std::string msg = Substitute("Error segment id: $0 vs $1", segment_id, pk_cols.size());
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }
    if (pk_cols.size() == 0 || pk_cols[segment_id] != nullptr) {
        return Status::OK();
    }
    return _load_segments(rowset, segment_id);
}

static const size_t large_compaction_memory_threshold = 1000000000;

Status CompactionState::_load_segments(Rowset* rowset, uint32_t segment_id) {
    auto& schema = rowset->schema();
    vector<uint32_t> pk_columns;
    for (size_t i = 0; i < schema.num_key_columns(); i++) {
        pk_columns.push_back(static_cast<uint32_t>(i));
    }

    vectorized::Schema pkey_schema = ChunkHelper::convert_schema_to_format_v2(schema, pk_columns);

    std::unique_ptr<vectorized::Column> pk_column;
    if (!PrimaryKeyEncoder::create_column(pkey_schema, &pk_column, true).ok()) {
        CHECK(false) << "create column for primary key encoder failed";
    }

    RowsetReleaseGuard guard(rowset->shared_from_this());
    OlapReaderStatistics stats;
    auto res = rowset->get_segment_iterators2(pkey_schema, nullptr, 0, &stats);
    if (!res.ok()) {
        return res.status();
    }

    auto& itrs = res.value();
    CHECK(itrs.size() == rowset->num_segments()) << "itrs.size != num_segments";

    auto update_manager = StorageEngine::instance()->update_manager();
    auto tracker = update_manager->compaction_state_mem_tracker();

    // only hold pkey, so can use larger chunk size
    auto chunk_shared_ptr = ChunkHelper::new_chunk(pkey_schema, config::vector_chunk_size);
    auto chunk = chunk_shared_ptr.get();

    auto itr = itrs[segment_id].get();
    if (itr == nullptr) {
        return Status::OK();
    }
    auto& dest = pk_cols[segment_id];
    auto col = pk_column->clone();
    if (itr != nullptr) {
        const auto num_rows = rowset->segments()[segment_id]->num_rows();
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
    }
    dest = std::move(col);
    _memory_usage += dest->memory_usage();
    tracker->consume(dest->memory_usage());

    if (tracker->any_limit_exceeded()) {
        // currently we can only log error here, and allow memory over usage
        LOG(ERROR) << " memory limit exceeded when loading compaction state pk tablet_id:"
                   << rowset->rowset_meta()->tablet_id() << " rowset #rows:" << rowset->num_rows()
                   << " size:" << rowset->data_disk_size() << " seg:" << segment_id << "/" << rowset->num_segments()
                   << " #rows:" << rowset->segments()[segment_id]->num_rows() << " memory:" << _memory_usage
                   << " stats:" << update_manager->memory_stats();
    }

    if (_memory_usage > large_compaction_memory_threshold) {
        LOG(INFO) << " loading large compaction state tablet_id:" << rowset->rowset_meta()->tablet_id()
                  << " rowset #rows:" << rowset->num_rows() << " size:" << rowset->data_disk_size()
                  << " seg:" << segment_id << "/" << rowset->num_segments()
                  << " #rows:" << rowset->segments()[segment_id]->num_rows() << " memory:" << _memory_usage
                  << " stats:" << update_manager->memory_stats();
    }
    return Status::OK();
}

void CompactionState::release_segments(Rowset* rowset, uint32_t segment_id) {
    if (segment_id >= pk_cols.size() || pk_cols[segment_id] == nullptr) {
        return;
    }
    auto update_manager = StorageEngine::instance()->update_manager();
    auto tracker = update_manager->compaction_state_mem_tracker();
    _memory_usage -= pk_cols[segment_id]->memory_usage();
    tracker->release(pk_cols[segment_id]->memory_usage());
    pk_cols[segment_id]->reset_column();
}

Status CompactionState::_do_load(Rowset* rowset) {
    if (rowset->num_segments() == 0) {
        return Status::OK();
    }
    pk_cols.resize(rowset->num_segments());
    return _load_segments(rowset, 0);
}

} // namespace starrocks::vectorized
