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

#include "storage/delta_writer.h"

#include <utility>

#include "io/io_profiler.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "storage/compaction_manager.h"
#include "storage/memtable.h"
#include "storage/memtable_flush_executor.h"
#include "storage/memtable_rowset_writer_sink.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/segment_replicate_executor.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_updates.h"
#include "storage/txn_manager.h"
#include "storage/update_manager.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

StatusOr<std::unique_ptr<DeltaWriter>> DeltaWriter::open(const DeltaWriterOptions& opt, MemTracker* mem_tracker) {
    std::unique_ptr<DeltaWriter> writer(new DeltaWriter(opt, mem_tracker, StorageEngine::instance()));
    SCOPED_THREAD_LOCAL_MEM_SETTER(mem_tracker, false);
    RETURN_IF_ERROR(writer->_init());
    return std::move(writer);
}

DeltaWriter::DeltaWriter(DeltaWriterOptions opt, MemTracker* mem_tracker, StorageEngine* storage_engine)
        : _state(kUninitialized),
          _opt(std::move(opt)),
          _mem_tracker(mem_tracker),
          _storage_engine(storage_engine),
          _tablet(nullptr),
          _cur_rowset(nullptr),
          _rowset_writer(nullptr),
          _schema_initialized(false),
          _mem_table(nullptr),
          _mem_table_sink(nullptr),
          _tablet_schema(nullptr),
          _flush_token(nullptr),
          _replicate_token(nullptr),
          _segment_flush_token(nullptr),
          _with_rollback_log(true) {}

DeltaWriter::~DeltaWriter() {
    SCOPED_THREAD_LOCAL_MEM_SETTER(_mem_tracker, false);
    if (_flush_token != nullptr) {
        _flush_token->shutdown();
    }
    if (_replicate_token != nullptr) {
        _replicate_token->shutdown();
    }
    if (_segment_flush_token != nullptr) {
        _segment_flush_token->shutdown();
    }
    switch (get_state()) {
    case kUninitialized:
    case kCommitted:
        break;
    case kWriting:
    case kClosed:
    case kAborted:
        _garbage_collection();
        break;
    }
    _mem_table.reset();
    _mem_table_sink.reset();
    _rowset_writer.reset();
    _cur_rowset.reset();
}

void DeltaWriter::_garbage_collection() {
    Status rollback_status = Status::OK();
    if (_tablet != nullptr) {
        rollback_status = _storage_engine->txn_manager()->rollback_txn(_opt.partition_id, _tablet, _opt.txn_id,
                                                                       _with_rollback_log);
        _tablet->remove_in_writing_data_size(_opt.txn_id);
    }
    // has to check rollback status, because the rowset maybe committed in this thread and
    // published in another thread, then rollback will failed.
    // when rollback failed should not delete rowset
    if (rollback_status.ok()) {
        _storage_engine->add_unused_rowset(_cur_rowset);
    }
}

// return True when referenced_column_ids contains all elements in sort key
static bool contains_all_of(const std::vector<int32_t>& referenced_column_ids,
                            const std::vector<ColumnId>& sort_key_idxes) {
    return std::includes(referenced_column_ids.begin(), referenced_column_ids.end(), sort_key_idxes.begin(),
                         sort_key_idxes.end());
}

// return True when referenced_column_ids contains any element in sort key
static bool contains_any_of(const std::vector<int32_t>& referenced_column_ids,
                            const std::vector<ColumnId>& sort_key_idxes, size_t num_key_columns) {
    std::unordered_set<ColumnId> sort_key_idxes_set(sort_key_idxes.begin(), sort_key_idxes.end());
    return std::any_of(referenced_column_ids.begin(), referenced_column_ids.end(),
                       // Only check non-pk column id
                       [&](int32_t id) { return id >= num_key_columns && sort_key_idxes_set.count(id) > 0; });
}

bool DeltaWriter::is_partial_update_with_sort_key_conflict(const PartialUpdateMode& partial_update_mode,
                                                           const std::vector<int32_t>& referenced_column_ids,
                                                           const std::vector<ColumnId>& sort_key_idxes,
                                                           size_t num_key_columns) {
    // In the current implementation, UNKNOWN_MODE and AUTO_MODE can be considered as ROW_MODE
    if (partial_update_mode == PartialUpdateMode::ROW_MODE || partial_update_mode == PartialUpdateMode::AUTO_MODE ||
        partial_update_mode == PartialUpdateMode::UNKNOWN_MODE ||
        partial_update_mode == PartialUpdateMode::COLUMN_UPSERT_MODE) {
        // Using Row Mode partial update, then the column to be updated must contain the sort key column,
        // because they need sort key to decide their order when segment is generated.
        // Column mode with upsert will insert new rows, which also need sort key to decide their order.
        if (!contains_all_of(referenced_column_ids, sort_key_idxes)) {
            return true;
        }
    }
    if (partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE ||
        partial_update_mode == PartialUpdateMode::COLUMN_UPSERT_MODE) {
        // Using Column Mode with UPDATE command, then the column to be updated cannot contain
        // a sort key column that is not a primary key column. That is because we won't change row's
        // order in original segment, so we can't support partial update columns that contains sort key.
        if (contains_any_of(referenced_column_ids, sort_key_idxes, num_key_columns)) {
            return true;
        }
    }
    return false;
}

Status DeltaWriter::_init() {
    SCOPED_THREAD_LOCAL_MEM_SETTER(_mem_tracker, false);

    _replica_state = _opt.replica_state;

    TabletManager* tablet_mgr = _storage_engine->tablet_manager();
    _tablet = tablet_mgr->get_tablet(_opt.tablet_id, false);
    if (_tablet == nullptr) {
        std::stringstream ss;
        ss << "Fail to get tablet, perhaps this table is doing schema change, or it has already been deleted. Please "
              "try again. tablet_id="
           << _opt.tablet_id;
        LOG(WARNING) << ss.str();
        Status st = Status::InternalError(ss.str());
        _set_state(kUninitialized, st);
        return st;
    }
    if (_tablet->updates() != nullptr) {
        auto tracker = _storage_engine->update_manager()->mem_tracker();
        if (tracker->limit_exceeded()) {
            auto msg = strings::Substitute(
                    "primary key memory usage exceeds the limit. tablet_id: $0, consumption: $1, limit: $2."
                    " Memory stats of top five tablets: $3",
                    _opt.tablet_id, tracker->consumption(), tracker->limit(),
                    _storage_engine->update_manager()->topn_memory_stats(5));
            LOG(WARNING) << msg;
            Status st = Status::MemoryLimitExceeded(msg);
            _set_state(kUninitialized, st);
            return st;
        }
        if (_tablet->updates()->is_error()) {
            auto msg = fmt::format("Tablet is in error state, tablet_id: {} {}", _tablet->tablet_id(),
                                   _tablet->updates()->get_error_msg());
            Status st = Status::ServiceUnavailable(msg);
            _set_state(kUninitialized, st);
            return st;
        }
    }

    if (_tablet->version_count() > config::tablet_max_versions) {
        if (config::enable_event_based_compaction_framework) {
            StorageEngine::instance()->compaction_manager()->update_tablet_async(_tablet);
        }
        auto msg = fmt::format(
                "Failed to load data into tablet {}, because of too many versions, current/limit: {}/{}. You can "
                "reduce the loading job concurrency, or increase loading data batch size. If you are loading data with "
                "Routine Load, you can increase FE configs routine_load_task_consume_second and "
                "max_routine_load_batch_size,",
                _opt.tablet_id, _tablet->version_count(), config::tablet_max_versions);
        LOG(ERROR) << msg;
        Status st = Status::ServiceUnavailable(msg);
        _set_state(kUninitialized, st);
        return st;
    }

    if (_opt.immutable_tablet_size > 0 &&
        _tablet->data_size() + _tablet->in_writing_data_size() > _opt.immutable_tablet_size) {
        _is_immutable.store(true, std::memory_order_relaxed);
    }

    // The tablet may have been migrated during delta writer init,
    // and the latest tablet needs to be obtained when loading.
    // Here, the while loop checks whether the obtained tablet has changed
    // to get the latest tablet.
    while (true) {
        std::shared_lock base_migration_rlock(_tablet->get_migration_lock());
        TabletSharedPtr new_tablet;
        if (!_tablet->is_migrating()) {
            // maybe migration just finish, get the tablet again
            new_tablet = tablet_mgr->get_tablet(_opt.tablet_id);
            if (new_tablet == nullptr) {
                Status st = Status::NotFound(fmt::format("Not found tablet. tablet_id: {}", _opt.tablet_id));
                _set_state(kAborted, st);
                return st;
            }
            if (_tablet != new_tablet) {
                _tablet = new_tablet;
                continue;
            }
        }

        std::lock_guard push_lock(_tablet->get_push_lock());
        auto st = _storage_engine->txn_manager()->prepare_txn(_opt.partition_id, _tablet, _opt.txn_id, _opt.load_id);
        if (!st.ok()) {
            _set_state(kAborted, st);
            return st;
        }
        break;
    }

    // from here, make sure to set state to kAborted if error happens
    RowsetWriterContext writer_context;

    const std::size_t partial_cols_num = [this]() {
        if (_opt.slots->size() > 0 && _opt.slots->back()->col_name() == "__op") {
            return _opt.slots->size() - 1;
        } else {
            return _opt.slots->size();
        }
    }();

    // build tablet schema in request level
    auto tablet_schema_ptr = _tablet->tablet_schema();
    RETURN_IF_ERROR(_build_current_tablet_schema(_opt.index_id, _opt.ptable_schema_param, tablet_schema_ptr));
    size_t real_num_columns = _tablet_schema->num_columns();
    if (_tablet->is_column_with_row_store()) {
        if (_tablet_schema->columns().back().name() != Schema::FULL_ROW_COLUMN) {
            return Status::InternalError("bad column_with_row schema, no __row column");
        }
        real_num_columns -= 1;
    }

    // maybe partial update, change to partial tablet schema
    if (_tablet_schema->keys_type() == KeysType::PRIMARY_KEYS && partial_cols_num < real_num_columns) {
        writer_context.referenced_column_ids.reserve(partial_cols_num);
        for (auto i = 0; i < partial_cols_num; ++i) {
            const auto& slot_col_name = (*_opt.slots)[i]->col_name();
            int32_t index = _tablet_schema->field_index(slot_col_name);
            if (index < 0) {
                auto msg = strings::Substitute("Invalid column name: $0", slot_col_name);
                LOG(WARNING) << msg;
                Status st = Status::InvalidArgument(msg);
                _set_state(kAborted, st);
                return st;
            }
            writer_context.referenced_column_ids.push_back(index);
        }
        if (_opt.partial_update_mode == PartialUpdateMode::ROW_MODE) {
            // no need to control memtable row when using column mode, because we don't need to fill missing column
            int64_t average_row_size = _tablet->updates()->get_average_row_size();
            if (average_row_size != 0) {
                _memtable_buffer_row = config::write_buffer_size / average_row_size;
            } else {
                // If tablet is a new created tablet and has no historical data, average_row_size is 0
                // And we use schema size as average row size. If there are complex type(i.e. BITMAP/ARRAY) or varchar,
                // we will consider it as 16 bytes.
                average_row_size = _tablet_schema->estimate_row_size(16);
                _memtable_buffer_row = config::write_buffer_size / average_row_size;
            }
        }
        auto sort_key_idxes = _tablet_schema->sort_key_idxes();
        std::sort(sort_key_idxes.begin(), sort_key_idxes.end());
        if (is_partial_update_with_sort_key_conflict(_opt.partial_update_mode, writer_context.referenced_column_ids,
                                                     sort_key_idxes, _tablet_schema->num_key_columns())) {
            _partial_schema_with_sort_key_conflict = true;
        }
        if (!_opt.merge_condition.empty()) {
            writer_context.merge_condition = _opt.merge_condition;
        }
        auto partial_update_schema = TabletSchema::create(_tablet_schema, writer_context.referenced_column_ids);
        // In column mode partial update, we need to modify sort key idxes and short key column num in partial
        // tablet schema
        if (_opt.partial_update_mode == PartialUpdateMode::COLUMN_UPSERT_MODE ||
            _opt.partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) {
            std::vector<ColumnId> sort_key_idxes(_tablet_schema->num_key_columns());
            std::iota(sort_key_idxes.begin(), sort_key_idxes.end(), 0);
            partial_update_schema->set_num_short_key_columns(1);
            partial_update_schema->set_sort_key_idxes(sort_key_idxes);
        }

        writer_context.tablet_schema = partial_update_schema;
        writer_context.full_tablet_schema = _tablet_schema;
        writer_context.is_partial_update = true;
        writer_context.partial_update_mode = _opt.partial_update_mode;
        writer_context.column_to_expr_value = _opt.column_to_expr_value;
        _tablet_schema = partial_update_schema;
    } else {
        if (_tablet_schema->keys_type() == KeysType::PRIMARY_KEYS && !_opt.merge_condition.empty()) {
            writer_context.merge_condition = _opt.merge_condition;
        }
        writer_context.tablet_schema = _tablet_schema;
    }

    auto sort_key_idxes = tablet_schema_ptr->sort_key_idxes();
    std::sort(sort_key_idxes.begin(), sort_key_idxes.end());
    bool auto_increment_in_sort_key = false;
    for (auto& idx : sort_key_idxes) {
        auto& col = tablet_schema_ptr->column(idx);
        if (col.is_auto_increment()) {
            auto_increment_in_sort_key = true;
            break;
        }
    }

    if (auto_increment_in_sort_key && _opt.miss_auto_increment_column) {
        LOG(WARNING) << "auto increment column in sort key do not support partial update";
        return Status::NotSupported("auto increment column in sort key do not support partial update");
    }
    writer_context.rowset_id = _storage_engine->next_rowset_id();
    writer_context.tablet_uid = _tablet->tablet_uid();
    writer_context.tablet_id = _opt.tablet_id;
    writer_context.partition_id = _opt.partition_id;
    writer_context.tablet_schema_hash = _opt.schema_hash;
    writer_context.rowset_path_prefix = _tablet->schema_hash_path();
    writer_context.rowset_state = PREPARED;
    writer_context.txn_id = _opt.txn_id;
    writer_context.load_id = _opt.load_id;
    writer_context.segments_overlap = OVERLAPPING;
    writer_context.global_dicts = _opt.global_dicts;
    writer_context.miss_auto_increment_column = _opt.miss_auto_increment_column;
    Status st = RowsetFactory::create_rowset_writer(writer_context, &_rowset_writer);
    if (!st.ok()) {
        auto msg = strings::Substitute("Fail to create rowset writer. tablet_id: $0, error: $1", _opt.tablet_id,
                                       st.to_string());
        LOG(WARNING) << msg;
        st = Status::InternalError(msg);
        _set_state(kAborted, st);
        return st;
    }
    _mem_table_sink = std::make_unique<MemTableRowsetWriterSink>(_rowset_writer.get());
    _flush_token = _storage_engine->memtable_flush_executor()->create_flush_token();
    if (_replica_state == Primary && _opt.replicas.size() > 1) {
        _replicate_token = _storage_engine->segment_replicate_executor()->create_replicate_token(&_opt);
    }
    if (replica_state() == Secondary) {
        _segment_flush_token = StorageEngine::instance()->segment_flush_executor()->create_flush_token();
    }
    _set_state(kWriting, Status::OK());

    VLOG(2) << "DeltaWriter [tablet_id=" << _opt.tablet_id << ", load_id=" << print_id(_opt.load_id)
            << ", replica_state=" << _replica_state_name(_replica_state) << "] open success.";

    // no need after initialization.
    _opt.ptable_schema_param = nullptr;
    return Status::OK();
}

State DeltaWriter::get_state() const {
    std::lock_guard l(_state_lock);
    return _state;
}

Status DeltaWriter::get_err_status() const {
    std::lock_guard l(_state_lock);
    return _err_status;
}

void DeltaWriter::_set_state(State state, const Status& st) {
    std::lock_guard l(_state_lock);
    _state = state;
    if (!st.ok() && _err_status.ok()) {
        _err_status = st;
    }
}

Status DeltaWriter::_check_partial_update_with_sort_key(const Chunk& chunk) {
    if (_tablet->updates() != nullptr && _partial_schema_with_sort_key_conflict) {
        bool ok = true;
        if (_opt.slots != nullptr && _opt.slots->back()->col_name() == "__op") {
            size_t op_column_id = chunk.num_columns() - 1;
            const auto& op_column = chunk.get_column_by_index(op_column_id);
            auto* ops = reinterpret_cast<const uint8_t*>(op_column->raw_data());
            ok = !std::any_of(ops, ops + chunk.num_rows(), [](auto op) { return op == TOpType::UPSERT; });
        } else {
            ok = false;
        }
        if (!ok) {
            string msg;
            if (_opt.partial_update_mode != PartialUpdateMode::COLUMN_UPDATE_MODE) {
                msg = "partial update on table with sort key must provide all sort key columns";
            } else {
                msg = "column mode partial update on table with sort key cannot update sort key column";
            }
            LOG(WARNING) << msg;
            return Status::NotSupported(msg);
        }
    }
    return Status::OK();
}

Status DeltaWriter::write(const Chunk& chunk, const uint32_t* indexes, uint32_t from, uint32_t size) {
    SCOPED_THREAD_LOCAL_MEM_SETTER(_mem_tracker, false);
    RETURN_IF_ERROR(_check_partial_update_with_sort_key(chunk));

    // Delay the creation memtables until we write data.
    // Because for the tablet which doesn't have any written data, we will not use their memtables.
    if (_mem_table == nullptr) {
        // When loading memory usage is larger than hard limit, we will reject new loading task.
        if (!config::enable_new_load_on_memory_limit_exceeded &&
            is_tracker_hit_hard_limit(GlobalEnv::GetInstance()->load_mem_tracker(),
                                      config::load_process_max_memory_hard_limit_ratio)) {
            return Status::MemoryLimitExceeded(
                    "memory limit exceeded, please reduce load frequency or increase config "
                    "`load_process_max_memory_hard_limit_ratio` or add more BE nodes");
        }
        _reset_mem_table();
    }
    auto state = get_state();
    if (state != kWriting) {
        auto err_st = get_err_status();
        // no error just in wrong state
        if (err_st.ok()) {
            err_st = Status::InternalError(
                    fmt::format("Fail to prepare. tablet_id: {}, state: {}", _opt.tablet_id, _state_name(state)));
        }
        return err_st;
    }
    if (_replica_state == Secondary) {
        return Status::InternalError(fmt::format("Fail to write chunk, tablet_id: {}, replica_state: {}",
                                                 _opt.tablet_id, _replica_state_name(_replica_state)));
    }

    if (_tablet->keys_type() == KeysType::PRIMARY_KEYS && !_mem_table->check_supported_column_partial_update(chunk)) {
        return Status::InternalError(
                fmt::format("can't partial update for column with row. tablet_id: {}", _opt.tablet_id));
    }
    Status st;
    ASSIGN_OR_RETURN(auto full, _mem_table->insert(chunk, indexes, from, size));
    _last_write_ts = butil::gettimeofday_s();
    _write_buffer_size = _mem_table->write_buffer_size();
    if (_mem_tracker->limit_exceeded()) {
        VLOG(2) << "Flushing memory table due to memory limit exceeded";
        st = _flush_memtable();
        _reset_mem_table();
    } else if (_mem_tracker->parent() && _mem_tracker->parent()->limit_exceeded()) {
        VLOG(2) << "Flushing memory table due to parent memory limit exceeded";
        st = _flush_memtable();
        _reset_mem_table();
    } else if (full) {
        st = flush_memtable_async();
        _reset_mem_table();
    }
    if (!st.ok()) {
        _set_state(kAborted, st);
    }
    return st;
}

Status DeltaWriter::write_segment(const SegmentPB& segment_pb, butil::IOBuf& data) {
    auto state = get_state();
    if (state != kWriting) {
        auto err_st = get_err_status();
        // no error just in wrong state
        if (err_st.ok()) {
            err_st = Status::InternalError(
                    fmt::format("Fail to write segment. tablet_id: {}, state: {}", _opt.tablet_id, _state_name(state)));
        }
        return err_st;
    }
    if (_replica_state != Secondary) {
        return Status::InternalError(fmt::format("Fail to write segment: {}, tablet_id: {}, replica_state: {}",
                                                 segment_pb.segment_id(), _opt.tablet_id,
                                                 _replica_state_name(_replica_state)));
    }

    _tablet->add_in_writing_data_size(_opt.txn_id, segment_pb.data_size());
    auto scope = IOProfiler::scope(IOProfiler::TAG_LOAD, _tablet->tablet_id());
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        RETURN_IF_ERROR(_rowset_writer->flush_segment(segment_pb, data));
    }
    auto io_stat = scope.current_scoped_tls_io();
    StarRocksMetrics::instance()->segment_flush_total.increment(1);
    StarRocksMetrics::instance()->segment_flush_duration_us.increment(duration_ns / 1000);
    auto io_time_us = (io_stat.write_time_ns + io_stat.sync_time_ns) / 1000;
    StarRocksMetrics::instance()->segment_flush_io_time_us.increment(io_time_us);
    StarRocksMetrics::instance()->segment_flush_bytes_total.increment(segment_pb.data_size());
    VLOG(1) << "Flush segment tablet " << _opt.tablet_id << " segment: " << segment_pb.DebugString()
            << ", duration: " << duration_ns / 1000 << "us, io_time: " << io_time_us << "us";
    return Status::OK();
}

Status DeltaWriter::close() {
    SCOPED_THREAD_LOCAL_MEM_SETTER(_mem_tracker, false);
    auto state = get_state();
    switch (state) {
    case kUninitialized:
    case kAborted: {
        auto err_st = get_err_status();
        if (!err_st.ok()) {
            return err_st;
        }
    }
        // no error just in wrong state
    case kCommitted:
        return Status::InternalError(fmt::format("Fail to close delta writer. tablet_id: {}, state: {}", _opt.tablet_id,
                                                 _state_name(state)));
    case kClosed:
        return Status::OK();
    case kWriting:
        Status st = Status::OK();
        st = flush_memtable_async(true);
        _set_state(st.ok() ? kClosed : kAborted, st);
        return st;
    }
    return Status::OK();
}

Status DeltaWriter::flush_memtable_async(bool eos) {
    _last_write_ts = 0;
    _write_buffer_size = 0;
    // _mem_table is nullptr means write() has not been called
    if (_mem_table != nullptr) {
        RETURN_IF_ERROR(_mem_table->finalize());
    }
    if (_mem_table != nullptr && _opt.miss_auto_increment_column && _replica_state == Primary &&
        _mem_table->get_result_chunk() != nullptr) {
        RETURN_IF_ERROR(_fill_auto_increment_id(*_mem_table->get_result_chunk()));
    }
    if (_replica_state == Primary) {
        // have secondary replica
        if (_replicate_token != nullptr) {
            // Although there maybe no data, but we still need send eos to seconary replica
            if ((_mem_table != nullptr && _mem_table->get_result_chunk() != nullptr) || eos) {
                auto replicate_token = _replicate_token.get();
                return _flush_token->submit(
                        std::move(_mem_table), eos, [replicate_token, this](std::unique_ptr<SegmentPB> seg, bool eos) {
                            if (seg) {
                                _tablet->add_in_writing_data_size(_opt.txn_id, seg->data_size());
                            }
                            if (_opt.immutable_tablet_size > 0 &&
                                _tablet->data_size() + _tablet->in_writing_data_size() > _opt.immutable_tablet_size) {
                                _is_immutable.store(true, std::memory_order_relaxed);
                            }
                            VLOG(1) << "flush memtable, tablet=" << _tablet->tablet_id() << ", txn=" << _opt.txn_id
                                    << " _immutable_tablet_size=" << _opt.immutable_tablet_size
                                    << ", segment_size=" << (seg ? seg->data_size() : 0)
                                    << ", tablet_data_size=" << _tablet->data_size()
                                    << ", in_writing_data_size=" << _tablet->in_writing_data_size()
                                    << ", is_immutable=" << _is_immutable.load(std::memory_order_relaxed);
                            auto st = replicate_token->submit(std::move(seg), eos);
                            if (!st.ok()) {
                                LOG(WARNING) << "Failed to submit sync tablet " << _tablet->tablet_id()
                                             << " segment err=" << st;
                                replicate_token->set_status(st);
                            }
                        });
            }
        } else {
            if (_mem_table != nullptr && _mem_table->get_result_chunk() != nullptr) {
                return _flush_token->submit(
                        std::move(_mem_table), eos, [this](std::unique_ptr<SegmentPB> seg, bool eos) {
                            if (seg) {
                                _tablet->add_in_writing_data_size(_opt.txn_id, seg->data_size());
                            }
                            if (_opt.immutable_tablet_size > 0 &&
                                _tablet->data_size() + _tablet->in_writing_data_size() > _opt.immutable_tablet_size) {
                                _is_immutable.store(true, std::memory_order_relaxed);
                            }
                            VLOG(1) << "flush memtable, tablet=" << _tablet->tablet_id() << ", txn=" << _opt.txn_id
                                    << " _immutable_tablet_size=" << _opt.immutable_tablet_size
                                    << ", segment_size=" << (seg ? seg->data_size() : 0)
                                    << ", tablet_data_size=" << _tablet->data_size()
                                    << ", in_writing_data_size=" << _tablet->in_writing_data_size()
                                    << ", is_immutable=" << _is_immutable.load(std::memory_order_relaxed);
                        });
            }
        }
    } else if (_replica_state == Peer) {
        if (_mem_table != nullptr && _mem_table->get_result_chunk() != nullptr) {
            return _flush_token->submit(std::move(_mem_table), eos, [this](std::unique_ptr<SegmentPB> seg, bool eos) {
                if (seg) {
                    _tablet->add_in_writing_data_size(_opt.txn_id, seg->data_size());
                }
                if (_opt.immutable_tablet_size > 0 &&
                    _tablet->data_size() + _tablet->in_writing_data_size() > _opt.immutable_tablet_size) {
                    _is_immutable.store(true, std::memory_order_relaxed);
                }
                VLOG(1) << "flush memtable, tablet=" << _tablet->tablet_id() << ", txn=" << _opt.txn_id
                        << " immutable_tablet_size=" << _opt.immutable_tablet_size
                        << ", segment_size=" << (seg ? seg->data_size() : 0)
                        << ", tablet_data_size=" << _tablet->data_size()
                        << ", in_writing_data_size=" << _tablet->in_writing_data_size()
                        << ", is_immutable=" << _is_immutable.load(std::memory_order_relaxed);
            });
        }
    }
    return Status::OK();
}

Status DeltaWriter::_flush_memtable() {
    RETURN_IF_ERROR(flush_memtable_async());
    MonotonicStopWatch watch;
    watch.start();
    Status st = _flush_token->wait();
    StarRocksMetrics::instance()->delta_writer_wait_flush_duration_us.increment(watch.elapsed_time() / 1000);
    return st;
}

Status DeltaWriter::_build_current_tablet_schema(int64_t index_id, const POlapTableSchemaParam* ptable_schema_param,
                                                 const TabletSchemaCSPtr& ori_tablet_schema) {
    // new tablet schema if new table
    // find the right index id
    int i = 0;
    if (ptable_schema_param != nullptr) {
        for (; i < ptable_schema_param->indexes_size(); i++) {
            if (ptable_schema_param->indexes(i).id() == index_id) break;
        }
        if (i < ptable_schema_param->indexes_size()) {
            if (ptable_schema_param->indexes_size() > 0 && ptable_schema_param->indexes(i).has_column_param() &&
                ptable_schema_param->indexes(i).column_param().columns_desc_size() != 0 &&
                ptable_schema_param->indexes(i).column_param().columns_desc(0).unique_id() >= 0 &&
                ptable_schema_param->version() != ori_tablet_schema->schema_version()) {
                ASSIGN_OR_RETURN(_tablet_schema,
                                 TabletSchema::create(*ori_tablet_schema, ptable_schema_param->indexes(i).schema_id(),
                                                      ptable_schema_param->version(),
                                                      ptable_schema_param->indexes(i).column_param()));
                if (_tablet_schema->schema_version() > ori_tablet_schema->schema_version()) {
                    _tablet->update_max_version_schema(_tablet_schema);
                }
                return Status::OK();
            }
        }
    }
    _tablet_schema = ori_tablet_schema;

    return Status::OK();
}

void DeltaWriter::_reset_mem_table() {
    if (!_schema_initialized) {
        _vectorized_schema = MemTable::convert_schema(_tablet_schema, _opt.slots);
        _schema_initialized = true;
    }
    if (_tablet_schema->keys_type() == KeysType::PRIMARY_KEYS && !_opt.merge_condition.empty()) {
        _mem_table = std::make_unique<MemTable>(_tablet->tablet_id(), &_vectorized_schema, _opt.slots,
                                                _mem_table_sink.get(), _opt.merge_condition, _mem_tracker);
    } else {
        _mem_table = std::make_unique<MemTable>(_tablet->tablet_id(), &_vectorized_schema, _opt.slots,
                                                _mem_table_sink.get(), "", _mem_tracker);
    }
    _mem_table->set_write_buffer_row(_memtable_buffer_row);
    _write_buffer_size = _mem_table->write_buffer_size();
}

Status DeltaWriter::commit() {
    Span span;
    if (_opt.parent_span) {
        span = Tracer::Instance().add_span("delta_writer_commit", _opt.parent_span);
        span->SetAttribute("txn_id", _opt.txn_id);
        span->SetAttribute("tablet_id", _opt.tablet_id);
    } else {
        span = Tracer::Instance().start_trace_txn_tablet("delta_writer_commit", _opt.txn_id, _opt.tablet_id);
    }
    auto scoped = trace::Scope(span);
    SCOPED_THREAD_LOCAL_MEM_SETTER(_mem_tracker, false);
    auto state = get_state();
    switch (state) {
    case kUninitialized:
    case kAborted: {
        auto err_st = get_err_status();
        if (!err_st.ok()) {
            return err_st;
        }
    }
        // no error just in wrong state
    case kWriting:
        return Status::InternalError(fmt::format("Fail to commit delta writer. tablet_id: {}, state: {}",
                                                 _opt.tablet_id, _state_name(state)));
    case kCommitted:
        return Status::OK();
    case kClosed:
        break;
    }

    MonotonicStopWatch watch;
    watch.start();
    if (auto st = _flush_token->wait(); UNLIKELY(!st.ok())) {
        LOG(WARNING) << st;
        _set_state(kAborted, st);
        return st;
    }
    auto flush_ts = watch.elapsed_time();

    if (auto res = _rowset_writer->build(); res.ok()) {
        _cur_rowset = std::move(res).value();
    } else {
        LOG(WARNING) << "Failed to build rowset. tablet_id: " << _opt.tablet_id << " err: " << res.status();
        _set_state(kAborted, res.status());
        return res.status();
    }

    if (_tablet->keys_type() == KeysType::PRIMARY_KEYS) {
        auto st = _storage_engine->update_manager()->on_rowset_finished(_tablet.get(), _cur_rowset.get());
        if (!st.ok()) {
            _set_state(kAborted, st);
            return st;
        }
    }
    auto pk_finish_ts = watch.elapsed_time();

    if (_replicate_token != nullptr) {
        if (auto st = _replicate_token->wait(); UNLIKELY(!st.ok())) {
            LOG(WARNING) << st;
            _set_state(kAborted, st);
            return st;
        }
    }
    auto replica_ts = watch.elapsed_time();

    auto res = _storage_engine->txn_manager()->commit_txn(_opt.partition_id, _tablet, _opt.txn_id, _opt.load_id,
                                                          _cur_rowset, false);

    if (!res.ok()) {
        _storage_engine->update_manager()->on_rowset_cancel(_tablet.get(), _cur_rowset.get());
    }

    if (!res.ok() && !res.is_already_exist()) {
        _set_state(kAborted, res);
        return res;
    }
    {
        std::lock_guard l(_state_lock);
        if (_state == kClosed) {
            _state = kCommitted;
        } else {
            return Status::InternalError(fmt::format("Delta writer has been aborted. tablet_id: {}, state: {}",
                                                     _opt.tablet_id, _state_name(state)));
        }
    }
    VLOG(1) << "Closed delta writer. tablet_id: " << _tablet->tablet_id() << ", stats: " << _flush_token->get_stats();
    StarRocksMetrics::instance()->delta_writer_wait_flush_duration_us.increment(flush_ts / 1000);
    StarRocksMetrics::instance()->delta_writer_wait_replica_duration_us.increment((replica_ts - pk_finish_ts) / 1000);
    return Status::OK();
}

void DeltaWriter::cancel(const Status& st) {
    _set_state(kAborted, st);
    if (_flush_token != nullptr) {
        _flush_token->cancel(st);
    }
    if (_replicate_token != nullptr) {
        _replicate_token->cancel(st);
    }
    if (_segment_flush_token != nullptr) {
        _segment_flush_token->cancel(st);
    }
}

void DeltaWriter::abort(bool with_log) {
    _set_state(kAborted, Status::Cancelled("aborted by others"));
    _with_rollback_log = with_log;
    if (_flush_token != nullptr) {
        // Wait until all background tasks finished/cancelled.
        // https://github.com/StarRocks/starrocks/issues/8906
        _flush_token->shutdown();
    }
    if (_replicate_token != nullptr) {
        _replicate_token->shutdown();
    }
    if (_segment_flush_token != nullptr) {
        _segment_flush_token->shutdown();
    }

    VLOG(1) << "Aborted delta writer. tablet_id: " << _tablet->tablet_id() << " txn_id: " << _opt.txn_id
            << " load_id: " << print_id(_opt.load_id) << " partition_id: " << partition_id();
}

int64_t DeltaWriter::partition_id() const {
    return _opt.partition_id;
}

const char* DeltaWriter::_state_name(State state) const {
    switch (state) {
    case kUninitialized:
        return "kUninitialized";
    case kAborted:
        return "kAborted";
    case kWriting:
        return "kWriting";
    case kCommitted:
        return "kCommitted";
    case kClosed:
        return "kClosed";
    }
    return "";
}

const char* DeltaWriter::_replica_state_name(ReplicaState state) const {
    switch (state) {
    case Primary:
        return "Primary Replica";
    case Secondary:
        return "Secondary Replica";
    case Peer:
        return "Peer Replica";
    }
    return "";
}

Status DeltaWriter::_fill_auto_increment_id(const Chunk& chunk) {
    // 1. get pk column from chunk
    vector<uint32_t> pk_columns;
    for (size_t i = 0; i < _tablet_schema->num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
    }
    Schema pkey_schema = ChunkHelper::convert_schema(_tablet_schema, pk_columns);
    std::unique_ptr<Column> pk_column;
    if (!PrimaryKeyEncoder::create_column(pkey_schema, &pk_column).ok()) {
        CHECK(false) << "create column for primary key encoder failed";
    }
    auto col = pk_column->clone();

    PrimaryKeyEncoder::encode(pkey_schema, chunk, 0, chunk.num_rows(), col.get());
    std::unique_ptr<Column> upserts = std::move(col);

    std::vector<uint64_t> rss_rowids;
    rss_rowids.resize(upserts->size());

    // 2. probe index
    RETURN_IF_ERROR(_tablet->updates()->get_rss_rowids_by_pk(_tablet.get(), *upserts, nullptr, &rss_rowids));

    Filter filter;
    uint32_t gen_num = 0;
    for (unsigned long v : rss_rowids) {
        uint32_t rssid = v >> 32;
        if (rssid == (uint32_t)-1) {
            filter.emplace_back(1);
            ++gen_num;
        } else {
            filter.emplace_back(0);
        }
    }

    // 3. fill the non-existing rows
    std::vector<int64_t> ids(gen_num);
    int64_t table_id = _tablet->tablet_meta()->table_id();
    RETURN_IF_ERROR(StorageEngine::instance()->get_next_increment_id_interval(table_id, gen_num, ids));

    for (int i = 0; i < _vectorized_schema.num_fields(); i++) {
        const TabletColumn& tablet_column = _tablet_schema->column(i);
        if (tablet_column.is_auto_increment()) {
            auto& column = chunk.get_column_by_index(i);
            RETURN_IF_ERROR((std::dynamic_pointer_cast<Int64Column>(column))->fill_range(ids, filter));
            break;
        }
    }

    return Status::OK();
}

} // namespace starrocks
