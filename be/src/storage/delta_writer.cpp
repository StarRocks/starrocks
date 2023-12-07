// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/delta_writer.h"

#include <utility>

#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "storage/compaction_manager.h"
#include "storage/memtable.h"
#include "storage/memtable_flush_executor.h"
#include "storage/memtable_rowset_writer_sink.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/segment_replicate_executor.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_updates.h"
#include "storage/txn_manager.h"
#include "storage/update_manager.h"

namespace starrocks::vectorized {

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
          _with_rollback_log(true) {}

DeltaWriter::~DeltaWriter() {
    SCOPED_THREAD_LOCAL_MEM_SETTER(_mem_tracker, false);
    if (_flush_token != nullptr) {
        _flush_token->shutdown();
    }
    if (_replicate_token != nullptr) {
        _replicate_token->shutdown();
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
    }
    // has to check rollback status, because the rowset maybe committed in this thread and
    // published in another thread, then rollback will failed.
    // when rollback failed should not delete rowset
    if (rollback_status.ok()) {
        _storage_engine->add_unused_rowset(_cur_rowset);
    }
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
                    "Primary-key index exceeds the limit. tablet_id: $0, consumption: $1, limit: $2."
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
    // maybe partial update, change to partial tablet schema
    if (_tablet->tablet_schema().keys_type() == KeysType::PRIMARY_KEYS &&
        partial_cols_num < _tablet->tablet_schema().num_columns()) {
        writer_context.referenced_column_ids.reserve(partial_cols_num);
        for (auto i = 0; i < partial_cols_num; ++i) {
            const auto& slot_col_name = (*_opt.slots)[i]->col_name();
            int32_t index = _tablet->field_index(slot_col_name);
            if (index < 0) {
                auto msg = strings::Substitute("Invalid column name: $0", slot_col_name);
                LOG(WARNING) << msg;
                Status st = Status::InvalidArgument(msg);
                _set_state(kAborted, st);
                return st;
            }
            writer_context.referenced_column_ids.push_back(index);
        }
        int64_t average_row_size = _tablet->updates()->get_average_row_size();
        if (average_row_size != 0) {
            _memtable_buffer_row = config::write_buffer_size / average_row_size;
        } else {
            // If tablet is a new created tablet and has no historical data, average_row_size is 0
            // And we use schema size as average row size. If there are complex type(i.e. BITMAP/ARRAY) or varchar,
            // we will consider it as 16 bytes.
            average_row_size = _tablet->tablet_schema().estimate_row_size(16);
            _memtable_buffer_row = config::write_buffer_size / average_row_size;
        }

        writer_context.partial_update_tablet_schema =
                TabletSchema::create(_tablet->tablet_schema(), writer_context.referenced_column_ids);
        auto sort_key_idxes = _tablet->tablet_schema().sort_key_idxes();
        std::sort(sort_key_idxes.begin(), sort_key_idxes.end());
        if (!std::includes(writer_context.referenced_column_ids.begin(), writer_context.referenced_column_ids.end(),
                           sort_key_idxes.begin(), sort_key_idxes.end())) {
            _partial_schema_with_sort_key = true;
        }
        writer_context.tablet_schema = writer_context.partial_update_tablet_schema.get();
    } else {
        writer_context.tablet_schema = &_tablet->tablet_schema();
        if (_tablet->tablet_schema().keys_type() == KeysType::PRIMARY_KEYS && !_opt.merge_condition.empty()) {
            writer_context.merge_condition = _opt.merge_condition;
        }
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
    _tablet_schema = writer_context.tablet_schema;
    _flush_token = _storage_engine->memtable_flush_executor()->create_flush_token();
    if (_replica_state == Primary && _opt.replicas.size() > 1) {
        _replicate_token = _storage_engine->segment_replicate_executor()->create_replicate_token(&_opt);
    }
    _set_state(kWriting, Status::OK());

    VLOG(2) << "DeltaWriter [tablet_id=" << _opt.tablet_id << ", load_id=" << print_id(_opt.load_id)
            << ", replica_state=" << _replica_state_name(_replica_state) << "] open success.";

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
    if (_tablet->updates() != nullptr && _partial_schema_with_sort_key && _opt.slots != nullptr &&
        _opt.slots->back()->col_name() == "__op") {
        size_t op_column_id = chunk.num_columns() - 1;
        auto op_column = chunk.get_column_by_index(op_column_id);
        auto* ops = reinterpret_cast<const uint8_t*>(op_column->raw_data());
        for (size_t i = 0; i < chunk.num_rows(); i++) {
            if (ops[i] == TOpType::UPSERT) {
                LOG(WARNING) << "table with sort key do not support partial update";
                return Status::NotSupported("table with sort key do not support partial update");
            }
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
    Status st;
    bool full = _mem_table->insert(chunk, indexes, from, size);
    if (_mem_tracker->limit_exceeded()) {
        VLOG(2) << "Flushing memory table due to memory limit exceeded";
        st = _flush_memtable();
        _reset_mem_table();
    } else if (_mem_tracker->parent() && _mem_tracker->parent()->limit_exceeded()) {
        VLOG(2) << "Flushing memory table due to parent memory limit exceeded";
        st = _flush_memtable();
        _reset_mem_table();
    } else if (full) {
        st = _flush_memtable_async();
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
    VLOG(1) << "Flush segment tablet " << _opt.tablet_id << " segment " << segment_pb.DebugString();

    return _rowset_writer->flush_segment(segment_pb, data);
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
        st = _flush_memtable_async(true);
        _set_state(st.ok() ? kClosed : kAborted, st);
        return st;
    }
    return Status::OK();
}

Status DeltaWriter::_flush_memtable_async(bool eos) {
    // _mem_table is nullptr means write() has not been called
    if (_mem_table != nullptr) {
        RETURN_IF_ERROR(_mem_table->finalize());
    }
    if (_replica_state == Primary) {
        // have secondary replica
        if (_replicate_token != nullptr) {
            // Although there maybe no data, but we still need send eos to seconary replica
            auto replicate_token = _replicate_token.get();
            return _flush_token->submit(std::move(_mem_table), eos,
                                        [replicate_token](std::unique_ptr<SegmentPB> seg, bool eos) {
                                            auto st = replicate_token->submit(std::move(seg), eos);
                                            if (!st.ok()) {
                                                LOG(WARNING) << "Failed to submit sync segment err=" << st;
                                                replicate_token->set_status(st);
                                            }
                                        });
        } else {
            if (_mem_table != nullptr) {
                return _flush_token->submit(std::move(_mem_table), eos, nullptr);
            }
        }
    } else if (_replica_state == Peer) {
        if (_mem_table != nullptr) {
            return _flush_token->submit(std::move(_mem_table), eos, nullptr);
        }
    }
    return Status::OK();
}

Status DeltaWriter::_flush_memtable() {
    RETURN_IF_ERROR(_flush_memtable_async());
    return _flush_token->wait();
}

void DeltaWriter::_reset_mem_table() {
    if (!_schema_initialized) {
        _vectorized_schema = MemTable::convert_schema(_tablet_schema, _opt.slots);
        _schema_initialized = true;
    }
    if (_tablet->tablet_schema().keys_type() == KeysType::PRIMARY_KEYS && !_opt.merge_condition.empty()) {
        _mem_table = std::make_unique<MemTable>(_tablet->tablet_id(), &_vectorized_schema, _opt.slots,
                                                _mem_table_sink.get(), _opt.merge_condition, _mem_tracker);
    } else {
        _mem_table = std::make_unique<MemTable>(_tablet->tablet_id(), &_vectorized_schema, _opt.slots,
                                                _mem_table_sink.get(), "", _mem_tracker);
    }
    _mem_table->set_write_buffer_row(_memtable_buffer_row);
    _mem_table->set_partial_schema_with_sort_key(_partial_schema_with_sort_key);
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

    if (auto st = _flush_token->wait(); UNLIKELY(!st.ok())) {
        LOG(WARNING) << st;
        _set_state(kAborted, st);
        return st;
    }

    if (auto res = _rowset_writer->build(); res.ok()) {
        _cur_rowset = std::move(res).value();
    } else {
        LOG(WARNING) << "Failed to build rowset. tablet_id: " << _opt.tablet_id << " err: " << res.status();
        _set_state(kAborted, res.status());
        return res.status();
    }

    _cur_rowset->set_schema(&_tablet->tablet_schema());
    if (_tablet->keys_type() == KeysType::PRIMARY_KEYS) {
        auto st = _storage_engine->update_manager()->on_rowset_finished(_tablet.get(), _cur_rowset.get());
        if (!st.ok()) {
            _set_state(kAborted, st);
            return st;
        }
    }

    if (_replicate_token != nullptr) {
        if (auto st = _replicate_token->wait(); UNLIKELY(!st.ok())) {
            LOG(WARNING) << st;
            _set_state(kAborted, st);
            return st;
        }
    }

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

    VLOG(1) << "Aborted delta writer. tablet_id: " << _tablet->tablet_id() << " txn_id: " << _opt.txn_id
            << " load_id: " << print_id(_opt.load_id);
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

} // namespace starrocks::vectorized
