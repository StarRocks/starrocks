// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/vectorized/delta_writer.h"

#include "runtime/current_thread.h"
#include "storage/memtable_flush_executor.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/schema.h"
#include "storage/storage_engine.h"
#include "storage/tablet_updates.h"
#include "storage/update_manager.h"
#include "storage/vectorized/memtable.h"

namespace starrocks::vectorized {

StatusOr<std::unique_ptr<DeltaWriter>> DeltaWriter::open(const DeltaWriterOptions& opt, MemTracker* mem_tracker) {
    std::unique_ptr<DeltaWriter> writer(new DeltaWriter(opt, mem_tracker, StorageEngine::instance()));
    SCOPED_THREAD_LOCAL_MEM_SETTER(mem_tracker, false);
    RETURN_IF_ERROR(writer->_init());
    return std::move(writer);
}

DeltaWriter::DeltaWriter(const DeltaWriterOptions& opt, MemTracker* mem_tracker, StorageEngine* storage_engine)
        : _state(kUninitialized),
          _opt(opt),
          _mem_tracker(mem_tracker),
          _storage_engine(storage_engine),
          _tablet(nullptr),
          _cur_rowset(nullptr),
          _rowset_writer(nullptr),
          _mem_table(nullptr),
          _tablet_schema(nullptr),
          _flush_token(nullptr) {}

DeltaWriter::~DeltaWriter() {
    SCOPED_THREAD_LOCAL_MEM_SETTER(_mem_tracker, false);
    switch (_get_state()) {
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
    _rowset_writer.reset();
    _cur_rowset.reset();
}

void DeltaWriter::_garbage_collection() {
    Status rollback_status = Status::OK();
    if (_tablet != nullptr) {
        TxnManager* txn_mgr = _storage_engine->txn_manager();
        rollback_status = txn_mgr->rollback_txn(_opt.partition_id, _tablet, _opt.txn_id);
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
    TabletManager* tablet_mgr = _storage_engine->tablet_manager();
    _tablet = tablet_mgr->get_tablet(_opt.tablet_id, false);
    if (_tablet == nullptr) {
        _set_state(kAborted);
        std::stringstream ss;
        ss << "Fail to get tablet. tablet_id=" << _opt.tablet_id;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    if (_tablet->updates() != nullptr) {
        auto tracker = _storage_engine->update_manager()->mem_tracker();
        if (tracker->limit_exceeded()) {
            _set_state(kAborted);
            auto msg = Substitute("Update memory limit exceed tablet:$0 $1 > $2", _tablet->tablet_id(),
                                  tracker->consumption(), tracker->limit());
            LOG(WARNING) << msg;
            return Status::MemoryLimitExceeded(msg);
        }
        if (_tablet->updates()->is_error()) {
            _set_state(kAborted);
            LOG(WARNING) << "Fail to init delta writer. tablet in error tablet:" << _tablet->tablet_id();
            return Status::ServiceUnavailable("Tablet in error state");
        }
    }
    if (_tablet->version_count() > config::tablet_max_versions) {
        _set_state(kAborted);
        LOG(WARNING) << "Fail to init delta writer: version limit exceeded. tablet=" << _tablet->tablet_id()
                     << " version count=" << _tablet->version_count() << " limit=" << config::tablet_max_versions;
        return Status::ServiceUnavailable("too many tablet versions");
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
            new_tablet = tablet_mgr->get_tablet(_opt.tablet_id, _opt.schema_hash);
            if (new_tablet == nullptr) {
                _set_state(kAborted);
                std::stringstream ss;
                ss << "Fail to get tablet. tablet_id=" << _opt.tablet_id;
                LOG(WARNING) << ss.str();
                return Status::InternalError(ss.str());
            }
            if (_tablet != new_tablet) {
                _tablet = new_tablet;
                continue;
            }
        }

        std::lock_guard push_lock(_tablet->get_push_lock());
        Status olap_status =
                _storage_engine->txn_manager()->prepare_txn(_opt.partition_id, _tablet, _opt.txn_id, _opt.load_id);
        if (!olap_status.ok()) {
            _set_state(kAborted);
            std::stringstream ss;
            ss << "Fail to prepare transaction. tablet_id=" << _opt.tablet_id << " err=" << olap_status;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        break;
    }

    RowsetWriterContext writer_context(kDataFormatV2, config::storage_format_version);

    std::size_t partial_col_num = _opt.slots->size();
    if (partial_col_num > 0 && _opt.slots->back()->col_name() == "__op") {
        --partial_col_num;
    }
    // maybe partial update, change to partial tablet schema
    if (_tablet->tablet_schema().keys_type() == KeysType::PRIMARY_KEYS &&
        partial_col_num < _tablet->tablet_schema().num_columns()) {
        writer_context.referenced_column_ids.reserve(partial_col_num);
        for (auto i = 0; i < partial_col_num; ++i) {
            const auto& slot_col_name = (*_opt.slots)[i]->col_name();
            int32_t index = _tablet->field_index(slot_col_name);
            if (index < 0) {
                std::stringstream ss;
                ss << "invalid column name: " << slot_col_name;
                LOG(WARNING) << ss.str();
                return Status::InternalError(ss.str());
            }
            writer_context.referenced_column_ids.push_back(index);
        }
        writer_context.partial_update_tablet_schema =
                TabletSchema::create(_tablet->tablet_schema(), writer_context.referenced_column_ids);
        writer_context.tablet_schema = writer_context.partial_update_tablet_schema.get();
    } else {
        writer_context.tablet_schema = &_tablet->tablet_schema();
    }

    writer_context.rowset_id = _storage_engine->next_rowset_id();
    writer_context.tablet_uid = _tablet->tablet_uid();
    writer_context.tablet_id = _opt.tablet_id;
    writer_context.partition_id = _opt.partition_id;
    writer_context.tablet_schema_hash = _opt.schema_hash;
    writer_context.rowset_type = BETA_ROWSET;
    writer_context.rowset_path_prefix = _tablet->schema_hash_path();
    writer_context.rowset_state = PREPARED;
    writer_context.txn_id = _opt.txn_id;
    writer_context.load_id = _opt.load_id;
    writer_context.segments_overlap = OVERLAPPING;
    writer_context.global_dicts = _opt.global_dicts;
    Status st = RowsetFactory::create_rowset_writer(writer_context, &_rowset_writer);
    if (!st.ok()) {
        _set_state(kAborted);
        std::stringstream ss;
        ss << "Fail to create rowset writer. tablet_id=" << _opt.tablet_id << " err=" << st;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    _tablet_schema = writer_context.tablet_schema;
    _reset_mem_table();
    _flush_token = _storage_engine->memtable_flush_executor()->create_flush_token();
    _set_state(kWriting);
    return Status::OK();
}

Status DeltaWriter::write(const Chunk& chunk, const uint32_t* indexes, uint32_t from, uint32_t size) {
    SCOPED_THREAD_LOCAL_MEM_SETTER(_mem_tracker, false);
    Status st;
    auto state = _get_state();
    switch (state) {
    case kUninitialized:
    case kCommitted:
    case kAborted:
    case kClosed:
        return Status::InternalError(fmt::format("cannot write delta writer in {} state", _state_name(state)));
    case kWriting:
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
            _set_state(kAborted);
        }
    }
    return Status::OK();
}

Status DeltaWriter::close() {
    SCOPED_THREAD_LOCAL_MEM_SETTER(_mem_tracker, false);
    Status st;
    auto state = _get_state();
    switch (state) {
    case kUninitialized:
    case kCommitted:
    case kAborted:
        return Status::InternalError(fmt::format("cannot close delta writer in {} state", _state_name(state)));
    case kClosed:
        return Status::OK();
    case kWriting:
        st = _flush_memtable_async();
        _set_state(st.ok() ? kClosed : kAborted);
        return st;
    }
    return Status::OK();
}

Status DeltaWriter::_flush_memtable_async() {
    RETURN_IF_ERROR(_mem_table->finalize());
    return _flush_token->submit(std::move(_mem_table));
}

Status DeltaWriter::_flush_memtable() {
    RETURN_IF_ERROR(_flush_memtable_async());
    return _flush_token->wait();
}

void DeltaWriter::_reset_mem_table() {
    _mem_table = std::make_unique<MemTable>(_tablet->tablet_id(), _tablet_schema, _opt.slots, _rowset_writer.get(),
                                            _mem_tracker);
}

Status DeltaWriter::commit() {
    SCOPED_THREAD_LOCAL_MEM_SETTER(_mem_tracker, false);
    auto state = _get_state();
    switch (state) {
    case kUninitialized:
    case kAborted:
    case kWriting:
        return Status::InternalError(fmt::format("cannot commit delta writer in {} state", _state_name(state)));
    case kCommitted:
        return Status::OK();
    case kClosed:
        break;
    }

    if (auto st = _flush_token->wait(); UNLIKELY(!st.ok())) {
        LOG(WARNING) << st;
        _set_state(kAborted);
        return st;
    }

    if (auto res = _rowset_writer->build(); res.ok()) {
        _cur_rowset = std::move(res).value();
    } else {
        LOG(WARNING) << res.status();
        _set_state(kAborted);
        return res.status();
    }

    _cur_rowset->set_schema(&_tablet->tablet_schema());

    if (_tablet->keys_type() == KeysType::PRIMARY_KEYS) {
        auto st = _storage_engine->update_manager()->on_rowset_finished(_tablet.get(), _cur_rowset.get());
        if (!st.ok()) {
            _set_state(kAborted);
            return st;
        }
    }

    auto res = _storage_engine->txn_manager()->commit_txn(_opt.partition_id, _tablet, _opt.txn_id, _opt.load_id,
                                                          _cur_rowset, false);

    if (res != OLAP_SUCCESS) {
        _storage_engine->update_manager()->on_rowset_cancel(_tablet.get(), _cur_rowset.get());
    }

    if (res != OLAP_SUCCESS && res != OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST) {
        _set_state(kAborted);
        return Status::InternalError("Fail to commit transaction");
    }
    State curr_state = kClosed;
    if (!_state.compare_exchange_strong(curr_state, kCommitted, std::memory_order_acq_rel)) {
        return Status::InternalError("delta writer has been aborted");
    }
    LOG(INFO) << "Closed delta writer. tablet_id=" << _tablet->tablet_id() << " stats=" << _flush_token->get_stats();
    return Status::OK();
}

void DeltaWriter::abort() {
    _set_state(kAborted);
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

} // namespace starrocks::vectorized
