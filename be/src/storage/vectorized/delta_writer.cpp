// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

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

StatusOr<std::unique_ptr<DeltaWriter>> DeltaWriter::open(DeltaWriterOptions* req, MemTracker* mem_tracker) {
    std::unique_ptr<DeltaWriter> writer(new DeltaWriter(req, mem_tracker, StorageEngine::instance()));
    RETURN_IF_ERROR(writer->_init());
    return std::move(writer);
}

DeltaWriter::DeltaWriter(DeltaWriterOptions* req, MemTracker* mem_tracker, StorageEngine* storage_engine)
        : _state(kUninitialized),
          _req(*req),
          _mem_tracker(mem_tracker),
          _storage_engine(storage_engine),
          _tablet(nullptr),
          _cur_rowset(nullptr),
          _rowset_writer(nullptr),
          _mem_table(nullptr),
          _tablet_schema(nullptr),
          _flush_token(nullptr) {}

DeltaWriter::~DeltaWriter() {
    switch (_get_state()) {
    case kCommitted:
        break;
    case kUninitialized:
    case kWriting:
    case kAborted:
        _garbage_collection();
        break;
    }
}

void DeltaWriter::_garbage_collection() {
    OLAPStatus rollback_status = OLAP_SUCCESS;
    if (_tablet != nullptr) {
        TxnManager* txn_mgr = _storage_engine->txn_manager();
        rollback_status = txn_mgr->rollback_txn(_req.partition_id, _tablet, _req.txn_id);
    }
    // has to check rollback status, because the rowset maybe committed in this thread and
    // published in another thread, then rollback will failed.
    // when rollback failed should not delete rowset
    if (rollback_status == OLAP_SUCCESS) {
        _storage_engine->add_unused_rowset(_cur_rowset);
    }
}

Status DeltaWriter::_init() {
    TabletManager* tablet_mgr = _storage_engine->tablet_manager();
    _tablet = tablet_mgr->get_tablet(_req.tablet_id, false);
    if (_tablet == nullptr) {
        _set_state(kAborted);
        std::stringstream ss;
        ss << "Fail to get tablet. tablet_id=" << _req.tablet_id;
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
            new_tablet = tablet_mgr->get_tablet(_req.tablet_id, _req.schema_hash);
            if (new_tablet == nullptr) {
                _set_state(kAborted);
                std::stringstream ss;
                ss << "Fail to get tablet. tablet_id=" << _req.tablet_id;
                LOG(WARNING) << ss.str();
                return Status::InternalError(ss.str());
            }
            if (_tablet != new_tablet) {
                _tablet = new_tablet;
                continue;
            }
        }

        std::lock_guard push_lock(_tablet->get_push_lock());
        OLAPStatus olap_status =
                _storage_engine->txn_manager()->prepare_txn(_req.partition_id, _tablet, _req.txn_id, _req.load_id);
        if (olap_status != OLAPStatus::OLAP_SUCCESS) {
            _set_state(kAborted);
            std::stringstream ss;
            ss << "Fail to prepare transaction. tablet_id=" << _req.tablet_id << " err=" << olap_status;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        break;
    }

    RowsetWriterContext writer_context(kDataFormatV2, config::storage_format_version);
    writer_context.rowset_id = _storage_engine->next_rowset_id();
    writer_context.tablet_uid = _tablet->tablet_uid();
    writer_context.tablet_id = _req.tablet_id;
    writer_context.partition_id = _req.partition_id;
    writer_context.tablet_schema_hash = _req.schema_hash;
    writer_context.rowset_type = BETA_ROWSET;
    writer_context.rowset_path_prefix = _tablet->schema_hash_path();
    writer_context.tablet_schema = &(_tablet->tablet_schema());
    writer_context.rowset_state = PREPARED;
    writer_context.txn_id = _req.txn_id;
    writer_context.load_id = _req.load_id;
    writer_context.segments_overlap = OVERLAPPING;
    writer_context.global_dicts = _req.global_dicts;
    Status st = RowsetFactory::create_rowset_writer(writer_context, &_rowset_writer);
    if (!st.ok()) {
        _set_state(kAborted);
        std::stringstream ss;
        ss << "Fail to create rowset writer. tablet_id=" << _req.tablet_id << " err=" << st;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    _tablet_schema = &(_tablet->tablet_schema());
    _reset_mem_table();
    _flush_token = _storage_engine->memtable_flush_executor()->create_flush_token();
    _set_state(kWriting);
    return Status::OK();
}

Status DeltaWriter::write(const Chunk& chunk, const uint32_t* indexes, uint32_t from, uint32_t size) {
    Status st;
    switch (_get_state()) {
    case kUninitialized:
        return Status::InternalError("cannot write delta writer in kUninitialized state");
    case kCommitted:
        return Status::InternalError("cannot write delta writer in kCommitted state");
    case kAborted:
        return Status::InternalError("cannot write delta writer in kAborted state");
    case kWriting:
        DCHECK(_mem_table != nullptr) << _tablet->tablet_id();
        bool full = _mem_table->insert(chunk, indexes, from, size);
        if (full) {
            st = _flush_memtable_async();
            _reset_mem_table();
        } else if (_mem_tracker->limit_exceeded()) {
            VLOG(2) << "Flushing memory table due to memory limit exceeded";
            st = _flush_memtable_sync();
            _reset_mem_table();
        } else if (_mem_tracker->parent() && _mem_tracker->parent()->limit_exceeded()) {
            VLOG(2) << "Flushing memory table due to parent memory limit exceeded";
            st = _flush_memtable_sync();
            _reset_mem_table();
        } else {
            // nothing to do
        }
        if (!st.ok()) {
            _set_state(kAborted);
        }
    }
    return Status::OK();
}

Status DeltaWriter::_flush_memtable_async() {
    auto st = _mem_table->finalize();
    if (!st.ok()) {
        LOG(WARNING) << "Closed delta writer due to memory table finalize failed: " << st;
        return st;
    }
    st = _flush_token->submit(std::move(_mem_table));
    if (!st.ok()) {
        LOG(WARNING) << "Closed delta writer due to submit memory table to thread pool failed: " << st;
        return st;
    }
    return Status::OK();
}

Status DeltaWriter::_flush_memtable_sync() {
    auto st = _mem_table->finalize();
    if (!st.ok()) {
        LOG(WARNING) << "Closed delta writer due to memory table finalize failed: " << st;
        return st;
    }
    st = _flush_token->submit(std::move(_mem_table));
    if (!st.ok()) {
        LOG(WARNING) << "Closed delta writer due to submit memory table to thread pool failed: " << st;
        return st;
    }
    if (_flush_token->wait() != OLAPStatus::OLAP_SUCCESS) {
        return Status::InternalError("fail to flush memtable");
    }
    return Status::OK();
}

void DeltaWriter::_reset_mem_table() {
    _mem_table = std::make_unique<MemTable>(_tablet->tablet_id(), _tablet_schema, _req.slots, _rowset_writer.get(),
                                            _mem_tracker);
}

Status DeltaWriter::commit() {
    switch (_get_state()) {
    case kUninitialized:
        return Status::InternalError("cannot commit delta writer in kUninitialized state");
    case kAborted:
        return Status::InternalError("cannot commit delta writer in kAborted state");
    case kCommitted:
        return Status::OK();
    case kWriting:
        break;
    }

    auto st = _flush_memtable_async();
    if (!st.ok()) {
        _set_state(kAborted);
        return st;
    }
    if (_flush_token->wait() != OLAPStatus::OLAP_SUCCESS) {
        _set_state(kAborted);
        return Status::InternalError("Fail to flush memtable");
    }
    _cur_rowset = _rowset_writer->build();
    if (_cur_rowset == nullptr) {
        _set_state(kAborted);
        return Status::InternalError("Fail to build rowset");
    }
    OLAPStatus res = _storage_engine->txn_manager()->commit_txn(_req.partition_id, _tablet, _req.txn_id, _req.load_id,
                                                                _cur_rowset, false);
    if (res != OLAP_SUCCESS && res != OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST) {
        _set_state(kAborted);
        return Status::InternalError("Fail to commit transaction");
    }

    if (_tablet->keys_type() == KeysType::PRIMARY_KEYS) {
        st = _storage_engine->update_manager()->on_rowset_finished(_tablet.get(), _cur_rowset.get());
        if (!st.ok()) {
            _set_state(kAborted);
            return st;
        }
    }
    State curr_state = kWriting;
    if (!_state.compare_exchange_strong(curr_state, kCommitted, std::memory_order_acq_rel)) {
        return Status::InternalError("delta writer has been cancelled");
    }
    return Status::OK();
}

void DeltaWriter::abort() {
    _set_state(kAborted);
}

int64_t DeltaWriter::partition_id() const {
    return _req.partition_id;
}

} // namespace starrocks::vectorized
