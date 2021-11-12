// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/delta_writer.h"

#include "runtime/current_thread.h"
#include "storage/data_dir.h"
#include "storage/memtable_flush_executor.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/schema.h"
#include "storage/schema_change.h"
#include "storage/storage_engine.h"
#include "storage/tablet_updates.h"
#include "storage/update_manager.h"
#include "storage/vectorized/memtable.h"
#include "util/defer_op.h"

namespace starrocks::vectorized {

Status DeltaWriter::open(WriteRequest* req, MemTracker* mem_tracker, std::shared_ptr<DeltaWriter>* writer) {
    *writer = std::shared_ptr<DeltaWriter>(new DeltaWriter(req, mem_tracker, StorageEngine::instance()));
    return Status::OK();
}

DeltaWriter::DeltaWriter(WriteRequest* req, MemTracker* parent, StorageEngine* storage_engine)
        : _req(*req),
          _tablet(nullptr),
          _cur_rowset(nullptr),
          _rowset_writer(nullptr),
          _tablet_schema(nullptr),
          _delta_written_success(false),
          _storage_engine(storage_engine) {
    _mem_tracker = std::make_unique<MemTracker>(-1, "delta writer", parent, true);
}

DeltaWriter::~DeltaWriter() {
    if (_is_init && !_delta_written_success) {
        _garbage_collection();
    }

    _mem_table.reset();

    if (!_is_init) {
        return;
    }

    if (_flush_token != nullptr) {
        // cancel and wait all memtables in flush queue to be finished
        _flush_token->cancel();
    }
}

void DeltaWriter::_garbage_collection() {
    OLAPStatus rollback_status = OLAP_SUCCESS;
    TxnManager* txn_mgr = _storage_engine->txn_manager();
    if (_tablet != nullptr) {
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
        std::stringstream ss;
        ss << "Fail to get tablet. tablet_id=" << _req.tablet_id;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    if (_tablet->updates() != nullptr) {
        auto tracker = _storage_engine->update_manager()->mem_tracker();
        if (tracker->limit_exceeded()) {
            auto msg = Substitute("Update memory limit exceed tablet:$0 $1 > $2", _tablet->tablet_id(),
                                  tracker->consumption(), tracker->limit());
            LOG(WARNING) << msg;
            return Status::MemoryLimitExceeded(msg);
        }
        if (_tablet->updates()->is_error()) {
            LOG(WARNING) << "Fail to init delta writer. tablet in error tablet:" << _tablet->tablet_id();
            return Status::ServiceUnavailable("Tablet in error state");
        }
    }
    if (_tablet->version_count() > config::tablet_max_versions) {
        LOG(WARNING) << "Fail to init delta writer. tablet=" << _tablet->full_name()
                     << ", version count=" << _tablet->version_count() << ", limit=" << config::tablet_max_versions;
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
    writer_context.rowset_path_prefix = _tablet->tablet_path();
    writer_context.tablet_schema = &(_tablet->tablet_schema());
    writer_context.rowset_state = PREPARED;
    writer_context.txn_id = _req.txn_id;
    writer_context.load_id = _req.load_id;
    writer_context.segments_overlap = OVERLAPPING;
    writer_context.global_dicts = _req.global_dicts;
    Status st = RowsetFactory::create_rowset_writer(writer_context, &_rowset_writer);
    if (!st.ok()) {
        std::stringstream ss;
        ss << "Fail to create rowset writer. tablet_id=" << _req.tablet_id << " err=" << st;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    _tablet_schema = &(_tablet->tablet_schema());
    _reset_mem_table();

    // create flush handler
    OLAPStatus olap_status = _storage_engine->memtable_flush_executor()->create_flush_token(&_flush_token);
    if (olap_status != OLAPStatus::OLAP_SUCCESS) {
        std::stringstream ss;
        ss << "Fail to create flush token. tablet_id=" << _req.tablet_id;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    _is_init = true;
    return Status::OK();
}

Status DeltaWriter::write(Chunk* chunk, const uint32_t* indexes, uint32_t from, uint32_t size) {
    MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(_mem_tracker.get());
    DeferOp op([&] { tls_thread_status.set_mem_tracker(prev_tracker); });

    if (_is_cancelled) {
        return Status::OK();
    }
    if (!_is_init) {
        RETURN_IF_ERROR(_init());
    }

    bool flush = _mem_table->insert(chunk, indexes, from, size);

    if (flush || _mem_table->is_full()) {
        RETURN_IF_ERROR(_flush_memtable_async());
        // create a new memtable for new incoming data
        _reset_mem_table();
    }

    return Status::OK();
}

Status DeltaWriter::_flush_memtable_async() {
    RETURN_IF_ERROR(_mem_table->finalize());
    return _flush_token->submit(_mem_table);
}

Status DeltaWriter::flush_memtable_async() {
    MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(_mem_tracker.get());
    DeferOp op([&] { tls_thread_status.set_mem_tracker(prev_tracker); });

    if (_is_cancelled) {
        return Status::OK();
    }

    if (_flush_token->get_stats().cur_flush_count < 1) {
        // equal means there is no memtable in flush queue, just flush this memtable
        VLOG(3) << "flush memtable to reduce mem consumption. memtable size: " << _mem_table->memory_usage()
                << ", tablet: " << _req.tablet_id << ", load id: " << print_id(_req.load_id);
        RETURN_IF_ERROR(_flush_memtable_async());
        _reset_mem_table();
    } else {
        // this means there should be at least one memtable in flush queue.
    }
    return Status::OK();
}

Status DeltaWriter::wait_memtable_flushed() {
    if (_is_cancelled) {
        return Status::OK();
    }

    // wait all memtables in flush queue to be flushed.
    OLAPStatus st = _flush_token->wait();
    if (st != OLAP_SUCCESS) {
        std::stringstream ss;
        ss << "failed to wait memtable flushed. err: " << st;
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

void DeltaWriter::_reset_mem_table() {
    _mem_table = std::make_shared<MemTable>(_tablet->tablet_id(), _tablet_schema, _req.slots, _rowset_writer.get(),
                                            _mem_tracker.get());
}

Status DeltaWriter::close() {
    MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(_mem_tracker.get());
    DeferOp op([&] { tls_thread_status.set_mem_tracker(prev_tracker); });

    if (_is_cancelled) {
        return Status::OK();
    }
    if (!_is_init) {
        // if this delta writer is not initialized, but close() is called.
        // which means this tablet has no data loaded, but at least one tablet
        // in same partition has data loaded.
        // so we have to also init this DeltaWriter, so that it can create a empty rowset
        // for this tablet when being closd.
        RETURN_IF_ERROR(_init());
    }

    RETURN_IF_ERROR(_flush_memtable_async());
    _mem_table.reset();
    return Status::OK();
}

Status DeltaWriter::close_wait(google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec) {
    MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(_mem_tracker.get());
    DeferOp op([&] { tls_thread_status.set_mem_tracker(prev_tracker); });

    if (_is_cancelled) {
        return Status::OK();
    }
    DCHECK(_is_init);
    DCHECK(_mem_table == nullptr) << "Must call close before close_wait";
    // return error if previous flush failed
    if (_flush_token->wait() != OLAPStatus::OLAP_SUCCESS) {
        return Status::InternalError("Fail to flush memtable");
    }

    // use rowset meta manager to save meta
    _cur_rowset = _rowset_writer->build();
    if (_cur_rowset == nullptr) {
        return Status::InternalError("Fail to build rowset");
    }
    OLAPStatus res = _storage_engine->txn_manager()->commit_txn(_req.partition_id, _tablet, _req.txn_id, _req.load_id,
                                                                _cur_rowset, false);
    if (res != OLAP_SUCCESS && res != OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST) {
        return Status::InternalError("Fail to commit transaction");
    }

#ifndef BE_TEST
    PTabletInfo* tablet_info = tablet_vec->Add();
    tablet_info->set_tablet_id(_tablet->tablet_id());
    tablet_info->set_schema_hash(_tablet->schema_hash());
    const auto& rowset_global_dict_columns_valid_info = _rowset_writer->global_dict_columns_valid_info();
    for (const auto& item : rowset_global_dict_columns_valid_info) {
        if (item.second == true) {
            tablet_info->add_valid_dict_cache_columns(item.first);
        } else {
            tablet_info->add_invalid_dict_cache_columns(item.first);
        }
    }

#endif

    if (_tablet->keys_type() == KeysType::PRIMARY_KEYS) {
        RETURN_IF_ERROR(_storage_engine->update_manager()->on_rowset_finished(_tablet.get(), _cur_rowset.get()));
    }
    _delta_written_success = true;

    const FlushStatistic& stat = _flush_token->get_stats();
    LOG(INFO) << "Closed delta writer. tablet_id=" << _tablet->tablet_id() << " stats=" << stat;
    return Status::OK();
}

Status DeltaWriter::cancel() {
    MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(_mem_tracker.get());
    DeferOp op([&] { tls_thread_status.set_mem_tracker(prev_tracker); });

    if (_is_cancelled) {
        return Status::OK();
    }
    if (!_is_init) {
        return Status::OK();
    }
    _mem_table.reset();
    if (_flush_token != nullptr) {
        // cancel and wait all memtables in flush queue to be finished
        _flush_token->cancel();
    }
    _is_cancelled = true;
    return Status::OK();
}

int64_t DeltaWriter::mem_consumption() const {
    return _mem_tracker->consumption();
}

int64_t DeltaWriter::partition_id() const {
    return _req.partition_id;
}

} // namespace starrocks::vectorized
