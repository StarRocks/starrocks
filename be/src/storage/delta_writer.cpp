// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/delta_writer.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/delta_writer.h"

#include <memory>

#include "storage/data_dir.h"
#include "storage/memtable.h"
#include "storage/memtable_flush_executor.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/schema.h"
#include "storage/schema_change.h"
#include "storage/storage_engine.h"

namespace starrocks {

OLAPStatus DeltaWriter::open(WriteRequest* req, MemTracker* mem_tracker, DeltaWriter** writer) {
    *writer = new DeltaWriter(req, mem_tracker, StorageEngine::instance());
    return OLAP_SUCCESS;
}

DeltaWriter::DeltaWriter(WriteRequest* req, MemTracker* parent, StorageEngine* storage_engine)
        : _req(*req),
          _tablet(nullptr),
          _cur_rowset(nullptr),
          _new_rowset(nullptr),
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

OLAPStatus DeltaWriter::init() {
    TabletManager* tablet_mgr = _storage_engine->tablet_manager();
    _tablet = tablet_mgr->get_tablet(_req.tablet_id, _req.schema_hash);
    if (_tablet == nullptr) {
        LOG(WARNING) << "fail to find tablet. tablet_id=" << _req.tablet_id << ", schema_hash=" << _req.schema_hash;
        return OLAP_ERR_TABLE_NOT_FOUND;
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
                LOG(WARNING) << "fail to find tablet. tablet_id=" << _req.tablet_id
                             << ", schema_hash=" << _req.schema_hash;
                return OLAP_ERR_TABLE_NOT_FOUND;
            }
            if (_tablet != new_tablet) {
                _tablet = new_tablet;
                continue;
            }
        }

        std::lock_guard push_lock(_tablet->get_push_lock());
        RETURN_NOT_OK(
                _storage_engine->txn_manager()->prepare_txn(_req.partition_id, _tablet, _req.txn_id, _req.load_id));
        break;
    }

    RowsetWriterContext writer_context(kDataFormatUnknown, config::storage_format_version);
    writer_context.mem_tracker = _mem_tracker.get();
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
    if (Status st = RowsetFactory::create_rowset_writer(writer_context, &_rowset_writer); !st.ok()) {
        return OLAP_ERR_OTHER_ERROR;
    }

    _tablet_schema = &(_tablet->tablet_schema());
    _schema = std::make_unique<Schema>(*_tablet_schema);
    _reset_mem_table();

    // create flush handler
    RETURN_NOT_OK(_storage_engine->memtable_flush_executor()->create_flush_token(&_flush_token));

    _is_init = true;
    return OLAP_SUCCESS;
}

OLAPStatus DeltaWriter::write(Tuple* tuple) {
    if (!_is_init) {
        RETURN_NOT_OK(init());
    }

    _mem_table->insert(tuple);

    // if memtable is full, push it to the flush executor,
    // and create a new memtable for incoming data
    if (_mem_table->memory_usage() >= config::write_buffer_size) {
        RETURN_NOT_OK(_flush_memtable_async());
        // create a new memtable for new incoming data
        _reset_mem_table();
    }
    return OLAP_SUCCESS;
}

OLAPStatus DeltaWriter::_flush_memtable_async() {
    return _flush_token->submit(_mem_table);
}

Status DeltaWriter::flush_memtable_async() {
    if (mem_consumption() == _mem_table->memory_usage()) {
        // equal means there is no memtable in flush queue, just flush this memtable
        VLOG(3) << "flush memtable to reduce mem consumption. memtable size: " << _mem_table->memory_usage()
                << ", tablet: " << _req.tablet_id << ", load id: " << print_id(_req.load_id);
        OLAPStatus st = _flush_memtable_async();
        if (st != OLAP_SUCCESS) {
            std::stringstream ss;
            ss << "failed to flush memtable. err: " << st;
            return Status::InternalError(ss.str());
        }
        _reset_mem_table();
    } else {
        DCHECK(mem_consumption() > _mem_table->memory_usage());
        // this means there should be at least one memtable in flush queue.
    }
    return Status::OK();
}

Status DeltaWriter::wait_memtable_flushed() {
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
    _mem_table =
            std::make_shared<MemTable>(_tablet->tablet_id(), _schema.get(), _tablet_schema, _req.slots, _req.tuple_desc,
                                       _tablet->keys_type(), _rowset_writer.get(), _mem_tracker.get());
}

OLAPStatus DeltaWriter::close() {
    if (!_is_init) {
        // if this delta writer is not initialized, but close() is called.
        // which means this tablet has no data loaded, but at least one tablet
        // in same partition has data loaded.
        // so we have to also init this DeltaWriter, so that it can create a empty rowset
        // for this tablet when being closd.
        RETURN_NOT_OK(init());
    }

    RETURN_NOT_OK(_flush_memtable_async());
    _mem_table.reset();
    return OLAP_SUCCESS;
}

OLAPStatus DeltaWriter::close_wait(google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec) {
    DCHECK(_is_init) << "delta writer is supposed be to initialized before close_wait() being called";
    // return error if previous flush failed
    RETURN_NOT_OK(_flush_token->wait());
    DCHECK_EQ(_mem_tracker->consumption(), 0);

    // use rowset meta manager to save meta
    _cur_rowset = _rowset_writer->build();
    if (_cur_rowset == nullptr) {
        LOG(WARNING) << "fail to build rowset";
        return OLAP_ERR_MALLOC_ERROR;
    }
    OLAPStatus res = _storage_engine->txn_manager()->commit_txn(_req.partition_id, _tablet, _req.txn_id, _req.load_id,
                                                                _cur_rowset, false);
    if (res != OLAP_SUCCESS && res != OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST) {
        LOG(WARNING) << "Failed to commit txn: " << _req.txn_id << " for rowset: " << _cur_rowset->rowset_id();
        return res;
    }

#ifndef BE_TEST
    PTabletInfo* tablet_info = tablet_vec->Add();
    tablet_info->set_tablet_id(_tablet->tablet_id());
    tablet_info->set_schema_hash(_tablet->schema_hash());
#endif

    _delta_written_success = true;

    const FlushStatistic& stat = _flush_token->get_stats();
    LOG(INFO) << "close delta writer for tablet: " << _tablet->tablet_id() << ", stats: " << stat;
    return OLAP_SUCCESS;
}

OLAPStatus DeltaWriter::cancel() {
    if (!_is_init) {
        return OLAP_SUCCESS;
    }
    _mem_table.reset();
    if (_flush_token != nullptr) {
        // cancel and wait all memtables in flush queue to be finished
        _flush_token->cancel();
    }
    DCHECK_EQ(_mem_tracker->consumption(), 0);
    return OLAP_SUCCESS;
}

int64_t DeltaWriter::mem_consumption() const {
    return _mem_tracker->consumption();
}

int64_t DeltaWriter::partition_id() const {
    return _req.partition_id;
}

} // namespace starrocks
