// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/task/engine_checksum_task.cpp

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

#include "storage/task/engine_checksum_task.h"

#include <memory>

#include "storage/reader.h"
#include "storage/row.h"
#include "util/defer_op.h"

namespace starrocks {

EngineChecksumTask::EngineChecksumTask(TTabletId tablet_id, TSchemaHash schema_hash, TVersion version,
                                       TVersionHash version_hash, uint32_t* checksum)
        : _tablet_id(tablet_id),
          _schema_hash(schema_hash),
          _version(version),
          _version_hash(version_hash),
          _checksum(checksum) {}

OLAPStatus EngineChecksumTask::execute() {
    OLAPStatus res = _compute_checksum();
    return res;
} // execute

OLAPStatus EngineChecksumTask::_compute_checksum() {
    LOG(INFO) << "begin to process compute checksum."
              << "tablet_id=" << _tablet_id << ", schema_hash=" << _schema_hash << ", version=" << _version;
    OLAPStatus res = OLAP_SUCCESS;

    if (_checksum == nullptr) {
        LOG(WARNING) << "invalid output parameter which is null pointer.";
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }

    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(_tablet_id, _schema_hash);
    if (nullptr == tablet.get()) {
        LOG(WARNING) << "can't find tablet. tablet_id=" << _tablet_id << " schema_hash=" << _schema_hash;
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    if (tablet->updates() != nullptr) {
        *_checksum = 0;
        LOG(INFO) << "Skipped compute checksum for updatable tablet";
        return OLAP_SUCCESS;
    }

    Reader reader;
    ReaderParams reader_params;
    reader_params.tablet = tablet;
    reader_params.reader_type = READER_CHECKSUM;
    reader_params.version = Version(0, _version);

    {
        std::shared_lock rdlock(tablet->get_header_lock());
        const RowsetSharedPtr message = tablet->rowset_with_max_version();
        if (message == nullptr) {
            LOG(FATAL) << "fail to get latest version. tablet_id=" << _tablet_id;
            return OLAP_ERR_VERSION_NOT_EXIST;
        }

        if (Status st = tablet->capture_rs_readers(reader_params.version, &reader_params.rs_readers); !st.ok()) {
            LOG(WARNING) << "fail to init reader for tablet " << tablet->full_name() << ": " << st;
            return OLAP_ERR_OTHER_ERROR;
        }
    }

    for (size_t i = 0; i < tablet->tablet_schema().num_columns(); ++i) {
        reader_params.return_columns.push_back(i);
    }

    res = reader.init(reader_params);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "initiate reader fail. res=" << res;
        return res;
    }

    RowCursor row;
    std::unique_ptr<MemTracker> tracker(new MemTracker(-1));

    // release the memory of object pool.
    // The memory of object allocate from ObjectPool is recorded in the mem_tracker.
    // TODO: add mem_tracker for ObjectPool?
    DeferOp release_object_pool_memory([&tracker] { return tracker->release(tracker->consumption()); });
    std::unique_ptr<MemPool> mem_pool(new MemPool(tracker.get()));
    std::unique_ptr<ObjectPool> agg_object_pool(new ObjectPool());
    res = row.init(tablet->tablet_schema(), reader_params.return_columns);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to init row cursor. res=" << res;
        return res;
    }
    row.allocate_memory_for_string_type(tablet->tablet_schema());

    bool eof = false;
    uint32_t row_checksum = 0;
    while (true) {
        OLAPStatus res = reader.next_row_with_aggregation(&row, mem_pool.get(), agg_object_pool.get(), &eof);
        if (res == OLAP_SUCCESS && eof) {
            VLOG(3) << "reader reads to the end.";
            break;
        } else if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to read in reader. res=" << res;
            return res;
        }
        // The value of checksum is independent of the sorting of data rows.
        row_checksum ^= hash_row(row, 0);
        // the memory allocate by mem pool has been copied,
        // so we should release memory immediately
        mem_pool->clear();
        agg_object_pool = std::make_unique<ObjectPool>();
    }

    LOG(INFO) << "success to finish compute checksum. checksum=" << row_checksum;
    *_checksum = row_checksum;
    return OLAP_SUCCESS;
}

} // namespace starrocks
