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

#include "runtime/current_thread.h"
#include "storage/reader.h"
#include "storage/vectorized/chunk_helper.h"
#include "storage/vectorized/tablet_reader.h"
#include "util/defer_op.h"

namespace starrocks {

EngineChecksumTask::EngineChecksumTask(MemTracker* mem_tracker, TTabletId tablet_id, TSchemaHash schema_hash,
                                       TVersion version, TVersionHash version_hash, uint32_t* checksum)
        : _tablet_id(tablet_id),
          _schema_hash(schema_hash),
          _version(version),
          _version_hash(version_hash),
          _checksum(checksum) {
    _mem_tracker = std::make_unique<MemTracker>(-1, "checksum instance", mem_tracker);
}

OLAPStatus EngineChecksumTask::execute() {
    MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(_mem_tracker.get());
    DeferOp op([&] { tls_thread_status.set_mem_tracker(prev_tracker); });

    OLAPStatus res = _compute_checksum();
    return res;
} // execute

OLAPStatus EngineChecksumTask::_compute_checksum() {
    LOG(INFO) << "begin to process compute checksum."
              << "tablet_id=" << _tablet_id << ", schema_hash=" << _schema_hash << ", version=" << _version;

    if (_checksum == nullptr) {
        LOG(WARNING) << "invalid output parameter which is null pointer.";
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }

    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(_tablet_id);
    if (nullptr == tablet.get()) {
        LOG(WARNING) << "can't find tablet. tablet_id=" << _tablet_id << " schema_hash=" << _schema_hash;
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    if (tablet->updates() != nullptr) {
        *_checksum = 0;
        LOG(INFO) << "Skipped compute checksum for updatable tablet";
        return OLAP_SUCCESS;
    }

    std::vector<uint32_t> return_columns;
    const TabletSchema& tablet_schema = tablet->tablet_schema();

    size_t num_columns = tablet_schema.num_columns();
    for (size_t i = 0; i < num_columns; ++i) {
        FieldType type = tablet_schema.column(i).type();
        // The approximation of FLOAT/DOUBLE in a certain precision range, the binary of byte is not
        // a fixed value, so these two types are ignored in calculating checksum.
        // And also HLL/OBJCET/PERCENTILE is too large to calculate the checksum.
        if (type == OLAP_FIELD_TYPE_FLOAT || type == OLAP_FIELD_TYPE_DOUBLE || type == OLAP_FIELD_TYPE_HLL ||
            type == OLAP_FIELD_TYPE_OBJECT || type == OLAP_FIELD_TYPE_PERCENTILE) {
            continue;
        }
        return_columns.push_back(i);
    }

    vectorized::Schema schema = vectorized::ChunkHelper::convert_schema_to_format_v2(tablet_schema, return_columns);

    vectorized::TabletReader reader(tablet, Version(0, _version), schema);

    Status st = reader.prepare();
    if (!st.ok()) {
        LOG(WARNING) << "Failed to prepare tablet reader. tablet=" << tablet->full_name()
                     << ", error:" << st.to_string();
        return OLAP_ERR_ROWSET_READER_INIT;
    }

    vectorized::TabletReaderParams reader_params;
    reader_params.reader_type = READER_CHECKSUM;
    reader_params.chunk_size = config::vector_chunk_size;

    st = reader.open(reader_params);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to open tablet reader. tablet=" << tablet->full_name() << ", error:" << st.to_string();
        return OLAP_ERR_ROWSET_READER_INIT;
    }

    uint32_t checksum = 0;
    uint32_t num_rows = 0;
    uint32_t hash_codes[config::vector_chunk_size];
    memset(hash_codes, 0, sizeof(uint32_t) * config::vector_chunk_size);

    auto chunk = vectorized::ChunkHelper::new_chunk(schema, reader_params.chunk_size);
    st = reader.get_next(chunk.get());

    while (st.ok()) {
#ifndef BE_TEST
        Status st = _mem_tracker->check_mem_limit("ConsistencyCheck");
        if (!st.ok()) {
            LOG(WARNING) << "failed to finish compute checksum. " << st.message() << std::endl;
            return OLAP_ERR_OTHER_ERROR;
        }
#endif

        num_rows = chunk->num_rows();
        for (auto& column : chunk->columns()) {
            column->crc32_hash(hash_codes, 0, num_rows);
            for (int i = 0; i < num_rows; ++i) {
                checksum ^= hash_codes[i];
            }
        }
        chunk->reset();
        st = reader.get_next(chunk.get());
    }

    if (!st.is_end_of_file() && !st.ok()) {
        LOG(WARNING) << "Failed to do checksum. tablet=" << tablet->full_name() << ", error:=" << st.to_string();
        return OLAP_ERR_CHECKSUM_ERROR;
    }

    LOG(INFO) << "success to finish compute checksum. checksum=" << checksum;
    *_checksum = checksum;
    return OLAP_SUCCESS;
}

} // namespace starrocks
