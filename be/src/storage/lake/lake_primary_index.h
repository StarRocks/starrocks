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

#pragma once

#include <string>
#include <unordered_map>

#include "storage/lake/tablet_metadata.h"
#include "storage/lake/types_fwd.h"
#include "storage/primary_index.h"

namespace starrocks {

namespace lake {

class Tablet;
class MetaFileBuilder;
class TabletManager;

class LakePrimaryIndex : public PrimaryIndex {
public:
    LakePrimaryIndex() : PrimaryIndex() {}
    LakePrimaryIndex(const Schema& pk_schema) : PrimaryIndex(pk_schema) {}
    ~LakePrimaryIndex() override;

    // Fetch all primary keys from the tablet associated with this index into memory
    // to build a hash index.
    //
    // [thread-safe]
    Status lake_load(TabletManager* tablet_mgr, const TabletMetadataPtr& metadata, int64_t base_version,
                     const MetaFileBuilder* builder);

    bool is_load(int64_t base_version);

    int64_t data_version() const { return _data_version; }
    void update_data_version(int64_t version) { _data_version = version; }

    std::unique_ptr<std::lock_guard<std::shared_timed_mutex>> fetch_guard() {
        return std::make_unique<std::lock_guard<std::shared_timed_mutex>>(_mutex);
    }

    std::unique_ptr<std::lock_guard<std::shared_timed_mutex>> try_fetch_guard() {
        if (_mutex.try_lock()) {
            return std::make_unique<std::lock_guard<std::shared_timed_mutex>>(_mutex, std::adopt_lock);
        }
        return nullptr;
    }

    std::shared_timed_mutex* get_index_lock() { return &_mutex; }

    void set_enable_persistent_index(bool enable_persistent_index) {
        _enable_persistent_index = enable_persistent_index;
    }

    Status apply_opcompaction(const TabletMetadata& metadata, const TxnLogPB_OpCompaction& op_compaction);

    Status commit(const TabletMetadataPtr& metadata, MetaFileBuilder* builder);

    double get_local_pk_index_write_amp_score();

    void set_local_pk_index_write_amp_score(double score);

    // This function is used for handling delete operation in cloud native PK table.
    // It is different from another pk index implementation (such as in-memory index or local persistent index),
    // because it need `rowset_id` to setup the rebuild point.
    //
    // |metadata| Used to decide the index type.
    //
    // |key_col| contains the *encoded* primary keys to be deleted from this index.
    // The position of deleted keys will be appended into |new_deletes|.
    //
    // |rowset_id| The rowset that keys belong to. Used for setup rebuild point (cloud native index only).
    Status erase(const TabletMetadataPtr& metadata, const Column& pks, DeletesMap* deletes, uint32_t rowset_id);

private:
    Status _do_lake_load(TabletManager* tablet_mgr, const TabletMetadataPtr& metadata, int64_t base_version,
                         const MetaFileBuilder* builder);

private:
    // We don't support multi version in PrimaryIndex yet, but we will record latest data version for some checking
    int64_t _data_version = 0;
    // make sure at most 1 thread is read or write primary index
    std::shared_timed_mutex _mutex;
    bool _enable_persistent_index = false;
};

} // namespace lake
} // namespace starrocks
