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

#include "storage/primary_key_recover.h"

namespace starrocks {

class LocalPrimaryKeyRecover : public PrimaryKeyRecover {
public:
    LocalPrimaryKeyRecover(Tablet* tablet, UpdateManager* update_mgr) : _tablet(tablet), _update_mgr(update_mgr) {}
    ~LocalPrimaryKeyRecover() {}

    Status pre_cleanup() override;

    // Primary key schema
    starrocks::Schema generate_pkey_schema() override;

    Status rowset_iterator(const starrocks::Schema& pkey_schema, OlapReaderStatistics& stats,
                           const std::function<Status(const std::vector<ChunkIteratorPtr>&,
                                                      const std::vector<std::unique_ptr<RandomAccessFile>>&,
                                                      const std::vector<uint32_t>&, uint32_t)>& handler) override;

    // generate delvec and save
    Status finalize_delvec(const PrimaryIndex::DeletesMap& new_deletes) override;

    int64_t tablet_id() override;

    // Sorrt rowset by rowsetid
    // also consider sorting in data loading and compact concurrency scenarios
    static Status sort_rowsets(std::vector<RowsetSharedPtr>* rowsets);

private:
    Tablet* _tablet;
    UpdateManager* _update_mgr;
    // latest apply version
    EditVersion _latest_applied_version;
    // rocksdb write batch for atomic commit
    rocksdb::WriteBatch _wb;
};

} // namespace starrocks