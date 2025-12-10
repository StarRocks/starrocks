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

#include "storage/lake/tablet_metadata.h"
#include "storage/lake/types_fwd.h"
#include "storage/primary_key_compaction_conflict_resolver.h"
#include "storage/tablet_manager.h"

namespace starrocks::lake {

class Rowset;
class UpdateManager;
class MetaFileBuilder;
class LakePrimaryIndex;

class LakePrimaryKeyCompactionConflictResolver : public PrimaryKeyCompactionConflictResolver {
public:
    explicit LakePrimaryKeyCompactionConflictResolver(const TabletMetadata* metadata, Rowset* rowset,
                                                      TabletManager* tablet_mgr, MetaFileBuilder* builder,
                                                      LakePrimaryIndex* index, int64_t txn_id, int64_t base_version,
                                                      std::map<uint32_t, size_t>* segment_id_to_add_dels,
                                                      std::vector<std::pair<uint32_t, DelVectorPtr>>* delvecs,
                                                      int32_t subtask_count = 0,
                                                      std::vector<int32_t> success_subtask_ids = {})
            : _metadata(metadata),
              _rowset(rowset),
              _tablet_mgr(tablet_mgr),
              _builder(builder),
              _index(index),
              _txn_id(txn_id),
              _base_version(base_version),
              _segment_id_to_add_dels(segment_id_to_add_dels),
              _delvecs(delvecs),
              _subtask_count(subtask_count),
              _success_subtask_ids(std::move(success_subtask_ids)) {}
    ~LakePrimaryKeyCompactionConflictResolver() {}

    StatusOr<std::string> filename() const override;
    Schema generate_pkey_schema() override;
    Status segment_iterator(
            const std::function<Status(const CompactConflictResolveParams&, const std::vector<ChunkIteratorPtr>&,
                                       const std::function<void(uint32_t, const DelVectorPtr&, uint32_t)>&)>& handler)
            override;

    Status segment_iterator(
            const std::function<Status(const CompactConflictResolveParams&, const std::vector<SegmentPtr>&,
                                       const std::function<void(uint32_t, const DelVectorPtr&, uint32_t)>&)>& handler)
            override;

    int32_t subtask_count() const override { return _subtask_count; }
    const std::vector<int32_t>& success_subtask_ids() const override { return _success_subtask_ids; }
    int64_t tablet_id() const override;
    int64_t txn_id() const override { return _txn_id; }

private:
    // input
    const TabletMetadata* _metadata = nullptr;
    Rowset* _rowset = nullptr;
    TabletManager* _tablet_mgr = nullptr;
    MetaFileBuilder* _builder = nullptr;
    LakePrimaryIndex* _index = nullptr;
    int64_t _txn_id = 0;
    int64_t _base_version = 0;
    // output
    // <segment id -> del num>
    std::map<uint32_t, size_t>* _segment_id_to_add_dels = nullptr;
    // <rssid -> Delvec>
    std::vector<std::pair<uint32_t, starrocks::DelVectorPtr>>* _delvecs = nullptr;
    // For parallel compaction: number of subtasks (0 for single compaction)
    int32_t _subtask_count = 0;
    // For parallel compaction partial success: list of successful subtask IDs
    std::vector<int32_t> _success_subtask_ids;
};

} // namespace starrocks::lake