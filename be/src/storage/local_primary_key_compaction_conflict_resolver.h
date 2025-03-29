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

#include "storage/primary_key_compaction_conflict_resolver.h"

namespace starrocks {

class Tablet;
class Rowset;
class KVStore;
class PrimaryIndex;

class LocalPrimaryKeyCompactionConflictResolver : public PrimaryKeyCompactionConflictResolver {
public:
    explicit LocalPrimaryKeyCompactionConflictResolver(Tablet* tablet, Rowset* rowset, PrimaryIndex* index,
                                                       int64_t base_version, int64_t new_version, size_t* total_deletes,
                                                       std::vector<std::pair<uint32_t, DelVectorPtr>>* delvecs)
            : _tablet(tablet),
              _rowset(rowset),
              _index(index),
              _base_version(base_version),
              _new_version(new_version),
              _total_deletes(total_deletes),
              _delvecs(delvecs) {}
    ~LocalPrimaryKeyCompactionConflictResolver() {}

    StatusOr<std::string> filename() const override;
    Schema generate_pkey_schema() override;
    Status segment_iterator(
            const std::function<Status(const CompactConflictResolveParams&, const std::vector<ChunkIteratorPtr>&,
                                       const std::function<void(uint32_t, const DelVectorPtr&, uint32_t)>&)>& handler)
            override;
    Status breakpoint_check() override;

private:
    // input
    Tablet* _tablet = nullptr;
    Rowset* _rowset = nullptr;
    PrimaryIndex* _index = nullptr;
    int64_t _base_version = 0;
    int64_t _new_version = 0;
    // output
    size_t* _total_deletes = nullptr;
    // <rssid -> Delvec>
    std::vector<std::pair<uint32_t, starrocks::DelVectorPtr>>* _delvecs = nullptr;
};

} // namespace starrocks