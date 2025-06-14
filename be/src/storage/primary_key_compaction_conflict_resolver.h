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

#include "common/status.h"
#include "storage/chunk_iterator.h"
#include "storage/del_vector.h"

namespace starrocks {

class DelvecLoader;
class Schema;
class PrimaryIndex;

struct CompactConflictResolveParams {
    int64_t tablet_id = 0;
    uint32_t rowset_id = 0;
    int64_t base_version = 0;
    int64_t new_version = 0;
    DelvecLoader* delvec_loader = 0;
    PrimaryIndex* index = 0;
};

class PrimaryKeyCompactionConflictResolver {
public:
    virtual ~PrimaryKeyCompactionConflictResolver() = default;
    virtual StatusOr<std::string> filename() const = 0;
    virtual Schema generate_pkey_schema() = 0;
    virtual Status breakpoint_check() { return Status::OK(); }
    virtual Status segment_iterator(
            const std::function<Status(const CompactConflictResolveParams&, const std::vector<ChunkIteratorPtr>&,
                                       const std::function<void(uint32_t, const DelVectorPtr&, uint32_t)>&)>&
                    handler) = 0;

    Status execute();
};

} // namespace starrocks