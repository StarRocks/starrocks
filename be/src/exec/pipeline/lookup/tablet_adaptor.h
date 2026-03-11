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

#include <memory>

#include "base/status.h"
#include "common/object_pool.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_iterator.h"
#include "storage/range.h"
#include "storage/rowset/common.h"

namespace starrocks {

class GlobalLateMaterilizationContext;
class LookUpTabletAdaptor {
public:
    LookUpTabletAdaptor() = default;
    virtual ~LookUpTabletAdaptor() = default;

    virtual Status init(int64_t tablet_id) = 0;
    virtual Status capture(GlobalLateMaterilizationContext* glm_ctx) = 0;

    virtual Status init_schema(RuntimeState* state) = 0;

    virtual Status init_access_path(RuntimeState* state, ObjectPool* pool) = 0;

    virtual Status init_global_dicts(RuntimeState* state, ObjectPool* pool,
                                     const std::vector<SlotDescriptor*>& slots) = 0;
    virtual Status init_read_columns(const std::vector<SlotDescriptor*>& slots) = 0;

    using ChunkIteratorPtr = std::shared_ptr<ChunkIterator>;
    virtual StatusOr<ChunkIteratorPtr> get_iterator(int64_t rssid, SparseRange<rowid_t> row_id_range) = 0;
};

using LookUpTabletAdaptorPtr = std::unique_ptr<LookUpTabletAdaptor>;

StatusOr<LookUpTabletAdaptorPtr> create_look_up_tablet_adaptor(RowPositionDescriptor::Type type);

} // namespace starrocks