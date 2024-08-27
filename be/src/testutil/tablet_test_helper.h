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

#include "gen_cpp/AgentService_types.h"
#include "storage/delta_writer.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/tablet_reader.h"

namespace starrocks {
class TabletTestHelper {
public:
    static TCreateTabletReq gen_create_tablet_req(TTabletId tablet_id, TKeysType::type type,
                                                  TStorageType::type storage_type);
    static std::shared_ptr<RowsetWriter> create_rowset_writer(const Tablet& tablet, RowsetId rowset_id,
                                                              Version version);
    static std::shared_ptr<TabletReader> create_rowset_reader(const TabletSharedPtr& tablet, const Schema& schema,
                                                              Version version);
    static std::shared_ptr<DeltaWriter> create_delta_writer(TTabletId tablet_id,
                                                            const std::vector<SlotDescriptor*>& slots,
                                                            MemTracker* mem_tracker);
    static std::vector<ChunkIteratorPtr> create_segment_iterators(const Tablet& tablet, Version version,
                                                                  OlapReaderStatistics* stats);
};
} // namespace starrocks
