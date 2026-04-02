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

#include <unordered_map>
#include <unordered_set>

#include "common/statusor.h"
#include "storage/del_vector.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet.h"
#include "storage/lake/update_manager.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/olap_common.h"

namespace starrocks::lake {

class LakeDelvecLoader : public DelvecLoader {
public:
    LakeDelvecLoader(TabletManager* tablet_manager, const MetaFileBuilder* pk_builder, bool fill_cache,
                     LakeIOOptions lake_io_opts)
            : _tablet_manager(tablet_manager),
              _pk_builder(pk_builder),
              _fill_cache(fill_cache),
              _lake_io_opts(std::move(lake_io_opts)) {}
    Status load(const TabletSegmentId& tsid, int64_t version, DelVectorPtr* pdelvec) override;
    Status load_from_meta(const TabletMetadataPtr& metadata, const DelvecPagePB& delvec_page, DelVectorPtr* pdelvec);
    Status load_from_file(const TabletSegmentId& tsid, int64_t version, DelVectorPtr* pdelvec);

    // Concurrently load delvecs for a set of segment IDs. Results are stored
    // in _preloaded_delvecs and checked by subsequent load() calls.
    // Uses std::async to issue multiple range reads in parallel, hiding the
    // per-request HTTP round-trip latency (~2-3ms) across many segments.
    Status batch_load(int64_t tablet_id, int64_t version, const std::unordered_set<uint32_t>& segment_ids);

private:
    TabletManager* _tablet_manager;
    const MetaFileBuilder* _pk_builder = nullptr;
    bool _fill_cache = false;
    LakeIOOptions _lake_io_opts;

    // Delvecs preloaded by batch_load(), keyed by segment_id.
    std::unordered_map<uint32_t, DelVectorPtr> _preloaded_delvecs;
};

} // namespace starrocks::lake