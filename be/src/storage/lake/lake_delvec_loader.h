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

#include <map>
#include <mutex>
#include <tuple>

#include "common/statusor.h"
#include "storage/del_vector.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet.h"
#include "storage/lake/update_manager.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/olap_common.h"

namespace starrocks::lake {

// Task-scoped delvec store for compaction. Vertical compaction creates a delvec loader per
// column-group pass, and every pass loads the same delvecs again; the metadata cache does not
// keep them alive because the pass loop keeps re-inserting the (much larger) segment objects
// over them. Holding the delvecs at the task scope removes that reload entirely; memory is one
// delvec per input segment, released with the task.
class CompactionDelvecHolder {
public:
    bool find(const TabletSegmentId& tsid, int64_t version, DelVector* out) {
        std::lock_guard<std::mutex> l(_mutex);
        auto it = _delvecs.find(std::make_tuple(tsid.tablet_id, tsid.segment_id, version));
        if (it == _delvecs.end()) {
            return false;
        }
        out->copy_from(*it->second);
        return true;
    }
    void put(const TabletSegmentId& tsid, int64_t version, const DelVector& delvec) {
        auto copy = std::make_shared<DelVector>();
        copy->copy_from(delvec);
        std::lock_guard<std::mutex> l(_mutex);
        _delvecs.emplace(std::make_tuple(tsid.tablet_id, tsid.segment_id, version), std::move(copy));
    }

private:
    std::mutex _mutex;
    std::map<std::tuple<int64_t, uint32_t, int64_t>, DelVectorPtr> _delvecs;
};

class LakeDelvecLoader : public DelvecLoader {
public:
    LakeDelvecLoader(TabletManager* tablet_manager, const MetaFileBuilder* pk_builder, bool fill_cache,
                     LakeIOOptions lake_io_opts, TabletMetadataPtr cached_metadata = nullptr,
                     std::shared_ptr<CompactionDelvecHolder> holder = nullptr)
            : _tablet_manager(tablet_manager),
              _pk_builder(pk_builder),
              _fill_cache(fill_cache),
              _lake_io_opts(std::move(lake_io_opts)),
              _cached_metadata(std::move(cached_metadata)),
              _holder(std::move(holder)) {}
    Status load(const TabletSegmentId& tsid, int64_t version, DelVectorPtr* pdelvec) override;
    Status load_from_meta(const TabletMetadataPtr& metadata, const DelvecPagePB& delvec_page, DelVectorPtr* pdelvec);
    Status load_from_file(const TabletSegmentId& tsid, int64_t version, DelVectorPtr* pdelvec);

private:
    TabletManager* _tablet_manager;
    const MetaFileBuilder* _pk_builder = nullptr;
    bool _fill_cache = false;
    LakeIOOptions _lake_io_opts;
    // Reused across load_from_file calls whose (tablet_id, version) match, to skip
    // get_tablet_metadata and its TabletMetadataPB deep copy on the hot path.
    TabletMetadataPtr _cached_metadata;
    // Optional task-scoped store; set only on the compaction read path (hold_segments).
    std::shared_ptr<CompactionDelvecHolder> _holder;
};

} // namespace starrocks::lake