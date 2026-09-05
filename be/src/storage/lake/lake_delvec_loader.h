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

#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>
#include <set>
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
//
// Entries are handed out as shared pointers, not copies: the compaction read path only ever
// inspects a delvec (cardinality / roaring iteration), and a copy per (segment, pass) lookup
// would scale CPU and allocation with bitmap size times the column-group count.
class CompactionDelvecHolder {
public:
    // Single-flight get-or-load: at most one caller runs |load| for a given (segment, version);
    // concurrent callers of the same key wait on the condition variable and share the loaded
    // instance. Range-split parallel compaction shares one holder across concurrent PK subtasks,
    // and without per-key in-flight coordination they would miss together and each re-read and
    // re-parse the same delvec file, multiplying remote IO and transient bitmap memory by the
    // subtask count. A failed load clears the in-flight mark before notifying -- errors are not
    // cached -- so a waiter takes over and retry semantics survive.
    Status get_or_load(const TabletSegmentId& tsid, int64_t version, DelVectorPtr* pdelvec,
                       const std::function<Status(DelVectorPtr*)>& load) {
        const auto key = std::make_tuple(tsid.tablet_id, tsid.segment_id, version);
        std::unique_lock<std::mutex> lk(_mutex);
        while (true) {
            auto it = _delvecs.find(key);
            if (it != _delvecs.end()) {
                *pdelvec = it->second;
                return Status::OK();
            }
            if (_loading.insert(key).second) {
                break;
            }
            _cv.wait(lk);
        }
        lk.unlock();
        DelVectorPtr loaded;
        auto st = load(&loaded);
        lk.lock();
        _loading.erase(key);
        if (st.ok()) {
            _delvecs.emplace(key, loaded);
        }
        _cv.notify_all();
        if (!st.ok()) {
            return st;
        }
        *pdelvec = std::move(loaded);
        return Status::OK();
    }

private:
    using Key = std::tuple<int64_t, uint32_t, int64_t>;
    std::mutex _mutex;
    std::condition_variable _cv;
    std::map<Key, DelVectorPtr> _delvecs;
    // Keys with a load in flight; guarded by _mutex.
    std::set<Key> _loading;
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