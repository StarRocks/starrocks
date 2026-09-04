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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/options.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "base/uid_util.h"
#include "fs/fs.h"
#include "platform/store_path.h"
#include "storage/lake/location_provider.h"

namespace starrocks {

class MemTracker;
class TableMetricsManager;

struct EngineOptions {
    // list paths that tablet will be put into.
    std::vector<StorePath> store_paths;
    // BE's UUID. It will be reset every time BE restarts.
    UniqueId backend_uid{0, 0};
    MemTracker* compaction_mem_tracker = nullptr;
    MemTracker* update_mem_tracker = nullptr;
    TableMetricsManager* table_metrics_mgr = nullptr;
    // if start as cn, no need to write cluster id
    bool need_write_cluster_id = true;
};

// Options only applies to cloud-native table r/w IO
struct LakeIOOptions {
    // Cache remote file locally on read requests.
    // This options can be ignored if the underlying filesystem does not support local cache.
    bool fill_data_cache = false;

    bool skip_disk_cache = false;
    // Specify different buffer size for different read scenarios
    int64_t buffer_size = -1;
    bool fill_metadata_cache = false;
    // Keep the loaded Segment objects alive on the Rowset instance and reuse them on the next
    // segments() call. Vertical compaction reads the same rowsets once per column group; without
    // this, every pass reloads and reparses every segment whenever the metadata cache cannot hold
    // them all, which is CPU-bound and proportional to the column count.
    bool hold_segments = false;
    bool use_page_cache = false;
    // Compaction direct-read mode: route every coalesce-enabled column of a segment through one
    // shared SharedBufferedInputStream and register all their IO ranges in a single batch, so
    // adjacent column regions merge into few large object-storage reads. Only meaningful together
    // with skip_disk_cache; per-column read amplification is what it removes.
    bool coalesce_across_columns = false;
    bool cache_file_only = false; // only used for CACHE SELECT
    // Callback to warmup SST files, invoked at most once per tablet during CACHE SELECT.
    // Protected by sst_warmup_done (CAS guard) to ensure single execution across segments.
    std::function<Status()> sst_warmup_fn;
    std::shared_ptr<std::atomic<bool>> sst_warmup_done;
    std::shared_ptr<FileSystem> fs;
    std::shared_ptr<starrocks::lake::LocationProvider> location_provider;
};

} // namespace starrocks
