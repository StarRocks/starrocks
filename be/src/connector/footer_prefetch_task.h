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
#include <string>
#include <vector>

#include "exec/hdfs_scanner/hdfs_scanner.h" // DataCacheOptions
#include "exec/pipeline/scan/footer_prefetch_state.h"
#include "fs/fs.h"

namespace starrocks::pipeline {

// Definition of the opaque context forward-declared in footer_prefetch_state.h. Built once
// per connector scan and shared (const) into every FooterPrefetchItem, so a metadata task
// can open a file the same cache-wrapped way the real scan does without touching the
// provider / RuntimeState. One FileSystem per scan (files share scheme + cloud config).
struct FooterOpenContext {
    std::shared_ptr<FileSystem> fs;
    DataCacheOptions datacache_options;
    bool case_sensitive = false;
};

} // namespace starrocks::pipeline

namespace starrocks::connector {

// Output of building the footer-prefetch sidecar for a scan: the per-file items plus which
// caches can hold a warmed footer (drives FooterPrefetchState's warm-mode decisions).
struct FooterPrefetchPlan {
    std::vector<pipeline::FooterPrefetchItem> items;
    bool metacache_on = false;
    bool datacache_populate_on = false;
};

// Stable identity for a scan range's root file -- derivable both when building the prefetch
// list (provider) and when the real scan starts the morsel (create_chunk_source), so the two
// agree without re-resolving the native path. Uses full_path when present, else
// partition_id + relative_path.
std::string footer_prefetch_key(const TScanRange& scan_range);

// Whether a single warm actually wrote the footer into each cache (a no-op on cache hit).
struct FooterWarmResult {
    bool wrote_pagecache = false;
    bool wrote_blockcache = false;
};

// Warm one file's footer, off the row path: one get_file_metadata through the cache-wrapped
// stream -- the read populates BlockCache (when datacache populate is on) and, when metacache
// is on, the parse inserts into PageCache. A cache hit warms nothing. Errors are swallowed --
// prefetch must never poison the scan.
FooterWarmResult warm_footer(const pipeline::FooterPrefetchItem& item, bool metacache_on);

} // namespace starrocks::connector
