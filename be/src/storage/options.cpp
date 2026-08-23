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

#include "storage/options.h"

#include "common/config_scan_io_fwd.h"

namespace starrocks {

namespace {

// True when cachefs will fetch a whole starlet_star_cache_block_size_bytes block to serve this
// read, which makes the size of the request that reaches the object store not ours to choose.
// Two cases escape that: the read bypasses cachefs outright, or it goes through cachefs with
// nothing to cache and so reads just the bytes asked for.
bool reads_whole_cache_blocks(const LakeIOOptions& lake_io_opts) {
    return !lake_io_opts.skip_disk_cache && lake_io_opts.fill_data_cache;
}

} // namespace

int64_t lake_scan_buffer_size(const LakeIOOptions& lake_io_opts) {
    if (lake_io_opts.buffer_size >= 0) {
        return lake_io_opts.buffer_size;
    }
    // Where the request size is block-aligned for us, a smaller bound buys nothing: leave the
    // size to starlet, as before. Where it is ours to choose, choosing it smaller cuts read
    // bandwidth, because the scan asks for exact page ranges and a large read-ahead only rounds
    // every request up and fetches bytes the scan never looks at.
    if (reads_whole_cache_blocks(lake_io_opts)) {
        return -1;
    }
    return config::lake_scan_min_remote_read_bytes;
}

bool should_enable_io_coalesce_lake_read(const LakeIOOptions& lake_io_opts) {
    if (config::io_coalesce_lake_read_enable) {
        return true;
    }
    // Coalescing merges a column's page ranges into fewer, larger reads, which is only
    // something we can do where the request size is ours to choose in the first place. Those
    // are exactly the reads lake_scan_min_remote_read_bytes bounds, and it is the same reads
    // that pay for that smaller bound in request count -- merging them back up is what keeps
    // the bound from being a trade.
    return !reads_whole_cache_blocks(lake_io_opts);
}

} // namespace starrocks
