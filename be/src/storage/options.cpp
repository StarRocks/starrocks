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

#include "common/config.h"

namespace starrocks {

int64_t lake_scan_buffer_size(const LakeIOOptions& lake_io_opts) {
    if (lake_io_opts.buffer_size >= 0) {
        return lake_io_opts.buffer_size;
    }
    // Only lower the bound where the datacache is not already rounding the reads out for us.
    // On a miss it intends to cache, cachefs fetches a whole starlet_star_cache_block_size_bytes
    // block, so the request that reaches the object store is a whole 1 MiB block no matter what
    // we ask for here and a smaller bound buys nothing. Two cases escape that alignment, and
    // they are the ones worth shrinking:
    //
    //   skip_disk_cache    the read bypasses cachefs entirely and goes straight to the object
    //                      store, so its size is exactly what we ask for
    //   !fill_data_cache   the read still goes through cachefs, but with nothing to cache it
    //                      reads just the requested bytes rather than rounding out to the block
    //
    // In both the request size is ours to choose, and choosing it smaller cuts read bandwidth:
    // the scan asks for exact page ranges, so a large read-ahead only rounds every request up
    // and fetches bytes the scan never looks at.
    if (lake_io_opts.skip_disk_cache || !lake_io_opts.fill_data_cache) {
        return config::lake_scan_min_remote_read_bytes;
    }
    // Block-aligned reads: leave the size to starlet, as before.
    return -1;
}

} // namespace starrocks
