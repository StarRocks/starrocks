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

#ifdef USE_STAROS
#include "staros_integration/staros_starcache.h"

#include "common/config_staros_worker_fwd.h"
#include "fslib/star_cache_configuration.h"
#include "fslib/star_cache_handler.h"

namespace starrocks {

namespace fslib = staros::starlet::fslib;

// must keep each config the same
void update_staros_starcache() {
    if (fslib::FLAGS_use_star_cache != config::starlet_use_star_cache) {
        fslib::FLAGS_use_star_cache = config::starlet_use_star_cache;
        (void)fslib::star_cache_init(fslib::FLAGS_use_star_cache);
    }

    if (fslib::FLAGS_star_cache_mem_size_percent != config::starlet_star_cache_mem_size_percent) {
        fslib::FLAGS_star_cache_mem_size_percent = config::starlet_star_cache_mem_size_percent;
        (void)fslib::star_cache_update_memory_quota_percent(fslib::FLAGS_star_cache_mem_size_percent);
    }

    if (fslib::FLAGS_star_cache_mem_size_bytes != config::starlet_star_cache_mem_size_bytes) {
        fslib::FLAGS_star_cache_mem_size_bytes = config::starlet_star_cache_mem_size_bytes;
        (void)fslib::star_cache_update_memory_quota_bytes(fslib::FLAGS_star_cache_mem_size_bytes);
    }
}

} // namespace starrocks
#endif // USE_STAROS
