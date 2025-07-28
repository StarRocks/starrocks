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

#include "join_hash_table_descriptor.h"

namespace starrocks {

// if the same hash values are clustered, after the first probe, all related hash buckets are cached, without too many
// misses. So check time locality of probe keys here.
void HashTableProbeState::consider_probe_time_locality() {
    if (active_coroutines > 0) {
        // redo decision
        if ((probe_chunks & (detect_step - 1)) == 0) {
            int window_size = std::min(active_coroutines * 4, 50);
            if (probe_row_count > window_size) {
                phmap::flat_hash_map<uint32_t, uint32_t, StdHash<uint32_t>> occurrence;
                occurrence.reserve(probe_row_count);
                uint32_t unique_size = 0;
                bool enable_interleaving = true;
                uint32_t target = probe_row_count >> 3;
                for (auto i = 0; i < probe_row_count; i++) {
                    if (occurrence[next[i]] == 0) {
                        ++unique_size;
                        if (unique_size >= target) {
                            break;
                        }
                    }
                    occurrence[next[i]]++;
                    if (i >= window_size) {
                        occurrence[next[i - window_size]]--;
                    }
                }
                if (unique_size < target) {
                    active_coroutines = 0;
                    enable_interleaving = false;
                }
                // enlarge step if the decision is the same, otherwise reduce it
                if (enable_interleaving == last_enable_interleaving) {
                    detect_step = detect_step >= 1024 ? detect_step : (detect_step << 1);
                } else {
                    last_enable_interleaving = enable_interleaving;
                    detect_step = 1;
                }
            } else {
                active_coroutines = 0;
            }
        } else if (!last_enable_interleaving) {
            active_coroutines = 0;
        }
    }
    ++probe_chunks;
}

} // namespace starrocks