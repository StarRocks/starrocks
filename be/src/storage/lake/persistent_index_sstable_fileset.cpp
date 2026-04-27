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

#include "storage/lake/persistent_index_sstable_fileset.h"

#include <atomic>
#include <condition_variable>
#include <mutex>

#include "base/debug/trace.h"
#include "common/config_exec_env_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "common/thread/threadpool.h"
#include "gutil/walltime.h"
#include "runtime/exec_env.h"
#include "storage/lake/persistent_index_sstable.h"
#include "storage/persistent_index.h"
#include "storage/sstable/comparator.h"

namespace starrocks::lake {

Status PersistentIndexSstableFileset::init(std::vector<std::unique_ptr<PersistentIndexSstable>>& sstables) {
    if (is_inited()) {
        return Status::InternalError("sstable fileset is already initialized");
    }
    const sstable::Comparator* comparator = sstable::BytewiseComparator();
    for (auto&& sstable : sstables) {
        if (sstable->sstable_pb().has_range()) {
            DCHECK(sstable->sstable_pb().has_fileset_id());
            // Make sure sstable is inorder via comparator
            if (!_sstable_map.empty()) {
                const auto& last_end_key = _sstable_map.rbegin()->first.second;
                if (comparator->Compare(Slice(last_end_key), Slice(sstable->sstable_pb().range().start_key())) >= 0) {
                    return Status::InternalError("sstables are not in order or have overlap key range");
                }
                if (_fileset_id != sstable->sstable_pb().fileset_id()) {
                    return Status::InternalError("inconsistent fileset_id in sstables");
                }
            }
            _fileset_id = sstable->sstable_pb().fileset_id();
            // Extract keys before moving sstable to avoid undefined behavior
            std::string start_key = sstable->sstable_pb().range().start_key();
            std::string end_key = sstable->sstable_pb().range().end_key();
            _sstable_map.emplace(std::make_pair(std::move(start_key), std::move(end_key)), std::move(sstable));
        } else {
            if (_standalone_sstable != nullptr) {
                return Status::InternalError("more than one standalone sstable in fileset");
            }
            // Preserve the existing fileset_id from metadata if present, to keep it
            // consistent with what compaction txn_log records. Only generate a new
            // random ID for truly legacy sstables (e.g., created before fileset_id
            // was introduced).
            if (sstable->sstable_pb().has_fileset_id()) {
                _fileset_id = sstable->sstable_pb().fileset_id();
            } else {
                _fileset_id = UniqueId::gen_uid();
                sstable->set_fileset_id(_fileset_id);
            }
            _standalone_sstable = std::move(sstable);
        }
    }
    return Status::OK();
}

Status PersistentIndexSstableFileset::init(std::unique_ptr<PersistentIndexSstable>& sstable) {
    if (is_inited()) {
        return Status::InternalError("sstable fileset is already initialized");
    }
    if (sstable->sstable_pb().has_range()) {
        if (!sstable->sstable_pb().has_fileset_id()) {
            // New fileset
            _fileset_id = UniqueId::gen_uid();
            sstable->set_fileset_id(_fileset_id);
        } else {
            _fileset_id = sstable->sstable_pb().fileset_id();
        }
        // Extract keys before moving sstable to avoid undefined behavior
        std::string start_key = sstable->sstable_pb().range().start_key();
        std::string end_key = sstable->sstable_pb().range().end_key();
        _sstable_map.emplace(std::make_pair(std::move(start_key), std::move(end_key)), std::move(sstable));
    } else {
        // Preserve the existing fileset_id from metadata if present (same reason as above).
        if (sstable->sstable_pb().has_fileset_id()) {
            _fileset_id = sstable->sstable_pb().fileset_id();
        } else {
            _fileset_id = UniqueId::gen_uid();
            sstable->set_fileset_id(_fileset_id);
        }
        _standalone_sstable = std::move(sstable);
    }
    return Status::OK();
}

bool PersistentIndexSstableFileset::append(std::unique_ptr<PersistentIndexSstable>& sstable) {
    const sstable::Comparator* comparator = sstable::BytewiseComparator();
    DCHECK(sstable->sstable_pb().has_range());
    // Make sure sstable is inorder via comparator
    if (!_sstable_map.empty()) {
        const auto& last_end_key = _sstable_map.rbegin()->first.second;
        if (comparator->Compare(Slice(last_end_key), Slice(sstable->sstable_pb().range().start_key())) >= 0) {
            return false;
        }
    } else {
        return false;
    }
    // Extract keys before moving sstable to avoid undefined behavior
    std::string start_key = sstable->sstable_pb().range().start_key();
    std::string end_key = sstable->sstable_pb().range().end_key();
    // This sstable belong to same fileset.
    sstable->set_fileset_id(_fileset_id);
    _sstable_map.emplace(std::make_pair(std::move(start_key), std::move(end_key)), std::move(sstable));
    return true;
}

Status PersistentIndexSstableFileset::multi_get(const Slice* keys, const KeyIndexSet& key_indexes, int64_t version,
                                                IndexValue* values, KeyIndexSet* found_key_indexes) const {
    const sstable::Comparator* comparator = sstable::BytewiseComparator();
    // 0. if single standalone sstable, directly get.
    if (_standalone_sstable != nullptr) {
        DCHECK(_sstable_map.empty());
        return _standalone_sstable->multi_get(keys, key_indexes, version, values, found_key_indexes);
    }
    // 1. divide key_indexes into different groups according to sstables
    std::unordered_map<PersistentIndexSstable*, KeyIndexSet> sstable_key_indexes_map;
    {
        TRACE_COUNTER_SCOPE_LATENCY_US("fileset_get_divide_us");
        for (auto& key_index : key_indexes) {
            auto it = _sstable_map.upper_bound(keys[key_index]);
            if (it != _sstable_map.begin()) {
                --it;
                const auto& [key_pair, sstable] = *it;
                const auto& [start_key, end_key] = key_pair;
                if (comparator->Compare(keys[key_index], Slice(end_key)) <= 0) {
                    // key in range [start_key, end_key]
                    sstable_key_indexes_map[sstable.get()].insert(key_index);
                }
            }
        }
    }
    // 2. multi get from each sstable.
    // The N per-sstable lookups are independent OSS reads. On a cold rebuild they dominate
    // upsert tail latency (`multi_get_us` ~2-3 s, serial OSS RTT). Fan them out to a
    // dedicated thread pool so a single fileset's lookups overlap.
    const size_t n_ssts = sstable_key_indexes_map.size();
    ThreadPool* fanout_pool = nullptr;
    auto* exec_env = ExecEnv::GetInstance();
    if (exec_env != nullptr) {
        fanout_pool = exec_env->pk_index_sstable_fanout_thread_pool();
    }
    const bool can_fanout = fanout_pool != nullptr && config::enable_pk_index_sstable_fanout &&
                            n_ssts >= static_cast<size_t>(std::max(2, config::pk_index_sstable_fanout_min_ssts));

    if (!can_fanout) {
        for (const auto& [sstable, sstable_key_indexes] : sstable_key_indexes_map) {
            RETURN_IF_ERROR(sstable->multi_get(keys, sstable_key_indexes, version, values, found_key_indexes));
        }
        return Status::OK();
    }

    TRACE_COUNTER_SCOPE_LATENCY_US("fileset_get_fanout_us");
    TRACE_COUNTER_INCREMENT("fileset_get_fanout_tasks", n_ssts);

    // Per-sstable key sets are disjoint by construction in step 1, so each task writes
    // to a disjoint slice of `values[key_index]` — no synchronization needed for `values`.
    // `found_key_indexes` is shared (std::set), so each task accumulates into a local
    // KeyIndexSet then merges under a mutex.
    std::mutex out_mu;
    Status worker_status; // OK by default; first error wins
    std::atomic<size_t> remaining{n_ssts};
    std::mutex done_mu;
    std::condition_variable done_cv;

    auto run_one = [&](PersistentIndexSstable* sst, const KeyIndexSet& sst_keys) {
        KeyIndexSet local_found;
        Status st = sst->multi_get(keys, sst_keys, version, values, &local_found);
        {
            std::lock_guard<std::mutex> lg(out_mu);
            if (!st.ok() && worker_status.ok()) {
                worker_status = st;
            }
            if (!local_found.empty()) {
                found_key_indexes->insert(local_found.begin(), local_found.end());
            }
        }
        if (remaining.fetch_sub(1) == 1) {
            std::lock_guard<std::mutex> lg(done_mu);
            done_cv.notify_one();
        }
    };

    int64_t inline_start = 0;
    int64_t inline_us = 0;
    for (const auto& [sstable, sstable_key_indexes] : sstable_key_indexes_map) {
        Status submit_st = fanout_pool->submit_func([&run_one, sstable, &sstable_key_indexes]() {
            run_one(sstable, sstable_key_indexes);
        });
        if (!submit_st.ok()) {
            // Pool full / shutdown / OOM: run inline. `remaining` is still decremented
            // inside run_one so the wait loop terminates correctly.
            inline_start = GetCurrentTimeMicros();
            run_one(sstable, sstable_key_indexes);
            inline_us += (GetCurrentTimeMicros() - inline_start);
        }
    }

    {
        std::unique_lock<std::mutex> lg(done_mu);
        done_cv.wait(lg, [&]() { return remaining.load() == 0; });
    }
    if (inline_us > 0) {
        TRACE_COUNTER_INCREMENT("fileset_get_fanout_inline_us", inline_us);
    }
    return worker_status;
}

const std::string& PersistentIndexSstableFileset::standalone_sstable_filename() const {
    return _standalone_sstable->sstable_pb().filename();
}

void PersistentIndexSstableFileset::get_all_sstable_pbs(PersistentIndexSstableMetaPB* sstable_pbs) const {
    for (const auto& [key_pair, sstable] : _sstable_map) {
        sstable_pbs->add_sstables()->CopyFrom(sstable->sstable_pb());
    }
    if (_standalone_sstable != nullptr) {
        sstable_pbs->add_sstables()->CopyFrom(_standalone_sstable->sstable_pb());
    }
}

bool PersistentIndexSstableFileset::contains_sst(const std::string& filename) const {
    for (const auto& [key_pair, sstable] : _sstable_map) {
        if (sstable->sstable_pb().filename() == filename) {
            return true;
        }
    }
    if (_standalone_sstable != nullptr && _standalone_sstable->sstable_pb().filename() == filename) {
        return true;
    }
    return false;
}

size_t PersistentIndexSstableFileset::memory_usage() const {
    size_t total_memory = 0;
    for (const auto& [key_pair, sstable] : _sstable_map) {
        total_memory += sstable->memory_usage();
    }
    if (_standalone_sstable != nullptr) {
        total_memory += _standalone_sstable->memory_usage();
    }
    return total_memory;
}

void PersistentIndexSstableFileset::print_debug_info(std::stringstream& ss) {
    ss << " Fileset ID: " << _fileset_id.to_string();
    ss << " Number of sstables: " << _sstable_map.size();
    for (const auto& [key_pair, sstable] : _sstable_map) {
        ss << " Sstable filesize: " << sstable->sstable_pb().filesize();
    }
    if (_standalone_sstable != nullptr) {
        ss << " Standalone sstable filesize: " << _standalone_sstable->sstable_pb().filesize();
    }
}

} // namespace starrocks::lake