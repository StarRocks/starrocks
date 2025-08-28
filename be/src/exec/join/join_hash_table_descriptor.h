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

#include <gen_cpp/PlanNodes_types.h>
#include <runtime/descriptors.h>
#include <runtime/runtime_state.h>

#include <coroutine>
#include <cstdint>
#include <set>

#include "column/chunk.h"
#include "column/column_hash.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "simd/simd.h"
#include "util/phmap/phmap.h"
#include "util/runtime_profile.h"

namespace starrocks {

class ColumnRef;

enum class JoinMatchFlag { NORMAL, ALL_NOT_MATCH, ALL_MATCH_ONE, MOST_MATCH_ONE };

struct JoinKeyDesc {
    const TypeDescriptor* type = nullptr;
    bool is_null_safe_equal;
    ColumnRef* col_ref = nullptr;
};

struct HashTableSlotDescriptor {
    SlotDescriptor* slot;
    bool need_output;
    bool need_lazy_materialize = false;
};

struct JoinHashTableItems {
    //TODO: memory continues problem?
    ChunkPtr build_chunk = nullptr;
    Columns key_columns;
    std::vector<HashTableSlotDescriptor> build_slots;
    std::vector<HashTableSlotDescriptor> probe_slots;
    // A hash value is the bucket index of the hash map. "JoinHashTableItems.first" is the
    // buckets of the hash map, and it holds the index of the first key value saved in each bucket,
    // while other keys can be found by following the indices saved in
    // "JoinHashTableItems.next". "JoinHashTableItems.next[0]" represents the end of
    // the list of keys in a bucket.
    // A paper (https://dare.uva.nl/search?identifier=5ccbb60a-38b8-4eeb-858a-e7735dd37487) talks
    // about the bucket-chained hash table of this kind.
    Buffer<uint32_t> first;
    Buffer<uint32_t> next;

    Buffer<uint8_t> key_bitset;
    struct DenseGroup {
        uint32_t start_index = 0;
        uint32_t bitset = 0;
    };
    Buffer<DenseGroup> dense_groups;

    Buffer<Slice> build_slice;
    ColumnPtr build_key_column = nullptr;
    Buffer<uint8_t> build_key_nulls;

    uint32_t bucket_size = 0;
    uint32_t log_bucket_size = 0;
    uint32_t row_count = 0; // real row count
    size_t build_column_count = 0;
    size_t output_build_column_count = 0;
    size_t lazy_output_build_column_count = 0;
    size_t probe_column_count = 0;
    size_t output_probe_column_count = 0;
    size_t lazy_output_probe_column_count = 0;

    int64_t min_value;
    int64_t max_value;

    bool with_other_conjunct = false;
    bool left_to_nullable = false;
    bool right_to_nullable = false;
    bool has_large_column = false;
    float keys_per_bucket = 0;
    size_t used_buckets = 0;
    bool cache_miss_serious = false;
    bool enable_late_materialization = false;
    bool is_collision_free_and_unique = false;

    float get_keys_per_bucket() const { return keys_per_bucket; }
    bool ht_cache_miss_serious() const { return cache_miss_serious; }

    void calculate_ht_info(size_t key_bytes) {
        if (used_buckets != 0) {
            // to avoid redo
            return;
        }

        used_buckets = first.empty() ? SIMD::count_nonzero(key_bitset) : SIMD::count_nonzero(first);
        keys_per_bucket = used_buckets == 0 ? 0 : row_count * 1.0 / used_buckets;
        size_t probe_bytes = key_bytes + row_count * sizeof(uint32_t);
        // cache miss is serious when
        // 1) the ht's size is enough large, for example, larger than (1UL << 27) bytes.
        // 2) smaller ht but most buckets have more than one keys
        cache_miss_serious = row_count > (1UL << 18) &&
                             ((probe_bytes > (1UL << 24) && keys_per_bucket >= 10) ||
                              (probe_bytes > (1UL << 25) && keys_per_bucket > 2) ||
                              (probe_bytes > (1UL << 26) && keys_per_bucket > 1.5) || probe_bytes > (1UL << 27));
        VLOG_QUERY << "ht cache miss serious = " << cache_miss_serious << " row# = " << row_count
                   << " , bytes = " << probe_bytes << " , depth = " << keys_per_bucket;

        is_collision_free_and_unique = used_buckets == row_count;
    }

    TJoinOp::type join_type = TJoinOp::INNER_JOIN;

    std::unique_ptr<MemPool> build_pool = nullptr;
    std::vector<JoinKeyDesc> join_keys;
};

struct HashTableProbeState {
    //TODO: memory release
    Buffer<uint8_t> is_nulls;
    Buffer<uint32_t> buckets;
    Buffer<uint32_t> next;
    Buffer<Slice> probe_slice;
    const Buffer<uint8_t>* null_array = nullptr;
    ColumnPtr probe_key_column;
    const Columns* key_columns = nullptr;
    ColumnPtr build_index_column;
    ColumnPtr probe_index_column;
    Buffer<uint32_t>& build_index;
    Buffer<uint32_t>& probe_index;

    // when exec right join
    // record the build items is matched or not
    // 0: not matched, 1: matched
    Buffer<uint8_t> build_match_index;
    Buffer<uint32_t> probe_match_index;
    Buffer<uint8_t> probe_match_filter;
    uint32_t count = 0; // current return values count
    // the rows of src probe chunk
    size_t probe_row_count = 0;

    // 0: normal
    // 1: all match one
    JoinMatchFlag match_flag = JoinMatchFlag::NORMAL; // all match one

    bool has_remain = false;
    // When one-to-many, one probe may not be able to probe all the data,
    // cur_probe_index records the position of the last probe
    uint32_t cur_probe_index = 0;
    uint32_t cur_build_index = 0;
    uint32_t cur_row_match_count = 0;

    // For nullaware left anti join there are other conjuncts, if the left or right table is null.
    // We need to find all rows (null does not match all rows). This variable helps us keep track of which rows are currently being processed.
    uint32_t cur_nullaware_build_index = 1;

    std::unique_ptr<MemPool> probe_pool = nullptr;

    RuntimeProfile::Counter* search_ht_timer = nullptr;
    RuntimeProfile::Counter* output_probe_column_timer = nullptr;
    RuntimeProfile::Counter* output_build_column_timer = nullptr;
    RuntimeProfile::Counter* probe_counter = nullptr;

    HashTableProbeState()
            : build_index_column(UInt32Column::create()),
              probe_index_column(UInt32Column::create()),
              build_index(down_cast<UInt32Column*>(build_index_column.get())->get_data()),
              probe_index(down_cast<UInt32Column*>(probe_index_column.get())->get_data()) {}

    struct ProbeCoroutine {
        struct ProbePromise {
            ProbeCoroutine get_return_object() { return std::coroutine_handle<ProbePromise>::from_promise(*this); }
            std::suspend_always initial_suspend() { return {}; }
            // as final_suspend() suspends coroutines, so should destroy manually in final.
            std::suspend_always final_suspend() noexcept { return {}; }
            void unhandled_exception() { exception = std::current_exception(); }
            void return_void() {}
            std::exception_ptr exception = nullptr;
        };

        using promise_type = ProbePromise;
        ProbeCoroutine(std::coroutine_handle<ProbePromise> h) : handle(h) {}
        ~ProbeCoroutine() = default;
        std::coroutine_handle<ProbePromise> handle;
        operator std::coroutine_handle<promise_type>() const { return std::move(handle); }
    };
    uint32_t match_count = 0;
    int active_coroutines = 0;
    // used to adaptively detect time locality
    size_t probe_chunks = 0;
    uint32_t detect_step = 1;
    bool last_enable_interleaving = true;

    std::set<std::coroutine_handle<ProbeCoroutine::ProbePromise>> handles;

    HashTableProbeState(const HashTableProbeState& rhs)
            : is_nulls(rhs.is_nulls),
              buckets(rhs.buckets),
              next(rhs.next),
              probe_slice(rhs.probe_slice),
              null_array(rhs.null_array),
              probe_key_column(rhs.probe_key_column == nullptr ? nullptr : rhs.probe_key_column->clone()),
              key_columns(rhs.key_columns),
              build_index_column(rhs.build_index_column == nullptr
                                         ? UInt32Column::create()->as_mutable_ptr() // to MutableColumnPtr
                                         : rhs.build_index_column->clone()),
              probe_index_column(rhs.probe_index_column == nullptr
                                         ? UInt32Column::create()->as_mutable_ptr() // to MutableColumnPtr
                                         : rhs.probe_index_column->clone()),
              build_index(down_cast<UInt32Column*>(build_index_column.get())->get_data()),
              probe_index(down_cast<UInt32Column*>(probe_index_column.get())->get_data()),
              build_match_index(rhs.build_match_index),
              probe_match_index(rhs.probe_match_index),
              probe_match_filter(rhs.probe_match_filter),
              count(rhs.count),
              probe_row_count(rhs.probe_row_count),
              match_flag(rhs.match_flag),
              has_remain(rhs.has_remain),
              cur_probe_index(rhs.cur_probe_index),
              cur_build_index(rhs.cur_build_index),
              cur_row_match_count(rhs.cur_row_match_count),
              probe_pool(rhs.probe_pool == nullptr ? nullptr : std::make_unique<MemPool>()),
              search_ht_timer(rhs.search_ht_timer),
              output_probe_column_timer(rhs.output_probe_column_timer),
              probe_counter(rhs.probe_counter) {}

    // Disable copy assignment.
    HashTableProbeState& operator=(const HashTableProbeState& rhs) = delete;
    // Disable move ctor and assignment.
    HashTableProbeState(HashTableProbeState&&) = delete;
    HashTableProbeState& operator=(HashTableProbeState&&) = delete;

    void consider_probe_time_locality();

    ~HashTableProbeState() {
        for (auto it = handles.begin(); it != handles.end(); it++) {
            it->destroy();
        }
        handles.clear();
    }
};

struct HashTableParam {
    bool with_other_conjunct = false;
    bool enable_late_materialization = false;
    bool enable_partition_hash_join = false;
    long column_view_concat_rows_limit = -1L;
    long column_view_concat_bytes_limit = -1L;

    TJoinOp::type join_type = TJoinOp::INNER_JOIN;
    const RowDescriptor* build_row_desc = nullptr;
    const RowDescriptor* probe_row_desc = nullptr;
    std::set<SlotId> build_output_slots;
    std::set<SlotId> probe_output_slots;
    std::set<SlotId> predicate_slots;
    std::vector<JoinKeyDesc> join_keys;

    RuntimeProfile::Counter* search_ht_timer = nullptr;
    RuntimeProfile::Counter* output_build_column_timer = nullptr;
    RuntimeProfile::Counter* output_probe_column_timer = nullptr;
    RuntimeProfile::Counter* probe_counter = nullptr;
};
} // namespace starrocks
