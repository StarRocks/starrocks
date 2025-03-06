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

#include "util/runtime_profile.h"
#define JOIN_HASH_MAP_H

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

#if defined(__aarch64__)
#include "arm_acle.h"
#endif
namespace starrocks {

class ColumnRef;

#define APPLY_FOR_JOIN_VARIANTS(M) \
    M(empty)                       \
    M(keyboolean)                  \
    M(key8)                        \
    M(key16)                       \
    M(key32)                       \
    M(key64)                       \
    M(key128)                      \
    M(keyfloat)                    \
    M(keydouble)                   \
    M(keystring)                   \
    M(keydate)                     \
    M(keydatetime)                 \
    M(keydecimal)                  \
    M(keydecimal32)                \
    M(keydecimal64)                \
    M(keydecimal128)               \
    M(slice)                       \
    M(fixed32)                     \
    M(fixed64)                     \
    M(fixed128)

enum class JoinHashMapType {
    empty,
    keyboolean,
    key8,
    key16,
    key32,
    key64,
    key128,
    keyfloat,
    keydouble,
    keystring,
    keydate,
    keydatetime,
    keydecimal,
    keydecimal32,
    keydecimal64,
    keydecimal128,
    slice,
    fixed32, // 4 bytes
    fixed64, // 8 bytes
    fixed128 // 16 bytes
};

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
    Buffer<Slice> build_slice;
    ColumnPtr build_key_column = nullptr;
    uint32_t bucket_size = 0;
    uint32_t row_count = 0; // real row count
    size_t build_column_count = 0;
    size_t output_build_column_count = 0;
    size_t lazy_output_build_column_count = 0;
    size_t probe_column_count = 0;
    size_t output_probe_column_count = 0;
    size_t lazy_output_probe_column_count = 0;
    bool with_other_conjunct = false;
    bool left_to_nullable = false;
    bool right_to_nullable = false;
    bool has_large_column = false;
    float keys_per_bucket = 0;
    size_t used_buckets = 0;
    bool cache_miss_serious = false;
    bool mor_reader_mode = false;
    bool enable_late_materialization = false;

    float get_keys_per_bucket() const { return keys_per_bucket; }
    bool ht_cache_miss_serious() const { return cache_miss_serious; }

    void calculate_ht_info(size_t key_bytes) {
        if (used_buckets == 0) { // to avoid redo
            used_buckets = SIMD::count_nonzero(first);
            keys_per_bucket = used_buckets == 0 ? 0 : row_count * 1.0 / used_buckets;
            size_t probe_bytes = key_bytes + row_count * sizeof(uint32_t);
            // cache miss is serious when
            // 1) the ht's size is enough large, for example, larger than (1UL << 27) bytes.
            // 2) smaller ht but most buckets have more than one keys
            cache_miss_serious = row_count > (1UL << 18) &&
                                 ((probe_bytes > (1UL << 25) && keys_per_bucket > 2) ||
                                  (probe_bytes > (1UL << 26) && keys_per_bucket > 1.5) || probe_bytes > (1UL << 27));
            VLOG_QUERY << "ht cache miss serious = " << cache_miss_serious << " row# = " << row_count
                       << " , bytes = " << probe_bytes << " , depth = " << keys_per_bucket;
        }
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
        ~ProbeCoroutine() {}
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
    bool mor_reader_mode = false;
};

template <class T>
struct JoinKeyHash {
    static const uint32_t CRC_SEED = 0x811C9DC5;
    std::size_t operator()(const T& value) const { return crc_hash_32(&value, sizeof(T), CRC_SEED); }
};

// The hash func used by the bucketing of the colocate table is crc,
// and the hash func used by HashJoin is also crc,
// which leads to a high conflict rate of HashJoin and affects performance.
// Therefore, there is no theoretical basis for adding an integer to the source value.
// The current test shows that the +2, +4 pair does not change the conflict rate,
// which may be related to the implementation of CRC or mod.
template <>
struct JoinKeyHash<int32_t> {
    static const uint32_t CRC_SEED = 0x811C9DC5;
    std::size_t operator()(const int32_t& value) const {
#if defined(__x86_64__) && defined(__SSE4_2__)
        size_t hash = _mm_crc32_u32(CRC_SEED, value + 2);
#elif defined(__x86_64__)
        size_t hash = crc_hash_32(&value, sizeof(value), CRC_SEED);
#else
        size_t hash = __crc32cw(CRC_SEED, value + 2);
#endif
        hash = (hash << 16u) | (hash >> 16u);
        return hash;
    }
};

template <>
struct JoinKeyHash<Slice> {
    static const uint32_t CRC_SEED = 0x811C9DC5;
    std::size_t operator()(const Slice& slice) const { return crc_hash_32(slice.data, slice.size, CRC_SEED); }
};

class JoinHashMapHelper {
public:
    // maxinum bucket size
    const static uint32_t MAX_BUCKET_SIZE = 1 << 31;

    static uint32_t calc_bucket_size(uint32_t size) {
        size_t expect_bucket_size = static_cast<size_t>(size) + (size - 1) / 4;
        // Limit the maximum hash table bucket size.
        if (expect_bucket_size >= MAX_BUCKET_SIZE) {
            return MAX_BUCKET_SIZE;
        }
        return phmap::priv::NormalizeCapacity(expect_bucket_size) + 1;
    }

    template <typename CppType>
    static uint32_t calc_bucket_num(const CppType& value, uint32_t bucket_size) {
        using HashFunc = JoinKeyHash<CppType>;

        return HashFunc()(value) & (bucket_size - 1);
    }

    template <typename CppType>
    static void calc_bucket_nums(const Buffer<CppType>& data, uint32_t bucket_size, Buffer<uint32_t>* buckets,
                                 uint32_t start, uint32_t count) {
        DCHECK(count <= buckets->size());
        for (size_t i = 0; i < count; i++) {
            (*buckets)[i] = calc_bucket_num<CppType>(data[start + i], bucket_size);
        }
    }

    static Slice get_hash_key(const Columns& key_columns, size_t row_idx, uint8_t* buffer) {
        size_t byte_size = 0;
        for (const auto& key_column : key_columns) {
            byte_size += key_column->serialize(row_idx, buffer + byte_size);
        }
        return {buffer, byte_size};
    }

    // combine keys into fixed size key by column.
    template <LogicalType LT>
    static void serialize_fixed_size_key_column(const Columns& key_columns, Column* fixed_size_key_column,
                                                uint32_t start, uint32_t count) {
        using CppType = typename RunTimeTypeTraits<LT>::CppType;
        using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

        auto& data = reinterpret_cast<ColumnType*>(fixed_size_key_column)->get_data();
        auto* buf = reinterpret_cast<uint8_t*>(&data[start]);

        const size_t byte_interval = sizeof(CppType);
        size_t byte_offset = 0;
        for (const auto& key_col : key_columns) {
            size_t offset = key_col->serialize_batch_at_interval(buf, byte_offset, byte_interval, start, count);
            byte_offset += offset;
        }
    }
};

template <LogicalType LT>
class JoinBuildFunc {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    static void prepare(RuntimeState* runtime, JoinHashTableItems* table_items);
    static const Buffer<CppType>& get_key_data(const JoinHashTableItems& table_items);
    static void construct_hash_table(RuntimeState* state, JoinHashTableItems* table_items,
                                     HashTableProbeState* probe_state);
};

template <LogicalType LT>
class DirectMappingJoinBuildFunc {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    static void prepare(RuntimeState* runtime, JoinHashTableItems* table_items);
    static const Buffer<CppType>& get_key_data(const JoinHashTableItems& table_items);
    static void construct_hash_table(RuntimeState* state, JoinHashTableItems* table_items,
                                     HashTableProbeState* probe_state);
};

template <LogicalType LT>
class FixedSizeJoinBuildFunc {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    static void prepare(RuntimeState* state, JoinHashTableItems* table_items);

    static const Buffer<CppType>& get_key_data(const JoinHashTableItems& table_items) {
        return ColumnHelper::as_raw_column<const ColumnType>(table_items.build_key_column)->get_data();
    }
    static void construct_hash_table(RuntimeState* state, JoinHashTableItems* table_items,
                                     HashTableProbeState* probe_state);

private:
    static void _build_columns(JoinHashTableItems* table_items, HashTableProbeState* probe_state,
                               const Columns& data_columns, uint32_t start, uint32_t count);

    static void _build_nullable_columns(JoinHashTableItems* table_items, HashTableProbeState* probe_state,
                                        const Columns& data_columns, const NullColumns& null_columns, uint32_t start,
                                        uint32_t count);
};

class SerializedJoinBuildFunc {
public:
    static void prepare(RuntimeState* state, JoinHashTableItems* table_items);
    static const Buffer<Slice>& get_key_data(const JoinHashTableItems& table_items) { return table_items.build_slice; }
    static void construct_hash_table(RuntimeState* state, JoinHashTableItems* table_items,
                                     HashTableProbeState* probe_state);

private:
    static void _build_columns(JoinHashTableItems* table_items, HashTableProbeState* probe_state,
                               const Columns& data_columns, uint32_t start, uint32_t count, uint8_t** ptr);

    static void _build_nullable_columns(JoinHashTableItems* table_items, HashTableProbeState* probe_state,
                                        const Columns& data_columns, const NullColumns& null_columns, uint32_t start,
                                        uint32_t count, uint8_t** ptr);
};

template <LogicalType LT>
class JoinProbeFunc {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    static void prepare(RuntimeState* state, HashTableProbeState* probe_state) {}
    static void lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state);
    static const Buffer<CppType>& get_key_data(const HashTableProbeState& probe_state);
    static bool equal(const CppType& x, const CppType& y) { return x == y; }
};

template <LogicalType LT>
class DirectMappingJoinProbeFunc {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    static void prepare(RuntimeState* state, HashTableProbeState* probe_state) {}
    static void lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state);
    static const Buffer<CppType>& get_key_data(const HashTableProbeState& probe_state);
    static bool equal(const CppType& x, const CppType& y) { return true; }
};

template <LogicalType LT>
class FixedSizeJoinProbeFunc {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<LT>::ColumnType;

    static void prepare(RuntimeState* state, HashTableProbeState* probe_state) {
        probe_state->is_nulls.resize(state->chunk_size());
        probe_state->probe_key_column = ColumnType::create(state->chunk_size());
    }

    // serialize and calculate hash values for probe keys.
    static void lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state);

    static const Buffer<CppType>& get_key_data(const HashTableProbeState& probe_state) {
        return ColumnHelper::as_raw_column<ColumnType>(probe_state.probe_key_column)->get_data();
    }

    static bool equal(const CppType& x, const CppType& y) { return x == y; }

private:
    static void _probe_column(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                              const Columns& data_columns);
    static void _probe_nullable_column(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                                       const Columns& data_columns, const NullColumns& null_columns);
};

class SerializedJoinProbeFunc {
public:
    static const Buffer<Slice>& get_key_data(const HashTableProbeState& probe_state) { return probe_state.probe_slice; }

    static void prepare(RuntimeState* state, HashTableProbeState* probe_state) {
        probe_state->probe_pool = std::make_unique<MemPool>();
        probe_state->probe_slice.resize(state->chunk_size());
        probe_state->is_nulls.resize(state->chunk_size());
    }

    static void lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state);

    static bool equal(const Slice& x, const Slice& y) { return x == y; }

private:
    static void _probe_column(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                              const Columns& data_columns, uint8_t* ptr);
    static void _probe_nullable_column(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                                       const Columns& data_columns, const NullColumns& null_columns, uint8_t* ptr);
};

// When hash table is empty, specific its implemention.
// TODO: Merge with JoinHashMap?
class JoinHashMapForEmpty {
public:
    explicit JoinHashMapForEmpty(JoinHashTableItems* table_items, HashTableProbeState* probe_state)
            : _table_items(table_items), _probe_state(probe_state) {}

    void build_prepare(RuntimeState* state) {}
    void probe_prepare(RuntimeState* state) {}
    void build(RuntimeState* state) {}
    void probe(RuntimeState* state, const Columns& key_columns, ChunkPtr* probe_chunk, ChunkPtr* chunk,
               bool* has_remain) {
        DCHECK_EQ(0, _table_items->row_count);
        *has_remain = false;
        _probe_state->match_flag = JoinMatchFlag::ALL_MATCH_ONE;
        switch (_table_items->join_type) {
        case TJoinOp::FULL_OUTER_JOIN:
        case TJoinOp::LEFT_ANTI_JOIN:
        case TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN:
        case TJoinOp::LEFT_OUTER_JOIN: {
            _probe_state->count = (*probe_chunk)->num_rows();
            _probe_output<false>(probe_chunk, chunk);
            _build_output<false>(chunk);

            if (_table_items->enable_late_materialization) {
                _probe_index_output(chunk);
            }
            break;
        }
        default: {
            break;
        }
        }
        return;
    }
    void probe_remain(RuntimeState* state, ChunkPtr* chunk, bool* has_remain) {
        // For RIGHT ANTI-JOIN, RIGHT SEMI-JOIN, FULL OUTER-JOIN, right table is empty,
        // do nothing for probe_remain.
        DCHECK_EQ(0, _table_items->row_count);
        *has_remain = false;
        return;
    }

    template <bool is_remain>
    void lazy_output(RuntimeState* state, ChunkPtr* probe_chunk, ChunkPtr* result_chunk) {
        if ((*result_chunk)->num_rows() < _probe_state->count) {
            _probe_state->match_flag = JoinMatchFlag::NORMAL;
            _probe_state->count = (*result_chunk)->num_rows();
        }

        (*result_chunk)->remove_column_by_slot_id(Chunk::HASH_JOIN_PROBE_INDEX_SLOT_ID);

        _probe_output<true>(probe_chunk, result_chunk);
        _build_output<true>(result_chunk);
        _probe_state->count = 0;
    }

private:
    template <bool is_lazy>
    void _probe_output(ChunkPtr* probe_chunk, ChunkPtr* chunk) {
        SCOPED_TIMER(_probe_state->output_probe_column_timer);
        bool to_nullable = _table_items->left_to_nullable;
        for (size_t i = 0; i < _table_items->probe_column_count; i++) {
            HashTableSlotDescriptor hash_table_slot = _table_items->probe_slots[i];
            SlotDescriptor* slot = hash_table_slot.slot;

            bool output = is_lazy ? hash_table_slot.need_lazy_materialize : hash_table_slot.need_output;
            if (output) {
                auto& column = (*probe_chunk)->get_column_by_slot_id(slot->id());
                if (!column->is_nullable()) {
                    _copy_probe_column(&column, chunk, slot, to_nullable);
                } else {
                    _copy_probe_nullable_column(&column, chunk, slot);
                }
            }
        }
    }

    void _copy_probe_column(ColumnPtr* src_column, ChunkPtr* chunk, const SlotDescriptor* slot, bool to_nullable) {
        if (_probe_state->match_flag == JoinMatchFlag::ALL_MATCH_ONE) {
            if (to_nullable) {
                MutableColumnPtr dest_column = NullableColumn::create((*src_column)->as_mutable_ptr(),
                                                                      NullColumn::create(_probe_state->count));
                (*chunk)->append_column(std::move(dest_column), slot->id());
            } else {
                (*chunk)->append_column(*src_column, slot->id());
            }
        } else {
            MutableColumnPtr dest_column = ColumnHelper::create_column(slot->type(), to_nullable);
            dest_column->append_selective(**src_column, _probe_state->probe_index.data(), 0, _probe_state->count);
            (*chunk)->append_column(std::move(dest_column), slot->id());
        }
    }

    void _copy_probe_nullable_column(ColumnPtr* src_column, ChunkPtr* chunk, const SlotDescriptor* slot) {
        if (_probe_state->match_flag == JoinMatchFlag::ALL_MATCH_ONE) {
            (*chunk)->append_column(*src_column, slot->id());
        } else {
            MutableColumnPtr dest_column = ColumnHelper::create_column(slot->type(), true);
            dest_column->append_selective(**src_column, _probe_state->probe_index.data(), 0, _probe_state->count);
            (*chunk)->append_column(std::move(dest_column), slot->id());
        }
    }

    template <bool is_lazy>
    void _build_output(ChunkPtr* chunk) {
        SCOPED_TIMER(_probe_state->output_build_column_timer);

        if (_table_items->mor_reader_mode) {
            return;
        }

        for (size_t i = 0; i < _table_items->build_column_count; i++) {
            HashTableSlotDescriptor hash_table_slot = _table_items->build_slots[i];
            SlotDescriptor* slot = hash_table_slot.slot;

            bool output = is_lazy ? hash_table_slot.need_lazy_materialize : hash_table_slot.need_output;
            if (output) {
                MutableColumnPtr dest_column = ColumnHelper::create_column(slot->type(), true);
                dest_column->append_nulls(_probe_state->count);
                (*chunk)->append_column(std::move(dest_column), slot->id());
            }
        }
    }

    void _probe_index_output(ChunkPtr* chunk) {
        _probe_state->probe_index_column->resize(_probe_state->count);
        auto* col = down_cast<UInt32Column*>(_probe_state->probe_index_column.get());
        std::iota(col->get_data().begin(), col->get_data().end(), 0);
        (*chunk)->append_column(_probe_state->probe_index_column, Chunk::HASH_JOIN_PROBE_INDEX_SLOT_ID);
    }

    JoinHashTableItems* _table_items = nullptr;
    HashTableProbeState* _probe_state = nullptr;
};

template <LogicalType LT, class BuildFunc, class ProbeFunc>
class JoinHashMap {
public:
    using CppType = typename RunTimeTypeTraits<LT>::CppType;

    explicit JoinHashMap(JoinHashTableItems* table_items, HashTableProbeState* probe_state)
            : _table_items(table_items), _probe_state(probe_state) {}

    void build_prepare(RuntimeState* state);
    void probe_prepare(RuntimeState* state);

    void build(RuntimeState* state);
    void probe(RuntimeState* state, const Columns& key_columns, ChunkPtr* probe_chunk, ChunkPtr* chunk,
               bool* has_remain);
    void probe_remain(RuntimeState* state, ChunkPtr* chunk, bool* has_remain);
    template <bool is_remain>
    void lazy_output(RuntimeState* state, ChunkPtr* probe_chunk, ChunkPtr* result_chunk);

private:
    template <bool is_lazy>
    void _probe_output(ChunkPtr* probe_chunk, ChunkPtr* chunk);
    template <bool is_lazy>
    void _probe_null_output(ChunkPtr* chunk, size_t count);

    template <bool is_lazy>
    void _build_output(ChunkPtr* chunk);
    void _build_default_output(ChunkPtr* chunk, size_t count);

    void _copy_probe_column(ColumnPtr* src_column, ChunkPtr* chunk, const SlotDescriptor* slot, bool to_nullable);

    void _copy_probe_nullable_column(ColumnPtr* src_column, ChunkPtr* chunk, const SlotDescriptor* slot);

    void _copy_build_column(const ColumnPtr& src_column, ChunkPtr* chunk, const SlotDescriptor* slot, bool to_nullable);

    void _copy_build_nullable_column(const ColumnPtr& src_column, ChunkPtr* chunk, const SlotDescriptor* slot);

    void _probe_index_output(ChunkPtr* chunk);
    void _build_index_output(ChunkPtr* chunk);

    void _search_ht(RuntimeState* state, ChunkPtr* probe_chunk);
    void _search_ht_remain(RuntimeState* state);

    template <bool first_probe>
    void _search_ht_impl(RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& data);

    // for one key inner join
    template <bool first_probe>
    void _probe_from_ht(RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data);

    HashTableProbeState::ProbeCoroutine _probe_from_ht(RuntimeState* state, const Buffer<CppType>& build_data,
                                                       const Buffer<CppType>& probe_data);

    template <bool first_probe>
    void _probe_coroutine(RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data);

    // for one key left outer join
    template <bool first_probe>
    void _probe_from_ht_for_left_outer_join(RuntimeState* state, const Buffer<CppType>& build_data,
                                            const Buffer<CppType>& probe_data);
    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_left_outer_join(RuntimeState* state,
                                                                           const Buffer<CppType>& build_data,
                                                                           const Buffer<CppType>& probe_data);
    // for one key left semi join
    template <bool first_probe>
    void _probe_from_ht_for_left_semi_join(RuntimeState* state, const Buffer<CppType>& build_data,
                                           const Buffer<CppType>& probe_data);

    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_left_semi_join(RuntimeState* state,
                                                                          const Buffer<CppType>& build_data,
                                                                          const Buffer<CppType>& probe_data);
    // for one key left anti join
    template <bool first_probe>
    void _probe_from_ht_for_left_anti_join(RuntimeState* state, const Buffer<CppType>& build_data,
                                           const Buffer<CppType>& probe_data);
    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_left_anti_join(RuntimeState* state,
                                                                          const Buffer<CppType>& build_data,
                                                                          const Buffer<CppType>& probe_data);

    // for one key right outer join
    template <bool first_probe>
    void _probe_from_ht_for_right_outer_join(RuntimeState* state, const Buffer<CppType>& build_data,
                                             const Buffer<CppType>& probe_data);
    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_right_outer_join(RuntimeState* state,
                                                                            const Buffer<CppType>& build_data,
                                                                            const Buffer<CppType>& probe_data);

    // for one key right semi join
    template <bool first_probe>
    void _probe_from_ht_for_right_semi_join(RuntimeState* state, const Buffer<CppType>& build_data,
                                            const Buffer<CppType>& probe_data);
    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_right_semi_join(RuntimeState* state,
                                                                           const Buffer<CppType>& build_data,
                                                                           const Buffer<CppType>& probe_data);

    // for one key right anti join
    template <bool first_probe>
    void _probe_from_ht_for_right_anti_join(RuntimeState* state, const Buffer<CppType>& build_data,
                                            const Buffer<CppType>& probe_data);
    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_right_anti_join(RuntimeState* state,
                                                                           const Buffer<CppType>& build_data,
                                                                           const Buffer<CppType>& probe_data);

    // for one key full outer join
    template <bool first_probe>
    void _probe_from_ht_for_full_outer_join(RuntimeState* state, const Buffer<CppType>& build_data,
                                            const Buffer<CppType>& probe_data);
    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_full_outer_join(RuntimeState* state,
                                                                           const Buffer<CppType>& build_data,
                                                                           const Buffer<CppType>& probe_data);

    // for left semi join with other join conjunct
    template <bool first_probe>
    void _probe_from_ht_for_left_semi_join_with_other_conjunct(RuntimeState* state, const Buffer<CppType>& build_data,
                                                               const Buffer<CppType>& probe_data);
    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_left_semi_join_with_other_conjunct(
            RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data);

    // for null aware anti join with other join conjunct
    template <bool first_probe>
    void _probe_from_ht_for_null_aware_anti_join_with_other_conjunct(RuntimeState* state,
                                                                     const Buffer<CppType>& build_data,
                                                                     const Buffer<CppType>& probe_data);
    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_null_aware_anti_join_with_other_conjunct(
            RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data);

    // for one key right outer join with other conjunct
    template <bool first_probe>
    void _probe_from_ht_for_right_outer_right_semi_right_anti_join_with_other_conjunct(
            RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data);
    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_right_outer_right_semi_right_anti_join_with_other_conjunct(
            RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data);

    // for one key full outer join with other join conjunct
    template <bool first_probe>
    void _probe_from_ht_for_left_outer_left_anti_full_outer_join_with_other_conjunct(RuntimeState* state,
                                                                                     const Buffer<CppType>& build_data,
                                                                                     const Buffer<CppType>& probe_data);
    HashTableProbeState::ProbeCoroutine _probe_from_ht_for_left_outer_left_anti_full_outer_join_with_other_conjunct(
            RuntimeState* state, const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data);

    JoinHashTableItems* _table_items = nullptr;
    HashTableProbeState* _probe_state = nullptr;
};

#define JoinHashMapForOneKey(LT) JoinHashMap<LT, JoinBuildFunc<LT>, JoinProbeFunc<LT>>
#define JoinHashMapForDirectMapping(LT) JoinHashMap<LT, DirectMappingJoinBuildFunc<LT>, DirectMappingJoinProbeFunc<LT>>
#define JoinHashMapForFixedSizeKey(LT) JoinHashMap<LT, FixedSizeJoinBuildFunc<LT>, FixedSizeJoinProbeFunc<LT>>
#define JoinHashMapForSerializedKey(LT) JoinHashMap<LT, SerializedJoinBuildFunc, SerializedJoinProbeFunc>

class JoinHashTable {
public:
    JoinHashTable() = default;
    ~JoinHashTable() = default;

    // Disable copy ctor and assignment.
    JoinHashTable(const JoinHashTable&) = delete;
    JoinHashTable& operator=(const JoinHashTable&) = delete;
    // Enable move ctor and move assignment.
    JoinHashTable(JoinHashTable&&) = default;
    JoinHashTable& operator=(JoinHashTable&&) = default;

    // Clone a new hash table with the same hash table as this,
    // and the different probe state from this.
    JoinHashTable clone_readable_table();
    void set_probe_profile(RuntimeProfile::Counter* search_ht_timer, RuntimeProfile::Counter* output_probe_column_timer,
                           RuntimeProfile::Counter* output_build_column_timer, RuntimeProfile::Counter* probe_counter);

    void create(const HashTableParam& param);
    void close();

    Status build(RuntimeState* state);
    void reset_probe_state(RuntimeState* state);
    Status probe(RuntimeState* state, const Columns& key_columns, ChunkPtr* probe_chunk, ChunkPtr* chunk, bool* eos);
    Status probe_remain(RuntimeState* state, ChunkPtr* chunk, bool* eos);
    template <bool is_remain>
    Status lazy_output(RuntimeState* state, ChunkPtr* probe_chunk, ChunkPtr* result_chunk);

    void append_chunk(const ChunkPtr& chunk, const Columns& key_columns);
    void merge_ht(const JoinHashTable& ht);
    // convert input column to spill schema order
    ChunkPtr convert_to_spill_schema(const ChunkPtr& chunk) const;

    const ChunkPtr& get_build_chunk() const { return _table_items->build_chunk; }
    Columns& get_key_columns() { return _table_items->key_columns; }
    const Columns& get_key_columns() const { return _table_items->key_columns; }
    uint32_t get_row_count() const { return _table_items->row_count; }
    size_t get_probe_column_count() const { return _table_items->probe_column_count; }
    size_t get_output_probe_column_count() const { return _table_items->output_probe_column_count; }
    size_t get_build_column_count() const { return _table_items->build_column_count; }
    size_t get_output_build_column_count() const { return _table_items->output_build_column_count; }
    size_t get_bucket_size() const { return _table_items->bucket_size; }
    float get_keys_per_bucket() const;
    void remove_duplicate_index(Filter* filter);
    JoinHashTableItems* table_items() const { return _table_items.get(); }

    int64_t mem_usage() const;

private:
    void _init_probe_column(const HashTableParam& param);
    void _init_build_column(const HashTableParam& param);
    void _init_mor_reader();
    void _init_join_keys();

    JoinHashMapType _choose_join_hash_map();
    static size_t _get_size_of_fixed_and_contiguous_type(LogicalType data_type);

    Status _upgrade_key_columns_if_overflow();

    void _remove_duplicate_index_for_left_outer_join(Filter* filter);
    void _remove_duplicate_index_for_left_semi_join(Filter* filter);
    void _remove_duplicate_index_for_left_anti_join(Filter* filter);
    void _remove_duplicate_index_for_right_outer_join(Filter* filter);
    void _remove_duplicate_index_for_right_semi_join(Filter* filter);
    void _remove_duplicate_index_for_right_anti_join(Filter* filter);
    void _remove_duplicate_index_for_full_outer_join(Filter* filter);

    std::unique_ptr<JoinHashMapForEmpty> _empty = nullptr;
    std::unique_ptr<JoinHashMapForDirectMapping(TYPE_BOOLEAN)> _keyboolean = nullptr;
    std::unique_ptr<JoinHashMapForDirectMapping(TYPE_TINYINT)> _key8 = nullptr;
    std::unique_ptr<JoinHashMapForDirectMapping(TYPE_SMALLINT)> _key16 = nullptr;
    std::unique_ptr<JoinHashMapForOneKey(TYPE_INT)> _key32 = nullptr;
    std::unique_ptr<JoinHashMapForOneKey(TYPE_BIGINT)> _key64 = nullptr;
    std::unique_ptr<JoinHashMapForOneKey(TYPE_LARGEINT)> _key128 = nullptr;
    std::unique_ptr<JoinHashMapForOneKey(TYPE_FLOAT)> _keyfloat = nullptr;
    std::unique_ptr<JoinHashMapForOneKey(TYPE_DOUBLE)> _keydouble = nullptr;
    std::unique_ptr<JoinHashMapForOneKey(TYPE_VARCHAR)> _keystring = nullptr;
    std::unique_ptr<JoinHashMapForOneKey(TYPE_DATE)> _keydate = nullptr;
    std::unique_ptr<JoinHashMapForOneKey(TYPE_DATETIME)> _keydatetime = nullptr;
    std::unique_ptr<JoinHashMapForOneKey(TYPE_DECIMALV2)> _keydecimal = nullptr;
    std::unique_ptr<JoinHashMapForOneKey(TYPE_DECIMAL32)> _keydecimal32 = nullptr;
    std::unique_ptr<JoinHashMapForOneKey(TYPE_DECIMAL64)> _keydecimal64 = nullptr;
    std::unique_ptr<JoinHashMapForOneKey(TYPE_DECIMAL128)> _keydecimal128 = nullptr;
    std::unique_ptr<JoinHashMapForSerializedKey(TYPE_VARCHAR)> _slice = nullptr;
    std::unique_ptr<JoinHashMapForFixedSizeKey(TYPE_INT)> _fixed32 = nullptr;
    std::unique_ptr<JoinHashMapForFixedSizeKey(TYPE_BIGINT)> _fixed64 = nullptr;
    std::unique_ptr<JoinHashMapForFixedSizeKey(TYPE_LARGEINT)> _fixed128 = nullptr;

    JoinHashMapType _hash_map_type = JoinHashMapType::empty;

    std::shared_ptr<JoinHashTableItems> _table_items;
    std::unique_ptr<HashTableProbeState> _probe_state = std::make_unique<HashTableProbeState>();
};
} // namespace starrocks

#ifndef JOIN_HASH_MAP_TPP
#include "exec/join_hash_map.tpp"
#endif

#undef JOIN_HASH_MAP_H
