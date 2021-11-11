// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <gen_cpp/PlanNodes_types.h>
#include <runtime/descriptors.h>
#include <runtime/runtime_state.h>

#include "column/chunk.h"
#include "column/column_hash.h"
#include "column/column_helper.h"
#include "util/phmap/phmap.h"

#if defined(__aarch64__)
#include "arm_acle.h"
#endif
namespace starrocks::vectorized {

#define APPLY_FOR_JOIN_VARIANTS(M) \
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
    PrimitiveType type;
    bool is_null_safe_equal;
};

struct JoinHashTableItems {
    //TODO: memory continus problem?
    ChunkPtr build_chunk = nullptr;
    Columns key_columns;
    Buffer<SlotDescriptor*> build_slots;
    Buffer<SlotDescriptor*> probe_slots;
    Buffer<TupleId> output_build_tuple_ids;
    Buffer<TupleId> output_probe_tuple_ids;
    const RowDescriptor* row_desc;
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
    ColumnPtr build_key_column;
    uint32_t bucket_size = 0;
    uint32_t row_count = 0; // real row count
    size_t build_column_count = 0;
    size_t probe_column_count = 0;
    bool with_other_conjunct = false;
    bool left_to_nullable = false;
    bool right_to_nullable = false;

    TJoinOp::type join_type = TJoinOp::INNER_JOIN;

    std::unique_ptr<MemPool> build_pool = nullptr;
    std::unique_ptr<MemPool> probe_pool = nullptr;
    std::vector<JoinKeyDesc> join_keys;

    RuntimeProfile::Counter* search_ht_timer = nullptr;
    RuntimeProfile::Counter* output_build_column_timer = nullptr;
    RuntimeProfile::Counter* output_probe_column_timer = nullptr;
    RuntimeProfile::Counter* output_tuple_column_timer = nullptr;
};

struct HashTableProbeState {
    //TODO: memory release
    Buffer<uint8_t> is_nulls;
    Buffer<uint32_t> buckets;
    Buffer<uint32_t> build_index;
    Buffer<uint32_t> probe_index;
    Buffer<uint32_t> next;
    Buffer<Slice> probe_slice;
    Buffer<uint8_t>* null_array = nullptr;
    ColumnPtr probe_key_column;
    const Columns* key_columns = nullptr;
    std::vector<JoinKeyDesc> join_keys;

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

    // true: generated chunk has null build tuple.
    // e.g. left join and there is not matched row in build table
    bool has_null_build_tuple = false;

    bool has_remain = false;
    // When one-to-many, one probe may not be able to probe all the data,
    // cur_probe_index records the position of the last probe
    uint32_t cur_probe_index = 0;
    uint32_t cur_row_match_count = 0;
};

struct HashTableParam {
    bool with_other_conjunct = false;
    TJoinOp::type join_type = TJoinOp::INNER_JOIN;
    const RowDescriptor* row_desc = nullptr;
    const RowDescriptor* build_row_desc = nullptr;
    const RowDescriptor* probe_row_desc = nullptr;
    std::vector<JoinKeyDesc> join_keys;

    RuntimeProfile::Counter* search_ht_timer = nullptr;
    RuntimeProfile::Counter* output_build_column_timer = nullptr;
    RuntimeProfile::Counter* output_probe_column_timer = nullptr;
    RuntimeProfile::Counter* output_tuple_column_timer = nullptr;
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
#if defined __x86_64__
        size_t hash = _mm_crc32_u32(CRC_SEED, value + 2);
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

template <typename T>
struct JoinKeyEqual {
    bool operator()(const T& x, const T& y) const { return x == y; }
};

template <>
struct JoinKeyEqual<Slice> {
    bool operator()(const Slice& x, const Slice& y) const {
        return (x.size == y.size) && (memcmp(x.data, y.data, x.size) == 0);
    }
};

class JoinHashMapHelper {
public:
    static uint32_t calc_bucket_size(uint32_t size) {
        size = size + (size - 1) / 7;
        return phmap::priv::NormalizeCapacity(size) + 1;
    }

    template <typename CppType>
    static uint32_t calc_bucket_num(const CppType& value, uint32_t bucket_size) {
        using HashFunc = JoinKeyHash<CppType>;

        return HashFunc()(value) & (bucket_size - 1);
    }

    template <typename CppType>
    static void calc_bucket_nums(const Buffer<CppType>& data, uint32_t bucket_size, Buffer<uint32_t>* buckets,
                                 uint32_t start, uint32_t count) {
        for (size_t i = 0; i < count; i++) {
            (*buckets)[i] = calc_bucket_num<CppType>(data[start + i], bucket_size);
        }
    }

    static void prepare_map_index(HashTableProbeState* probe_state) {
        probe_state->build_index.resize(config::vector_chunk_size + 8);
        probe_state->probe_index.resize(config::vector_chunk_size + 8);
        probe_state->next.resize(config::vector_chunk_size);
        probe_state->probe_match_index.resize(config::vector_chunk_size);
        probe_state->probe_match_filter.resize(config::vector_chunk_size);
        probe_state->buckets.resize(config::vector_chunk_size);
    }

    static Slice get_hash_key(const Columns& key_columns, size_t row_idx, uint8_t* buffer) {
        size_t byte_size = 0;
        for (const auto& key_column : key_columns) {
            byte_size += key_column->serialize(row_idx, buffer + byte_size);
        }
        return {buffer, byte_size};
    }

    // combine keys into fixed size key by column.
    template <PrimitiveType PT>
    static void serialize_fixed_size_key_column(const Columns& key_columns, Column* fixed_size_key_column,
                                                uint32_t start, uint32_t count) {
        using CppType = typename RunTimeTypeTraits<PT>::CppType;
        using ColumnType = typename RunTimeTypeTraits<PT>::ColumnType;

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

template <PrimitiveType PT>
class JoinBuildFunc {
public:
    using CppType = typename RunTimeTypeTraits<PT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<PT>::ColumnType;

    static Status prepare([[maybe_unused]] RuntimeState* runtime, [[maybe_unused]] JoinHashTableItems* table_items,
                          [[maybe_unused]] HashTableProbeState* probe_state) {
        return Status::OK();
    }

    static const Buffer<CppType>& get_key_data(const JoinHashTableItems& table_items);
    static Status construct_hash_table(JoinHashTableItems* table_items, HashTableProbeState* probe_state);
};

template <PrimitiveType PT>
class FixedSizeJoinBuildFunc {
public:
    using CppType = typename RunTimeTypeTraits<PT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<PT>::ColumnType;

    static Status prepare(RuntimeState* state, JoinHashTableItems* table_items, HashTableProbeState* probe_state);

    static const Buffer<CppType>& get_key_data(const JoinHashTableItems& table_items) {
        return ColumnHelper::as_raw_column<const ColumnType>(table_items.build_key_column)->get_data();
    }
    static Status construct_hash_table(JoinHashTableItems* table_items, HashTableProbeState* probe_state);

private:
    static void _build_columns(JoinHashTableItems* table_items, HashTableProbeState* probe_state,
                               const Columns& data_columns, uint32_t start, uint32_t count);

    static void _build_nullable_columns(JoinHashTableItems* table_items, HashTableProbeState* probe_state,
                                        const Columns& data_columns, const NullColumns& null_columns, uint32_t start,
                                        uint32_t count);
};

class SerializedJoinBuildFunc {
public:
    static Status prepare(RuntimeState* state, JoinHashTableItems* table_items, HashTableProbeState* probe_state);
    static const Buffer<Slice>& get_key_data(const JoinHashTableItems& table_items) { return table_items.build_slice; }
    static Status construct_hash_table(JoinHashTableItems* table_items, HashTableProbeState* probe_state);

private:
    static void _build_columns(JoinHashTableItems* table_items, HashTableProbeState* probe_state,
                               const Columns& data_columns, uint32_t start, uint32_t count, uint8_t** ptr);

    static void _build_nullable_columns(JoinHashTableItems* table_items, HashTableProbeState* probe_state,
                                        const Columns& data_columns, const NullColumns& null_columns, uint32_t start,
                                        uint32_t count, uint8_t** ptr);
};

template <PrimitiveType PT>
class JoinProbeFunc {
public:
    using CppType = typename RunTimeTypeTraits<PT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<PT>::ColumnType;

    static void prepare(JoinHashTableItems* table_items, HashTableProbeState* probe_state) {}

    static Status lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state);

    static const Buffer<CppType>& get_key_data(const HashTableProbeState& probe_state);
};

template <PrimitiveType PT>
class FixedSizeJoinProbeFunc {
public:
    using CppType = typename RunTimeTypeTraits<PT>::CppType;
    using ColumnType = typename RunTimeTypeTraits<PT>::ColumnType;

    static Status prepare(JoinHashTableItems* table_items, HashTableProbeState* probe_state) {
        probe_state->probe_key_column = ColumnType::create(probe_state->probe_row_count);
        probe_state->is_nulls.resize(config::vector_chunk_size);
        return Status::OK();
    }

    // serialize and calculate hash values for probe keys.
    static Status lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state);

    static const Buffer<CppType>& get_key_data(const HashTableProbeState& probe_state) {
        return ColumnHelper::as_raw_column<ColumnType>(probe_state.probe_key_column)->get_data();
    }

private:
    static void _probe_column(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                              const Columns& data_columns);
    static void _probe_nullable_column(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                                       const Columns& data_columns, const NullColumns& null_columns);
};

class SerializedJoinProbeFunc {
public:
    static const Buffer<Slice>& get_key_data(const HashTableProbeState& probe_state) { return probe_state.probe_slice; }

    static void prepare(JoinHashTableItems* table_items, HashTableProbeState* probe_state) {
        table_items->probe_pool->clear();
        probe_state->probe_slice.resize(probe_state->probe_row_count);
        probe_state->is_nulls.resize(config::vector_chunk_size);
    }

    static Status lookup_init(const JoinHashTableItems& table_items, HashTableProbeState* probe_state);

private:
    static void _probe_column(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                              const Columns& data_columns, uint8_t* ptr);
    static void _probe_nullable_column(const JoinHashTableItems& table_items, HashTableProbeState* probe_state,
                                       const Columns& data_columns, const NullColumns& null_columns, uint8_t* ptr);
};

template <PrimitiveType PT, class BuildFunc, class ProbeFunc>
class JoinHashMap {
public:
    using CppType = typename RunTimeTypeTraits<PT>::CppType;

    explicit JoinHashMap(JoinHashTableItems* table_items, HashTableProbeState* probe_state)
            : _table_items(table_items), _probe_state(probe_state) {}

    Status build(RuntimeState* state);
    Status probe(const Columns& key_columns, ChunkPtr* probe_chunk, ChunkPtr* chunk, bool* has_remain);
    Status probe_remain(ChunkPtr* chunk, bool* has_remain);

private:
    Status _probe_output(ChunkPtr* probe_chunk, ChunkPtr* chunk);
    void _probe_tuple_output(ChunkPtr* probe_chunk, ChunkPtr* chunk);
    Status _probe_null_output(ChunkPtr* chunk, size_t count);

    Status _build_output(ChunkPtr* chunk);
    void _build_tuple_output(ChunkPtr* chunk);
    Status _build_default_output(ChunkPtr* chunk, size_t count);

    void _copy_probe_column(ColumnPtr* src_column, ChunkPtr* chunk, const SlotDescriptor* slot, bool to_nullable);

    void _copy_probe_nullable_column(ColumnPtr* src_column, ChunkPtr* chunk, const SlotDescriptor* slot);

    void _copy_build_column(const ColumnPtr& src_column, ChunkPtr* chunk, const SlotDescriptor* slot, bool to_nullable);

    void _copy_build_nullable_column(const ColumnPtr& src_column, ChunkPtr* chunk, const SlotDescriptor* slot);

    Status _search_ht(ChunkPtr* probe_chunk);
    void _search_ht_remain();

    template <bool first_probe>
    void _search_ht_impl(const Buffer<CppType>& build_data, const Buffer<CppType>& data);

    // for one key inner join
    template <bool first_probe>
    void _probe_from_ht(const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data);

    // for one key left outer join
    template <bool first_probe>
    void _probe_from_ht_for_left_outer_join(const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data);

    // for one key left semi join
    template <bool first_probe>
    void _probe_from_ht_for_left_semi_join(const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data);

    // for one key left anti join
    template <bool first_probe>
    void _probe_from_ht_for_left_anti_join(const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data);

    // for one key right outer join
    template <bool first_probe>
    void _probe_from_ht_for_right_outer_join(const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data);

    // for one key right semi join
    template <bool first_probe>
    void _probe_from_ht_for_right_semi_join(const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data);

    // for one key right anti join
    template <bool first_probe>
    void _probe_from_ht_for_right_anti_join(const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data);

    // for one key full outer join
    template <bool first_probe>
    void _probe_from_ht_for_full_outer_join(const Buffer<CppType>& build_data, const Buffer<CppType>& probe_data);

    // for left outer join with other join conjunct
    template <bool first_probe>
    void _probe_from_ht_for_left_outer_join_with_other_conjunct(const Buffer<CppType>& build_data,
                                                                const Buffer<CppType>& probe_data);

    // for left semi join with other join conjunct
    template <bool first_probe>
    void _probe_from_ht_for_left_semi_join_with_other_conjunct(const Buffer<CppType>& build_data,
                                                               const Buffer<CppType>& probe_data);

    // for left anti join with other join conjunct
    template <bool first_probe>
    void _probe_from_ht_for_left_anti_join_with_other_conjunct(const Buffer<CppType>& build_data,
                                                               const Buffer<CppType>& probe_data);

    // for one key right outer join with other conjunct
    template <bool first_probe>
    void _probe_from_ht_for_right_outer_join_with_other_conjunct(const Buffer<CppType>& build_data,
                                                                 const Buffer<CppType>& probe_data);

    // for one key right semi join with other join conjunct
    template <bool first_probe>
    void _probe_from_ht_for_right_semi_join_with_other_conjunct(const Buffer<CppType>& build_data,
                                                                const Buffer<CppType>& probe_data);

    // for one key right anti join with other join conjunct
    template <bool first_probe>
    void _probe_from_ht_for_right_anti_join_with_other_conjunct(const Buffer<CppType>& build_data,
                                                                const Buffer<CppType>& probe_data);

    // for one key full outer join with other join conjunct
    template <bool first_probe>
    void _probe_from_ht_for_full_outer_join_with_other_conjunct(const Buffer<CppType>& build_data,
                                                                const Buffer<CppType>& probe_data);

    JoinHashTableItems* _table_items = nullptr;
    HashTableProbeState* _probe_state = nullptr;
};

#define JoinHashMapForOneKey(PT) JoinHashMap<PT, JoinBuildFunc<PT>, JoinProbeFunc<PT>>
#define JoinHashMapForFixedSizeKey(PT) JoinHashMap<PT, FixedSizeJoinBuildFunc<PT>, FixedSizeJoinProbeFunc<PT>>
#define JoinHashMapForSerializedKey(PT) JoinHashMap<PT, SerializedJoinBuildFunc, SerializedJoinProbeFunc>

class JoinHashTable {
public:
    ~JoinHashTable();

    void create(const HashTableParam& param);
    void close();

    Status build(RuntimeState* state);
    Status probe(const Columns& key_columns, ChunkPtr* probe_chunk, ChunkPtr* chunk, bool* eos);
    Status probe_remain(ChunkPtr* chunk, bool* eos);

    Status append_chunk(RuntimeState* state, const ChunkPtr& chunk);

    const ChunkPtr& get_build_chunk() const { return _table_items.build_chunk; }
    Columns& get_key_columns() { return _table_items.key_columns; }
    uint32_t get_row_count() const { return _table_items.row_count; }
    size_t get_probe_column_count() const { return _table_items.probe_column_count; }
    size_t get_build_column_count() const { return _table_items.build_column_count; }
    size_t get_bucket_size() const { return _table_items.bucket_size; }

    void remove_duplicate_index(Column::Filter* filter);

    int64_t mem_usage() {
        int64_t usage = 0;
        if (_table_items.build_chunk != nullptr) {
            usage += _table_items.build_chunk->memory_usage();
        }
        usage += _table_items.first.capacity() * sizeof(uint32_t);
        usage += _table_items.next.capacity() * sizeof(uint32_t);
        if (_table_items.build_pool != nullptr) {
            usage += _table_items.build_pool->total_reserved_bytes();
        }
        if (_table_items.probe_pool != nullptr) {
            usage += _table_items.probe_pool->total_reserved_bytes();
        }
        if (_table_items.build_key_column != nullptr) {
            usage += _table_items.build_key_column->memory_usage();
        }
        usage += _table_items.build_slice.size() * sizeof(Slice);
        return usage;
    }

private:
    JoinHashMapType _choose_join_hash_map();
    static size_t _get_size_of_fixed_and_contiguous_type(PrimitiveType data_type);

    void _remove_duplicate_index_for_left_outer_join(Column::Filter* filter);
    void _remove_duplicate_index_for_left_semi_join(Column::Filter* filter);
    void _remove_duplicate_index_for_left_anti_join(Column::Filter* filter);
    void _remove_duplicate_index_for_right_outer_join(Column::Filter* filter);
    void _remove_duplicate_index_for_right_semi_join(Column::Filter* filter);
    void _remove_duplicate_index_for_right_anti_join(Column::Filter* filter);
    void _remove_duplicate_index_for_full_outer_join(Column::Filter* filter);

    std::unique_ptr<JoinHashMapForOneKey(TYPE_BOOLEAN)> _keyboolean = nullptr;
    std::unique_ptr<JoinHashMapForOneKey(TYPE_TINYINT)> _key8 = nullptr;
    std::unique_ptr<JoinHashMapForOneKey(TYPE_SMALLINT)> _key16 = nullptr;
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

    JoinHashTableItems _table_items;
    HashTableProbeState _probe_state;
};
} // namespace starrocks::vectorized

#include "exec/vectorized/join_hash_map.tpp"