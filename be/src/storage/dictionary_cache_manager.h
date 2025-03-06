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

#include <map>
#include <memory>
#include <unordered_map>

#include "column/chunk.h"
#include "column/column.h"
#include "column/datum.h"
#include "common/compiler_util.h"
#include "common/status.h"
#include "exec/dictionary_cache_writer.h"
#include "fmt/format.h"
#include "service/internal_service.h"
#include "storage/chunk_helper.h"
#include "storage/primary_key_encoder.h"
#include "storage/type_traits.h"
#include "util/phmap/phmap.h"
#include "util/xxh3.h"

namespace starrocks {

#if defined(_MSC_VER) && (defined(_M_X64) || defined(_M_IX86))
#include <mmintrin.h>
#define PREFETCH_ADDR(addr) _mm_prefetch((const char*)(addr), _MM_HINT_T0)
#elif defined(__GNUC__)
#define PREFETCH_ADDR(addr) __builtin_prefetch(static_cast<const void*>(addr), 0 /* rw==read */, 3 /* locality */)
#endif // __GNUC__

#define SKIP_DECODE_FLAG 2

enum DictionaryCacheEncoderType {
    PK_ENCODE = 0,
};

template <LogicalType logical_type>
struct DictionaryCacheTypeTraits {
    using CppType = typename RunTimeTypeTraits<logical_type>::CppType;
    using ColumnType = typename RunTimeTypeTraits<logical_type>::ColumnType;
};

template <LogicalType logical_type>
struct DictionaryCacheHashTraits {
    using CppType = typename DictionaryCacheTypeTraits<logical_type>::CppType;
    inline size_t operator()(const CppType& v) const { return StdHashWithSeed<CppType, PhmapSeed1>()(v); }
};

template <>
struct DictionaryCacheHashTraits<TYPE_VARCHAR> {
    inline size_t operator()(const Slice& v) const { return XXH3_64bits(v.data, v.size); }
};

class DictionaryCache {
public:
    DictionaryCache(DictionaryCacheEncoderType type) : _type(type) {}
    virtual ~DictionaryCache() = default;

    virtual inline Status insert(const Datum& k, const Datum& v, const uint8_t& flag) = 0;

    virtual inline Status lookup(Column* src, Column* dest, std::vector<uint8_t>& value_encode_flags,
                                 Column* null_column) = 0;

    virtual inline size_t memory_usage() = 0;

    virtual std::mutex& lock() = 0;

protected:
    DictionaryCacheEncoderType _type;
};

class DictionaryCacheUtil;
template <LogicalType KeyLogicalType, LogicalType ValueLogicalType>
class DictionaryCacheImpl final : public DictionaryCache {
public:
    static constexpr uint8_t PREFETCHN = 8;

    DictionaryCacheImpl(DictionaryCacheEncoderType type) : DictionaryCache(type), _estimated_memory_useage(0) {}
    virtual ~DictionaryCacheImpl() override = default;

    using KeyCppType = typename DictionaryCacheTypeTraits<KeyLogicalType>::CppType;
    using ValueCppType = typename DictionaryCacheTypeTraits<ValueLogicalType>::CppType;

    using ValueColumnType = typename DictionaryCacheTypeTraits<ValueLogicalType>::ColumnType;

    virtual inline Status insert(const Datum& k, const Datum& v, const uint8_t& flag) override {
        switch (_type) {
        case DictionaryCacheEncoderType::PK_ENCODE: {
            KeyCppType key;
            ValueCppType value;

            // Memory structure for string type value:
            //  Slice 1 -> |  buffer  |
            //             |buffer size|
            //  Slice 2 -> |  buffer  |
            //             |buffer size|
            //  Slice 3 -> |  buffer  |
            //             |buffer size|
            if constexpr (std::is_same_v<KeyCppType, Slice>) {
                const Slice& input = k.get<KeyCppType>();
                auto* buffer = reinterpret_cast<char*>(_pool.allocate(input.size));
                RETURN_IF_UNLIKELY_NULL(buffer, Status::MemoryAllocFailed("alloc mem for dictionary failed"));
                memcpy(buffer, input.data, input.size);
                key.data = buffer;
                key.size = input.size;
            } else {
                key = k.get<KeyCppType>();
            }

            // Memory structure for string type value:
            //             | fast decode flag |
            //  Slice 1 -> |  buffer  |
            //             |buffer size|
            //             | fast decode flag |
            //  Slice 2 -> |  buffer  |
            //             |buffer size|
            //             | fast decode flag |
            //  Slice 3 -> |  buffer  |
            //             |buffer size|
            if constexpr (std::is_same_v<ValueCppType, Slice>) {
                const Slice& input = v.get<ValueCppType>();
                size_t allocate_size = input.size + sizeof(uint8_t);
                auto* buffer = reinterpret_cast<char*>(_pool.allocate(allocate_size));
                RETURN_IF_UNLIKELY_NULL(buffer, Status::MemoryAllocFailed("alloc mem for dictionary failed"));
                memcpy(buffer, &flag, sizeof(uint8_t));
                buffer += sizeof(uint8_t);
                memcpy(buffer, input.data, input.size);
                value.data = buffer;
                value.size = input.size;
            } else {
                value = v.get<ValueCppType>();
            }

            auto r = _dictionary.insert({key, value});
            if (!r.second) {
                return Status::InternalError("duplicate key found when refreshing dictionary");
            }
            _estimated_memory_useage.fetch_add(_get_element_memory_usage<KeyCppType, ValueCppType>(key, value));
            return Status::OK();
        }
        default:
            return Status::InternalError("Unknow encoder for dictionary cache");
        }
        return Status::OK();
    }

    virtual inline Status lookup(Column* src, Column* dest, std::vector<uint8_t>& value_encode_flags,
                                 Column* null_column) override {
        switch (_type) {
        case DictionaryCacheEncoderType::PK_ENCODE: {
            size_t size = src->size();
            RETURN_IF(size == 0, Status::OK());
            const auto* raw_data = reinterpret_cast<const KeyCppType*>(src->raw_data());
            DCHECK(value_encode_flags.size() == size);
            // avoid memory reallocation when looking up hash table
            if constexpr (!std::is_same_v<ValueCppType, Slice>) {
                dest->reserve(size);
            }

            if (LIKELY(size >= 2 * PREFETCHN)) {
                size_t beg_index = 0;
                size_t loop = size / PREFETCHN;

                // use interleaving style prefetch technique to accelerate lookup, basic idea:
                // 1. prefetch N key
                // 2. prefetch N value
                // 3. actual lookup
                // 4. If the ValueCppType is Slice, save the pointer and append to dest when the
                //    lookup is finished
                size_t prefetch_hashes[PREFETCHN];
                std::vector<Slice> slices;
                size_t slice_size = 0;
                if constexpr (std::is_same_v<ValueCppType, Slice>) {
                    slices.resize(size);
                }
                for (size_t i = 0; i < loop; i++) {
                    beg_index = i * PREFETCHN;

                    if constexpr (std::is_same_v<KeyCppType, Slice>) {
                        for (size_t j = 0; j < PREFETCHN; j++) {
                            PREFETCH_ADDR(&raw_data[beg_index + j]);
                            PREFETCH_ADDR(raw_data[beg_index + j].data);
                        }
                    } else {
                        if (LIKELY(i != loop - 1)) PREFETCH_ADDR(&raw_data[beg_index + PREFETCHN]);
                        // ensure current N are cached
                        (void)raw_data[beg_index];
                    }

                    for (size_t j = 0; j < PREFETCHN; j++) {
                        prefetch_hashes[j] = DictionaryCacheHashTraits<KeyLogicalType>()(raw_data[beg_index + j]);
                        _dictionary.prefetch_hash(prefetch_hashes[j]);
                    }

                    for (size_t j = 0; j < PREFETCHN; j++) {
                        auto iter = _dictionary.find(raw_data[beg_index + j], prefetch_hashes[j]);
                        bool null_if_not_exist = false;
                        if (iter == _dictionary.end()) {
                            if (null_column == nullptr) {
                                return Status::NotFound("key not found in dictionary cache");
                            } else {
                                null_if_not_exist = true;
                            }
                        }

                        // Save the Slice instead of appending data here.
                        // Because sometime the data in Slice is relative large
                        // and it may pollutes the cpu cache which contain the
                        // prefetched content using for the following look up.
                        if constexpr (std::is_same_v<ValueCppType, Slice>) {
                            slices[beg_index + j].data = (null_if_not_exist ? nullptr : iter->second.data);
                            slices[beg_index + j].size = (null_if_not_exist ? 0 : iter->second.size);

                            slice_size += slices[beg_index + j].size;
                        } else {
                            null_if_not_exist ? _append_nullable(dest, null_column) : _append_value(dest, iter->second);
                        }
                    }
                }

                if constexpr (std::is_same_v<ValueCppType, Slice>) {
                    for (size_t i = loop * PREFETCHN; i < size; i++) {
                        auto iter = _dictionary.find(raw_data[i]);
                        bool null_if_not_exist = false;
                        if (iter == _dictionary.end()) {
                            if (null_column == nullptr) {
                                return Status::NotFound("key not found in dictionary cache");
                            } else {
                                null_if_not_exist = true;
                            }
                        }
                        slices[i].data = (null_if_not_exist ? nullptr : iter->second.data);
                        slices[i].size = (null_if_not_exist ? 0 : iter->second.size);

                        slice_size += slices[i].size;
                    }

                    // avoid memory reallocation when looking up hash table
                    down_cast<BinaryColumn*>(dest)->reserve(size, slice_size);
                    for (size_t i = 0; i < size; i++) {
                        if (slices[i].data == nullptr) {
                            value_encode_flags[i] = SKIP_DECODE_FLAG;
                            _append_nullable(dest, null_column);
                            continue;
                        }
                        value_encode_flags[i] = *(reinterpret_cast<uint8_t*>(slices[i].data) - 1);
                        _append_value(dest, slices[i]);
                    }
                } else {
                    beg_index = loop * PREFETCHN;
                    for (size_t i = beg_index; i < size; i++) {
                        auto iter = _dictionary.find(raw_data[i]);
                        bool null_if_not_exist = false;
                        if (iter == _dictionary.end()) {
                            if (null_column == nullptr) {
                                return Status::NotFound("key not found in dictionary cache");
                            } else {
                                null_if_not_exist = true;
                            }
                        }
                        null_if_not_exist ? _append_nullable(dest, null_column) : _append_value(dest, iter->second);
                        if constexpr (std::is_same_v<ValueCppType, Slice>) {
                            value_encode_flags[i] = null_if_not_exist
                                                            ? SKIP_DECODE_FLAG
                                                            : *(reinterpret_cast<uint8_t*>(iter->second.data) - 1);
                        }
                    }
                }
            } else {
                for (size_t i = 0; i < size; i++) {
                    auto iter = _dictionary.find(raw_data[i]);
                    bool null_if_not_exist = false;
                    if (iter == _dictionary.end()) {
                        if (null_column == nullptr) {
                            return Status::NotFound("key not found in dictionary cache");
                        } else {
                            null_if_not_exist = true;
                        }
                    }
                    null_if_not_exist ? _append_nullable(dest, null_column) : _append_value(dest, iter->second);
                    if constexpr (std::is_same_v<ValueCppType, Slice>) {
                        value_encode_flags[i] = null_if_not_exist
                                                        ? SKIP_DECODE_FLAG
                                                        : *(reinterpret_cast<uint8_t*>(iter->second.data) - 1);
                    }
                }
            }
            break;
        }
        default:
            return Status::InternalError("Unknow encoder for dictionary cache");
        }

        return Status::OK();
    }

    virtual inline size_t memory_usage() override { return _estimated_memory_useage.load(); }

    virtual std::mutex& lock() override { return _lock; }

private:
    // Avoid creating Datum
    inline void _append_value(Column* dest, const ValueCppType& v) { down_cast<ValueColumnType*>(dest)->append(v); }

    inline void _append_nullable(Column* dest, Column* null_column) {
        down_cast<ValueColumnType*>(dest)->append_default(1);
        down_cast<UInt8Column*>(null_column)->get_data()[dest->size() - 1] = 1;
    }

    template <class KeyCppType, class ValueCppType>
    inline size_t _get_element_memory_usage(const KeyCppType& k, const ValueCppType& v) {
        switch (_type) {
        case DictionaryCacheEncoderType::PK_ENCODE: {
            size_t size = 0;
            if constexpr (std::is_same_v<KeyCppType, Slice>) {
                size = k.size;
            } else {
                size = sizeof(KeyCppType);
            }

            if constexpr (std::is_same_v<ValueCppType, Slice>) {
                size += v.size;
            } else {
                size += sizeof(ValueCppType);
            }
            return size;
        }
        default:
            break;
        }
        return 0;
    }

    phmap::parallel_flat_hash_map<KeyCppType, ValueCppType, DictionaryCacheHashTraits<KeyLogicalType>,
                                  phmap::priv::hash_default_eq<KeyCppType>,
                                  phmap::priv::Allocator<phmap::priv::Pair<const KeyCppType, ValueCppType>>, 4,
                                  phmap::NullMutex, true>
            _dictionary;
    std::atomic<size_t> _estimated_memory_useage;
    std::mutex _lock;
    MemPool _pool;
};

using DictionaryCachePtr = std::shared_ptr<DictionaryCache>;

class DictionaryCacheUtil {
public:
    DictionaryCacheUtil() = default;
    ~DictionaryCacheUtil();

    // using primary key encoding function
    static MutableColumnPtr encode_columns(const Schema& schema, const Chunk* chunk,
                                           const DictionaryCacheEncoderType& encoder_type = PK_ENCODE) {
        switch (encoder_type) {
        case PK_ENCODE: {
            MutableColumnPtr encoded_columns;
            if (!PrimaryKeyEncoder::create_column(schema, &encoded_columns).ok()) {
                std::stringstream ss;
                ss << "create column for primary key encoder failed";
                LOG(WARNING) << ss.str();
                return nullptr;
            }
            if (chunk->num_rows() > 0) {
                PrimaryKeyEncoder::encode(schema, *chunk, 0, chunk->num_rows(), encoded_columns.get());
            }
            return encoded_columns;
        }
        default:
            break;
        }
        return nullptr;
    }

    static Status decode_columns(const Schema& schema, Column* column, Chunk* decoded_chunk,
                                 std::vector<uint8_t>* value_encode_flags,
                                 const DictionaryCacheEncoderType& encoder_type = PK_ENCODE) {
        switch (encoder_type) {
        case PK_ENCODE: {
            return PrimaryKeyEncoder::decode(schema, *column, 0, column->size(), decoded_chunk, value_encode_flags);
        }
        default:
            break;
        }
        return Status::InternalError("decode failed for dictionary");
    }

    // This function is used for precheck the string content for value columns.
    // Check if it can use fast decoding method when lookup the dictionary cache.
    static void precheck_value_encode(const Chunk* chunk, std::vector<uint8_t>& value_encode_flags) {
        bool has_varchar = false;
        for (const auto& f : chunk->schema()->fields()) {
            if (f->type()->type() == TYPE_VARCHAR) {
                has_varchar = true;
                break;
            }
        }

        if (!has_varchar) {
            return;
        }

        size_t size = chunk->num_rows();
        for (int idx = 0; idx < chunk->num_columns(); idx++) {
            if (chunk->schema()->fields()[idx]->type()->type() != TYPE_VARCHAR) {
                continue;
            }

            const auto& src = chunk->get_column_by_index(idx);
            const auto* raw_data = reinterpret_cast<const Slice*>(src->raw_data());
            for (int i = 0; i < size; i++) {
                bool contains_zero = false;
                for (int j = 0; j < raw_data[i].size; j++) {
                    if (raw_data[i][j] == '\0') {
                        contains_zero = true;
                        break;
                    }
                }
                if (value_encode_flags[i] && contains_zero) {
                    value_encode_flags[i] = 0;
                }
            }
        }
    }

    static LogicalType get_encoded_type(const Schema& schema,
                                        const DictionaryCacheEncoderType& encoder_type = PK_ENCODE) {
        switch (encoder_type) {
        case PK_ENCODE: {
            std::vector<uint32_t> idxes;
            for (uint32_t i = 0; i < schema.fields().size(); i++) {
                idxes.push_back((uint32_t)i);
            }
            return PrimaryKeyEncoder::encoded_primary_key_type(schema, idxes);
        }
        default:
            break;
        }
        return TYPE_NONE;
    }

    static DictionaryCachePtr create_dictionary_cache(const std::pair<LogicalType, LogicalType>& type,
                                                      const DictionaryCacheEncoderType& encoder_type = PK_ENCODE) {
        switch (encoder_type) {
        case PK_ENCODE: {
#define IF_TYPE(key_type, value_type)                 \
    if (type == std::make_pair(key_type, value_type)) \
    return std::make_shared<DictionaryCacheImpl<key_type, value_type>>(encoder_type)

            IF_TYPE(TYPE_BOOLEAN, TYPE_BOOLEAN);
            IF_TYPE(TYPE_BOOLEAN, TYPE_TINYINT);
            IF_TYPE(TYPE_BOOLEAN, TYPE_SMALLINT);
            IF_TYPE(TYPE_BOOLEAN, TYPE_INT);
            IF_TYPE(TYPE_BOOLEAN, TYPE_BIGINT);
            IF_TYPE(TYPE_BOOLEAN, TYPE_LARGEINT);
            IF_TYPE(TYPE_BOOLEAN, TYPE_VARCHAR);
            IF_TYPE(TYPE_BOOLEAN, TYPE_DATE);
            IF_TYPE(TYPE_BOOLEAN, TYPE_DATETIME);

            IF_TYPE(TYPE_TINYINT, TYPE_BOOLEAN);
            IF_TYPE(TYPE_TINYINT, TYPE_TINYINT);
            IF_TYPE(TYPE_TINYINT, TYPE_SMALLINT);
            IF_TYPE(TYPE_TINYINT, TYPE_INT);
            IF_TYPE(TYPE_TINYINT, TYPE_BIGINT);
            IF_TYPE(TYPE_TINYINT, TYPE_LARGEINT);
            IF_TYPE(TYPE_TINYINT, TYPE_VARCHAR);
            IF_TYPE(TYPE_TINYINT, TYPE_DATE);
            IF_TYPE(TYPE_TINYINT, TYPE_DATETIME);

            IF_TYPE(TYPE_SMALLINT, TYPE_BOOLEAN);
            IF_TYPE(TYPE_SMALLINT, TYPE_TINYINT);
            IF_TYPE(TYPE_SMALLINT, TYPE_SMALLINT);
            IF_TYPE(TYPE_SMALLINT, TYPE_INT);
            IF_TYPE(TYPE_SMALLINT, TYPE_BIGINT);
            IF_TYPE(TYPE_SMALLINT, TYPE_LARGEINT);
            IF_TYPE(TYPE_SMALLINT, TYPE_VARCHAR);
            IF_TYPE(TYPE_SMALLINT, TYPE_DATE);
            IF_TYPE(TYPE_SMALLINT, TYPE_DATETIME);

            IF_TYPE(TYPE_INT, TYPE_BOOLEAN);
            IF_TYPE(TYPE_INT, TYPE_TINYINT);
            IF_TYPE(TYPE_INT, TYPE_SMALLINT);
            IF_TYPE(TYPE_INT, TYPE_INT);
            IF_TYPE(TYPE_INT, TYPE_BIGINT);
            IF_TYPE(TYPE_INT, TYPE_LARGEINT);
            IF_TYPE(TYPE_INT, TYPE_VARCHAR);
            IF_TYPE(TYPE_INT, TYPE_DATE);
            IF_TYPE(TYPE_INT, TYPE_DATETIME);

            IF_TYPE(TYPE_BIGINT, TYPE_BOOLEAN);
            IF_TYPE(TYPE_BIGINT, TYPE_TINYINT);
            IF_TYPE(TYPE_BIGINT, TYPE_SMALLINT);
            IF_TYPE(TYPE_BIGINT, TYPE_INT);
            IF_TYPE(TYPE_BIGINT, TYPE_BIGINT);
            IF_TYPE(TYPE_BIGINT, TYPE_LARGEINT);
            IF_TYPE(TYPE_BIGINT, TYPE_VARCHAR);
            IF_TYPE(TYPE_BIGINT, TYPE_DATE);
            IF_TYPE(TYPE_BIGINT, TYPE_DATETIME);

            IF_TYPE(TYPE_LARGEINT, TYPE_BOOLEAN);
            IF_TYPE(TYPE_LARGEINT, TYPE_TINYINT);
            IF_TYPE(TYPE_LARGEINT, TYPE_SMALLINT);
            IF_TYPE(TYPE_LARGEINT, TYPE_INT);
            IF_TYPE(TYPE_LARGEINT, TYPE_BIGINT);
            IF_TYPE(TYPE_LARGEINT, TYPE_LARGEINT);
            IF_TYPE(TYPE_LARGEINT, TYPE_VARCHAR);
            IF_TYPE(TYPE_LARGEINT, TYPE_DATE);
            IF_TYPE(TYPE_LARGEINT, TYPE_DATETIME);

            IF_TYPE(TYPE_VARCHAR, TYPE_BOOLEAN);
            IF_TYPE(TYPE_VARCHAR, TYPE_TINYINT);
            IF_TYPE(TYPE_VARCHAR, TYPE_SMALLINT);
            IF_TYPE(TYPE_VARCHAR, TYPE_INT);
            IF_TYPE(TYPE_VARCHAR, TYPE_BIGINT);
            IF_TYPE(TYPE_VARCHAR, TYPE_LARGEINT);
            IF_TYPE(TYPE_VARCHAR, TYPE_VARCHAR);
            IF_TYPE(TYPE_VARCHAR, TYPE_DATE);
            IF_TYPE(TYPE_VARCHAR, TYPE_DATETIME);

            IF_TYPE(TYPE_DATE, TYPE_BOOLEAN);
            IF_TYPE(TYPE_DATE, TYPE_TINYINT);
            IF_TYPE(TYPE_DATE, TYPE_SMALLINT);
            IF_TYPE(TYPE_DATE, TYPE_INT);
            IF_TYPE(TYPE_DATE, TYPE_BIGINT);
            IF_TYPE(TYPE_DATE, TYPE_LARGEINT);
            IF_TYPE(TYPE_DATE, TYPE_VARCHAR);
            IF_TYPE(TYPE_DATE, TYPE_DATE);
            IF_TYPE(TYPE_DATE, TYPE_DATETIME);

            IF_TYPE(TYPE_DATETIME, TYPE_BOOLEAN);
            IF_TYPE(TYPE_DATETIME, TYPE_TINYINT);
            IF_TYPE(TYPE_DATETIME, TYPE_SMALLINT);
            IF_TYPE(TYPE_DATETIME, TYPE_INT);
            IF_TYPE(TYPE_DATETIME, TYPE_BIGINT);
            IF_TYPE(TYPE_DATETIME, TYPE_LARGEINT);
            IF_TYPE(TYPE_DATETIME, TYPE_VARCHAR);
            IF_TYPE(TYPE_DATETIME, TYPE_DATE);
            IF_TYPE(TYPE_DATETIME, TYPE_DATETIME);

#undef IF_TYPE
            break;
        }
        default:
            break;
        }
        return nullptr;
    }
};

/*
    Manager all dictionary cache in current BE node,
    the read/write for the cache satisfy the following properties:
    1. WRITE is atomic for a REFRESH request, no partial write at all
    2. Read is consistent between all BEs(with the cache) in a single transaction
*/
class DictionaryCacheManager {
public:
    using DictionaryId = int64_t;
    // DictionaryCacheTxnId is the monotonically increasing id allocated by FE.
    // It is used to identify every single dictionary cache refresh task.
    // DictionaryCacheTxnId is globally unique cross all dictionaries.
    using DictionaryCacheTxnId = int64_t;
    // DictionaryCacheTxnId -> DictionaryCache, ordered by DictionaryCacheTxnId for the same dictionary
    using OrderedMutableDictionaryCache = std::map<DictionaryCacheTxnId, DictionaryCachePtr>;

    DictionaryCacheManager() = default;
    ~DictionaryCacheManager() = default;

    DictionaryCacheManager(const DictionaryCacheManager&) = delete;
    const DictionaryCacheManager& operator=(const DictionaryCacheManager&) = delete;

    Status begin(const PProcessDictionaryCacheRequest* request);

    Status refresh(const PProcessDictionaryCacheRequest* request);

    Status commit(const PProcessDictionaryCacheRequest* request);

    void clear(DictionaryId dict_id, bool is_cancel = false /* trigger cancel */);

    void get_info(DictionaryId dict_id, PProcessDictionaryCacheResult& response);

    SchemaPtr get_dictionary_schema_by_id(const DictionaryId& dict_id);
    StatusOr<DictionaryCachePtr> get_dictionary_by_version(const DictionaryId& dict_id,
                                                           const DictionaryCacheTxnId& txn_id);

    inline static Status probe_given_dictionary_cache(const Schema& key_schema, const Schema& value_schema,
                                                      DictionaryCachePtr dictionary, const ChunkPtr& key_chunk,
                                                      ChunkPtr& value_chunk, Column* null_column) {
        DCHECK(value_chunk->num_rows() == 0);
        size_t size = key_chunk->num_rows();

        auto encoded_key_column = DictionaryCacheUtil::encode_columns(key_schema, key_chunk.get());
        auto encoded_value_column = DictionaryCacheUtil::encode_columns(value_schema, value_chunk.get());

        if (encoded_key_column == nullptr || encoded_value_column == nullptr) {
            return Status::InternalError("encode dictionary cache column failed when probing the dictionary cache");
        }

        std::vector<uint8_t> value_encode_flags(size, 1);
        RETURN_IF_ERROR(dictionary->lookup(encoded_key_column.get(), encoded_value_column.get(), value_encode_flags,
                                           null_column));
        DCHECK(encoded_value_column->size() == size);

        return DictionaryCacheUtil::decode_columns(value_schema, encoded_value_column.get(), value_chunk.get(),
                                                   &value_encode_flags);
    }

private:
    Status _refresh_encoded_chunk(DictionaryId dict_id, DictionaryCacheTxnId txn_id, const Column* encoded_key_column,
                                  const Column* encoded_value_column, const SchemaPtr& schema,
                                  LogicalType key_encoded_type, LogicalType value_encoded_type, long memory_limit,
                                  const std::vector<uint8_t>& value_encode_flags);

    // dictionary id -> DictionaryCache
    std::unordered_map<DictionaryId, DictionaryCachePtr> _dict_cache;
    // dictionary id -> cache version number
    std::unordered_map<DictionaryId, long> _dict_cache_versions;
    // protect _dict_cache && _dict_cache_versions
    std::shared_mutex _lock;

    // dictionary id -> dictionary cache schema (pre-encoded full schema), used for expression evaluation
    std::unordered_map<DictionaryId, SchemaPtr> _dict_cache_schema;
    // protext _dict_cache_schema
    std::shared_mutex _schema_lock;

    // dictionary id -> OrderedMutableDcitionaryCache
    // _mutable_dict_caches It is used to save the intermediate
    // results of the refresh and perform atomic replacement when completed.
    // Why we need OrderedMutableDictionaryCache for every dictionary:
    // For a certain dictionary, there may be multiple caches that cache intermediate results
    // which are ordered by DictionaryCacheTxnId. Because the dictionary cache sink will be failed or
    // cancelled so that the intermediate results cache will be left. If we must alloc a new memory
    // intermediate cache for another retry task with the larger txn id. If the task success, we should
    // swap the immutable cache using the mutable cache with the max txn id. So we need the ordered
    // data structure to maintance the mutable cache.
    std::unordered_map<DictionaryId, std::shared_ptr<OrderedMutableDictionaryCache>> _mutable_dict_caches;
    // indicate dictionary is cancelled or not, this should be erase before return error status
    std::set<DictionaryId> _dict_cancel;
    // protect _mutable_dict_caches && _dict_cancel
    std::shared_mutex _refresh_lock;
};

} // namespace starrocks