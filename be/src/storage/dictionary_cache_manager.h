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
#include "common/status.h"
#include "exec/dictionary_cache_writer.h"
#include "fmt/format.h"
#include "service/internal_service.h"
#include "storage/chunk_helper.h"
#include "storage/primary_key_encoder.h"
#include "storage/row_store_encoder_factory.h"
#include "storage/row_store_encoder_simple.h"
#include "util/faststring.h"
#include "util/phmap/phmap.h"
#include "util/xxh3.h"

namespace starrocks {

enum DictionaryCacheEncoderType {
    ROW_STORE_SIMPLE = 0,
};

template <LogicalType logical_type>
struct DictionaryCacheTypeTraits {
    using CppType = typename RunTimeTypeTraits<logical_type>::CppType;
};

template <>
struct DictionaryCacheTypeTraits<TYPE_VARCHAR> {
    using CppType = std::string;
};

template <LogicalType logical_type>
struct DictionaryCacheHashTraits {
    using CppType = typename DictionaryCacheTypeTraits<logical_type>::CppType;
    inline size_t operator()(const CppType& v) const { return StdHashWithSeed<CppType, PhmapSeed1>()(v); }
};

template <>
struct DictionaryCacheHashTraits<TYPE_VARCHAR> {
    inline size_t operator()(const std::string& v) const { return XXH3_64bits(v.data(), v.length()); }
};

class DictionaryCache {
public:
    DictionaryCache(DictionaryCacheEncoderType type) : _type(type) {}
    virtual ~DictionaryCache() = default;

    virtual inline Status insert(const Datum& k, const Datum& v) = 0;

    virtual inline Status lookup(const Datum& k, Column* dest) = 0;

    virtual inline size_t memory_usage() = 0;

protected:
    DictionaryCacheEncoderType _type;
};

template <LogicalType KeyLogicalType, LogicalType ValueLogicalType>
class DictionaryCacheImpl final : public DictionaryCache {
public:
    DictionaryCacheImpl(DictionaryCacheEncoderType type) : DictionaryCache(type), _total_memory_useage(0) {}
    virtual ~DictionaryCacheImpl() override = default;

    using KeyCppType = typename DictionaryCacheTypeTraits<KeyLogicalType>::CppType;
    using ValueCppType = typename DictionaryCacheTypeTraits<ValueLogicalType>::CppType;

    virtual inline Status insert(const Datum& k, const Datum& v) override {
        KeyCppType key;
        ValueCppType value;
        _get<KeyCppType>(k, key);
        _get<ValueCppType>(v, value);
        auto r = _dictionary.insert({key, value});
        if (!r.second) {
            return Status::InternalError("duplicate key found when refreshing dictionary");
        }
        _total_memory_useage.fetch_add(_get_element_memory_usage<KeyCppType, ValueCppType>(key, value));
        return Status::OK();
    }

    virtual inline Status lookup(const Datum& k, Column* dest) override {
        KeyCppType key;
        _get<KeyCppType>(k, key);
        auto iter = _dictionary.find(key);
        if (iter == _dictionary.end()) {
            return Status::NotFound("key not found in dictionary cache");
        }
        dest->append_datum(*_get_datum(iter->second));
        return Status::OK();
    }

    virtual inline size_t memory_usage() override { return _total_memory_useage.load(); }

private:
    template <class CppType>
    inline void _get(const Datum& d, CppType& v) {
        switch (_type) {
        case DictionaryCacheEncoderType::ROW_STORE_SIMPLE: {
            v = d.get<Slice>().to_string();
            break;
        }
        default:
            break;
        }
        return;
    }

    inline std::shared_ptr<Datum> _get_datum(const ValueCppType& v) {
        switch (_type) {
        case DictionaryCacheEncoderType::ROW_STORE_SIMPLE: {
            static_assert(std::is_same_v<ValueCppType, std::string>);
            return std::make_shared<Datum>(Slice(v));
        }
        default:
            break;
        }
        return nullptr;
    }

    template <class KeyCppType, class ValueCppType>
    inline size_t _get_element_memory_usage(const KeyCppType& k, const ValueCppType& v) {
        switch (_type) {
        case DictionaryCacheEncoderType::ROW_STORE_SIMPLE: {
            static_assert(std::is_same_v<KeyCppType, std::string>);
            static_assert(std::is_same_v<ValueCppType, std::string>);
            return k.size() + v.size();
        }
        default:
            break;
        }
        return 0;
    }

    phmap::parallel_flat_hash_map<KeyCppType, ValueCppType, DictionaryCacheHashTraits<KeyLogicalType>,
                                  phmap::priv::hash_default_eq<KeyCppType>,
                                  phmap::priv::Allocator<phmap::priv::Pair<const KeyCppType, ValueCppType>>, 4,
                                  std::shared_mutex, true>
            _dictionary;
    std::atomic<size_t> _total_memory_useage;
};

using DictionaryCachePtr = std::shared_ptr<DictionaryCache>;

class DictionaryCacheUtil {
public:
    DictionaryCacheUtil() = default;
    ~DictionaryCacheUtil();

    // using primary key encoding function
    static std::unique_ptr<Column> encode_columns(const Schema& schema, const Chunk* chunk,
                                                  const DictionaryCacheEncoderType& encoder_type = ROW_STORE_SIMPLE) {
        switch (encoder_type) {
        case ROW_STORE_SIMPLE: {
            auto encoded_column = std::make_unique<BinaryColumn>();
            auto encoder = RowStoreEncoderFactory::instance()->get_or_create_encoder(RowStoreEncoderType::SIMPLE);
            auto st = encoder->encode_columns_to_full_row_column(schema, chunk->columns(), *encoded_column.get());
            if (!st.ok()) {
                break;
            }
            return encoded_column;
        }
        default:
            break;
        }
        return nullptr;
    }

    static Status decode_columns(const Schema& schema, Column* column, Chunk* decoded_chunk,
                                 const DictionaryCacheEncoderType& encoder_type = ROW_STORE_SIMPLE) {
        switch (encoder_type) {
        case ROW_STORE_SIMPLE: {
            auto encoder = RowStoreEncoderFactory::instance()->get_or_create_encoder(RowStoreEncoderType::SIMPLE);
            const auto binary_column = down_cast<BinaryColumn*>(column);
            std::vector<uint32_t> column_ids(schema.fields().size());
            std::iota(column_ids.begin(), column_ids.end(), 0);
            std::vector<std::unique_ptr<Column>> columns;
            for (size_t i = 0; i < schema.fields().size(); ++i) {
                auto clone_column = decoded_chunk->get_column_by_index(i)->clone_empty();
                columns.emplace_back(std::move(clone_column));
            }

            RETURN_IF_ERROR(encoder->decode_columns_from_full_row_column(schema, *binary_column, column_ids, &columns));

            DCHECK(column->size() == columns[0]->size());
            for (size_t i = 0; i < decoded_chunk->columns().size(); ++i) {
                decoded_chunk->get_column_by_index(i).reset(columns[i].release());
            }
            DCHECK(decoded_chunk->num_rows() == column->size());
            return Status::OK();
        }
        default:
            break;
        }
        return Status::InternalError("decode failed for dictionary");
    }

    static LogicalType get_encoded_type(const Schema& schema,
                                        const DictionaryCacheEncoderType& encoder_type = ROW_STORE_SIMPLE) {
        switch (encoder_type) {
        case ROW_STORE_SIMPLE: {
            auto encoder = RowStoreEncoderFactory::instance()->get_or_create_encoder(RowStoreEncoderType::SIMPLE);
            if (!encoder->is_supported(schema).ok()) {
                break;
            }
            return TYPE_VARCHAR;
        }
        default:
            break;
        }
        return TYPE_NONE;
    }

    static DictionaryCachePtr create_dictionary_cache(
            const std::pair<LogicalType, LogicalType>& type,
            const DictionaryCacheEncoderType& encoder_type = ROW_STORE_SIMPLE) {
        switch (encoder_type) {
        case ROW_STORE_SIMPLE: {
            return std::make_shared<DictionaryCacheImpl<TYPE_VARCHAR, TYPE_VARCHAR>>(encoder_type);
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
                                                      ChunkPtr& value_chunk) {
        DCHECK(value_chunk->num_rows() == 0);
        size_t size = key_chunk->num_rows();

        auto encoded_key_column = DictionaryCacheUtil::encode_columns(key_schema, key_chunk.get());

        ChunkUniquePtr clone_value_chunk = value_chunk->clone_empty();
        auto encoded_value_column = DictionaryCacheUtil::encode_columns(value_schema, clone_value_chunk.get());

        if (encoded_key_column == nullptr || encoded_value_column == nullptr) {
            return Status::InternalError("encode dictionary cache column failed when probing the dictionary cache");
        }

        for (size_t i = 0; i < size; ++i) {
            auto key = encoded_key_column->get(i);
            RETURN_IF_ERROR(dictionary->lookup(key, encoded_value_column.get()));
        }
        DCHECK(encoded_value_column->size() == size);

        return DictionaryCacheUtil::decode_columns(value_schema, encoded_value_column.get(), value_chunk.get());
    }

private:
    Status _refresh_encoded_chunk(DictionaryId dict_id, DictionaryCacheTxnId txn_id, const Column* encoded_key_column,
                                  const Column* encoded_value_column, const SchemaPtr& schema,
                                  LogicalType key_encoded_type, LogicalType value_encoded_type, long memory_limit);

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