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

#include "storage/dictionary_cache_manager.h"

#include "exec/tablet_info.h"

namespace starrocks {

Status DictionaryCacheManager::begin(const PProcessDictionaryCacheRequest* request) {
    auto dict_id = request->dict_id();
    auto txn_id = request->txn_id();
    std::unique_lock wlock(_refresh_lock);
    if (LIKELY(_mutable_dict_caches.find(dict_id) == _mutable_dict_caches.end())) {
        _mutable_dict_caches[dict_id] = std::make_shared<OrderedMutableDictionaryCache>();
    } // It is ok, if dictionary id is duplicated

    if (LIKELY(_mutable_dict_caches[dict_id]->find(txn_id) == _mutable_dict_caches[dict_id]->end())) {
        (*_mutable_dict_caches[dict_id])[txn_id] = nullptr;
    } else {
        return Status::InternalError(
                fmt::format("duplicated dictionary cache refresh task, duplicated txn id: {}", txn_id));
    }

    return Status::OK();
}

Status DictionaryCacheManager::refresh(const PProcessDictionaryCacheRequest* request) {
    const auto& dict_id = request->dict_id();
    const auto& txn_id = request->txn_id();
    const auto& pchunk = request->chunk();
    const auto& pschema = request->schema();
    const auto& memory_limit = request->memory_limit();

    // 1. uncompress and deserialize chunk
    faststring uncompressed_buffer;
    auto schema = std::make_shared<OlapTableSchemaParam>();
    RETURN_IF_ERROR(schema->init(pschema));
    auto chunk = std::make_unique<Chunk>();
    RETURN_IF_ERROR(DictionaryCacheWriter::ChunkUtil::uncompress_and_deserialize_chunk(
            pchunk, *chunk.get(), &uncompressed_buffer, schema.get()));
    RETURN_IF_ERROR(DictionaryCacheWriter::ChunkUtil::check_chunk_has_null(*chunk.get()));

    // 2. split into key chunk and value chunk in ordered
    std::vector<std::string_view> col_names;
    std::vector<SlotId> key_slot_ids;
    std::vector<SlotId> value_slot_ids;
    for (int i = 0; i < schema->tuple_desc()->slots().size(); ++i) {
        const string& name = schema->tuple_desc()->slots()[i]->col_name();
        col_names.emplace_back(std::string_view(name.data(), name.size()));

        if (i < request->key_size()) {
            key_slot_ids.emplace_back(schema->tuple_desc()->slots()[i]->id());
        } else {
            value_slot_ids.emplace_back(schema->tuple_desc()->slots()[i]->id());
        }
    }

    // dictionary definition: col2 key, col3 key, col1 value, col4 value
    // OlapTableSchemaParam->tuple_desc()->slots(): col2 key, col3 key, col1 value, col4 value (with nullable attribute)
    // OlapTableSchemaParam->indexes()[0]->column_param->columns: col1, col2, col3, col4 (with nullable attribute)
    // chunk schema: col1, col2, col3, col4 (with nullable attribute)
    // dictionary_schema: col2, col3, col1, col4 (without nullable attribute)
    SchemaPtr dictionary_schema = ChunkHelper::convert_schema((schema->indexes()[0])->column_param->columns, col_names);
    DCHECK(dictionary_schema != nullptr);
    std::vector<int> keys(dictionary_schema->fields().size(), 0);
    // remove the nullable attribute if necessary
    dictionary_schema = ChunkHelper::get_non_nullable_schema(dictionary_schema, &keys);

    std::vector<ColumnId> key_col_ids(request->key_size());
    std::vector<ColumnId> value_col_ids(dictionary_schema->fields().size() - request->key_size());
    std::iota(key_col_ids.begin(), key_col_ids.end(), 0);
    std::iota(value_col_ids.begin(), value_col_ids.end(), request->key_size());

    SchemaPtr key_schema = std::make_shared<Schema>(dictionary_schema.get(), key_col_ids);
    SchemaPtr value_schema = std::make_shared<Schema>(dictionary_schema.get(), value_col_ids);

    ChunkPtr key_chunk = ChunkHelper::new_chunk(*key_schema.get(), chunk->num_rows());
    ChunkPtr value_chunk = ChunkHelper::new_chunk(*value_schema.get(), chunk->num_rows());

    for (size_t i = 0; i < key_slot_ids.size(); ++i) {
        const auto& key_slot_id = key_slot_ids[i];
        auto ori_key_column = chunk->get_column_by_slot_id(key_slot_id);
        if (ori_key_column->is_constant()) {
            ori_key_column = ColumnHelper::unpack_and_duplicate_const_column(ori_key_column->size(), ori_key_column);
        }
        if (ori_key_column->is_nullable()) {
            ori_key_column = ColumnHelper::update_column_nullable(false, ori_key_column, ori_key_column->size());
        }
        key_chunk->get_column_by_index(i).swap(ori_key_column);
    }
    for (size_t i = 0; i < value_slot_ids.size(); ++i) {
        const auto& value_slot_id = value_slot_ids[i];
        auto ori_value_column = chunk->get_column_by_slot_id(value_slot_id);
        if (ori_value_column->is_constant()) {
            ori_value_column =
                    ColumnHelper::unpack_and_duplicate_const_column(ori_value_column->size(), ori_value_column);
        }
        if (ori_value_column->is_nullable()) {
            ori_value_column = ColumnHelper::update_column_nullable(false, ori_value_column, ori_value_column->size());
        }
        value_chunk->get_column_by_index(i).swap(ori_value_column);
    }

    // 3. encode key / value chunk without any nullable attribute
    auto encoded_key_column = DictionaryCacheUtil::encode_columns(*key_schema.get(), key_chunk.get());
    if (encoded_key_column == nullptr) {
        return Status::InternalError(
                fmt::format("encode key chunk failed when refreshing dictionary cache, dictionary id: {}, txn id: {}",
                            dict_id, txn_id));
    }

    auto encoded_value_column = DictionaryCacheUtil::encode_columns(*value_schema.get(), value_chunk.get());
    if (encoded_value_column == nullptr) {
        return Status::InternalError(
                fmt::format("encode value chunk failed when refreshing dictionary cache, dictionary id: {}, txn id: {}",
                            dict_id, txn_id));
    }

    // release memory after get the encoded column
    key_chunk.reset();
    value_chunk.reset();
    chunk.reset();

    // 4. refresh
    return _refresh_encoded_chunk(dict_id, txn_id, encoded_key_column.get(), encoded_value_column.get(),
                                  dictionary_schema, DictionaryCacheUtil::get_encoded_type(*key_schema.get()),
                                  DictionaryCacheUtil::get_encoded_type(*value_schema.get()), memory_limit);
}

Status DictionaryCacheManager::_refresh_encoded_chunk(DictionaryId dict_id, DictionaryCacheTxnId txn_id,
                                                      const Column* encoded_key_column,
                                                      const Column* encoded_value_column, const SchemaPtr& schema,
                                                      LogicalType key_encoded_type, LogicalType value_encoded_type,
                                                      long memory_limit) {
    DCHECK(key_encoded_type != TYPE_NONE);
    DCHECK(value_encoded_type != TYPE_NONE);

    // 1. fill the dictionary schema if necessary
    {
        DCHECK(schema != nullptr);
        std::unique_lock wlock(_schema_lock);
        if (UNLIKELY(_dict_cache_schema.find(dict_id) == _dict_cache_schema.end())) {
            _dict_cache_schema[dict_id] = schema;
        }
    }

    // 2. get and init mutable cache using <dict_id, txn_id>
    DictionaryCachePtr mutable_cache = nullptr;
    {
        std::unique_lock rlock(_refresh_lock);
        if (LIKELY(_mutable_dict_caches.find(dict_id) != _mutable_dict_caches.end() &&
                   _mutable_dict_caches[dict_id]->find(txn_id) != _mutable_dict_caches[dict_id]->end())) {
            if ((*_mutable_dict_caches[dict_id])[txn_id] == nullptr) {
                auto p = DictionaryCacheUtil::create_dictionary_cache(
                        std::pair<LogicalType, LogicalType>(key_encoded_type, value_encoded_type));
                if (p == nullptr) {
                    return Status::InternalError("Invalid dictionary type");
                }
                (*_mutable_dict_caches[dict_id])[txn_id] = p;
            }
            mutable_cache = (*_mutable_dict_caches[dict_id])[txn_id];
        } else {
            if (_dict_cancel.find(dict_id) != _dict_cancel.end()) {
                _dict_cancel.erase(dict_id);
                return Status::InternalError(fmt::format("cancel refreshing dictionary: {}", dict_id));
            }
            return Status::NotFound(fmt::format(
                    "refresh buffer for refreshing dictionary does not exist, dictionary id: {}, txn id: {}", dict_id,
                    txn_id)); /* happen when BE crash during refreshing dictionary */
        }
    }

    DCHECK(mutable_cache != nullptr);

    // 3. update mutable cache
    DCHECK(encoded_key_column != nullptr);
    DCHECK(encoded_value_column != nullptr);
    DCHECK(encoded_key_column->size() == encoded_value_column->size());

    for (size_t i = 0; i < encoded_key_column->size(); ++i) {
        auto key = encoded_key_column->get(i);
        auto value = encoded_value_column->get(i);
        RETURN_IF_ERROR(mutable_cache->insert(key, value));
    }

    if (mutable_cache->memory_usage() > memory_limit) {
        // hit the memory limit, we should clear the cache as soon as possible
        this->clear(dict_id);
        return Status::InternalError(
                fmt::format("Reach the memory limit: {} bytes for dictionary, id: {}", memory_limit, dict_id));
    }

    return Status::OK();
}

Status DictionaryCacheManager::commit(const PProcessDictionaryCacheRequest* request) {
    auto dict_id = request->dict_id();
    auto txn_id = request->txn_id();
    std::unique_lock wlock1(_lock);
    std::unique_lock wlock2(_refresh_lock);

    if (UNLIKELY(_mutable_dict_caches.find(dict_id) == _mutable_dict_caches.end() ||
                 _mutable_dict_caches[dict_id]->find(txn_id) == _mutable_dict_caches[dict_id]->end())) {
        if (_dict_cancel.find(dict_id) != _dict_cancel.end()) {
            _dict_cancel.erase(dict_id);
            return Status::InternalError(fmt::format("cancel refreshing dictionary: {}", dict_id));
        }
        return Status::InternalError(
                fmt::format("commit failed the dictionary cache task: current txn id: {}", txn_id));
    } // This may happen if the current BE node crash just before commit

    auto ordered_mutable_cache = _mutable_dict_caches[dict_id];
    if (ordered_mutable_cache->crbegin()->first != txn_id) {
        return Status::InternalError(fmt::format(
                "The commit refresh dictionary cache task is not the latest one, current txn id: {}, max txn id: {}",
                txn_id, ordered_mutable_cache->crbegin()->first));
    }

    _dict_cache[dict_id] = nullptr;
    _dict_cache[dict_id].swap((*ordered_mutable_cache)[txn_id]);
    // The successful txn id will be used as the version number
    _dict_cache_versions[dict_id] = txn_id;

    _mutable_dict_caches.erase(dict_id);
    return Status::OK();
}

void DictionaryCacheManager::clear(DictionaryId dict_id, bool is_cancel) {
    std::unique_lock wlock1(_lock);
    std::unique_lock wlock2(_schema_lock);
    std::unique_lock wlock3(_refresh_lock);

    if (_mutable_dict_caches.find(dict_id) != _mutable_dict_caches.end()) {
        _mutable_dict_caches.erase(_mutable_dict_caches.find(dict_id));
    }

    if (is_cancel) {
        _dict_cancel.insert(dict_id);
        return;
    }

    if (_dict_cache.find(dict_id) != _dict_cache.end()) {
        _dict_cache.erase(_dict_cache.find(dict_id));
    }

    if (_dict_cache_versions.find(dict_id) != _dict_cache_versions.end()) {
        _dict_cache_versions.erase(_dict_cache_versions.find(dict_id));
    }

    if (_dict_cache_schema.find(dict_id) != _dict_cache_schema.end()) {
        _dict_cache_schema.erase(_dict_cache_schema.find(dict_id));
    }
}

void DictionaryCacheManager::get_info(DictionaryId dict_id, PProcessDictionaryCacheResult& response) {
    std::shared_lock wlock1(_lock);

    long dictionary_memory_usage = 0;
    if (_dict_cache.find(dict_id) != _dict_cache.end() && _dict_cache[dict_id] != nullptr) {
        dictionary_memory_usage = _dict_cache[dict_id]->memory_usage();
    }

    response.set_dictionary_memory_usage(dictionary_memory_usage);
}

SchemaPtr DictionaryCacheManager::get_dictionary_schema_by_id(const DictionaryId& dict_id) {
    std::shared_lock rlock(_schema_lock);
    if (UNLIKELY(_dict_cache_schema.find(dict_id) == _dict_cache_schema.end())) {
        return nullptr;
    }
    return _dict_cache_schema[dict_id];
}

StatusOr<DictionaryCachePtr> DictionaryCacheManager::get_dictionary_by_version(const DictionaryId& dict_id,
                                                                               const DictionaryCacheTxnId& txn_id) {
    DictionaryCachePtr dictionary = nullptr;
    {
        std::shared_lock wlock(_lock);
        if (UNLIKELY(_dict_cache_versions.find(dict_id) == _dict_cache_versions.end() ||
                     _dict_cache_versions[dict_id] != txn_id)) {
            return Status::NotFound(fmt::format(
                    "can not found dictionary by verison: {}, dictionary maybe dropped or updated, dictionary id: {}",
                    txn_id, dict_id));
        }
        DCHECK(_dict_cache.find(dict_id) != _dict_cache.end());
        dictionary = _dict_cache[dict_id];
    }
    if (dictionary == nullptr) {
        return Status::InternalError(fmt::format("empty dictionary cache, id: {}", dict_id));
    }

    return dictionary;
}

} // namespace starrocks
