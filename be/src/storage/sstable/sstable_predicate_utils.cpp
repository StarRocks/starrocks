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

#include "storage/sstable/sstable_predicate_utils.h"

#include "storage/chunk_helper.h"
#include "storage/primary_key_encoder.h"
#include "storage/sstable/sstable_predicate.h"
#include "storage/tablet_schema.h"

namespace starrocks::sstable {

StatusOr<KeyToChunkConverterUPtr> KeyToChunkConverter::create(const TabletSchemaPB& tablet_schema_pb) {
    // convert context for key
    std::shared_ptr<TabletSchema> tablet_schema = std::make_shared<TabletSchema>(tablet_schema_pb);
    std::vector<ColumnId> pk_columns(tablet_schema->num_key_columns());
    for (auto i = 0; i < tablet_schema->num_key_columns(); i++) {
        pk_columns[i] = (ColumnId)i;
    }
    auto pkey_schema = ChunkHelper::convert_schema(tablet_schema, pk_columns);
    auto pk_chunk = ChunkHelper::new_chunk(pkey_schema, 1);
    MutableColumnPtr pk_column;
    RETURN_IF_ERROR(PrimaryKeyEncoder::create_column(pkey_schema, &pk_column));

    auto converter = std::make_unique<KeyToChunkConverter>(pkey_schema, pk_chunk, pk_column);
    return converter;
}

StatusOr<ChunkUniquePtr> KeyToChunkConverter::convert_to_chunk(const std::string& key) {
    auto chunk = _pk_chunk->clone_empty();

    _pk_column->reset_column();
    if (_pk_column->is_binary()) {
        Slice slice(key.data(), key.size());
        down_cast<BinaryColumn*>(_pk_column.get())->append(slice);
    } else if (_pk_column->is_large_binary()) {
        Slice slice(key.data(), key.size());
        down_cast<LargeBinaryColumn*>(_pk_column.get())->append(slice);
    } else {
        _pk_column->deserialize_and_append(reinterpret_cast<const uint8_t*>(key.data()));
    }
    RETURN_IF_ERROR(PrimaryKeyEncoder::decode(_pkey_schema, *_pk_column, 0, 1, chunk.get(), nullptr));
    return chunk;
}

Status CachedPredicateEvaluator::evaluate_with_cache(const SstablePredicateSPtr& predicate, const std::string& key,
                                                     uint8_t* selection) {
    DCHECK(predicate != nullptr);
    DCHECK(!key.empty());
    DCHECK(selection != nullptr);

    if (_cache_predicate != nullptr && _cache_predicate->equals(*predicate) && _cache_key == key) {
        *selection = _result;
    } else {
        RETURN_IF_ERROR(predicate->evaluate(key, selection));

        _cache_predicate = predicate;
        _cache_key = key;
        _result = *selection;
    }
    return Status::OK();
}

} // namespace starrocks::sstable
