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

#include "exprs/dictionary_get_expr.h"

#include <fmt/format.h>

#include "column/column_helper.h"
#include "column/struct_column.h"
#include "storage/chunk_helper.h"
#include "storage/storage_engine.h"

namespace starrocks {

DictionaryGetExpr::DictionaryGetExpr(const TExprNode& node)
        : Expr(node), _dictionary_get_expr(node.dictionary_get_expr) {}

StatusOr<ColumnPtr> DictionaryGetExpr::evaluate_checked(ExprContext* context, Chunk* ptr) {
    Columns columns(children().size());
    // calculate all child expression which used to construct keys
    size_t size = ptr != nullptr ? ptr->num_rows() : 1;
    for (int i = 0; i < _children.size(); ++i) {
        columns[i] = _children[i]->evaluate(context, ptr);
    }

    for (auto& column : columns) {
        if (column->has_null()) {
            return Status::InternalError("invalid parameter for dictionary_get function: get NULL paramenter");
        }
        if (column->is_constant()) {
            column = ColumnHelper::unpack_and_duplicate_const_column(size, column);
        }
        if (column->is_nullable()) {
            column = ColumnHelper::update_column_nullable(false, column, size);
        }
    }

    ChunkPtr key_chunk = _key_chunk->clone_empty();
    ChunkPtr value_chunk = _value_chunk->clone_empty();
    ColumnPtr nullable_struct_column = _nullable_struct_column->clone_empty();

    key_chunk->set_num_rows(size);
    // assign the key chunk
    for (int i = 0; i < _dictionary_get_expr.key_size; ++i) {
        ColumnPtr key_column = columns[1 + i];
        key_chunk->update_column_by_index(key_column, i);
    }
    value_chunk->reserve(size);

    MutableColumnPtr null_column = UInt8Column::create(size, 0);
    // assign the value chunk
    RETURN_IF_ERROR(DictionaryCacheManager::probe_given_dictionary_cache(
            *_key_chunk->schema().get(), *_value_chunk->schema().get(), _dictionary, key_chunk, value_chunk,
            _dictionary_get_expr.null_if_not_exist ? null_column.get() : nullptr));

    // merge the value chunk into a single struct column and return
    auto fields =
            down_cast<StructColumn*>(down_cast<NullableColumn*>(nullable_struct_column.get())->data_column().get())
                    ->fields_column();
    for (size_t i = 0; i < value_chunk->columns().size(); ++i) {
        auto column = value_chunk->columns()[i];
        fields[i]->append(*column, 0, column->size());
    }
    down_cast<NullableColumn*>(nullable_struct_column.get())
            ->set_has_null(SIMD::contain_nonzero(down_cast<UInt8Column*>(null_column.get())->get_data(), 0));
    down_cast<NullableColumn*>(nullable_struct_column.get())->mutable_null_column()->swap_column(*null_column);

    return nullable_struct_column;
}

Status DictionaryGetExpr::prepare(RuntimeState* state, ExprContext* context) {
    RETURN_IF_ERROR(Expr::prepare(state, context));
    _runtime_state = state;

    _schema = StorageEngine::instance()->dictionary_cache_manager()->get_dictionary_schema_by_id(
            _dictionary_get_expr.dict_id);
    if (_schema == nullptr) {
        return Status::InternalError(
                fmt::format("open dictionary expression failed, there is no cache for dictionary: {}",
                            _dictionary_get_expr.dict_id));
    }
    auto res = StorageEngine::instance()->dictionary_cache_manager()->get_dictionary_by_version(
            _dictionary_get_expr.dict_id, _dictionary_get_expr.txn_id);
    if (!res.ok()) {
        return Status::InternalError(fmt::format("open dictionary expression failed {}", res.status().message()));
    }
    _dictionary = std::move(res.value());
    DCHECK(_dictionary != nullptr);

    // init key / value chunk and struct column template for evaluation
    std::vector<ColumnId> key_cids(_dictionary_get_expr.key_size);
    std::iota(key_cids.begin(), key_cids.end(), 0);
    _key_chunk = ChunkHelper::new_chunk(Schema(_schema.get(), key_cids), 0);

    std::vector<ColumnId> value_cids(_schema->fields().size() - _dictionary_get_expr.key_size);
    std::iota(value_cids.begin(), value_cids.end(), _dictionary_get_expr.key_size);
    _value_chunk = ChunkHelper::new_chunk(Schema(_schema.get(), value_cids), 0);

    DCHECK(_key_chunk != nullptr);
    DCHECK(_value_chunk != nullptr);

    auto value_schema = _value_chunk->schema();
    DCHECK(value_schema != nullptr);
    std::vector<std::string> value_columns_name(value_schema->fields().size());
    for (size_t i = 0; i < value_schema->fields().size(); ++i) {
        value_columns_name[i] = value_schema->field(i)->name();
    }

    // construct nullable struct column
    Columns sub_columns;
    for (const ColumnPtr& column : _value_chunk->columns()) {
        auto sub_null_column = UInt8Column::create(0, 0);
        sub_columns.emplace_back(NullableColumn::create(column, std::move(sub_null_column)));
    }
    auto null_column = UInt8Column::create(0, 0);
    _nullable_struct_column = NullableColumn::create(
            StructColumn::create(std::move(sub_columns), std::move(value_columns_name)), std::move(null_column));
    DCHECK(_nullable_struct_column != nullptr);

    return Status::OK();
}

} // namespace starrocks
