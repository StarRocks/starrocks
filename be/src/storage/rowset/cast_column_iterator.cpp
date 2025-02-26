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

#include "storage/rowset/cast_column_iterator.h"

#include "column/column_helper.h"
#include "common/object_pool.h"
#include "exprs/cast_expr.h"
#include "exprs/column_ref.h"

namespace starrocks {

CastColumnIterator::CastColumnIterator(std::unique_ptr<ColumnIterator> source_iter, const TypeDescriptor& source_type,
                                       const TypeDescriptor& target_type, bool nullable_source)
        : ColumnIteratorDecorator(source_iter.release(), kTakesOwnership),
          _obj_pool(new ObjectPool()),
          _cast_expr(nullptr),
          _source_chunk() {
    auto slot_id = SlotId{0};
    auto column = ColumnHelper::create_column(source_type, nullable_source);
    auto slot_desc = SlotDescriptor(slot_id, "", source_type);
    auto column_ref = _obj_pool->add(new ColumnRef(&slot_desc));
    CHECK(column != nullptr) << "source type=" << source_type;
    _source_chunk.append_column(std::move(column), slot_id);
    _cast_expr = VectorizedCastExprFactory::from_type(source_type, target_type, column_ref, _obj_pool.get(), false);
    CHECK(_cast_expr != nullptr) << "Fail to create cast expr for source type=" << source_type
                                 << " target type=" << target_type;
}

CastColumnIterator::~CastColumnIterator() = default;

void CastColumnIterator::do_cast(Column* target) {
    auto cast_result = _cast_expr->evaluate(nullptr, &_source_chunk);
    cast_result = ColumnHelper::unfold_const_column(_cast_expr->type(), _source_chunk.num_rows(), cast_result);
    if ((target->is_nullable() == cast_result->is_nullable()) && (target->size() == 0)) {
        target->swap_column(*cast_result);
    } else if (!target->is_nullable() && cast_result->is_nullable()) {
        auto sz = cast_result->size();
        target->append(*(down_cast<NullableColumn*>(cast_result.get())->data_column()), 0, sz);
    } else {
        target->append(*cast_result, 0, cast_result->size());
    }
}

Status CastColumnIterator::next_batch(size_t* n, Column* dst) {
    _source_chunk.reset();
    auto source_column = _source_chunk.get_column_by_index(0);
    RETURN_IF_ERROR(_parent->next_batch(n, source_column.get()));
    do_cast(dst);
    return Status::OK();
}

Status CastColumnIterator::next_batch(const SparseRange<>& range, Column* dst) {
    _source_chunk.reset();
    auto source_column = _source_chunk.get_column_by_index(0);
    RETURN_IF_ERROR(_parent->next_batch(range, source_column.get()));
    do_cast(dst);
    return Status::OK();
}

Status CastColumnIterator::fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) {
    _source_chunk.reset();
    auto source_column = _source_chunk.get_column_by_index(0);
    RETURN_IF_ERROR(_parent->fetch_values_by_rowid(rowids, size, source_column.get()));
    do_cast(values);
    return Status::OK();
}

} // namespace starrocks
