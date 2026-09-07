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
#include "runtime/descriptors.h"
#include "types/logical_type.h"

namespace starrocks {

namespace {

// A zone map keeps the source column's min/max in the form that column writes them, and
// ColumnReader::_get_zone_map_parse_type() reads them back as the predicate's type. Forwarding a zone
// map across a cast is therefore only sound when that reinterpretation preserves both the value and
// the ordering.
bool zone_map_survives_cast(LogicalType source_type, LogicalType target_type) {
    // Integer widening, the shape a fast schema evolution leaves behind: min/max are decimal literals
    // that every integer type reads alike, ordered numerically on both sides.
    if (is_integer_type(source_type) && is_integer_type(target_type)) {
        return true;
    }
    // A CHAR or VARCHAR length change, the second shape. CHAR and VARCHAR share the byte-wise
    // ordering, and _get_zone_map_parse_type() already forces the CHAR parse that strips the padding a
    // CHAR zone map carries.
    if (is_string_type(source_type) && is_string_type(target_type)) {
        return true;
    }
    // A DATE widened to DATETIME, the third shape. A DATE zone map is written as "YYYY-MM-DD";
    // TimestampValue::from_string() reads a date-only string as that day's midnight, which is exactly
    // where the cast puts every stored DATE, so the reinterpreted range is both exact and ordered the
    // same way. Only this direction. Turning a DATETIME back into a DATE is a rewriting schema change
    // today, so no one reads a DATETIME zone map through a DATE predicate and there is no pruning to
    // win by opening it -- and the cost of being wrong is asymmetric: DateValue::from_string()
    // answers a string it cannot read by substituting 1400-01-01 instead of failing, and a min and a
    // max that both collapse there prune the column away in silence.
    if (source_type == TYPE_DATE && target_type == TYPE_DATETIME) {
        return true;
    }
    // Everything else compares one type's written form against another type's reader. A string zone
    // map against a numeric predicate and the reverse are the reason this function exists, but the
    // float widenings are no safer: FloatToBuffer() writes the shortest decimal that reads back as the
    // same FLOAT, and that decimal read as a DOUBLE is a different number which can land below the
    // page's true maximum -- enough to drop a page that holds a match.
    return false;
}

} // namespace

CastColumnIterator::CastColumnIterator(std::unique_ptr<ColumnIterator> source_iter, const TypeDescriptor& source_type,
                                       const TypeDescriptor& target_type, bool nullable_source)
        : ColumnIteratorDecorator(source_iter.release(), kTakesOwnership),
          _obj_pool(new ObjectPool()),

          _source_chunk(),
          _zone_map_forwardable(zone_map_survives_cast(source_type.type, target_type.type)) {
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

Status CastColumnIterator::do_cast(Column* target) {
    ASSIGN_OR_RETURN(auto cast_result, _cast_expr->evaluate_checked(nullptr, &_source_chunk));
    cast_result = ColumnHelper::unfold_const_column(_cast_expr->type(), _source_chunk.num_rows(), cast_result);
    if ((target->is_nullable() == cast_result->is_nullable()) && (target->size() == 0)) {
        target->swap_column(*(cast_result->as_mutable_raw_ptr()));
    } else if (!target->is_nullable() && cast_result->is_nullable()) {
        auto sz = cast_result->size();
        target->append(*(down_cast<const NullableColumn*>(cast_result.get())->data_column()), 0, sz);
    } else {
        target->append(*cast_result, 0, cast_result->size());
    }
    return Status::OK();
}

Status CastColumnIterator::next_batch(size_t* n, Column* dst) {
    _source_chunk.reset();
    auto* source_column = _source_chunk.get_column_raw_ptr_by_index(0);
    RETURN_IF_ERROR(_parent->next_batch(n, source_column));
    RETURN_IF_ERROR(do_cast(dst));
    return Status::OK();
}

Status CastColumnIterator::next_batch(const SparseRange<>& range, Column* dst) {
    _source_chunk.reset();
    auto* source_column = _source_chunk.get_column_raw_ptr_by_index(0);
    RETURN_IF_ERROR(_parent->next_batch(range, source_column));
    RETURN_IF_ERROR(do_cast(dst));
    return Status::OK();
}

Status CastColumnIterator::fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) {
    _source_chunk.reset();
    auto* source_column = _source_chunk.get_column_raw_ptr_by_index(0);
    RETURN_IF_ERROR(_parent->fetch_values_by_rowid(rowids, size, source_column));
    RETURN_IF_ERROR(do_cast(values));
    return Status::OK();
}

StatusOr<std::vector<std::pair<int64_t, int64_t>>> CastColumnIterator::get_io_range_vec(const SparseRange<>& range,
                                                                                        Column* dst) {
    auto* source_column = _source_chunk.get_column_raw_ptr_by_index(0);
    return _parent->get_io_range_vec(range, source_column);
}

} // namespace starrocks
