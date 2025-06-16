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

#include "column/row_id_column.h"

#include <cstdint>

#include "column/column.h"
#include "column/datum.h"
#include "column/vectorized_fwd.h"
#include "exec/sorting/sort_helper.h"

namespace starrocks {

RowIdColumn::RowIdColumn()
        : _be_ids(UInt32Column::create()), _seg_ids(UInt32Column::create()), _ord_ids(UInt32Column::create()) {}

RowIdColumn::RowIdColumn(size_t n)
        : _be_ids(UInt32Column::create(n)), _seg_ids(UInt32Column::create(n)), _ord_ids(UInt32Column::create(n)) {}

RowIdColumn::RowIdColumn(UInt32Column::Ptr be_ids, UInt32Column::Ptr seg_ids, UInt32Column::Ptr ord_ids)
        : _be_ids(std::move(be_ids)), _seg_ids(std::move(seg_ids)), _ord_ids(std::move(ord_ids)) {}

RowIdColumn::RowIdColumn(const RowIdColumn& rhs)
        : _be_ids(UInt32Column::static_pointer_cast(rhs._be_ids->clone())),
          _seg_ids(UInt32Column::static_pointer_cast(rhs._seg_ids->clone())),
          _ord_ids(UInt32Column::static_pointer_cast(rhs._ord_ids->clone())) {}

RowIdColumn::RowIdColumn(RowIdColumn&& rhs) noexcept
        : _be_ids(std::move(rhs._be_ids)), _seg_ids(std::move(rhs._seg_ids)), _ord_ids(std::move(rhs._ord_ids)) {}

void RowIdColumn::assign(size_t n, size_t idx) {
    _be_ids->assign(n, idx);
    _seg_ids->assign(n, idx);
    _ord_ids->assign(n, idx);
}

void RowIdColumn::append_datum(const Datum& datum) {
    DatumRowId row_id = datum.get<DatumRowId>();
    _be_ids->append_datum(std::get<0>(row_id));
    _seg_ids->append_datum(std::get<1>(row_id));
    _ord_ids->append_datum(std::get<2>(row_id));
}

void RowIdColumn::append(const Column& src, size_t offset, size_t count) {
    if (count == 0) {
        return;
    }
    const auto& src_column = down_cast<const RowIdColumn&>(src);
    _be_ids->append(*src_column._be_ids, offset, count);
    _seg_ids->append(*src_column._seg_ids, offset, count);
    _ord_ids->append(*src_column._ord_ids, offset, count);
}

void RowIdColumn::append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    if (size == 0) {
        return;
    }
    const auto& src_column = down_cast<const RowIdColumn&>(src);
    _be_ids->append_selective(*src_column._be_ids, indexes, from, size);
    _seg_ids->append_selective(*src_column._seg_ids, indexes, from, size);
    _ord_ids->append_selective(*src_column._ord_ids, indexes, from, size);
}

void RowIdColumn::append_value_multiple_times(const Column& src, uint32_t idx, uint32_t count) {
    if (count == 0) {
        return;
    }
    const auto& src_column = down_cast<const RowIdColumn&>(src);
    _be_ids->append_value_multiple_times(*src_column._be_ids, idx, count);
    _seg_ids->append_value_multiple_times(*src_column._seg_ids, idx, count);
    _ord_ids->append_value_multiple_times(*src_column._ord_ids, idx, count);
}

void RowIdColumn::append_value_multiple_times(const void* value, size_t count) {
    const auto* datum = reinterpret_cast<const Datum*>(value);
    const auto& row_id = datum->get<DatumRowId>();

    for (size_t i = 0; i < count; i++) {
        append_datum(row_id);
    }
}

void RowIdColumn::remove_first_n_values(size_t count) {
    _be_ids->remove_first_n_values(count);
    _seg_ids->remove_first_n_values(count);
    _ord_ids->remove_first_n_values(count);
}

uint32_t RowIdColumn::max_one_element_serialize_size() const {
    return _be_ids->max_one_element_serialize_size() + _seg_ids->max_one_element_serialize_size() +
           _ord_ids->max_one_element_serialize_size();
}

uint32_t RowIdColumn::serialize(size_t idx, uint8_t* pos) const {
    uint32_t ret = _be_ids->serialize(idx, pos);
    ret = _seg_ids->serialize(idx, pos + ret);
    ret = _ord_ids->serialize(idx, pos + ret);
    return ret;
}

uint32_t RowIdColumn::serialize_default(uint8_t* pos) const {
    uint32_t ret = _be_ids->serialize_default(pos);
    ret += _seg_ids->serialize_default(pos + ret);
    ret += _ord_ids->serialize_default(pos + ret);
    return ret;
}
void RowIdColumn::serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                  uint32_t max_one_row_size) const {
    for (size_t i = 0; i < chunk_size; i++) {
        slice_sizes[i] += serialize(i, dst + i * max_one_row_size + slice_sizes[i]);
    }
}

const uint8_t* RowIdColumn::deserialize_and_append(const uint8_t* pos) {
    pos = _be_ids->deserialize_and_append(pos);
    pos = _seg_ids->deserialize_and_append(pos);
    pos = _ord_ids->deserialize_and_append(pos);
    return pos;
}
void RowIdColumn::deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) {
    reserve(chunk_size);
    for (size_t i = 0; i < chunk_size; i++) {
        srcs[i].data = (char*)deserialize_and_append((uint8_t*)srcs[i].data);
    }
}

MutableColumnPtr RowIdColumn::clone_empty() const {
    return RowIdColumn::create();
}

size_t RowIdColumn::filter_range(const Filter& filter, size_t from, size_t to) {
    size_t result = _be_ids->filter_range(filter, from, to);
    size_t tmp = _seg_ids->filter_range(filter, from, to);
    DCHECK_EQ(result, tmp);
    tmp = _ord_ids->filter_range(filter, from, to);
    DCHECK_EQ(result, tmp);
    return result;
}

int RowIdColumn::compare_at(size_t left, size_t right, const Column& rhs, int null_first) const {
    const auto& rhs_column = down_cast<const RowIdColumn&>(rhs);
    const auto& lhs_value = get(left).get<DatumRowId>();
    const auto& rhs_value = rhs_column.get(right).get<DatumRowId>();
    return SorterComparator<DatumRowId>::compare(lhs_value, rhs_value);
}

StatusOr<ColumnPtr> RowIdColumn::replicate(const Buffer<uint32_t>& offsets) {
    ASSIGN_OR_RETURN(auto be_ids, _be_ids->replicate(offsets));
    ASSIGN_OR_RETURN(auto seg_ids, _seg_ids->replicate(offsets));
    ASSIGN_OR_RETURN(auto ord_ids, _ord_ids->replicate(offsets));
    return RowIdColumn::create(UInt32Column::static_pointer_cast(std::move(be_ids)),
                               UInt32Column::static_pointer_cast(std::move(seg_ids)),
                               UInt32Column::static_pointer_cast(std::move(ord_ids)));
}

Datum RowIdColumn::get(size_t idx) const {
    uint32_t be_id = _be_ids->get(idx).get<uint32_t>();
    uint32_t seg_id = _seg_ids->get(idx).get<uint32_t>();
    uint32_t ord_id = _ord_ids->get(idx).get<uint32_t>();
    return std::make_tuple(be_id, seg_id, ord_id);
}

void RowIdColumn::swap_column(Column& rhs) {
    RowIdColumn& r = down_cast<RowIdColumn&>(rhs);
    _be_ids->swap_column(*r._be_ids);
    _seg_ids->swap_column(*r._seg_ids);
    _ord_ids->swap_column(*r._ord_ids);
}

void RowIdColumn::reset_column() {
    _be_ids->reset_column();
    _seg_ids->reset_column();
    _ord_ids->reset_column();
}

std::string RowIdColumn::debug_item(size_t idx) const {
    std::stringstream ss;
    DatumRowId row_id = get(idx).get<DatumRowId>();
    ss << "(" << std::get<0>(row_id) << ", " << std::get<1>(row_id) << ", " << std::get<2>(row_id) << ")";
    return ss.str();
}

std::string RowIdColumn::debug_string() const {
    std::stringstream ss;
    for (size_t i = 0; i < size(); i++) {
        if (i > 0) {
            ss << ", ";
        }
        ss << debug_item(i);
    }
    return ss.str();
}

void RowIdColumn::check_or_die() const {
    DCHECK_EQ(_be_ids->size(), _seg_ids->size());
    DCHECK_EQ(_be_ids->size(), _ord_ids->size());
    _be_ids->check_or_die();
    _seg_ids->check_or_die();
    _ord_ids->check_or_die();
}

} // namespace starrocks