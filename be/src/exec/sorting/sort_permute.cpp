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

#include "exec/sorting/sort_permute.h"

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column.h"
#include "column/column_visitor_adapter.h"
#include "column/const_column.h"
#include "column/decimalv3_column.h"
#include "column/fixed_length_column_base.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/object_column.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exec/sorting/sorting.h"
#include "gutil/casts.h"
#include "gutil/strings/fastmem.h"
#include "sort_permute.h"

namespace starrocks {

bool TieIterator::next() {
    if (_inner_range_first >= end) {
        return false;
    }

    // Find the first `1`
    if (_inner_range_first == 0 && tie[_inner_range_first] == 1) {
        // Just start from the 0
    } else {
        _inner_range_first = SIMD::find_nonzero(tie, _inner_range_first + 1, end - (_inner_range_first + 1));
        if (_inner_range_first >= end) {
            return false;
        }
        _inner_range_first--;
    }

    // Find the zero, or the end of range
    _inner_range_last = SIMD::find_zero(tie, _inner_range_first + 1, end - _inner_range_first + 1);
    _inner_range_last = std::min(_inner_range_last, end);

    if (_inner_range_first >= _inner_range_last) {
        return false;
    }

    range_first = _inner_range_first;
    range_last = _inner_range_last;
    _inner_range_first = _inner_range_last;
    return true;
}

template <class PV>
class ColumnsContainer {
public:
    static_assert(SingleContainerPermutationView<PV>,
                  "This template should only be instantiated for a permutation view of a single continous container.");

    ColumnsContainer(const Column* col) : _col{col} {}

    std::pair<const Column*, size_t> get(PV::value_type perm) const {
        return std::make_pair(_col, perm.index_in_chunk);
    }

    bool is_nullable() const { return _col->is_nullable(); }

    std::pair<const Column*, const Column*> get_columns_nullable() const {
        const auto* src_column = down_cast<const NullableColumn*>(_col);
        return std::make_pair(src_column->null_column().get(), src_column->data_column().get());
    }

private:
    const Column* _col;
};

template <>
class ColumnsContainer<PermutationView> {
public:
    ColumnsContainer(const Columns& cols) : _cols{cols} {}

    std::pair<const Column*, size_t> get(PermutationItem perm) const {
        return std::make_pair(_cols[perm.chunk_index].get(), perm.index_in_chunk);
    }

    bool is_nullable() const { return _cols.at(0)->is_nullable(); }

    std::pair<Columns, Columns> get_columns_nullable() const {
        Columns null_columns, data_columns;
        for (auto& col : _cols) {
            const auto* src_column = down_cast<const NullableColumn*>(col.get());
            null_columns.push_back(src_column->null_column());
            data_columns.push_back(src_column->data_column());
        }

        return std::make_pair(std::move(null_columns), std::move(data_columns));
    }

private:
    const Columns& _cols;
};

// Append permutation to column, implements `materialize_by_permutation` function
template <class PV>
class BaseColumnAppendPermutation final : public ColumnVisitorMutableAdapter<BaseColumnAppendPermutation<PV>> {
public:
    explicit BaseColumnAppendPermutation(const ColumnsContainer<PV>& columns, const PV& perm)
            : ColumnVisitorMutableAdapter<BaseColumnAppendPermutation<PV>>(this),
              _columns_container(columns),
              _perm(perm) {}

    Status do_visit(NullableColumn* dst) {
        if (_perm.empty()) {
            return Status::OK();
        }

        uint32_t orig_size = dst->size();
        auto [null_columns, data_columns] = _columns_container.get_columns_nullable();
        if (_columns_container.is_nullable()) {
            materialize_column_by_permutation(dst->null_column().get(), null_columns, _perm);
            materialize_column_by_permutation(dst->data_column().get(), data_columns, _perm);
            if (!dst->has_null()) {
                dst->set_has_null(SIMD::count_nonzero(&dst->null_column()->get_data()[orig_size], _perm.size()));
            }
        } else {
            dst->null_column()->resize(orig_size + _perm.size());
            materialize_column_by_permutation(dst->data_column().get(), data_columns, _perm);
        }
        DCHECK_EQ(dst->null_column()->size(), dst->data_column()->size());

        return Status::OK();
    }

    template <typename T>
    Status do_visit(DecimalV3Column<T>* dst) {
        using Container = typename DecimalV3Column<T>::Container;
        using ColumnType = DecimalV3Column<T>;

        auto& data = dst->get_data();
        size_t output = data.size();
        data.resize(output + _perm.size());

        for (auto& p : _perm) {
            auto [column, index] = _columns_container.get(p);
            const Container& container = down_cast<const ColumnType*>(column)->get_data();
            data[output++] = container[index];
        }

        return Status::OK();
    }

    template <typename T>
    Status do_visit(FixedLengthColumnBase<T>* dst) {
        using Container = typename FixedLengthColumnBase<T>::Container;
        using ColumnType = FixedLengthColumnBase<T>;

        auto& data = dst->get_data();
        size_t output = data.size();
        data.resize(output + _perm.size());

        for (auto& p : _perm) {
            auto [column, index] = _columns_container.get(p);
            const Container& container = down_cast<const ColumnType*>(column)->get_data();
            data[output++] = container[index];
        }

        return Status::OK();
    }

    Status do_visit(ConstColumn* dst) {
        for (auto& p : _perm) {
            auto [column, index] = _columns_container.get(p);
            dst->append(*column, index, 1);
        }

        return Status::OK();
    }

    Status do_visit(ArrayColumn* dst) {
        for (auto& p : _perm) {
            auto [column, index] = _columns_container.get(p);
            dst->append(*column, index, 1);
        }

        return Status::OK();
    }

    Status do_visit(MapColumn* dst) {
        for (auto& p : _perm) {
            auto [column, index] = _columns_container.get(p);
            dst->append(*column, index, 1);
        }

        return Status::OK();
    }

    Status do_visit(StructColumn* dst) {
        // TODO(SmithCruise) Not tested.
        for (auto& p : _perm) {
            auto [column, index] = _columns_container.get(p);
            dst->append(*column, index, 1);
        }

        return Status::OK();
    }

    template <typename T>
    Status do_visit(BinaryColumnBase<T>* dst) {
        using Container = typename BinaryColumnBase<T>::BinaryDataProxyContainer;

        auto& offsets = dst->get_offset();
        auto& bytes = dst->get_bytes();
        size_t old_rows = dst->size();
        size_t num_offsets = offsets.size();
        size_t num_bytes = bytes.size();
        offsets.resize(num_offsets + _perm.size());
        for (auto& p : _perm) {
            auto [column, index] = _columns_container.get(p);
            const Container& container = down_cast<const BinaryColumnBase<T>*>(column)->get_proxy_data();
            Slice slice = container[index];
            offsets[num_offsets] = offsets[num_offsets - 1] + slice.get_size();
            ++num_offsets;
            num_bytes += slice.get_size();
        }

        bytes.resize(num_bytes);
        for (size_t i = 0; i < _perm.size(); i++) {
            auto [column, index] = _columns_container.get(_perm[i]);
            const Container& container = down_cast<const BinaryColumnBase<T>*>(column)->get_proxy_data();
            Slice slice = container[index];
            strings::memcpy_inlined(bytes.data() + offsets[old_rows + i], slice.get_data(), slice.get_size());
        }

        dst->invalidate_slice_cache();

        return Status::OK();
    }

    template <typename T>
    Status do_visit(ObjectColumn<T>* dst) {
        for (auto& p : _perm) {
            auto [column, index] = _columns_container.get(p);
            dst->append(*column, index, 1);
        }

        return Status::OK();
    }

    Status do_visit(JsonColumn* dst) {
        for (auto& p : _perm) {
            auto [column, index] = _columns_container.get(p);
            dst->append(*column, index, 1);
        }

        return Status::OK();
    }

private:
    const ColumnsContainer<PV> _columns_container;
    const PV& _perm;
};

using ColumnAppendPermutation = BaseColumnAppendPermutation<PermutationView>;
using ColumnAppendSmallPermutation = BaseColumnAppendPermutation<SmallPermutationView>;
using ColumnAppendLargePermutation = BaseColumnAppendPermutation<LargePermutationView>;

void materialize_column_by_permutation(Column* dst, const Columns& columns, const PermutationView& perm) {
    ColumnAppendPermutation visitor(columns, perm);
    Status st = dst->accept_mutable(&visitor);
    CHECK(st.ok());
}

void materialize_by_permutation(Chunk* dst, const std::vector<ChunkPtr>& chunks, const PermutationView& perm) {
    if (chunks.empty() || perm.empty()) {
        return;
    }

    DCHECK_LT(std::max_element(perm.begin(), perm.end(),
                               [](auto& lhs, auto& rhs) { return lhs.chunk_index < rhs.chunk_index; })
                      ->chunk_index,
              chunks.size());
    DCHECK_EQ(dst->num_columns(), chunks[0]->columns().size());

    for (size_t col_index = 0; col_index < dst->num_columns(); col_index++) {
        Columns tmp_columns;
        tmp_columns.reserve(chunks.size());
        for (const auto& chunk : chunks) {
            tmp_columns.push_back(chunk->get_column_by_index(col_index));
        }
        materialize_column_by_permutation(dst->get_column_by_index(col_index).get(), tmp_columns, perm);
    }
}

template <SingleContainerPermutationView PV>
void materialize_column_by_permutation(Column* dst, const Column* column, const PV& perm) {
    BaseColumnAppendPermutation<PV> visitor(ColumnsContainer<PV>{column}, perm);
    Status st = dst->accept_mutable(&visitor);
    CHECK(st.ok());
}

template <SingleContainerPermutationView PV>
void materialize_by_permutation(Chunk* dst, const ChunkPtr& chunk, const PV& perm) {
    if (perm.empty()) {
        return;
    }

    DCHECK_LT(std::max_element(perm.begin(), perm.end(),
                               [](auto& lhs, auto& rhs) { return lhs.index_in_chunk < rhs.index_in_chunk; })
                      ->index_in_chunk,
              chunk->num_rows());
    DCHECK_EQ(dst->num_columns(), chunk->columns().size());

    for (size_t col_index = 0; col_index < dst->num_columns(); col_index++) {
        materialize_column_by_permutation(dst->get_column_by_index(col_index).get(),
                                          chunk->get_column_by_index(col_index), perm);
    }
}

template void materialize_by_permutation<SmallPermutationView>(Chunk* dst, const ChunkPtr& chunk,
                                                               const SmallPermutationView& perm);
template void materialize_by_permutation<LargePermutationView>(Chunk* dst, const ChunkPtr& chunk,
                                                               const LargePermutationView& perm);

} // namespace starrocks
