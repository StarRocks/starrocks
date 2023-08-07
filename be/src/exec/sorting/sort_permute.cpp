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

namespace starrocks {

bool TieIterator::next() {
    if (_inner_range_first >= end) {
        return false;
    }

    // Find the first `1`
    if (_inner_range_first == 0 && tie[_inner_range_first] == 1) {
        // Just start from the 0
    } else {
        _inner_range_first = SIMD::find_nonzero(tie, _inner_range_first + 1);
        if (_inner_range_first >= end) {
            return false;
        }
        _inner_range_first--;
    }

    // Find the zero, or the end of range
    _inner_range_last = SIMD::find_zero(tie, _inner_range_first + 1);
    _inner_range_last = std::min(_inner_range_last, end);

    if (_inner_range_first >= _inner_range_last) {
        return false;
    }

    range_first = _inner_range_first;
    range_last = _inner_range_last;
    _inner_range_first = _inner_range_last;
    return true;
}

// Append permutation to column, implements `materialize_by_permutation` function
class ColumnAppendPermutation final : public ColumnVisitorMutableAdapter<ColumnAppendPermutation> {
public:
    explicit ColumnAppendPermutation(const Columns& columns, const PermutationView& perm)
            : ColumnVisitorMutableAdapter(this), _columns(columns), _perm(perm) {}

    Status do_visit(NullableColumn* dst) {
        if (_columns.empty() || _perm.empty()) {
            return Status::OK();
        }

        uint32_t orig_size = dst->size();
        Columns null_columns, data_columns;
        for (auto& col : _columns) {
            const auto* src_column = down_cast<const NullableColumn*>(col.get());
            null_columns.push_back(src_column->null_column());
            data_columns.push_back(src_column->data_column());
        }
        if (_columns[0]->is_nullable()) {
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
        std::vector<const Container*> srcs;
        for (auto& column : _columns) {
            srcs.push_back(&(down_cast<const ColumnType*>(column.get())->get_data()));
        }

        for (auto& p : _perm) {
            data[output++] = (*srcs[p.chunk_index])[p.index_in_chunk];
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
        std::vector<const Container*> srcs;
        for (auto& column : _columns) {
            srcs.push_back(&(down_cast<const ColumnType*>(column.get())->get_data()));
        }

        for (auto& p : _perm) {
            data[output++] = (*srcs[p.chunk_index])[p.index_in_chunk];
        }

        return Status::OK();
    }

    Status do_visit(ConstColumn* dst) {
        if (_columns.empty() || _perm.empty()) {
            return Status::OK();
        }
        for (auto& p : _perm) {
            dst->append(*_columns[p.chunk_index], p.index_in_chunk, 1);
        }

        return Status::OK();
    }

    Status do_visit(ArrayColumn* dst) {
        if (_columns.empty() || _perm.empty()) {
            return Status::OK();
        }

        for (auto& p : _perm) {
            dst->append(*_columns[p.chunk_index], p.index_in_chunk, 1);
        }

        return Status::OK();
    }

    Status do_visit(MapColumn* dst) {
        if (_columns.empty() || _perm.empty()) {
            return Status::OK();
        }

        for (auto& p : _perm) {
            dst->append(*_columns[p.chunk_index], p.index_in_chunk, 1);
        }

        return Status::OK();
    }

    Status do_visit(StructColumn* dst) {
        // TODO(SmithCruise) Not tested.
        if (_columns.empty() || _perm.empty()) {
            return Status::OK();
        }

        for (auto& p : _perm) {
            dst->append(*_columns[p.chunk_index], p.index_in_chunk, 1);
        }

        return Status::OK();
    }

    template <typename T>
    Status do_visit(BinaryColumnBase<T>* dst) {
        using Container = typename BinaryColumnBase<T>::BinaryDataProxyContainer;
        std::vector<const Container*> srcs;
        for (auto& column : _columns) {
            srcs.push_back(&(down_cast<const BinaryColumnBase<T>*>(column.get())->get_proxy_data()));
        }

        auto& offsets = dst->get_offset();
        auto& bytes = dst->get_bytes();
        size_t old_rows = dst->size();
        size_t num_offsets = offsets.size();
        size_t num_bytes = bytes.size();
        offsets.resize(num_offsets + _perm.size());
        for (auto& p : _perm) {
            Slice slice = (*srcs[p.chunk_index])[p.index_in_chunk];
            offsets[num_offsets] = offsets[num_offsets - 1] + slice.get_size();
            ++num_offsets;
            num_bytes += slice.get_size();
        }

        bytes.resize(num_bytes);
        for (size_t i = 0; i < _perm.size(); i++) {
            Slice slice = (*srcs[_perm[i].chunk_index])[_perm[i].index_in_chunk];
            strings::memcpy_inlined(bytes.data() + offsets[old_rows + i], slice.get_data(), slice.get_size());
        }

        dst->invalidate_slice_cache();

        return Status::OK();
    }

    template <typename T>
    Status do_visit(ObjectColumn<T>* dst) {
        for (auto& p : _perm) {
            dst->append(*_columns[p.chunk_index], p.index_in_chunk, 1);
        }

        return Status::OK();
    }

    Status do_visit(JsonColumn* dst) {
        for (auto& p : _perm) {
            dst->append(*_columns[p.chunk_index], p.index_in_chunk, 1);
        }

        return Status::OK();
    }

private:
    const Columns& _columns;
    const PermutationView& _perm;
};

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
} // namespace starrocks
