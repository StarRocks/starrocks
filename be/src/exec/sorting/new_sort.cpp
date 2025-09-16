
#include "exec/sorting/new_sort.h"

#include <openssl/ssl.h>

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
#include "exec/sorting/sort_helper.h"
#include "exec/sorting/sorting.h"
#include "gutil/casts.h"
#include "gutil/strings/fastmem.h"
#include "util/orlp/pdqsort.h"

namespace starrocks {
class GetInlineSizes final : public ColumnVisitorAdapter<GetInlineSizes> {
public:
    explicit GetInlineSizes() : ColumnVisitorAdapter<GetInlineSizes>(this) {}

    Status do_visit(const NullableColumn& dst) {
        RETURN_IF_ERROR(dst.null_column()->accept(this));
        RETURN_IF_ERROR(dst.data_column()->accept(this));

        return Status::OK();
    }

    template <typename T>
    Status do_visit(const DecimalV3Column<T>& dst) {

        return generic_visit(dst);
    }

    template <typename T>
    Status do_visit(const FixedLengthColumnBase<T>& dst) {
        _inline_sizes.push_back(sizeof(T));
        _alignments.push_back(alignof(T));

        return Status::OK();
    }

    Status do_visit(const ConstColumn& dst) { return generic_visit(dst); }

    Status do_visit(const ArrayColumn& dst) { return generic_visit(dst); }

    Status do_visit(const MapColumn& dst) { return generic_visit(dst); }

    Status do_visit(const StructColumn& dst) {
        // TODO(SmithCruise) Not tested.
        return generic_visit(dst);
    }

    template <typename T>
    Status do_visit(const BinaryColumnBase<T>& dst) {
        _inline_sizes.push_back(sizeof(Slice));
        _alignments.push_back(alignof(Slice));

        return Status::OK();
    }

    template <typename T>
    Status do_visit(const ObjectColumn<T>& dst) {
        return generic_visit(dst);
    }

    Status do_visit(const JsonColumn& dst) { return generic_visit(dst); }

    std::pair<std::vector<size_t>, std::vector<size_t>> get_size_info() && {
        return std::make_pair(std::move(_inline_sizes), std::move(_alignments));
    }

private:
    template <class COL_TYPE>
    Status generic_visit(const COL_TYPE& dst) {
        return Status::OK();
    }

    std::vector<size_t> _inline_sizes{};
    std::vector<size_t> _alignments{};
};

class CreateInlinedData final : public ColumnVisitorAdapter<CreateInlinedData> {
public:
    explicit CreateInlinedData(InlinedData& inlined_data) : ColumnVisitorAdapter<CreateInlinedData>(this), _inlined_data{inlined_data} {}

    Status do_visit(const NullableColumn& dst) {
        RETURN_IF_ERROR(dst.null_column()->accept(this));
        RETURN_IF_ERROR(dst.data_column()->accept(this));

        return Status::OK();
    }

    template <typename T>
    Status do_visit(const DecimalV3Column<T>& dst) {

        return generic_visit(dst);
    }

    template <typename T>
    Status do_visit(const FixedLengthColumnBase<T>& dst) {
        auto& container = dst.get_data();
        for (size_t i = 0; i < container.size(); ++i) {
            _inlined_data.set(i, _curr_inlined_idx, container[i]);
        }
        ++_curr_inlined_idx;

        return Status::OK();
    }

    Status do_visit(const ConstColumn& dst) { return generic_visit(dst); }

    Status do_visit(const ArrayColumn& dst) { return generic_visit(dst); }

    Status do_visit(const MapColumn& dst) { return generic_visit(dst); }

    Status do_visit(const StructColumn& dst) {
        // TODO(SmithCruise) Not tested.
        return generic_visit(dst);
    }

    template <typename T>
    Status do_visit(const BinaryColumnBase<T>& dst) {
        const size_t col_size = dst.size();
        for (size_t i = 0; i < col_size; ++i) {
            _inlined_data.set(i, _curr_inlined_idx, dst.get_slice(i));
        }
        ++_curr_inlined_idx;

        return Status::OK();
    }

    template <typename T>
    Status do_visit(const ObjectColumn<T>& dst) {
        return generic_visit(dst);
    }

    Status do_visit(const JsonColumn& dst) { return generic_visit(dst); }

    size_t get_curr_inlined_index() const { return _curr_inlined_idx; }

private:
    template <class COL_TYPE>
    Status generic_visit(const COL_TYPE& dst) {
        return Status::OK();
    }

    size_t _curr_inlined_idx{0};

    InlinedData& _inlined_data;
};

Status inline_data(const Columns& cols, std::unique_ptr<InlinedData>& out) {
    GetInlineSizes size_visitor{};
    for (auto& col : cols) {
        RETURN_IF_ERROR(col->accept(&size_visitor));
    }
    auto [sizes, alignments] = std::move(size_visitor).get_size_info();
    out = std::make_unique<InlinedData>(std::move(sizes), std::move(alignments), cols.at(0)->size());

    CreateInlinedData bla{*out};
    for (auto& col : cols) {
        RETURN_IF_ERROR(col->accept(&bla));
    }
    const auto row_idx_inlined_idx = bla.get_curr_inlined_index();
    const auto size = cols.at(0)->size();
    for (InlinedData::index_type i = 0; i < size; ++i) {
        out->set(i, row_idx_inlined_idx, i);
    }
    return Status::OK();
}

// Append permutation to column, implements `materialize_by_permutation` function
class MaterializeFromInlinedData : public ColumnVisitorMutableAdapter<MaterializeFromInlinedData> {
public:
    explicit MaterializeFromInlinedData(const Column* src_column, const InlinedData& inlined_data, const SortRange range,
                                     std::optional<size_t*> inlined_index)
            : ColumnVisitorMutableAdapter<MaterializeFromInlinedData>(this),
              _curr_inlined_index(inlined_index),
              _src_column(src_column),
              _inlined_data{inlined_data},
              _range{range} {}

    Status do_visit(NullableColumn* dst) {
        const size_t orig_size = dst->size();
        auto& srcDataColumn = static_cast<const NullableColumn*>(_src_column)->data_column_ref();
        _src_column = static_cast<const NullableColumn*>(_src_column)->null_column();
        RETURN_IF_ERROR(dst->null_column_mutable_ptr()->accept_mutable(this));
        if (!dst->has_null()) {
            dst->set_has_null(
                    SIMD::count_nonzero(&dst->null_column()->get_data()[orig_size], _range.second - _range.first));
        }
        _src_column = &srcDataColumn;
        RETURN_IF_ERROR(dst->data_column_mutable_ptr()->accept_mutable(this));

        return Status::OK();
    }

    template <typename T>
    Status do_visit(DecimalV3Column<T>* dst) {
        return generic_visit(dst);
    }

    template <typename T>
    Status do_visit(FixedLengthColumnBase<T>* dst) {
        using Container = typename FixedLengthColumnBase<T>::Container;
        using ColumnType = FixedLengthColumnBase<T>;

        auto& data = dst->get_data();
        size_t output = data.size();
        data.resize(output + (_range.second - _range.first));

        auto end = _inlined_data.begin() + _range.second;

        if (_curr_inlined_index.has_value()) {
            auto get_value = _inlined_data.get_value<T>(*_curr_inlined_index.value());
            for (auto curr = _inlined_data.begin() + _range.first; curr != end; ++curr) {
                data[output++] = get_value(*curr);
            }
            ++(*_curr_inlined_index.value());
        } else {
            auto get_idx = _inlined_data.get_idx();
            auto src_container = static_cast<const ColumnType*>(_src_column)->get_data();
            for (auto curr = _inlined_data.begin() + _range.first; curr != end; ++curr) {
                data[output++] = src_container[get_idx(*curr)];
            }
        }

        return Status::OK();
    }

    Status do_visit(ConstColumn* dst) { return generic_visit(dst); }

    Status do_visit(ArrayColumn* dst) { return generic_visit(dst); }

    Status do_visit(MapColumn* dst) { return generic_visit(dst); }

    Status do_visit(StructColumn* dst) {
        // TODO(SmithCruise) Not tested.
        return generic_visit(dst);
    }

    template <typename T>
    Status do_visit(BinaryColumnBase<T>* dst) {
        using ColumnType = BinaryColumnBase<T>;

        auto& offsets = dst->get_offset();
        auto& bytes = dst->get_bytes();

        const auto begin = _inlined_data.begin() + _range.first;
        const auto end = _inlined_data.begin() + _range.second;

        size_t added_bytes{0};
        std::vector<Slice> slices{};
        slices.reserve(_range.second - _range.first);
        if (_curr_inlined_index.has_value()) {
            auto get_slice = _inlined_data.get_str(*_curr_inlined_index.value());
            for (auto curr = begin; curr != end; ++curr) {
                Slice slice = get_slice(*curr);
                added_bytes += slice.get_size();
                slices.push_back(slice);
            }
            ++(*_curr_inlined_index.value());

        } else {
            auto get_idx = _inlined_data.get_idx();
            for (auto curr = begin; curr != end; ++curr) {
                Slice slice = static_cast<const ColumnType*>(_src_column)->get_slice(get_idx(*curr));
                added_bytes += slice.get_size();
                slices.push_back(slice);
            }
        }

        bytes.resize(bytes.size() + added_bytes);
        offsets.reserve(offsets.size() + slices.size());

        DCHECK(!offsets.empty());
        auto curr_offset = offsets.back();
        auto* const byte_ptr = bytes.data();

        for (Slice slice : slices) {
            strings::memcpy_inlined(byte_ptr + curr_offset, slice.get_data(), slice.get_size());
            curr_offset += slice.get_size();
            offsets.push_back(curr_offset);
        }

        dst->invalidate_slice_cache();

        return Status::OK();
    }

    template <typename T>
    Status do_visit(ObjectColumn<T>* dst) {
        return generic_visit(dst);
    }

    Status do_visit(JsonColumn* dst) { return generic_visit(dst); }

private:
    template <class COL_TYPE>
    Status generic_visit(COL_TYPE* dst) {
        auto end = _inlined_data.begin() + _range.second;

        auto get_index = _inlined_data.get_idx();
        for (auto curr = _inlined_data.begin() + _range.first; curr != end; ++curr) {
            dst->append(*_src_column, get_index(*curr), 1);
        }

        return Status::OK();
    }

    const std::optional<size_t*> _curr_inlined_index;
    const Column* _src_column;
    const InlinedData& _inlined_data;
    const SortRange _range;
};

Status materialize(ChunkPtr& dst, const ChunkPtr& src, const InlinedData& inlined_data, SortRange range) {
    const size_t chunk_cols = dst->num_columns();
    size_t currInlinedIndex{0};
    for (size_t col_index = 0; col_index < chunk_cols; col_index++) {
        MaterializeFromInlinedData visitor{src->get_column_by_index(col_index), inlined_data, range, &currInlinedIndex};
        RETURN_IF_ERROR(dst->get_column_by_index(col_index)->accept_mutable(&visitor));
    }
    return Status::OK();
}

template <class Cmp>
void update_tie(std::span<uint8_t>& tie, InlinedDataView& data, Cmp& cmp) {
    DCHECK_EQ(tie.size(), data.size());
    DCHECK(tie.empty() || tie[0] == 0);
    for (size_t i = 1; i < tie.size(); ++i) {
        DCHECK_EQ(1, tie[i]);
        if (cmp(data[i - 1], data[i]) != 0) {
            tie[i] = 0;
        }
    }
}

// Returns the range of the non-null entries, so that these can be sorted in the next step
template <class GetNull>
void partition_null_column(const SortDesc& sort_desc, InlinedDataView& dataView,
                           std::optional<std::span<uint8_t>>& opt_tie, GetNull& get_null) {
    const bool null_first = sort_desc.is_null_first();
    auto null_first_pred = [get_null](InlinedDataProxy proxy) { return get_null(proxy) == 1; };
    auto null_last_pred = [get_null](InlinedDataProxy proxy) { return get_null(proxy) != 1; };

    const auto pivot_iter = null_first ? std::partition(dataView.begin(), dataView.end(), null_first_pred)
                                       : std::partition(dataView.begin(), dataView.end(), null_last_pred);

    // The corresponding data column makes directly uses the returned non-null range instead of the tie.
    // That means we do not need to update if the data column is the last column to be sorted.
    if (opt_tie.has_value()) {
        std::span<uint8_t> tie = opt_tie.value();
        DCHECK_EQ(0, tie[0]);
        tie[pivot_iter - dataView.begin()] = 0;
    }

    const ptrdiff_t pivot_idx = pivot_iter - dataView.begin();
    if (null_first) {
        dataView = InlinedDataView{pivot_iter, dataView.end()};
        if (opt_tie.has_value()) {
            opt_tie = opt_tie.value().subspan(pivot_idx);
        }
    } else {
        dataView = InlinedDataView{dataView.begin(), pivot_iter};
        if (opt_tie.has_value()) {
            opt_tie = opt_tie.value().subspan(0, pivot_idx);
        }
    }
}

struct no_function {};

template <class Cmp, class NullCmp>
Status sort_column(const std::atomic<bool>& cancel, const SortDesc& sort_desc, InlinedData& inlined_data, Tie& tie,
                   bool updateTie, Cmp& cmp, NullCmp& getNullValue) {
    auto lesser = [cmp](InlinedDataProxy lhs, InlinedDataProxy rhs) { return cmp(lhs, rhs) < 0; };
    auto greater = [cmp](InlinedDataProxy lhs, InlinedDataProxy rhs) { return cmp(lhs, rhs) > 0; };

    TieIterator iterator{tie};
    while (iterator.next()) {
        DCHECK_EQ(0, tie[0]);
        if (UNLIKELY(cancel.load(std::memory_order_acquire))) {
            return Status::Cancelled("Sort cancelled");
        }
        DCHECK(iterator.range_first < iterator.range_last);
        std::optional<std::span<uint8_t>> tieSpan{};
        if (updateTie) {
            tieSpan = std::span<uint8_t>{tie.begin() + iterator.range_first, tie.begin() + iterator.range_last};
        }
        InlinedDataView dataView{inlined_data.begin() + iterator.range_first, inlined_data.begin() + iterator.range_last};
        if constexpr (!std::is_same_v<NullCmp, no_function>) {
            partition_null_column(sort_desc, dataView, tieSpan, getNullValue);
        }

        if (sort_desc.asc_order()) {
            ::pdqsort(dataView.begin(), dataView.end(), lesser);
        } else {
            ::pdqsort(dataView.begin(), dataView.end(), greater);
        }

        if (updateTie) {
            update_tie(tieSpan.value(), dataView, cmp);
        }
    }
    return Status::OK();
}

// Sort a column by permtuation
class InlinedDataColumnSorter final : public ColumnVisitorAdapter<InlinedDataColumnSorter> {
public:
    explicit InlinedDataColumnSorter(const std::atomic<bool>& cancel, const SortDesc& desc, InlinedData& inlined_data,
                                  size_t& inlined_index, Tie& tie, bool updateTie)
            : ColumnVisitorAdapter<InlinedDataColumnSorter>(this),
              _cancel{cancel},
              _sort_desc{desc},
              _curr_inlined_index(inlined_index),
              _inlined_data{inlined_data},
              _tie{tie},
              _updateTie{updateTie} {}

    Status do_visit(const NullableColumn& column) {
        _last_column_null_column = true;
        ++_curr_inlined_index;
        return column.data_column()->accept(this);
    }

    Status do_visit(const ConstColumn& column) {
        // noop
        return Status::OK();
    }

    Status do_visit(const ArrayColumn& column) { return generic_visit(column); }

    Status do_visit(const MapColumn& column) { return Status::InternalError("Sort does not support map columns"); }

    Status do_visit(const StructColumn& column) { return generic_visit(column); }

    template <typename T>
    Status do_visit(const BinaryColumnBase<T>& column) {
        auto get_str = _inlined_data.get_str(_curr_inlined_index);
        auto cmp = [get_str](InlinedDataProxy lhs, InlinedDataProxy rhs) { return get_str(lhs).compare(get_str(rhs)); };
        RETURN_IF_ERROR(sort(cmp));

        ++_curr_inlined_index;
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const FixedLengthColumnBase<T>& column) {
        auto get_value = _inlined_data.get_value<T>(_curr_inlined_index);
        auto cmp = [get_value](InlinedDataProxy lhs, InlinedDataProxy rhs) {
            return SorterComparator<T>::compare(get_value(lhs), get_value(rhs));
        };
        RETURN_IF_ERROR(sort(cmp));
        ++_curr_inlined_index;
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const ObjectColumn<T>& column) {
        DCHECK(false) << "not support object column sort_and_tie";

        return Status::NotSupported("not support object column sort_and_tie");
    }

    Status do_visit(const JsonColumn& column) {
        auto get_idx = _inlined_data.get_idx();
        auto cmp = [get_idx, &column](InlinedDataProxy lhs, InlinedDataProxy rhs) {
            return column.get_object(get_idx(lhs))->compare(*column.get_object(get_idx(rhs)));
        };
        return sort(cmp);
    }

private:
    template <class ColumnType>
    Status generic_visit(const ColumnType& column) {
        auto get_idx = _inlined_data.get_idx();
        auto nan_direction = _sort_desc.nan_direction();
        auto cmp = [nan_direction, get_idx, &column](InlinedDataProxy lhs, InlinedDataProxy rhs) {
            return column.compare_at(get_idx(lhs), get_idx(rhs), column, nan_direction);
        };
        return sort(cmp);
    }

    template <class Cmp>
    Status sort(const Cmp& cmp) {
        if (_last_column_null_column) {
            auto get_null = _inlined_data.get_value<uint8_t>(_curr_inlined_index - 1);
            return sort_column(_cancel, _sort_desc, _inlined_data, _tie, _updateTie, cmp, get_null);
        } else {
            no_function dummy{};
            return sort_column(_cancel, _sort_desc, _inlined_data, _tie, _updateTie, cmp, dummy);
        }
    }

    const std::atomic<bool>& _cancel;
    SortDesc _sort_desc;
    size_t& _curr_inlined_index;
    InlinedData& _inlined_data;
    Tie& _tie;
    bool _updateTie;
    bool _last_column_null_column{false};
};

Status materialize(Chunk& target_chunk, const ChunkPtr& src_chunk, const std::vector<size_t>& order_by_indexes,
                   const InlinedData& inlined_data, SortRange range) {
    // First restore the order by columns
    size_t currInlinedIndex{0};
    for (auto order_by_idx : order_by_indexes) {
        MaterializeFromInlinedData mat{src_chunk->get_column_by_index(order_by_idx), inlined_data, range, &currInlinedIndex};
        RETURN_IF_ERROR(target_chunk.get_column_by_index(order_by_idx)->accept_mutable(&mat));
    }
    // materialize all other columns
    for (size_t i = 0; i < src_chunk->columns().size(); ++i) {
        if (std::find(std::begin(order_by_indexes), std::end(order_by_indexes), i) != std::end(order_by_indexes)) {
            continue;
        }
        MaterializeFromInlinedData mat{src_chunk->get_column_by_index(i), inlined_data, range, std::nullopt};
        RETURN_IF_ERROR(target_chunk.get_column_by_index(i)->accept_mutable(&mat));
    }
    return Status::OK();
}

Status sort(const std::atomic<bool>& cancel, Columns& columns, InlinedData& inlined_data, const SortDescs& sort_descs) {
    Tie tie(inlined_data.size(), 1);
    tie.at(0) = 0;
    size_t currInlinedIndex{0};
    for (size_t i = 0; i < columns.size(); ++i) {
        const bool updateTie = i + 1 != columns.size();
        InlinedDataColumnSorter sorter{cancel, sort_descs.get_column_desc(i), inlined_data, currInlinedIndex, tie, updateTie};
        RETURN_IF_ERROR(columns.at(i)->accept(&sorter));
    }
    return Status::OK();
}
} // namespace starrocks
