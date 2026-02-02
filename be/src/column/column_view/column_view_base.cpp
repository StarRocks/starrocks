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

#include <base/simd/gather.h>
#include <column/column.h>
#include <column/column_helper.h>
#include <column/column_view/column_view_base.h>

namespace starrocks {
size_t ColumnViewBase::container_memory_usage() const {
    if (_concat_column) {
        return _concat_column->container_memory_usage();
    }
    size_t usage = 0;
    for (const auto& column : _habitats) {
        usage += column->container_memory_usage();
    }
    usage += sizeof(ColumnViewBase::LocationType) * 2 * _num_rows;
    return usage;
}

size_t ColumnViewBase::reference_memory_usage(size_t from, size_t size) const {
    _to_view();

    if (_concat_column) {
        return _concat_column->reference_memory_usage(from, size);
    }

    size_t usage = 0;
    for (auto i = from; i < from + size; i++) {
        const auto& column = _habitats[_habitat_idx[i]];
        if (column->is_constant() || column->only_null()) {
            continue;
        }
        usage += column->reference_memory_usage(i, 1);
    }

    for (const auto& column : _habitats) {
        if (column->is_constant() && !column->empty()) {
            usage += column->reference_memory_usage(0, 1);
        }
    }
    return usage;
}

size_t ColumnViewBase::memory_usage() const {
    if (_concat_column) {
        return _concat_column->memory_usage();
    }
    size_t usage = 0;
    for (const auto& column : _habitats) {
        usage += column->memory_usage();
    }
    usage += sizeof(ColumnViewBase::LocationType) * 2 * _num_rows;
    return usage;
}

static bool is_continuous(const std::vector<uint32_t>& selection) {
    const auto first = selection[0];
    for (auto i = 0; i < selection.size(); i++) {
        if (selection[i] != first + i) {
            return false;
        }
    }
    return true;
}

void ColumnViewBase::append(const Column& src, size_t offset, size_t count) {
    if (src.is_view()) {
        const ColumnViewBase& src_view = static_cast<const ColumnViewBase&>(src);
        src_view._to_view();
        if (src_view._concat_column) {
            append(*src_view._concat_column, offset, count);
            return;
        }
        std::vector<std::vector<uint32_t> > selections(src_view._habitats.size());
        for (auto hi = 0; hi < src_view._habitats.size(); ++hi) {
            selections[hi].reserve(src_view._habitats[hi]->size());
        }
        for (auto i = offset; i < offset + count; i++) {
            const auto habitat_idx = src_view._habitat_idx[i];
            const auto row_idx = src_view._row_idx[i];
            selections[habitat_idx].push_back(row_idx);
        }
        for (auto hi = 0; hi < selections.size(); ++hi) {
            auto& selection = selections[hi];
            auto& habitat_column = src_view._habitats[hi];
            if (selection.empty()) {
                continue;
            }
            if (selection.back() + 1 - selection.front() == selection.size() && is_continuous(selection)) {
                append(*habitat_column.get(), selection.front(), selection.size());
            } else {
                _append_selective(*habitat_column.get(), std::move(selection));
            }
        }
    } else {
        int habitat_idx = _habitats.size();
        ColumnPtr column_ptr = src.get_ptr();
        _habitats.push_back(column_ptr);
        _num_rows += count;
        _tasks.emplace_back([=]() { _append(habitat_idx, column_ptr, offset, count); });
    }
}

void ColumnViewBase::append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    std::vector<uint32_t> index_container(size);
    std::copy_n(indexes + from, size, index_container.begin());
    _append_selective(src, std::move(index_container));
}

void ColumnViewBase::_append_selective(const Column& src, std::vector<uint32_t>&& indexes) {
    int habitat_idx = _habitats.size();
    ColumnPtr column_ptr = src.get_ptr();
    _habitats.push_back(column_ptr);
    _num_rows += indexes.size();
    _tasks.emplace_back([=, indexes2 = std::move(indexes)] { _append_selective(habitat_idx, column_ptr, indexes2); });
}

void ColumnViewBase::append_default() {
    const auto habitat_idx = _habitats.size();
    _habitats.emplace_back(_default_column->clone());
    _num_rows += 1;
    _tasks.emplace_back([=]() {
        _habitat_idx.push_back(habitat_idx);
        _row_idx.push_back(0);
    });
}

void ColumnViewBase::_append(int habitat_idx, const ColumnPtr& src, size_t offset, size_t count) {
    _habitat_idx.resize(_habitat_idx.size() + count);
    _row_idx.resize(_row_idx.size() + count);
    const auto off = _habitat_idx.size() - count;
    std::fill(_habitat_idx.begin() + off, _habitat_idx.end(), habitat_idx);
    std::iota(_row_idx.begin() + off, _row_idx.end(), offset);
}

void ColumnViewBase::_append_selective(int habitat_idx, const ColumnPtr& src,
                                       const std::vector<uint32_t>& index_container) {
    const auto count = index_container.size();
    _habitat_idx.resize(_habitat_idx.size() + count);
    _row_idx.resize(_row_idx.size() + count);
    const auto off = _habitat_idx.size() - count;
    std::fill(_habitat_idx.begin() + off, _habitat_idx.end(), habitat_idx);
    std::ranges::copy(index_container, _row_idx.begin() + off);
}

void ColumnViewBase::append_to(Column& dest_column, const uint32_t* indexes, uint32_t from, uint32_t count) const {
    _to_view();
    DCHECK(from + count <= _num_rows);
    if (_concat_column) {
        dest_column.append_selective(*_concat_column, indexes, from, count);
        return;
    }
    for (auto i = from; i < from + count; ++i) {
        const auto n = indexes[i];
        const auto& habitat_column = _habitats[_habitat_idx[n]];
        const auto ordinal = habitat_column->is_constant() ? 0 : _row_idx[n];
        if (habitat_column->is_null(ordinal)) {
            DCHECK(dest_column.is_nullable());
            dest_column.append_nulls(1);
        } else {
            dest_column.append(*ColumnHelper::get_data_column(habitat_column.get()), ordinal, 1);
        }
    }
}

void ColumnViewBase::_concat_if_need() const {
    if (_concat_rows_limit != -1 && _num_rows >= _concat_rows_limit) {
        return;
    }

    if (_concat_bytes_limit != -1 && memory_usage() >= _concat_bytes_limit) {
        return;
    }
    auto dst_column = clone_empty();
    for (auto i = 0; i < _num_rows; ++i) {
        const auto& habitat_column = _habitats[_habitat_idx[i]];
        const auto ordinal = habitat_column->is_constant() ? 0 : _row_idx[i];
        if (habitat_column->is_null(ordinal)) {
            DCHECK(dst_column->is_nullable());
            dst_column->append_nulls(1);
        } else {
            dst_column->append(*ColumnHelper::get_data_column(habitat_column.get()), ordinal, 1);
        }
    }
    _concat_column = std::move(dst_column);
    _habitat_idx.clear();
    _row_idx.clear();
    _habitats.clear();
}

void ColumnViewBase::_to_view() const {
    std::call_once(_to_view_flag, [this]() {
        if (_tasks.empty()) {
            return;
        }
        DCHECK(_habitat_idx.size() == _row_idx.size());
        _habitat_idx.reserve(_num_rows);
        _row_idx.reserve(_num_rows);
        for (const auto& task : _tasks) {
            task();
        }
        _tasks.clear();
        _concat_if_need();
    });
}
} // namespace starrocks
