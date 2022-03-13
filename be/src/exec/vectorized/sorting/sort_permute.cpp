// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized//sorting/sort_permute.h"

#include "column/column.h"

namespace starrocks::vectorized {

size_t PermutatedColumn::num_rows() const {
    return column.size();
}

size_t PermutatedColumn::size() const {
    return column.size();
}

int PermutatedColumn::get_row_index(int row) const {
    return perm[row].index_in_chunk;
}

std::string PermutatedColumn::debug_string() const {
    return column.debug_string();
}

std::string PermutatedColumn::debug_string(std::pair<int, int> range) const {
    std::string res;
    res += "[";
    for (int i = range.first; i < range.second; i++) {
        res += column.debug_item(perm[i].index_in_chunk);
        res += ",";
    }
    res += "]";
    return res;
}

int PermutatedColumn::compare_at(const PermutatedColumn& rhs, int row, int sort_order, int null_first) const {
    return sort_order * column.compare_at(get_row_index(row), rhs.get_row_index(row), rhs.column, null_first);
}

int PermutatedColumn::compare_at(int lhs_row, int rhs_row, const PermutatedColumn& rhs, int sort_order,
                                 int null_first) const {
    return sort_order * column.compare_at(get_row_index(lhs_row), rhs.get_row_index(rhs_row), rhs.column, null_first);
}

} // namespace starrocks::vectorized