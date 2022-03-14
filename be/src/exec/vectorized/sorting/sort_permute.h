// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <vector>

#include "column/vectorized_fwd.h"
#include "simd/simd.h"

namespace starrocks::vectorized {

enum CompareStrategy {
    Default = 0,
    RowWise = 1,
    ColumnWise = 2,
    ColumnInc = 3,
};

struct PermutationItem {
    uint32_t chunk_index;
    uint32_t index_in_chunk;
    uint32_t permutation_index;
};

// Permutate items in a single chunk, so `chunk_index` is unnecessary
struct SmallPermuteItem {
    uint32_t index_in_chunk;

    bool operator==(const SmallPermuteItem& rhs) const { return index_in_chunk == rhs.index_in_chunk; }
};

// Inline data value into the permutation to optimize cache efficiency
template <class T>
struct InlinePermuteItem {
    // NOTE: do not inline a large value
    static_assert(sizeof(T) <= 16, "Do not inline a large value");

    T inline_value;
    uint32_t index_in_chunk;
};

template <class T>
using InlinePermutation = std::vector<InlinePermuteItem<T>>;

using Permutation = std::vector<PermutationItem>;
using SmallPermutation = std::vector<SmallPermuteItem>;

template <class T, class Container>
static inline InlinePermutation<T> create_inline_permutation(const SmallPermutation& other,
                                                             const Container& container) {
    InlinePermutation<T> inlined(other.size());
    for (int i = 0; i < other.size(); i++) {
        int index = other[i].index_in_chunk;
        inlined[i].index_in_chunk = index;
        inlined[i].inline_value = container[index];
    }
    return inlined;
}

template <class T>
static inline void restore_inline_permutation(const InlinePermutation<T>& inlined, SmallPermutation& output) {
    for (int i = 0; i < inlined.size(); i++) {
        output[i].index_in_chunk = inlined[i].index_in_chunk;
    }
}

inline SmallPermutation create_small_permutation(int rows) {
    SmallPermutation perm(rows);
    for (int i = 0; i < rows; i++) {
        perm[i].index_in_chunk = i;
    }
    return perm;
}

inline void restore_small_permutation(const SmallPermutation& perm, Permutation& output) {
    output.resize(perm.size());
    for (int i = 0; i < perm.size(); i++) {
        output[i].index_in_chunk = perm[i].index_in_chunk;
    }
}

// Tie and TieIterator
// Tie is a compact representation of equal ranges in a vector, in which `1` means equal and `0` means not equal.
// E.g. [0, 1, 1, 0, 1, 1, 1] means that, the elements from 0 to 2 are equal, the elements from 3 to 6 are equal.
// To iterate equal range in a tie, TieIterator could be employed.
using Tie = std::vector<uint8_t>;

struct TieIterator {
    const Tie& tie;
    const int begin;
    const int end;

    // For outer access
    int range_first;
    int range_last;

    TieIterator(const Tie& tie) : TieIterator(tie, 0, tie.size()) {}

    TieIterator(const Tie& tie, int begin, int end) : tie(tie), begin(begin), end(end) {
        range_first = begin;
        range_last = end;
        _inner_range_first = begin;
        _inner_range_last = end;
    }

    // Iterate the tie
    // Return false means the loop should terminate
    bool next() {
        if (_inner_range_first >= end) {
            return false;
        }
        _inner_range_first = SIMD::find_nonzero(tie, _inner_range_first + 1);
        if (_inner_range_first >= end) {
            return false;
        }
        _inner_range_first--;
        _inner_range_last = SIMD::find_zero(tie, _inner_range_first + 1);
        if (_inner_range_last > end) {
            return false;
        }

        range_first = _inner_range_first;
        range_last = _inner_range_last;
        _inner_range_first = _inner_range_last;
        return true;
    }

private:
    int _inner_range_first;
    int _inner_range_last;
};

// Compare result of column, value must be -1,0,1
using CompareVector = std::vector<int8_t>;

} // namespace starrocks::vectorized