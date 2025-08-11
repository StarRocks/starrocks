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

#pragma once

#include <vector>

#include "column/vectorized_fwd.h"
#include "glog/logging.h"
#include "simd/simd.h"
#include "util/array_view.hpp"

namespace starrocks {

struct PermutationItem {
    uint32_t chunk_index;
    uint32_t index_in_chunk;

    PermutationItem() = default;
    PermutationItem(uint32_t ci, uint32_t ii) : chunk_index(ci), index_in_chunk(ii) {}
};

// Permutate items in a single chunk, so `chunk_index` is unnecessary
struct SmallPermuteItem {
    uint32_t index_in_chunk;

    bool operator==(const SmallPermuteItem& rhs) const { return index_in_chunk == rhs.index_in_chunk; }
};

// Permutate items in a single chunk, so `chunk_index` is unnecessary
struct LargePermuteItem {
    uint64_t index_in_chunk;

    bool operator==(const LargePermuteItem& rhs) const { return index_in_chunk == rhs.index_in_chunk; }
};

// Inline data value into the permutation to optimize cache efficiency
template <class T, class P>
struct BaseInlinePermuteItem {
    // NOTE: do not inline a large value
    static_assert(sizeof(T) <= 32, "Do not inline a large value");

    T inline_value;
    P permutation;
};

template <class T>
using SmallInlinePermuteItem = BaseInlinePermuteItem<T, SmallPermuteItem>;
template <class T>
using LargeInlinePermuteItem = BaseInlinePermuteItem<T, LargePermuteItem>;
template <class T>
using InlinePermuteItem = BaseInlinePermuteItem<T, PermutationItem>;

using Permutation = std::vector<PermutationItem>;
using PermutationView = array_view<PermutationItem>;
using SmallPermutation = std::vector<SmallPermuteItem>;
using SmallPermutationView = array_view<SmallPermuteItem>;
using LargePermutation = std::vector<LargePermuteItem>;
using LargePermutationView = array_view<LargePermuteItem>;

template <class T>
concept SingleContainerPermutation = std::is_same_v<T, SmallPermutation> || std::is_same_v<T, LargePermutation>;
template <class T>
concept SingleContainerPermutationView =
        std::is_same_v<T, SmallPermutationView> || std::is_same_v<T, LargePermutationView>;

template <class T, class P>
using BaseInlinePermutation = std::vector<BaseInlinePermuteItem<T, P>>;
template <class T>
using SmallInlinePermutation = BaseInlinePermutation<T, SmallPermuteItem>;
template <class T>
using LargeInlinePermutation = BaseInlinePermutation<T, LargePermuteItem>;
template <class T>
using InlinePermutation = BaseInlinePermutation<T, PermutationItem>;

template <class T, SingleContainerPermutation P, bool CheckBound = false>
static inline BaseInlinePermutation<T, typename P::value_type> create_inline_permutation(const P& other,
                                                                                         const auto& container) {
    BaseInlinePermutation<T, typename P::value_type> inlined(other.size());
    for (size_t i = 0; i < other.size(); i++) {
        auto perm = other[i];
        if constexpr (CheckBound) {
            if (perm.index_in_chunk >= container.size()) {
                continue;
            }
        }
        inlined[i] = BaseInlinePermuteItem<T, typename P::value_type>{container[perm.index_in_chunk], std::move(perm)};
    }
    return inlined;
}

template <class T, class P>
static inline void restore_inline_permutation(const BaseInlinePermutation<T, typename P::value_type>& inlined,
                                              P& output) {
    for (size_t i = 0; i < inlined.size(); i++) {
        output[i] = inlined[i].permutation;
    }
}

inline SmallPermutation create_small_permutation(uint32_t rows) {
    SmallPermutation perm{};
    perm.reserve(rows);
    for (uint32_t i = 0; i < rows; i++) {
        perm.emplace_back(i);
    }
    return perm;
}

inline LargePermutation create_large_permutation(uint64_t rows) {
    LargePermutation perm{};
    perm.reserve(rows);
    for (uint64_t i = 0; i < rows; i++) {
        perm.emplace_back(i);
    }
    return perm;
}

inline void restore_small_permutation(const SmallPermutation& perm, Permutation& output) {
    output.resize(perm.size());
    for (int i = 0; i < perm.size(); i++) {
        output[i].index_in_chunk = perm[i].index_in_chunk;
    }
}

// Convert a permutation to selection vector, which could be used to filter chunk
template <class Permutation>
inline void permutate_to_selective(const Permutation& perm, std::vector<uint32_t>* select) {
    DCHECK(!!select);
    select->resize(perm.size());
    for (size_t i = 0; i < perm.size(); i++) {
        (*select)[i] = perm[i].index_in_chunk;
    }
}

// Materialize chunk by permutation
void materialize_by_permutation(Chunk* dst, const std::vector<ChunkPtr>& chunks, const PermutationView& perm);
void materialize_column_by_permutation(Column* dst, const Columns& columns, const PermutationView& perm);

template <SingleContainerPermutationView PV>
void materialize_by_permutation(Chunk* dst, const ChunkPtr& chunk, const PV& perm);
template <SingleContainerPermutationView PV>
void materialize_column_by_permutation(Column* dst, const Column* column, const PV& perm);

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
    bool next();

private:
    int _inner_range_first;
    int _inner_range_last;
};

// Compare result of column, value must be -1,0,1
using CompareVector = Buffer<int8_t>;

} // namespace starrocks
