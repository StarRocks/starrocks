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
#include "exec/chunks_sorter_topn.h"
namespace starrocks {
class ChunksSorterDistinctTopn : public ChunksSorterTopn {
public:
    ChunksSorterDistinctTopn(RuntimeState* state, const std::vector<ExprContext*>* sort_exprs,
                             const std::vector<bool>* is_asc_order, const std::vector<bool>* is_null_first,
                             const std::string& sort_keys, size_t offset, size_t limit, size_t max_buffered_chunks)
            : ChunksSorterTopn(state, sort_exprs, is_asc_order, is_null_first, sort_keys, offset, limit,
                               TTopNType::DENSE_RANK, max_buffered_chunks) {}

    ~ChunksSorterDistinctTopn() override;

private:
    Status _sort_chunks(RuntimeState* state) override;
    Status _filter_and_sort_data_for_dense_rank(RuntimeState* state, std::pair<Permutation, Permutation>& permutations,
                                                DataSegments& segments, size_t& distinct_top_n_of_first_size,
                                                size_t& distinct_top_n_of_second_size);
    Status _merge_sort_data_as_merged_segment_for_dense_rank(RuntimeState* state,
                                                             std::pair<Permutation, Permutation>& new_permutation,
                                                             DataSegments& segments,
                                                             size_t distinct_top_n_of_first_size,
                                                             size_t distinct_top_n_of_second_size);
    Status _hybrid_sort_common_for_dense_rank(RuntimeState* state, std::pair<Permutation, Permutation>& new_permutation,
                                              DataSegments& segments, size_t first_distinct_num,
                                              size_t second_distinct_num);
    // get distinct top n from segments if exsits, otherwise get as m
    Status _partial_sort_col_wise(RuntimeState* state, Permutation& permutation, DataSegments& segments, size_t limit,
                                  size_t& distinct_top_n);
};
} // namespace starrocks