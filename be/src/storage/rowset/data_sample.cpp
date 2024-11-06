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

#include "storage/rowset/data_sample.h"

#include <fmt/format.h>

#include <random>

namespace starrocks {

StatusOr<RowIdSparseRange> BlockDataSample::sample() {
    std::mt19937 mt(_random_seed);
    std::bernoulli_distribution dist(_probability_percent / 100.0);

    // Directly use member variables for rows_per_block and total_rows
    size_t sampled_blocks = 0;
    size_t total_blocks = _total_rows / _rows_per_block;
    RowIdSparseRange sampled_ranges;
    for (size_t i = 0; i < _total_rows; i += _rows_per_block) {
        if (dist(mt)) {
            sampled_blocks++;
            sampled_ranges.add(RowIdRange(i, std::min(i + _rows_per_block, _total_rows)));
        }
    }

    VLOG(2) << fmt::format("sample {}/{} blocks in segment", sampled_blocks, total_blocks);

    return sampled_ranges;
}

StatusOr<RowIdSparseRange> PageDataSample::sample() {
    int64_t probability_percent = _probability_percent;
    int64_t random_seed = _random_seed;
    size_t num_data_pages = _num_pages;
    size_t sampled_pages = 0;
    std::mt19937 mt(random_seed);
    std::bernoulli_distribution dist(probability_percent / 100.0);
    RowIdSparseRange sampled_ranges;
    for (size_t i = 0; i < num_data_pages; i++) {
        if (dist(mt)) {
            // FIXME(murphy) use rowid_t rather than ordinal_t
            auto [first_ordinal, last_ordinal] = _page_indexer(i);
            sampled_ranges.add(RowIdRange(first_ordinal, last_ordinal));
            sampled_pages++;
        }
    }

    VLOG(2) << fmt::format("sample {}/{} pages", sampled_pages, num_data_pages);

    return sampled_ranges;
}

} // namespace starrocks