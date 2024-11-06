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

#include <functional>
#include <memory>

#include "common/statusor.h"
#include "storage/range.h"
#include "storage/rowset/common.h"

namespace starrocks {

// Provide the oridinal of each page
using PageIndexer = std::function<std::pair<ordinal_t, ordinal_t>(size_t page_index)>;

class BlockDataSample;
class PageDataSample;

class DataSample {
public:
    virtual ~DataSample() = default;

    DataSample(int64_t probability_percent, int64_t random_seed)
            : _probability_percent(probability_percent), _random_seed(random_seed) {}

    static std::unique_ptr<BlockDataSample> make_block_sample(int64_t probability_percent, int64_t random_seed,
                                                              size_t rows_per_block, size_t total_rows) {
        return std::make_unique<BlockDataSample>(probability_percent, random_seed, rows_per_block, total_rows);
    }

    static std::unique_ptr<PageDataSample> make_page_sample(int64_t probability_percent, int64_t random_seed,
                                                            size_t num_pages, PageIndexer page_indexer) {
        return std::make_unique<PageDataSample>(probability_percent, random_seed, num_pages, std::move(page_indexer));
    }

    virtual StatusOr<RowIdSparseRange> sample() = 0;

protected:
    int64_t _probability_percent;
    int64_t _random_seed;
};

class BlockDataSample final : public DataSample {
public:
    BlockDataSample(int64_t probability_percent, int64_t random_seed, size_t rows_per_block, size_t total_rows)
            : DataSample(probability_percent, random_seed), _rows_per_block(rows_per_block), _total_rows(total_rows) {}

    StatusOr<RowIdSparseRange> sample() override;

private:
    size_t _rows_per_block;
    size_t _total_rows;
};

class PageDataSample final : public DataSample {
public:
    PageDataSample(int64_t probability_percent, int64_t random_seed, size_t num_pages, PageIndexer page_indexer)
            : DataSample(probability_percent, random_seed),
              _num_pages(num_pages),
              _page_indexer(std::move(page_indexer)) {}

    StatusOr<RowIdSparseRange> sample() override;

private:
    size_t _num_pages;
    PageIndexer _page_indexer;
};

} // namespace starrocks