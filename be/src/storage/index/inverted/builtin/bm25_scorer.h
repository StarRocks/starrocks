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

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "storage_primitive/bm25_search_option.h"
#include "storage_primitive/rowid_types.h"

namespace roaring {
class Roaring;
}

namespace starrocks {

class FreqsIterator;
class IndexReadOptions;

// I8: Phase-2 scoring engine. score-all now; WAND lands later as a sibling implementation behind the
// same run() interface (so segment_iterator / stats / storage never change for WAND).
class BM25Scorer {
public:
    virtual ~BM25Scorer() = default;
    // Accumulate per-row BM25 scores into id2score (segment rowid -> score).
    virtual Status run(std::unordered_map<rowid_t, double>* id2score) = 0;
};

// TAAT (term-at-a-time): for each query term, walk its posting blocks and add its contribution to
// every candidate doc. `term_ords` is per-segment dict ordinals aligned with `stats.idf` by query-term
// position; -1 means the term is absent in this segment. `candidates` (nullable) is the MATCH survivor
// bitmap -- only those rowids are scored; nullptr scores every posting doc. `topk` (0 = keep all) trims
// the result to the top-k rows by score after accumulation, matching the per-segment top-k a future WAND
// scorer emits; TAAT can only trim post-accumulation, since a row's score is complete only once every
// query term has been added.
class ScoreAllScorer : public BM25Scorer {
public:
    ScoreAllScorer(const BM25Stats& stats, FreqsIterator* freqs, const IndexReadOptions& read_opts,
                   std::vector<int64_t> term_ords, const roaring::Roaring* candidates, int64_t topk);
    ~ScoreAllScorer() override;

    Status run(std::unordered_map<rowid_t, double>* id2score) override;

private:
    const BM25Stats& _stats;
    FreqsIterator* _freqs;
    const IndexReadOptions& _read_opts;
    std::vector<int64_t> _term_ords;
    const roaring::Roaring* _candidates;
    int64_t _topk; // 0 = keep all matched rows; >0 = keep only the top-k by score (per segment)
};

} // namespace starrocks
