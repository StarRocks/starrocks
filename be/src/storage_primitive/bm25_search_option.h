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

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "common/global_types.h" // SlotId

namespace starrocks {

// I3: tablet-local BM25 statistics. Produced once per tablet in Phase-1 and consumed row-by-row in
// Phase-2. `idf` is indexed by query-term position (same order across all segments of the tablet).
struct BM25Stats {
    int64_t N = 0;                  // number of documents (rows) in the tablet
    double avgdl = 0.0;             // average document length = sum_len / N
    std::vector<double> idf;        // per query term, len == #query terms
    std::vector<std::string> terms; // tokenized query terms (aligned with idf); Phase-2 resolves them
                                    // to each segment's dict ordinals. Tokenized once in Phase-1.
    double k1 = 1.2;                // term-frequency saturation
    double b = 0.75;                // length normalization
    // WAND cross-segment pruning threshold shared by every WandScorer of one tablet scan (created in
    // Phase-1). A runtime monotonic max of each segment's k-th best score, not a corpus statistic: each
    // segment seeds its bound from it and publishes its k-th best back. Null = per-segment pruning.
    std::shared_ptr<std::atomic<double>> shared_threshold;
};
using BM25StatsPtr = std::shared_ptr<BM25Stats>;

// I5a: FE->BE BM25 request, constructed from TBM25SearchOptions. Carried through TabletReaderParams /
// SegmentReadOptions to the scan, mirroring VectorSearchOption.
struct BM25SearchOption {
    bool enable = false;
    std::string query;             // the MATCH query string (single column in v1)
    std::string column_id;         // stable ColumnId of the column whose builtin GIN index carries the freqs
    std::string score_column_name; // "__bm25_score"
    SlotId score_slot_id = -1;     // output slot the synthesized score column fills
    double k1 = 1.2;
    double b = 0.75;
    int64_t topk = 0; // LIMIT+OFFSET pushed into the scored scan; 0 = score all matched rows
};
using BM25SearchOptionPtr = std::shared_ptr<BM25SearchOption>;

} // namespace starrocks
