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
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/builtin/bm25_scorer.h"
#include "storage_primitive/rowid_types.h"

namespace roaring {
class Roaring;
}

namespace starrocks {

class BlockPostingIterator;
class FreqsIterator;
class IndexReadOptions;

// Block-max WAND top-k scorer (Broder et al. 2003; Ding & Suel 2011): DAAT traversal over one posting
// cursor per query term. Term-level bounds (max over the block directory) drive pivot selection; a
// stateless directory recheck of the pivot's covering blocks gates every decode, and a failed recheck
// skips a whole block range. Returns at most `topk` entries; every returned score is exact (bounds only
// gate which docs get fully scored), so downstream consumes it exactly like ScoreAllScorer's output.
class WandScorer : public BM25Scorer {
public:
    // shared_threshold (nullable) is a monotonically increasing accumulator of "some scorer's k-th
    // best score" shared by every scorer of one scan: each run seeds its pruning threshold from it and
    // publishes its own k-th best back. Under today's serial per-tablet segment scan this is exact
    // cross-segment carry-over; under concurrent scorers a stale read only prunes less, never wrong.
    WandScorer(const BM25Stats& stats, FreqsIterator* freqs, const IndexReadOptions& read_opts,
               std::vector<int64_t> term_ords, const roaring::Roaring* candidates, int64_t topk,
               std::atomic<double>* shared_threshold = nullptr);
    ~WandScorer() override;

    Status run(std::unordered_map<rowid_t, double>* id2score) override;

    // Documents fully scored (matched docs minus pruned); exposed for tests and diagnostics.
    int64_t docs_scored() const { return _docs_scored; }

private:
    struct TermCursor {
        std::unique_ptr<BlockPostingIterator> it;
        double idf = 0.0;
        double ub = 0.0;    // upper bound of this term's contribution over its whole posting list
        uint32_t doc = 0;   // current docid
        uint32_t idx = 0;   // index of `doc` within the decoded block
        bool valid = false; // false once the posting list is exhausted
    };

    Status _open_cursors();
    // Advance to the first posting with docid >= target; clears `valid` past the end of the list.
    Status _next_geq(TermCursor* c, uint32_t target);
    // Advance by exactly one posting (cheaper than _next_geq for the aligned-cursor case).
    Status _advance(TermCursor* c);
    // Local upper bound of c's contribution to any doc inside the block covering `docid`, plus that
    // block's last docid. Stateless directory binary search: no decode, no cursor movement. Returns 0
    // with *block_last = UINT32_MAX when c has no posting >= docid.
    double _block_ub_at(const TermCursor& c, uint32_t docid, uint32_t* block_last) const;

    const BM25Stats& _stats;
    FreqsIterator* _freqs;
    const IndexReadOptions& _read_opts;
    std::vector<int64_t> _term_ords;
    const roaring::Roaring* _candidates;
    int64_t _topk;
    std::atomic<double>* _shared_threshold;
    std::vector<TermCursor> _cursors;
    int64_t _docs_scored = 0;
};

} // namespace starrocks
