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

#include "storage/index/inverted/builtin/bm25_scorer.h"

#include <algorithm>
#include <roaring/roaring.hh>
#include <utility>
#include <vector>

#include "storage/index/inverted/builtin/block_posting_reader.h"
#include "storage/index/inverted/builtin/bm25_scoring.h"
#include "storage/index/inverted/builtin/builtin_inverted_reader.h"

namespace starrocks {

ScoreAllScorer::ScoreAllScorer(const BM25Stats& stats, FreqsIterator* freqs, const IndexReadOptions& read_opts,
                               std::vector<int64_t> term_ords, const roaring::Roaring* candidates, int64_t topk)
        : _stats(stats),
          _freqs(freqs),
          _read_opts(read_opts),
          _term_ords(std::move(term_ords)),
          _candidates(candidates),
          _topk(topk) {}

ScoreAllScorer::~ScoreAllScorer() = default;

Status ScoreAllScorer::run(std::unordered_map<rowid_t, double>* id2score) {
    for (size_t t = 0; t < _term_ords.size(); ++t) {
        if (_term_ords[t] < 0) {
            continue; // term absent in this segment -> no contribution
        }
        const double idf = _stats.idf[t];
        std::unique_ptr<BlockPostingIterator> cursor;
        RETURN_IF_ERROR(_freqs->new_posting_cursor(_read_opts, &cursor));
        RETURN_IF_ERROR(cursor->seek_to_term(static_cast<uint32_t>(_term_ords[t])));
        while (cursor->has_next_block()) {
            RETURN_IF_ERROR(cursor->next_block());
            const uint32_t* docids = cursor->docids();
            const uint32_t* tfs = cursor->tfs();
            const size_t n = cursor->cur_block_size();
            for (size_t i = 0; i < n; ++i) {
                const rowid_t d = docids[i];
                if (_candidates != nullptr && !_candidates->contains(d)) {
                    continue;
                }
                ASSIGN_OR_RETURN(uint32_t dl, _freqs->doc_len(d));
                (*id2score)[d] += bm25_term(tfs[i], dl, idf, _stats);
            }
        }
    }

    // topk > 0: keep only the top-k rows by score (per segment). A future WAND scorer emits its
    // per-segment top-k directly; TAAT cannot prune while accumulating (a row's score completes only
    // after every term), so trim once here after the full pass. Tie-break by rowid for determinism.
    if (_topk > 0 && static_cast<int64_t>(id2score->size()) > _topk) {
        std::vector<std::pair<rowid_t, double>> entries(id2score->begin(), id2score->end());
        auto better = [](const std::pair<rowid_t, double>& a, const std::pair<rowid_t, double>& b) {
            return a.second != b.second ? a.second > b.second : a.first < b.first;
        };
        std::nth_element(entries.begin(), entries.begin() + _topk, entries.end(), better);
        id2score->clear();
        for (int64_t i = 0; i < _topk; ++i) {
            id2score->emplace(entries[i].first, entries[i].second);
        }
    }
    return Status::OK();
}

} // namespace starrocks
