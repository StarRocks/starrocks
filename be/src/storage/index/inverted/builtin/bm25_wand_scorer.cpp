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

#include "storage/index/inverted/builtin/bm25_wand_scorer.h"

#include <algorithm>
#include <queue>
#include <roaring/roaring.hh>
#include <utility>

#include "storage/index/inverted/builtin/block_posting_reader.h"
#include "storage/index/inverted/builtin/bm25_scoring.h"
#include "storage/index/inverted/builtin/builtin_inverted_reader.h"

namespace starrocks {

namespace {
// atomic<double> has no fetch_max; CAS-max keeps the accumulator monotonically increasing.
void store_max(std::atomic<double>* target, double value) {
    double cur = target->load(std::memory_order_relaxed);
    while (cur < value && !target->compare_exchange_weak(cur, value, std::memory_order_relaxed)) {
    }
}
} // namespace

WandScorer::WandScorer(const BM25Stats& stats, FreqsIterator* freqs, const IndexReadOptions& read_opts,
                       std::vector<int64_t> term_ords, const roaring::Roaring* candidates, int64_t topk,
                       std::atomic<double>* shared_threshold)
        : _stats(stats),
          _freqs(freqs),
          _read_opts(read_opts),
          _term_ords(std::move(term_ords)),
          _candidates(candidates),
          _topk(topk),
          _shared_threshold(shared_threshold) {}

WandScorer::~WandScorer() = default;

Status WandScorer::_open_cursors() {
    for (size_t t = 0; t < _term_ords.size(); ++t) {
        if (_term_ords[t] < 0) {
            continue; // term absent in this segment
        }
        TermCursor c;
        RETURN_IF_ERROR(_freqs->new_posting_cursor(_read_opts, &c.it));
        RETURN_IF_ERROR(c.it->seek_to_term(static_cast<uint32_t>(_term_ords[t])));
        c.idf = _stats.idf[t];
        // Tight term-level bound from the directory: bm25_term is increasing in tf and decreasing in
        // doc_len, so per block bm25_term(max_tf, min_doclen) bounds every posting in it.
        for (uint32_t b = 0; b < c.it->num_blocks(); ++b) {
            c.ub = std::max(c.ub, bm25_term(c.it->block_max_tf(b), c.it->block_min_doclen(b), c.idf, _stats));
        }
        if (!c.it->has_next_block()) {
            continue; // defensive: the writer never emits an empty posting list
        }
        RETURN_IF_ERROR(c.it->next_block());
        c.idx = 0;
        c.doc = c.it->docids()[0];
        c.valid = true;
        _cursors.push_back(std::move(c));
    }
    return Status::OK();
}

Status WandScorer::_next_geq(TermCursor* c, uint32_t target) {
    while (true) {
        const uint32_t* docids = c->it->docids();
        const uint32_t* end = docids + c->it->cur_block_size();
        const uint32_t* pos = std::lower_bound(docids + c->idx, end, target);
        if (pos != end) {
            c->idx = static_cast<uint32_t>(pos - docids);
            c->doc = *pos;
            return Status::OK();
        }
        Status st = c->it->seek_block(target); // NotFound == past the last block == list exhausted
        if (st.is_not_found()) {
            c->valid = false;
            return Status::OK();
        }
        RETURN_IF_ERROR(st);
        c->idx = 0; // loop: lower_bound inside the freshly decoded covering block always hits
    }
}

Status WandScorer::_advance(TermCursor* c) {
    if (++c->idx < c->it->cur_block_size()) {
        c->doc = c->it->docids()[c->idx];
        return Status::OK();
    }
    if (!c->it->has_next_block()) {
        c->valid = false;
        return Status::OK();
    }
    RETURN_IF_ERROR(c->it->next_block());
    c->idx = 0;
    c->doc = c->it->docids()[0];
    return Status::OK();
}

double WandScorer::_block_ub_at(const TermCursor& c, uint32_t docid, uint32_t* block_last) const {
    uint32_t lo = 0;
    uint32_t hi = c.it->num_blocks();
    while (lo < hi) { // first block whose last_docid >= docid == the only block that can contain it
        uint32_t mid = lo + (hi - lo) / 2;
        if (c.it->block_last_docid(mid) < docid) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if (lo == c.it->num_blocks()) {
        *block_last = UINT32_MAX;
        return 0.0;
    }
    *block_last = c.it->block_last_docid(lo);
    return bm25_term(c.it->block_max_tf(lo), c.it->block_min_doclen(lo), c.idf, _stats);
}

Status WandScorer::run(std::unordered_map<rowid_t, double>* id2score) {
    if (_topk <= 0) {
        return Status::InternalError("WandScorer requires topk > 0");
    }
    RETURN_IF_ERROR(_open_cursors());
    const auto k = static_cast<size_t>(_topk);
    // Min-heap of the current k best. The pruning threshold is the score a doc must strictly beat,
    // fed by two monotone sources: this scorer's own k-th best once the heap fills, and (when shared)
    // the cross-scorer accumulator. With a positive seed the heap may legitimately end below k
    // entries -- docs at or under the seed are globally non-competitive and are never admitted.
    using Entry = std::pair<double, rowid_t>;
    std::priority_queue<Entry, std::vector<Entry>, std::greater<Entry>> heap;
    double threshold = (_shared_threshold != nullptr) ? _shared_threshold->load(std::memory_order_relaxed) : 0.0;

    std::vector<TermCursor*> order;
    while (true) {
        order.clear();
        for (auto& c : _cursors) {
            if (c.valid) {
                order.push_back(&c);
            }
        }
        if (order.empty()) {
            break;
        }
        std::sort(order.begin(), order.end(), [](const TermCursor* a, const TermCursor* b) { return a->doc < b->doc; });
        if (_shared_threshold != nullptr) {
            // Concurrent scorers may have raised the accumulator meanwhile; a stale read only prunes less.
            threshold = std::max(threshold, _shared_threshold->load(std::memory_order_relaxed));
        }

        // Level 1 -- pivot: first cursor whose term-bound prefix sum could still beat the threshold.
        // Term bounds are strictly positive (RSJ+1 idf > 0, block max_tf >= 1), so with no threshold
        // yet (empty heap, no seed) every cursor is admitted and the heap fills in pure DAAT order.
        size_t p = order.size();
        double acc = 0.0;
        for (size_t i = 0; i < order.size(); ++i) {
            acc += order[i]->ub;
            if (acc > threshold) {
                p = i;
                break;
            }
        }
        if (p == order.size()) {
            break; // even all remaining terms together cannot beat the k-th best
        }
        const uint32_t pivot = order[p]->doc;

        // pivot set S = every cursor positioned at or before pivot (only these can contain it).
        size_t pset = p;
        while (pset + 1 < order.size() && order[pset + 1]->doc <= pivot) {
            ++pset;
        }

        // Level 2 -- block-max recheck: bound pivot's score by its covering blocks' local maxima
        // (stateless directory reads, no decode). Meaningful whenever any threshold exists -- own
        // heap full or a positive shared seed.
        if (threshold > 0.0) {
            double block_sum = 0.0;
            uint32_t min_block_last = UINT32_MAX;
            for (size_t i = 0; i <= pset; ++i) {
                uint32_t bl;
                block_sum += _block_ub_at(*order[i], pivot, &bl);
                min_block_last = std::min(min_block_last, bl);
            }
            if (block_sum <= threshold) {
                // Every doc in [pivot, boundary) is provably non-competitive. boundary MUST also be
                // clamped by the first cursor OUTSIDE the pivot set: docs at or past it may score via
                // terms whose bounds were not counted (Ding & Suel's GetNewCandidate; dropping this
                // clamp silently corrupts the top-k).
                uint32_t boundary = (min_block_last == UINT32_MAX) ? UINT32_MAX : min_block_last + 1;
                if (pset + 1 < order.size()) {
                    boundary = std::min(boundary, order[pset + 1]->doc);
                }
                // Advance ONE pivot-set cursor (largest term bound frees the most bound mass); the
                // rest stay put so docs holding other terms remain discoverable as future pivots.
                size_t victim = 0;
                for (size_t i = 1; i <= pset; ++i) {
                    if (order[i]->ub > order[victim]->ub) {
                        victim = i;
                    }
                }
                RETURN_IF_ERROR(_next_geq(order[victim], boundary));
                continue;
            }
        }

        if (order[0]->doc == pivot) {
            // Cursors aligned on pivot: score it exactly (the only place a block gets decoded).
            if (_candidates == nullptr || _candidates->contains(pivot)) {
                ASSIGN_OR_RETURN(uint32_t dl, _freqs->doc_len(pivot));
                double score = 0.0;
                for (const TermCursor* c : order) {
                    if (c->doc == pivot) {
                        score += bm25_term(c->it->tfs()[c->idx], dl, c->idf, _stats);
                    }
                }
                ++_docs_scored;
                // Admission to the heap requires strictly beating the threshold (not just the heap
                // top): with a shared seed, locally-good but globally-dead docs stay out entirely.
                if (score > threshold) {
                    if (heap.size() < k) {
                        heap.emplace(score, pivot);
                    } else {
                        heap.pop(); // score > threshold >= heap top, so the evictee is non-competitive
                        heap.emplace(score, pivot);
                    }
                    if (heap.size() == k) {
                        // max(): the accumulator may already sit above this heap's own k-th best.
                        threshold = std::max(threshold, heap.top().first);
                        if (_shared_threshold != nullptr) {
                            // k real docs score >= heap top, so it lower-bounds the global k-th best.
                            store_max(_shared_threshold, heap.top().first);
                        }
                    }
                }
            }
            for (TermCursor* c : order) {
                if (c->doc == pivot) {
                    RETURN_IF_ERROR(_advance(c));
                }
            }
        } else {
            // Not aligned yet: jump the lagging cursor with the smallest docid up to the pivot.
            RETURN_IF_ERROR(_next_geq(order[0], pivot));
        }
    }

    for (; !heap.empty(); heap.pop()) {
        (*id2score)[heap.top().second] = heap.top().first;
    }
    return Status::OK();
}

} // namespace starrocks
