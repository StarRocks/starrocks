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

#include "storage/index/inverted/builtin/bm25_stats_provider.h"

#include <utility>

#include "storage/index/inverted/builtin/bm25_scoring.h"
#include "storage/index/inverted/builtin/builtin_inverted_reader.h"

namespace starrocks {

TabletLocalProvider::TabletLocalProvider(std::vector<Bm25SegmentHandle> segments, double k1, double b)
        : _segments(std::move(segments)), _k1(k1), _b(b) {}

TabletLocalProvider::~TabletLocalProvider() = default;

StatusOr<BM25Stats> TabletLocalProvider::get_stats(const std::vector<Slice>& query_terms) {
    BM25Stats stats;
    stats.k1 = _k1;
    stats.b = _b;
    // Echo the tokenized query terms into the stats (aligned with idf by position) so the object is a
    // self-contained Phase-1 result: Phase-2 resolves these to each segment's dict ordinals.
    stats.terms.reserve(query_terms.size());
    for (const auto& term : query_terms) {
        stats.terms.emplace_back(term.data, term.size);
    }
    std::vector<int64_t> df(query_terms.size(), 0);
    uint64_t sum_len = 0;

    for (const auto& handle : _segments) {
        BuiltinInvertedReader* reader = handle.reader;
        // BM25 has no brute-force fallback: a DOCS-only segment cannot be scored, so fail fast rather
        // than silently returning wrong (partial) statistics.
        if (!reader->has_freqs()) {
            return Status::InternalError(
                    "BM25 scoring requires index_options=DOCS_AND_FREQS but a segment was built with DOCS only");
        }
        stats.N += handle.num_rows;

        const IndexReadOptions& read_opts = *handle.read_opts;
        ASSIGN_OR_RETURN(auto freqs, reader->new_freqs_iterator(read_opts));
        sum_len += freqs->sum_len();

        std::vector<int64_t> ordinals;
        RETURN_IF_ERROR(reader->lookup_term_ordinals(read_opts, query_terms, &ordinals));
        for (size_t t = 0; t < query_terms.size(); ++t) {
            if (ordinals[t] < 0) {
                continue; // term not present in this segment's dictionary -> contributes 0 to df
            }
            ASSIGN_OR_RETURN(uint32_t seg_df, freqs->doc_freq(static_cast<uint32_t>(ordinals[t])));
            df[t] += seg_df;
        }
    }

    stats.avgdl = stats.N > 0 ? static_cast<double>(sum_len) / static_cast<double>(stats.N) : 0.0;
    stats.idf.resize(query_terms.size());
    for (size_t t = 0; t < query_terms.size(); ++t) {
        stats.idf[t] = bm25_idf(stats.N, df[t]);
    }
    return stats;
}

} // namespace starrocks
