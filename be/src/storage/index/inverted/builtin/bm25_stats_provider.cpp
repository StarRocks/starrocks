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

#include <atomic>
#include <memory>
#include <string>
#include <utility>

#include "common/config_storage_fwd.h"
#include "fmt/format.h"
#include "fs/fs.h"
#include "gutil/casts.h"
#include "storage/index/inverted/builtin/bm25_scoring.h"
#include "storage/index/inverted/builtin/builtin_gin_tokenizer.h"
#include "storage/index/inverted/builtin/builtin_inverted_reader.h"
#include "storage/index/inverted/inverted_index_option.h"
#include "storage/index/inverted/inverted_reader.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet_schema.h"

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

StatusOr<std::shared_ptr<BM25Stats>> build_tablet_bm25_stats(const TabletSchema& tablet_schema,
                                                             const BM25SearchOption& option,
                                                             const std::vector<std::shared_ptr<Segment>>& segments,
                                                             const LakeIOOptions& lake_io_opts, bool use_page_cache,
                                                             OlapReaderStatistics* stats) {
    // Resolve the FE's stable column_id to the column's unique id, the same way the MATCH predicate does:
    // field_index(column_id) -> tablet column -> its unique id. Phase-2 (segment iterator) re-resolves the
    // same column_id against its own segment schema, so the resolved id is not stored on the shared option.
    const std::string& bm25_col_id = option.column_id;
    const size_t bm25_field_idx = tablet_schema.field_index(bm25_col_id);
    if (bm25_field_idx >= tablet_schema.num_columns()) {
        return Status::InternalError(
                fmt::format("BM25 scoring: column_id '{}' not found in tablet schema", bm25_col_id));
    }
    const int32_t index_cid = tablet_schema.column(bm25_field_idx).unique_id();

    // Parser + lower_case from this column's builtin GIN index (needed to tokenize the query the same way
    // the index was built).
    std::shared_ptr<TabletIndex> index_meta;
    RETURN_IF_ERROR(tablet_schema.get_indexes_for_column(index_cid, GIN, index_meta));
    if (index_meta == nullptr) {
        return Status::InternalError(
                fmt::format("BM25 scoring requested but column uid {} has no builtin GIN index", index_cid));
    }
    const auto& props = index_meta->index_properties();
    const InvertedIndexParserType parser_type =
            get_inverted_index_parser_type_from_string(get_parser_string_from_properties(props));
    const bool lower_case = get_lower_case_from_properties(props);

    // Tokenize once; the identical terms are resolved to per-segment ordinals inside the provider.
    std::vector<std::string> terms;
    RETURN_IF_ERROR(tokenize_builtin_gin_query(parser_type, lower_case, Slice(option.query), &terms));
    auto result = std::make_shared<BM25Stats>();
    result->k1 = option.k1;
    result->b = option.b;
    if (terms.empty()) {
        // Empty / stopword-only query: no term contributes, so scores are all 0. Leave N/avgdl/idf empty.
        return result;
    }
    std::vector<Slice> term_slices(terms.begin(), terms.end());

    // Open each segment's builtin GIN reader via its own file handle. The read files and per-segment index
    // options must stay alive across TabletLocalProvider::get_stats (the handles reference them); `segments`
    // is owned by the caller and outlives this call.
    std::vector<std::unique_ptr<RandomAccessFile>> files_hold;
    std::vector<std::unique_ptr<IndexReadOptions>> opts_hold;
    std::vector<Bm25SegmentHandle> handles;
    SegmentReadOptions seg_opts; // only used for error-message context by the load path
    for (const auto& segment : segments) {
        if (segment == nullptr || segment->num_rows() == 0) {
            continue;
        }
        ASSIGN_OR_RETURN(auto rfile, segment->new_segment_read_file(lake_io_opts));
        auto index_opts = std::make_unique<IndexReadOptions>();
        index_opts->use_page_cache = use_page_cache && !config::disable_storage_page_cache;
        index_opts->lake_io_opts = lake_io_opts;
        index_opts->read_file = rfile.get();
        index_opts->stats = stats;
        index_opts->segment_rows = segment->num_rows();

        InvertedReader* reader = nullptr;
        RETURN_IF_ERROR(segment->get_inverted_reader(index_cid, seg_opts, *index_opts, &reader));
        if (reader == nullptr) {
            continue; // this segment has no GIN index on the column
        }
        handles.push_back(Bm25SegmentHandle{down_cast<BuiltinInvertedReader*>(reader),
                                            static_cast<int64_t>(segment->num_rows()), index_opts.get()});
        files_hold.push_back(std::move(rfile));
        opts_hold.push_back(std::move(index_opts));
    }

    TabletLocalProvider provider(std::move(handles), option.k1, option.b);
    ASSIGN_OR_RETURN(BM25Stats computed, provider.get_stats(term_slices));
    computed.terms = std::move(terms); // carried to Phase-2 so it resolves the same terms per segment
    // One WAND pruning threshold shared by every segment of this tablet scan (Phase-2 passes it to each
    // WandScorer). Segments seed their bound from it and publish their k-th best back, so later segments
    // prune harder.
    computed.shared_threshold = std::make_shared<std::atomic<double>>(0.0);
    *result = std::move(computed);
    return result;
}

} // namespace starrocks
