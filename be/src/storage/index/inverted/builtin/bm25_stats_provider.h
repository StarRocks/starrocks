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
#include <vector>

#include "base/string/slice.h"
#include "common/statusor.h"
#include "storage/rowset/options.h"
#include "storage_primitive/bm25_search_option.h"

namespace starrocks {

class BuiltinInvertedReader;
class Segment;
class TabletSchema;
struct OlapReaderStatistics;
struct LakeIOOptions;

// I4: produces tablet-local BM25Stats. Abstract so the delivery mechanism (tablet-local now, global
// DFS later) can change without touching the scorer.
class BM25StatsProvider {
public:
    virtual ~BM25StatsProvider() = default;
    // query_terms are the tokenized query terms; the returned idf[] is aligned with them by position.
    virtual StatusOr<BM25Stats> get_stats(const std::vector<Slice>& query_terms) = 0;
};

// One tablet segment already opened as a builtin inverted reader (must have has_freqs()), plus its row
// count (from the segment footer -- the reader does not carry it) and the per-segment read options
// (read_file / fs / stats differ per segment; caller owns them and keeps them alive across get_stats).
struct Bm25SegmentHandle {
    BuiltinInvertedReader* reader = nullptr;
    int64_t num_rows = 0;
    const IndexReadOptions* read_opts = nullptr;
};

// Serial (v1) Phase-1: walk the tablet's full segment set, sum N / sum_len / per-term df from cheap
// scalar reads (doc_freq column is one page per segment; posting/bitmap blobs are never read), then
// fold into tablet-level avgdl + idf[]. Segment-parallel fan-out is a later, drop-in change.
//
// TODO(bm25): segments are immutable, so per-(segment, term) df and per-segment (num_rows, sum_len) can
// be cached and reused across queries, avoiding a re-read of each segment's doc_freq column every time.
class TabletLocalProvider : public BM25StatsProvider {
public:
    TabletLocalProvider(std::vector<Bm25SegmentHandle> segments, double k1, double b);
    ~TabletLocalProvider() override;

    StatusOr<BM25Stats> get_stats(const std::vector<Slice>& query_terms) override;

private:
    std::vector<Bm25SegmentHandle> _segments;
    double _k1;
    double _b;
};

// Storage-agnostic BM25 Phase-1, shared by the lake and local scan paths: resolve the GIN column, tokenize
// the query with the index analyzer, open each segment's GIN reader, and fold N/avgdl/idf via
// TabletLocalProvider. `segments` is the tablet's full set at the query version -- the one storage-specific
// step the caller supplies. An empty/stopword-only query yields empty N/avgdl/idf (every score is then 0).
StatusOr<std::shared_ptr<BM25Stats>> build_tablet_bm25_stats(const TabletSchema& tablet_schema,
                                                             const BM25SearchOption& option,
                                                             const std::vector<std::shared_ptr<Segment>>& segments,
                                                             const LakeIOOptions& lake_io_opts, bool use_page_cache,
                                                             OlapReaderStatistics* stats);

} // namespace starrocks
