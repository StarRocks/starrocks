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
#include <limits>
#include <string>
#include <unordered_map>

#include "storage/index/inverted/inverted_index_option.h"
#include "storage/index/inverted/inverted_reader.h"

namespace starrocks {

class InvertedReader;
enum class InvertedIndexParserType;
enum class InvertedIndexReaderType;

class InvertedIndexIterator {
public:
    InvertedIndexIterator(const std::shared_ptr<TabletIndex>& index_meta, InvertedReader* reader,
                          OlapReaderStatistics* stats)
            : _index_meta(index_meta), _stats(stats), _reader(reader) {
        _analyser_type = get_inverted_index_parser_type_from_string(
                get_parser_string_from_properties(_index_meta->index_properties()));
    }

    virtual ~InvertedIndexIterator() = default;

    virtual Status read_from_inverted_index(const std::string& column_name, const void* query_value,
                                            InvertedIndexQueryType query_type, roaring::Roaring* bit_map);

    // Scored variant: also fills `row_to_score` (segment-local row id -> BM25
    // score) for a SQL score() column. Forwards to InvertedReader::query_scored.
    virtual Status read_from_inverted_index_scored(const std::string& column_name, const void* query_value,
                                                   InvertedIndexQueryType query_type, roaring::Roaring* bit_map,
                                                   std::unordered_map<uint32_t, float>* row_to_score);

    // Top-k pushdown for the scored path: the SegmentIterator sets the SQL LIMIT
    // here before applying the GIN predicate so the scored query only materializes
    // the best `limit` rows (0 = score every hit). Mirrors the vector ANN top-k.
    void set_bm25_topk_limit(int32_t limit) { _bm25_topk_limit = limit; }

    // Min/max BM25 score gate for the scored path: a `WHERE score() > c`
    // predicate is pushed here so the scored query only materializes hits whose
    // score is in [min, max] (-/+INFINITY = unbounded), filtered inside tantivy.
    void set_bm25_score_range(float min_score, float max_score) {
        _bm25_score_min = min_score;
        _bm25_score_max = max_score;
    }

    virtual Status read_null(const std::string& column_name, roaring::Roaring* bit_map);

    virtual InvertedIndexParserType get_inverted_index_analyser_type() const;

    virtual InvertedIndexReaderType get_inverted_index_reader_type() const;

    virtual bool is_untokenized() const { return _analyser_type == InvertedIndexParserType::PARSER_NONE; }

    virtual Status close() { return Status::OK(); }

protected:
    const std::shared_ptr<TabletIndex> _index_meta;
    OlapReaderStatistics* _stats;
    InvertedReader* _reader;
    InvertedIndexParserType _analyser_type;
    int32_t _bm25_topk_limit = 0;
    float _bm25_score_min = -std::numeric_limits<float>::infinity();
    float _bm25_score_max = std::numeric_limits<float>::infinity();
};

} // namespace starrocks