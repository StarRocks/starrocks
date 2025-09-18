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

#include "inverted_index_common.h"

namespace starrocks {

class GinQueryOptions {
public:
    GinQueryOptions() = default;

    void setQueryType(const InvertedIndexQueryType& _query_type) { query_type = _query_type; }
    void setEnablePhraseQuerySequentialOpt(bool _enable_phrase_query_sequential_opt) {
        enable_phrase_query_sequential_opt = _enable_phrase_query_sequential_opt;
    }
    void setMaxExpansions(const int32_t& _max_expansions) { max_expansions = _max_expansions; }

    void setParserType(const InvertedIndexParserType& _parser_type) { parser_type = _parser_type; }
    void setParserMode(const InvertedParserMode& _parser_mode) { parser_mode = _parser_mode; }
    void setCharFilterMap(const std::map<std::string, std::string>& _char_filter_map) {
        char_filter_map = std::move(_char_filter_map);
    }
    void setLowerCase(const bool _lower_case) { lower_case = _lower_case; }
    void setStopWords(const std::string& _stop_words) { stop_words = std::move(_stop_words); }

    const InvertedIndexQueryType& getQueryType() const { return query_type; }
    const InvertedIndexParserType& getParserType() const { return parser_type; }
    const InvertedParserMode& getParserMode() const { return parser_mode; }
    bool enablePhraseQuerySequentialOpt() const { return enable_phrase_query_sequential_opt; }
    int32_t maxExpansions() const { return max_expansions; }
    const std::map<std::string, std::string>& getCharFilterMap() const { return char_filter_map; }
    bool getLowerCase() const { return lower_case; }
    const std::string& getStopWords() const { return stop_words; }

private:
    // ----------- for query only ----------
    InvertedIndexQueryType query_type = InvertedIndexQueryType::UNKNOWN_QUERY;
    bool enable_phrase_query_sequential_opt = false;
    int32_t max_expansions = 50;

    // ----------- common ----------
    InvertedIndexParserType parser_type = InvertedIndexParserType::PARSER_UNKNOWN;
    InvertedParserMode parser_mode = InvertedParserMode::COARSE_GRAINED;
    std::map<std::string, std::string> char_filter_map{};
    bool lower_case = true;
    std::string stop_words = "";
};

} // namespace starrocks
