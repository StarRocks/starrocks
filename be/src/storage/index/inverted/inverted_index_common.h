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

#include <string>

namespace starrocks {

enum class InvertedImplementType {
    UNKNOWN = 0,
    CLUCENE = 1,
};

enum class InvertedIndexParserType {
    PARSER_UNKNOWN = 0,
    PARSER_NONE = 1,
    PARSER_STANDARD = 2,
    PARSER_ENGLISH = 3,
    PARSER_CHINESE = 4,
};

const std::string INVERTED_IMP_KEY = "imp_lib";
const std::string TYPE_CLUCENE = "clucene";
const std::string INVERTED_INDEX_PARSER_KEY = "parser";
const std::string INVERTED_INDEX_PARSER_UNKNOWN = "unknown";
const std::string INVERTED_INDEX_PARSER_NONE = "none";
const std::string INVERTED_INDEX_PARSER_STANDARD = "standard";
const std::string INVERTED_INDEX_PARSER_ENGLISH = "english";
const std::string INVERTED_INDEX_PARSER_CHINESE = "chinese";
const std::string LIKE_FN_NAME = "like";

const std::string INVERTED_INDEX_OMIT_TERM_FREQ_AND_POSITION_KEY = "omit_term_freq_and_position";
const std::string INVERTED_INDEX_OMIT_TERM_FREQ_AND_POSITION_YES = "true";
const std::string INVERTED_INDEX_OMIT_TERM_FREQ_AND_POSITION_NO = "false";

const std::string INVERTED_INDEX_TOKENIZED_KEY = "tokenized";

const std::string INVERTED_ENABLE_PHRASE_QUERY_SEQUENTIAL_OPT = "enable_phrase_query_sequential_opt";

const std::string GIN_MAX_EXPANSIONS = "gin_max_expansions";
constexpr int32_t GIN_MAX_EXPANSIONS_DEFAULT = 50;

enum class InvertedIndexReaderType {
    UNKNOWN = -1,
    TEXT = 0,
    STRING = 1,
    NUMERIC = 2,
};

enum class InvertedIndexQueryType {
    UNKNOWN_QUERY = -1,
    EQUAL_QUERY = 0,
    LESS_THAN_QUERY = 1,
    LESS_EQUAL_QUERY = 2,
    GREATER_THAN_QUERY = 3,
    GREATER_EQUAL_QUERY = 4,
    MATCH_WILDCARD_QUERY = 5,
    MATCH_ANY_QUERY = 6,
    MATCH_ALL_QUERY = 7,
    MATCH_PHRASE_QUERY = 8,
    MATCH_PHRASE_PREFIX_QUERY = 9,
    MATCH_REGEXP_QUERY = 10,
    MATCH_PHRASE_EDGE_QUERY = 11,
};

} // namespace starrocks
