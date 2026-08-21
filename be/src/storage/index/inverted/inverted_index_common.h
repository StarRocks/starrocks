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
#include <vector>

#include "common/global_types.h"
#include "storage/olap_common.h"
#include "util/slice.h"

namespace starrocks {

struct PhraseQueryValue {
    Slice text;
    int slop = 0;
};

// A MATCH_ANY/MATCH_ALL query whose terms were produced explicitly by the
// tokenize() SQL function. Readers must use these terms verbatim and must not
// run the index's analyzer a second time.
struct TokenizedQueryValue {
    std::vector<std::string> terms;
};

enum class InvertedImplementType {
    UNKNOWN = 0,
    CLUCENE = 1,
    BUILTIN = 2,
    TANTIVY = 3,
};

enum class InvertedIndexParserType {
    PARSER_UNKNOWN = 0,
    PARSER_NONE = 1,
    PARSER_STANDARD = 2,
    PARSER_ENGLISH = 3,
    PARSER_CHINESE = 4,
    PARSER_JIEBA = 5,
    PARSER_IK = 6,
    PARSER_NGRAM = 7,
};

const std::string INVERTED_IMP_KEY = "imp_lib";
const std::string TYPE_CLUCENE = "clucene";
const std::string TYPE_BUILTIN = "builtin";
const std::string TYPE_TANTIVY = "tantivy";

const std::string INVERTED_INDEX_PARSER_KEY = "parser";
const std::string INVERTED_INDEX_PARSER_UNKNOWN = "unknown";
const std::string INVERTED_INDEX_PARSER_NONE = "none";
const std::string INVERTED_INDEX_PARSER_STANDARD = "standard";
const std::string INVERTED_INDEX_PARSER_ENGLISH = "english";
const std::string INVERTED_INDEX_PARSER_CHINESE = "chinese";
const std::string INVERTED_INDEX_PARSER_JIEBA = "jieba";
const std::string INVERTED_INDEX_PARSER_IK = "ik";
const std::string INVERTED_INDEX_PARSER_NGRAM = "ngram";
const std::string INVERTED_INDEX_PARSER_MODE_KEY = "parser_mode";
const std::string INVERTED_INDEX_PARSER_MAX_WORD = "ik_max_word";
const std::string INVERTED_INDEX_PARSER_SMART = "ik_smart";
const std::string INVERTED_INDEX_MIN_GRAM_KEY = "min_gram";
const std::string INVERTED_INDEX_MAX_GRAM_KEY = "max_gram";
const std::string LIKE_FN_NAME = "like";

const std::string INVERTED_INDEX_DICT_GRAM_NUM_KEY = "dict_gram_num";

const std::string INVERTED_INDEX_TOKENIZED_KEY = "tokenized";
const std::string INVERTED_INDEX_ANALYZER_KEY = "analyzer";
const std::string INVERTED_INDEX_ANALYZER_DEFINITION_KEY = "analyzer_definition";
const std::string INVERTED_INDEX_ANALYZER_DIGEST_KEY = "analyzer_digest";

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
    MATCH_FUZZY_QUERY = 6,
    MATCH_ALL_QUERY = 7,
    MATCH_PHRASE_QUERY = 8,
    MATCH_ANY_QUERY = 9,
    MATCH_ALL_TERMS_QUERY = 10,
    MATCH_ANY_TERMS_QUERY = 11
};

} // namespace starrocks
