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

#include "storage/index/inverted/inverted_index_option.h"

#include <boost/algorithm/string/case_conv.hpp>

namespace starrocks {

void add_enable_phrase_query_sequential_opt_options(TabletIndex* tablet_index,
                                                    bool enable_phrase_query_sequential_opt) {
    tablet_index->add_search_properties(INVERTED_ENABLE_PHRASE_QUERY_SEQUENTIAL_OPT,
                                        std::to_string(enable_phrase_query_sequential_opt));
}

bool is_enable_phrase_query_sequential_opt(const TabletIndex& tablet_index) {
    auto it = tablet_index.search_properties().find(INVERTED_ENABLE_PHRASE_QUERY_SEQUENTIAL_OPT);
    if (it != tablet_index.search_properties().end()) {
        const auto value = it->second;
        return boost::algorithm::to_lower_copy(value) == "true";
    }
    return false;
}

void add_gin_max_expansions(TabletIndex* tablet_index, int32_t gin_max_expansions) {
    tablet_index->add_search_properties(GIN_MAX_EXPANSIONS, std::to_string(gin_max_expansions));
}

int32_t get_gin_max_expansions(const TabletIndex& tablet_index) {
    if (const auto it = tablet_index.search_properties().find(GIN_MAX_EXPANSIONS);
        it != tablet_index.search_properties().end()) {
        const auto value = it->second;
        return std::stoi(value);
    }
    return GIN_MAX_EXPANSIONS_DEFAULT;
}

StatusOr<InvertedImplementType> get_inverted_imp_type(const TabletIndex& tablet_index) {
    auto inverted_imp_prop = tablet_index.common_properties().find(INVERTED_IMP_KEY);
    if (inverted_imp_prop != tablet_index.common_properties().end()) {
        const auto& imp_type = inverted_imp_prop->second;
        if (boost::algorithm::to_lower_copy(imp_type) == TYPE_CLUCENE) {
            return InvertedImplementType::CLUCENE;
        } else {
            return Status::InvalidArgument("Do not support imp_type : " + imp_type);
        }
    } else {
        return Status::InvalidArgument("Can not get inverted imp type");
    }
}

std::string inverted_index_parser_type_to_string(InvertedIndexParserType parser_type) {
    switch (parser_type) {
    case InvertedIndexParserType::PARSER_NONE:
        return INVERTED_INDEX_PARSER_NONE;
    case InvertedIndexParserType::PARSER_STANDARD:
        return INVERTED_INDEX_PARSER_STANDARD;
    case InvertedIndexParserType::PARSER_ENGLISH:
        return INVERTED_INDEX_PARSER_ENGLISH;
    case InvertedIndexParserType::PARSER_CHINESE:
        return INVERTED_INDEX_PARSER_CHINESE;
    default:
        return INVERTED_INDEX_PARSER_UNKNOWN;
    }
}

InvertedIndexParserType get_inverted_index_parser_type_from_string(const std::string& parser_str) {
    std::string lower_value = boost::algorithm::to_lower_copy(parser_str);
    if (lower_value == INVERTED_INDEX_PARSER_NONE) {
        return InvertedIndexParserType::PARSER_NONE;
    } else if (lower_value == INVERTED_INDEX_PARSER_STANDARD) {
        return InvertedIndexParserType::PARSER_STANDARD;
    } else if (lower_value == INVERTED_INDEX_PARSER_ENGLISH) {
        return InvertedIndexParserType::PARSER_ENGLISH;
    } else if (lower_value == INVERTED_INDEX_PARSER_CHINESE) {
        return InvertedIndexParserType::PARSER_CHINESE;
    }

    return InvertedIndexParserType::PARSER_UNKNOWN;
}

std::string get_parser_string_from_properties(const std::map<std::string, std::string>& properties) {
    for (const auto& prop : properties) {
        if (boost::to_lower_copy(prop.first) == INVERTED_INDEX_PARSER_KEY) {
            return prop.second;
        }
    }
    return INVERTED_INDEX_PARSER_NONE;
}

InvertedParserMode get_parser_mode_from_properties(const std::map<std::string, std::string>& properties) {
    auto mode = InvertedParserMode::COARSE_GRAINED;
    for (const auto& [key, value] : properties) {
        if (boost::to_lower_copy(key) == INVERTED_INDEX_PARSER_MODE_KEY) {
            if (boost::to_lower_copy(value) == INVERTED_INDEX_PARSER_FINE_GRAINED) {
                mode = InvertedParserMode::FINE_GRAINED;
            }
            break;
        }
    }
    return mode;
}

std::string get_support_phrase_from_properties(const std::map<std::string, std::string>& properties) {
    for (const auto& [key, value] : properties) {
        if (boost::to_lower_copy(key) == INVERTED_INDEX_SUPPORT_PHRASE_KEY) {
            return value;
        }
    }
    return INVERTED_INDEX_SUPPORT_PHRASE_YES;
}

std::map<std::string, std::string> get_parser_char_filter_map_from_properties(
        const std::map<std::string, std::string>& properties) {
    std::map<std::string, std::string> char_filter_map;
    auto type_it = properties.find(INVERTED_INDEX_CHAR_FILTER_TYPE_KEY);
    auto pattern_it = properties.find(INVERTED_INDEX_CHAR_FILTER_PATTERN_KEY);
    if (type_it == properties.end() || type_it->second != INVERTED_INDEX_CHAR_FILTER_TYPE_REPLACE ||
        pattern_it == properties.end()) {
        return char_filter_map;
    }

    std::string replacement = " ";
    auto replacement_it = properties.find(INVERTED_INDEX_CHAR_FILTER_REPLACEMENT_KEY);
    if (replacement_it != properties.end()) {
        replacement = replacement_it->second;
    }

    char_filter_map.emplace(*type_it);
    char_filter_map.emplace(*pattern_it);
    char_filter_map.emplace(INVERTED_INDEX_CHAR_FILTER_REPLACEMENT_KEY, replacement);
    return char_filter_map;
}

int32_t get_ignore_above_from_properties(const std::map<std::string, std::string>& properties) {
    std::string ignore_above = INVERTED_INDEX_IGNORE_ABOVE_DEFAULT;
    for (const auto& [key, value] : properties) {
        if (boost::to_lower_copy(key) == INVERTED_INDEX_IGNORE_ABOVE_KEY) {
            ignore_above = value;
            break;
        }
    }
    return std::stoi(ignore_above);
}

bool get_lower_case_from_properties(const std::map<std::string, std::string>& properties) {
    std::string lower_case = INVERTED_INDEX_LOWER_CASE_YES;
    for (const auto& [key, value] : properties) {
        if (boost::to_lower_copy(key) == INVERTED_INDEX_LOWER_CASE_KEY) {
            lower_case = boost::to_lower_copy(value);
            break;
        }
    }
    return lower_case == INVERTED_INDEX_LOWER_CASE_YES;
}

std::string get_stop_words_from_properties(const std::map<std::string, std::string>& properties) {
    std::string stop_words = INVERTED_INDEX_STOP_WORDS_DEFAULT;
    for (const auto& [key, value] : properties) {
        if (boost::to_lower_copy(key) == INVERTED_INDEX_STOP_WORDS_KEY) {
            if (boost::to_lower_copy(value) == INVERTED_INDEX_STOP_WORDS_NONE) {
                stop_words = INVERTED_INDEX_STOP_WORDS_NONE;
            }
            break;
        }
    }
    return stop_words;
}

bool is_tokenized_from_properties(const std::map<std::string, std::string>& properties) {
    auto tokenized_res = properties.find(INVERTED_INDEX_TOKENIZED_KEY);
    if (tokenized_res != properties.end()) {
        if (boost::algorithm::to_lower_copy(tokenized_res->second) == "true") {
            return true;
        }
    }
    return false;
}

} // namespace starrocks
