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

#include "storage/inverted/inverted_index_option.h"

#include <boost/algorithm/string/case_conv.hpp>

namespace starrocks {

StatusOr<InvertedImplementType> get_inverted_imp_type(const TabletIndex& tablet_index) {
    auto inverted_imp_prop = tablet_index.common_properties().find(INVERTED_IMP_KEY);
    if (inverted_imp_prop != tablet_index.common_properties().end()) {
        const auto& imp_type = inverted_imp_prop->second;
        if (boost::algorithm::to_lower_copy(imp_type) == TYPE_CLUCENE) {
            return InvertedImplementType::CLUCENE;
        } else {
            return Status::InvertedIndexInvalidParams("Do not support imp_type : " + imp_type);
        }
    } else {
        return Status::InvertedIndexInvalidParams("Can not get inverted imp type");
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
    if (properties.find(INVERTED_INDEX_PARSER_KEY) != properties.end()) {
        return properties.at(INVERTED_INDEX_PARSER_KEY);
    } else {
        return INVERTED_INDEX_PARSER_NONE;
    }
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
