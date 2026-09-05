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

#include "storage/tablet_schema.h"

namespace starrocks {

StatusOr<InvertedImplementType> get_inverted_imp_type(const TabletIndex& tablet_index) {
    auto inverted_imp_prop = tablet_index.common_properties().find(INVERTED_IMP_KEY);
    if (inverted_imp_prop != tablet_index.common_properties().end()) {
        const auto& imp_type = inverted_imp_prop->second;
        const std::string imp_type_lower = boost::algorithm::to_lower_copy(imp_type);
        if (imp_type_lower == TYPE_CLUCENE) {
            return InvertedImplementType::CLUCENE;
        } else if (imp_type_lower == TYPE_BUILTIN) {
            return InvertedImplementType::BUILTIN;
        } else if (imp_type_lower == TYPE_TANTIVY) {
            return InvertedImplementType::TANTIVY;
        } else {
            return Status::InvalidArgument("Do not support imp_type : " + imp_type);
        }
    } else {
        return Status::InvalidArgument("Can not get inverted imp type");
    }
}

bool has_tantivy_index(const TabletSchema& tablet_schema) {
    for (const auto& index : *tablet_schema.indexes()) {
        if (index.index_type() != GIN) {
            continue;
        }
        auto imp_type = get_inverted_imp_type(index);
        if (imp_type.ok() && imp_type.value() == InvertedImplementType::TANTIVY) {
            return true;
        }
    }
    return false;
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
    case InvertedIndexParserType::PARSER_JIEBA:
        return INVERTED_INDEX_PARSER_JIEBA;
    case InvertedIndexParserType::PARSER_IK:
        return INVERTED_INDEX_PARSER_IK;
    case InvertedIndexParserType::PARSER_NGRAM:
        return INVERTED_INDEX_PARSER_NGRAM;
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
    } else if (lower_value == INVERTED_INDEX_PARSER_JIEBA) {
        return InvertedIndexParserType::PARSER_JIEBA;
    } else if (lower_value == INVERTED_INDEX_PARSER_IK) {
        return InvertedIndexParserType::PARSER_IK;
    } else if (lower_value == INVERTED_INDEX_PARSER_NGRAM) {
        return InvertedIndexParserType::PARSER_NGRAM;
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

std::string get_parser_mode_string_from_properties(const std::map<std::string, std::string>& properties) {
    if (const auto it = properties.find(INVERTED_INDEX_PARSER_MODE_KEY); it != properties.end()) {
        return boost::algorithm::to_lower_copy(it->second);
    }
    return INVERTED_INDEX_PARSER_MAX_WORD;
}

StatusOr<std::string> get_tantivy_ngram_tokenizer_name(const std::map<std::string, std::string>& properties) {
    const auto min_it = properties.find(INVERTED_INDEX_MIN_GRAM_KEY);
    const auto max_it = properties.find(INVERTED_INDEX_MAX_GRAM_KEY);
    if (min_it == properties.end() || max_it == properties.end()) {
        return Status::InvalidArgument("tantivy ngram parser requires both min_gram and max_gram");
    }

    try {
        const int min_gram = std::stoi(min_it->second);
        const int max_gram = std::stoi(max_it->second);
        if (min_gram <= 0 || max_gram <= 0) {
            return Status::InvalidArgument("tantivy ngram min_gram and max_gram must be greater than zero");
        }
        if (min_gram > max_gram) {
            return Status::InvalidArgument("tantivy ngram min_gram must not be greater than max_gram");
        }
        return "ngram:" + std::to_string(min_gram) + ":" + std::to_string(max_gram);
    } catch (const std::exception&) {
        return Status::InvalidArgument("tantivy ngram min_gram and max_gram must be positive integers");
    }
}

StatusOr<std::string> get_tantivy_analyzer_definition(const std::map<std::string, std::string>& properties) {
    if (const auto it = properties.find(INVERTED_INDEX_ANALYZER_DEFINITION_KEY); it != properties.end()) {
        if (it->second.empty()) {
            return Status::InvalidArgument("tantivy analyzer_definition must not be empty");
        }
        return it->second;
    }

    const auto parser = boost::algorithm::to_lower_copy(get_parser_string_from_properties(properties));
    if (parser == INVERTED_INDEX_PARSER_NONE) {
        return std::string("raw");
    }
    if (parser == INVERTED_INDEX_PARSER_ENGLISH || parser == INVERTED_INDEX_PARSER_STANDARD) {
        return parser;
    }
    if (parser == INVERTED_INDEX_PARSER_CHINESE || parser == "cjk") {
        return std::string("cjk");
    }
    if (parser == INVERTED_INDEX_PARSER_JIEBA) {
        return std::string("jieba");
    }
    if (parser == INVERTED_INDEX_PARSER_IK) {
        return get_parser_mode_string_from_properties(properties) == INVERTED_INDEX_PARSER_SMART
                       ? std::string("ik_smart")
                       : std::string("ik_max_word");
    }
    if (parser == INVERTED_INDEX_PARSER_NGRAM) {
        return get_tantivy_ngram_tokenizer_name(properties);
    }
    return Status::NotSupported("tantivy: unsupported parser '" + parser + "'");
}

std::string get_tantivy_analyzer_digest(const std::map<std::string, std::string>& properties) {
    if (const auto it = properties.find(INVERTED_INDEX_ANALYZER_DIGEST_KEY); it != properties.end()) {
        return it->second;
    }
    return "";
}

int32_t get_gram_num_from_properties(const std::map<std::string, std::string>& properties) {
    if (const auto it = properties.find(INVERTED_INDEX_DICT_GRAM_NUM_KEY); it != properties.end()) {
        const std::string& gram_num = it->second;
        try {
            return std::stoi(gram_num);
        } catch (const std::exception& e) {
            LOG(WARNING) << "Parsing gram num failed, reason: " << e.what() << ". Using default value -1.";
            return -1;
        }
    }
    return -1;
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
