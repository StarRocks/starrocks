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

#include "jieba_tokenizer.h"

#include "common/config.h"
#include "cppjieba/Jieba.hpp"

namespace starrocks {

cppjieba::Jieba& JiebaTokenizer::get_jieba_instance() {
    static cppjieba::Jieba jieba(
            config::inverted_index_dict_path + "/jieba.dict.utf8", config::inverted_index_dict_path + "/hmm_model.utf8",
            config::inverted_index_dict_path + "/user.dict.utf8", config::inverted_index_dict_path + "/idf.utf8",
            config::inverted_index_dict_path + "/stop_words.utf8");
    return jieba;
}

JiebaTokenizer::JiebaTokenizer() = default;

JiebaTokenizer::~JiebaTokenizer() = default;

StatusOr<std::vector<SliceToken>> JiebaTokenizer::tokenize(const Slice* text) {
    cppjieba::Jieba& jieba = get_jieba_instance();

    std::vector<std::string> words;
    jieba.CutAll(text->to_string(), words);

    std::vector<SliceToken> tokens;
    tokens.reserve(words.size());

    size_t pos = 0;
    for (const auto& word : words) {
        tokens.emplace_back(word, pos++);
    }
    return tokens;
}

} // namespace starrocks
