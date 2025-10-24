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

#include "storage/index/inverted/inverted_index_common.h"
#include "storage/index/inverted/tokenizer/builtin_simple_analyzer.h"
#include "storage/index/inverted/tokenizer/chinese_tokenizer.h"
#include "storage/index/inverted/tokenizer/jieba_tokenizer.h"
#include "storage/index/inverted/tokenizer/standard_tokenizer.h"
#include "storage/index/inverted/tokenizer/tokenizer.h"

namespace starrocks {
class TokenizerFactory {
public:
    static std::unique_ptr<Tokenizer> create(const InvertedIndexParserType type) {
        switch (type) {
        case InvertedIndexParserType::PARSER_NONE:
            return std::make_unique<NoneTokenizer>();
        case InvertedIndexParserType::PARSER_CHINESE:
            return std::make_unique<ChineseTokenizer>();
        case InvertedIndexParserType::PARSER_ENGLISH:
            return std::make_unique<SimpleAnalyzer>();
        case InvertedIndexParserType::PARSER_STANDARD:
            return std::make_unique<StandardTokenizer>();
        case InvertedIndexParserType::PARSER_JIEBA:
            return std::make_unique<JiebaTokenizer>();
        default:
            return nullptr;
        }
    }
};
} // namespace starrocks
