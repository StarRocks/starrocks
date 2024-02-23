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
#include <CLucene.h>
#include <CLucene/analysis/LanguageBasedAnalyzer.h>

#include "common/statusor.h"
#include "storage/inverted/inverted_index_common.hpp"

namespace starrocks {
StatusOr<std::unique_ptr<lucene::analysis::Analyzer>> get_analyzer(InvertedIndexParserType parser_type) {
    switch (parser_type) {
    case InvertedIndexParserType::PARSER_NONE:
    case InvertedIndexParserType::PARSER_ENGLISH:
        return std::make_unique<lucene::analysis::SimpleAnalyzer>();
    case InvertedIndexParserType::PARSER_STANDARD:
        return std::make_unique<lucene::analysis::standard::StandardAnalyzer>();
    case InvertedIndexParserType::PARSER_CHINESE: {
        auto chinese_analyzer = std::make_unique<lucene::analysis::LanguageBasedAnalyzer>();
        chinese_analyzer->setLanguage(L"cjk");
        chinese_analyzer->setStem(false);
        return chinese_analyzer;
    }
    default:
        return Status::NotSupported("Not support UNKNOWN parser_type");
    }
}

} // namespace starrocks