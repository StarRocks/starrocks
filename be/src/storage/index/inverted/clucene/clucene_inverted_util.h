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

#include <boost/locale/encoding_utf.hpp>
#include <codecvt>

#include "common/statusor.h"
#include "storage/index/inverted/inverted_index_common.h"

namespace starrocks {
inline StatusOr<std::unique_ptr<lucene::analysis::Analyzer>> get_analyzer(InvertedIndexParserType parser_type) {
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

inline Status tokenize_text(InvertedIndexParserType parser_type, std::string search_str,
                            std::vector<std::wstring>& result) {
    ASSIGN_OR_RETURN(auto analyzer, get_analyzer(parser_type));
    std::wstring search_wstr = boost::locale::conv::utf_to_utf<TCHAR>(search_str);
    lucene::util::StringReader reader(search_wstr.c_str(), search_wstr.size(), false);
    auto stream = analyzer->reusableTokenStream(L"", &reader);
    lucene::analysis::Token token;
    while (stream->next(&token)) {
        if (token.termLength() != 0) {
            std::wstring str(token.termBuffer(), token.termLength());
            result.emplace_back(str);
        }
    }
    return Status::OK();
}
} // namespace starrocks