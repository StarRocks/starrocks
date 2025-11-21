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

#include "inverted_index_analyzer.h"

#include "CLucene.h"
#include "CLucene/analysis/LanguageBasedAnalyzer.h"
#include "CLucene/analysis/standard95/StandardAnalyzer.h"
#include "storage/index/inverted/gin_query_options.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "util/defer_op.h"

namespace starrocks {

StatusOr<std::unique_ptr<lucene::analysis::Analyzer>> InvertedIndexAnalyzer::create_analyzer(
        InvertedIndexParserType parser_type) {
    switch (parser_type) {
    case InvertedIndexParserType::PARSER_NONE:
    case InvertedIndexParserType::PARSER_ENGLISH:
        return std::make_unique<lucene::analysis::SimpleAnalyzer<char>>();
    case InvertedIndexParserType::PARSER_STANDARD:
        return std::make_unique<lucene::analysis::standard95::StandardAnalyzer>();
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

StatusOr<std::vector<std::string>> InvertedIndexAnalyzer::get_analyse_result(const std::string& search_str,
                                                                             const std::wstring& field_name,
                                                                             const GinQueryOptions* gin_query_options) {
    return get_analyse_result(search_str, field_name, gin_query_options->getParserType(),
                              gin_query_options->getQueryType());
}

StatusOr<std::vector<std::string>> InvertedIndexAnalyzer::get_analyse_result(const std::string& search_str,
                                                                             const std::wstring& field_name,
                                                                             const InvertedIndexParserType& parser_type,
                                                                             const InvertedIndexQueryType& query_type) {
    if (parser_type == InvertedIndexParserType::PARSER_NONE || parser_type == InvertedIndexParserType::PARSER_UNKNOWN) {
        return std::vector{search_str};
    }
    ASSIGN_OR_RETURN(auto analyzer, create_analyzer(parser_type));
    auto reader = std::make_unique<lucene::util::SStringReader<char>>();
    reader->init(search_str.data(), static_cast<int32_t>(search_str.size()), false);
    const std::wstring field_ws(field_name.begin(), field_name.end());
    return analyse_result_internal(reader.get(), analyzer.get(), std::move(field_ws), query_type);
}

std::vector<std::string> InvertedIndexAnalyzer::analyse_result_internal(lucene::util::Reader* reader,
                                                                        lucene::analysis::Analyzer* analyzer,
                                                                        const std::wstring& field_name,
                                                                        const InvertedIndexQueryType& query_type,
                                                                        const bool& drop_duplicates) {
    std::unique_ptr<lucene::analysis::TokenStream> token_stream(analyzer->tokenStream(field_name.c_str(), reader));
    DeferOp d([&] {
        if (token_stream != nullptr) {
            token_stream->close();
        }
    });

    std::vector<std::string> analyse_result;
    lucene::analysis::Token token;
    while (token_stream->next(&token)) {
        if (token.termLength<char>() != 0) {
            analyse_result.emplace_back(token.termBuffer<char>(), token.termLength<char>());
        }
    }

    if (drop_duplicates && query_type == InvertedIndexQueryType::MATCH_ALL_QUERY) {
        std::set unrepeated_result(analyse_result.begin(), analyse_result.end());
        analyse_result.assign(unrepeated_result.begin(), unrepeated_result.end());
    }
    return analyse_result;
}

} // namespace starrocks
