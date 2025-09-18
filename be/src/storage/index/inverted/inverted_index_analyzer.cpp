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
#include "char_filter/char_filter_factory.h"
#include "common/config.h"
#include "storage/index/inverted/gin_query_options.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "util/defer_op.h"

namespace starrocks {

StatusOr<std::unique_ptr<lucene::analysis::Analyzer>> InvertedIndexAnalyzer::create_analyzer(
        const GinQueryOptions* gin_query_options) {
    RETURN_IF(gin_query_options == nullptr, Status::InvalidArgument("Null context was given while creating analyzer."));

    std::unique_ptr<lucene::analysis::Analyzer> analyzer;
    switch (gin_query_options->getParserType()) {
    case InvertedIndexParserType::PARSER_NONE:
    case InvertedIndexParserType::PARSER_ENGLISH:
        analyzer = std::make_unique<lucene::analysis::SimpleAnalyzer<char>>();
        break;
    case InvertedIndexParserType::PARSER_STANDARD:
        analyzer = std::make_unique<lucene::analysis::standard95::StandardAnalyzer>();
        break;
    case InvertedIndexParserType::PARSER_CHINESE: {
        auto chinese_analyzer = std::make_unique<lucene::analysis::LanguageBasedAnalyzer>(L"chinese", false);
        chinese_analyzer->initDict(config::inverted_index_dict_path);
        if (gin_query_options->getParserMode() == InvertedParserMode::COARSE_GRAINED) {
            chinese_analyzer->setMode(lucene::analysis::AnalyzerMode::Default);
        } else {
            chinese_analyzer->setMode(lucene::analysis::AnalyzerMode::All);
        }
        analyzer = std::move(chinese_analyzer);
        break;
    }
    default:
        return Status::NotSupported("Not support UNKNOWN parser_type");
    }

    // set lowercase
    analyzer->set_lowercase(gin_query_options->getLowerCase());

    auto stop_words = gin_query_options->getStopWords();
    if (stop_words == INVERTED_INDEX_STOP_WORDS_NONE) {
        analyzer->set_stopwords(nullptr);
    } else {
        analyzer->set_stopwords(&lucene::analysis::standard95::stop_words);
    }
    return analyzer;
}

std::unique_ptr<lucene::util::Reader> InvertedIndexAnalyzer::create_reader(
        const std::map<std::string, std::string>& char_filter_map) {
    std::unique_ptr<lucene::util::Reader> reader = std::make_unique<lucene::util::SStringReader<char>>();
    if (!char_filter_map.empty()) {
        reader = std::unique_ptr<lucene::util::Reader>(
                CharFilterFactory::create(char_filter_map.at(INVERTED_INDEX_CHAR_FILTER_TYPE_KEY), reader.release(),
                                          char_filter_map.at(INVERTED_INDEX_CHAR_FILTER_PATTERN_KEY),
                                          char_filter_map.at(INVERTED_INDEX_CHAR_FILTER_REPLACEMENT_KEY)));
    }
    return reader;
}

StatusOr<std::vector<std::string>> InvertedIndexAnalyzer::get_analyse_result(const std::string& search_str,
                                                                             const std::wstring& field_name,
                                                                             const GinQueryOptions* gin_query_options) {
    RETURN_IF(gin_query_options == nullptr,
              Status::InvalidArgument("Null context was given while getting analyse result."));
    if (gin_query_options->getParserType() == InvertedIndexParserType::PARSER_NONE ||
        gin_query_options->getParserType() == InvertedIndexParserType::PARSER_UNKNOWN) {
        return std::vector{search_str};
    }
    ASSIGN_OR_RETURN(const auto analyzer, create_analyzer(gin_query_options));
    const auto reader = create_reader(gin_query_options->getCharFilterMap());
    reader->init(search_str.data(), static_cast<int32_t>(search_str.size()), false);
    const std::wstring field_ws(field_name.begin(), field_name.end());
    return analyse_result_internal(reader.get(), analyzer.get(), std::move(field_ws),
                                   gin_query_options->getQueryType());
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
