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

#include "storage/inverted/clucene/match_operator.h"

#include <boost/algorithm/string/replace.hpp>

#include "storage/inverted/clucene/clucene_inverted_util.hpp"
#include "storage/inverted/clucene/clucene_roaring_hit_collector.hpp"

namespace starrocks {

Status MatchOperator::match(roaring::Roaring* result) {
    RoaringHitCollector result_collector(result);
    return _match_internal(&result_collector);
}

Status MatchTermOperator::_match_internal(lucene::search::HitCollector* hit_collector) {
    std::wstring search_word(_term.begin(), _term.end());
    lucene::index::Term term(_field_name.c_str(), search_word.c_str());
    lucene::search::TermQuery term_query(&term);
    _searcher->_search(&term_query, nullptr, hit_collector);
    return Status::OK();
}

Status MatchChineseTermOperator::_match_internal(lucene::search::HitCollector* hit_collector) {
    auto string_reader = std::make_unique<lucene::util::AStringReader>(_term.c_str());
    auto reader = std::make_unique<lucene::util::SimpleInputStreamReader>(string_reader.get(),
                                                                          lucene::util::SimpleInputStreamReader::UTF8);

    lucene::analysis::TokenStream* token_stream = _analyzer->tokenStream(_field_name.c_str(), reader.get());

    lucene::analysis::Token token;
    std::vector<std::wstring> analyse_result;

    while (token_stream->next(&token)) {
        if (token.termLength() != 0) {
            analyse_result.emplace_back(token.termBuffer(), token.termLength());
        }
    }

    token_stream->close();

    lucene::search::BooleanQuery query;
    for (const auto& t : analyse_result) {
        auto* term = _CLNEW lucene::index::Term(_field_name.c_str(), t.c_str());
        query.add(_CLNEW lucene::search::TermQuery(term), true, lucene::search::BooleanClause::SHOULD);
        _CLDECDELETE(term)
    }

    _searcher->_search(&query, nullptr, hit_collector);
    return Status::OK();
}

Status MatchRangeOperator::_match_internal(lucene::search::HitCollector* hit_collector) {
    std::wstring search_word(_bound.begin(), _bound.end());
    lucene::index::Term term(_field_name.c_str(), search_word.c_str());
    std::unique_ptr<lucene::search::RangeQuery> range_query = create_query(&term);
    _searcher->_search(range_query.get(), nullptr, hit_collector);
    return Status::OK();
}

Status MatchWildcardOperator::_match_internal(lucene::search::HitCollector* hit_collector) {
    auto wildcard_clucene_str = boost::algorithm::replace_all_copy(_wildcard, "%", "*");
    std::wstring search_word(wildcard_clucene_str.begin(), wildcard_clucene_str.end());
    lucene::index::Term term(_field_name.c_str(), search_word.c_str());

    lucene::search::WildcardQuery wildcard_query(&term);
    _searcher->_search(&wildcard_query, nullptr, hit_collector);
    return Status::OK();
}

Status MatchPhraseOperator::_match_internal(lucene::search::HitCollector* hit_collector) {
    lucene::search::PhraseQuery phrase_query;
    ASSIGN_OR_RETURN(auto analyzer, get_analyzer(_parser_type));
    std::wstring wstr(_compound_term.begin(), _compound_term.end());
    lucene::util::StringReader reader(wstr.c_str(), wstr.size(), false);
    auto stream = analyzer->reusableTokenStream(L"", &reader);
    lucene::analysis::Token token;
    while (stream->next(&token)) {
        std::wstring search_word(token.termBuffer(), token.termLength());
        // memory of term hosts to phrase query
        auto* phrase_term = _CLNEW lucene::index::Term(_field_name.c_str(), search_word.c_str());
        phrase_query.add(phrase_term);
        _CLDECDELETE(phrase_term)
    }
    phrase_query.setSlop(_slop);
    _searcher->_search(&phrase_query, nullptr, hit_collector);
    return Status::OK();
}

} // namespace starrocks