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

#include "storage/inverted/clucene/clucene_roaring_hit_collector.h"

namespace starrocks {

Status MatchOperator::match(roaring::Roaring* result) {
    RoaringHitCollector result_collector(result);
    return _match_internal(&result_collector);
}
Status MatchOperator::match(roaring::Roaring* result, std::vector<float_t>* scores) {
    RoaringHitWithScoreCollector result_collector(result, scores);
    return _match_internal(&result_collector);
}

Status MatchTermOperator::_match_internal(lucene::search::HitCollector* hit_collector) {
    std::wstring search_word(_term.begin(), _term.end());
    std::unique_ptr<lucene::index::Term, void (*)(lucene::index::Term*)> term{
            _CLNEW lucene::index::Term(_field_name.c_str(), search_word.c_str()),
            [](lucene::index::Term* term) { _CLDECDELETE(term) }};

    lucene::search::TermQuery term_query(term.get());
    _searcher->_search(&term_query, nullptr, hit_collector);
    return Status::OK();
}

Status MatchChineseTermOperator::_match_internal(lucene::search::HitCollector* hit_collector) {
    auto reader = _CLNEW lucene::util::SimpleInputStreamReader(_CLNEW lucene::util::AStringReader(_term.c_str()),
                                                               lucene::util::SimpleInputStreamReader::UTF8);

    lucene::analysis::TokenStream* token_stream = _analyzer->tokenStream(_field_name.c_str(), reader);

    lucene::analysis::Token token;
    std::vector<std::wstring> analyse_result;

    while (token_stream->next(&token)) {
        if (token.termLength() != 0) {
            analyse_result.emplace_back(token.termBuffer(), token.termLength());
        }
    }

    token_stream->close();
    _CLDELETE(reader)

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
    std::unique_ptr<lucene::index::Term, void (*)(lucene::index::Term*)> term{
            _CLNEW lucene::index::Term(_field_name.c_str(), search_word.c_str()),
            [](lucene::index::Term* term) { _CLDECDELETE(term) }};

    std::unique_ptr<lucene::search::RangeQuery> range_query = create_query(term.get());
    _searcher->_search(range_query.get(), nullptr, hit_collector);
    return Status::OK();
}

Status MatchWildcardOperator::_match_internal(lucene::search::HitCollector* hit_collector) {
    std::wstring search_word(_wildcard.begin(), _wildcard.end());
    std::unique_ptr<lucene::index::Term, void (*)(lucene::index::Term*)> term{
            _CLNEW lucene::index::Term(_field_name.c_str(), search_word.c_str()),
            [](lucene::index::Term* term) { _CLDECDELETE(term) }};

    lucene::search::WildcardQuery wildcard_query(term.get());
    _searcher->_search(&wildcard_query, nullptr, hit_collector);
    return Status::OK();
}

Status MatchPhraseOperator::_match_internal(lucene::search::HitCollector* hit_collector) {
    lucene::search::PhraseQuery phrase_query;
    for (const auto& term : _terms) {
        std::wstring search_word(term.begin(), term.end());
        auto* phrase_term = _CLNEW lucene::index::Term(_field_name.c_str(), search_word.c_str());
        phrase_query.add(phrase_term);
        _CLDECDELETE(phrase_term)
    }
    phrase_query.setSlop(_slop);
    _searcher->_search(&phrase_query, nullptr, hit_collector);
    return Status::OK();
}

} // namespace starrocks