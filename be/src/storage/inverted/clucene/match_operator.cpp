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
    lucene::util::StringReader reader(_compound_term.c_str(), _compound_term.size(), false);
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