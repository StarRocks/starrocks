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

#include "storage/index/inverted/clucene/match_operator.h"

#include <CLucene/index/Term.h>
#include <CLucene/search/query/TermIterator.h>

#include <boost/algorithm/string/replace.hpp>
#include <boost/locale/encoding_utf.hpp>

#include "common/statusor.h"
#include "storage/index/inverted/clucene/clucene_roaring_hit_collector.h"
#include "storage/index/inverted/inverted_index_analyzer.h"

namespace starrocks {

Status MatchOperator::match(roaring::Roaring& result) {
    return _match_internal(result);
}

Status MatchTermOperator::_match_internal(roaring::Roaring& result) {
    std::vector<Term*> terms;
    std::vector<TermDocs*> _term_docs;
    std::vector<TermIterator> iterators;

    ASSIGN_OR_RETURN(const auto _terms,
                     InvertedIndexAnalyzer::get_analyse_result(_search_str, _field_name, _inverted_index_ctx));
    for (const auto& term : _terms) {
        std::wstring ws_term = boost::locale::conv::utf_to_utf<TCHAR>(term);
        Term* t = _CLNEW Term(_field_name.c_str(), ws_term.c_str());
        terms.push_back(t);
        TermDocs* term_doc = _searcher->getReader()->termDocs(t, nullptr);
        _term_docs.push_back(term_doc);
        iterators.emplace_back(term_doc);
    }

    std::sort(iterators.begin(), iterators.end(),
              [](const TermIterator& a, const TermIterator& b) { return a.docFreq() < b.docFreq(); });

    auto func = [&result](const TermIterator& term_docs, bool first) {
        roaring::Roaring roaring;
        DocRange doc_range;
        while (term_docs.readRange(&doc_range)) {
            if (doc_range.type_ == DocRangeType::kMany) {
                roaring.addMany(doc_range.doc_many_size_, doc_range.doc_many->data());
            } else {
                roaring.addRange(doc_range.doc_range.first, doc_range.doc_range.second);
            }
        }

        if (first) {
            result.swap(roaring);
        } else {
            result &= roaring;
        }
    };

    bool first = true;
    for (auto& iterator : iterators) {
        func(iterator, first);
        first = false;
    }
    return Status::OK();
}

Status MatchRangeOperator::_match_internal(roaring::Roaring& result) {
    std::wstring search_word(_bound.begin(), _bound.end());
    lucene::index::Term term(_field_name.c_str(), search_word.c_str());
    std::unique_ptr<lucene::search::RangeQuery> range_query = create_query(&term);
    RoaringHitCollector result_collector(&result);
    _searcher->_search(range_query.get(), nullptr, &result_collector);
    return Status::OK();
}

Status MatchWildcardOperator::_match_internal(roaring::Roaring& result) {
    auto wildcard_clucene_str = boost::algorithm::replace_all_copy(_wildcard, "%", "*");
    std::wstring search_word(wildcard_clucene_str.begin(), wildcard_clucene_str.end());
    lucene::index::Term term(_field_name.c_str(), search_word.c_str());

    lucene::search::WildcardQuery wildcard_query(&term);

    RoaringHitCollector result_collector(&result);
    _searcher->_search(&wildcard_query, nullptr, &result_collector);
    return Status::OK();
}

Status MatchPhraseOperator::_match_internal(roaring::Roaring& result) {
    lucene::search::PhraseQuery phrase_query;
    ASSIGN_OR_RETURN(const auto terms,
                     InvertedIndexAnalyzer::get_analyse_result(_query_str, _field_name, _inverted_index_ctx));
    for (const auto& term : terms) {
        std::wstring search_word = boost::locale::conv::utf_to_utf<TCHAR>(term);
        // memory of term hosts to phrase query
        auto* phrase_term = _CLNEW lucene::index::Term(_field_name.c_str(), search_word.c_str());
        phrase_query.add(phrase_term);
        _CLDECDELETE(phrase_term)
    }
    phrase_query.setSlop(_slop);

    RoaringHitCollector result_collector(&result);
    _searcher->_search(&phrase_query, nullptr, &result_collector);
    return Status::OK();
}

} // namespace starrocks