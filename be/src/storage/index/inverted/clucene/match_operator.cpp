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

#include <CLucene/config/repl_wchar.h>
#include <CLucene/index/Term.h>
#include <CLucene/search/query/TermIterator.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/locale/encoding_utf.hpp>
#include <charconv>

#include "common/statusor.h"
#include "http/action/datacache_action.h"
#include "storage/index/inverted/clucene/clucene_roaring_hit_collector.h"
#include "storage/index/inverted/gin_query_options.h"
#include "storage/index/inverted/inverted_index_analyzer.h"

namespace starrocks {

template <typename Derived>
bool PhraseMatcherBase<Derived>::matches(int32_t doc) {
    reset(doc);
    return static_cast<Derived*>(this)->next_match();
}

template <typename Derived>
void PhraseMatcherBase<Derived>::reset(int32_t doc) {
    for (PostingsAndPosition& posting : _postings) {
        if (posting._postings.docID() != doc) {
            posting._postings.advance(doc);
        }
        posting._freq = posting._postings.freq();
        posting._pos = -1;
        posting._upTo = 0;
    }
}

template <typename Derived>
bool PhraseMatcherBase<Derived>::advance_position(PostingsAndPosition& posting, int32_t target) {
    while (posting._pos < target) {
        if (posting._upTo == posting._freq) {
            return false;
        } else {
            posting._pos = posting._postings.nextPosition();
            posting._upTo += 1;
        }
    }
    return true;
}

bool ExactPhraseMatcher::next_match() {
    PostingsAndPosition& lead = _postings[0];
    if (lead._upTo < lead._freq) {
        lead._pos = lead._postings.nextPosition();
        lead._upTo += 1;
    } else {
        return false;
    }

    while (true) {
        int32_t phrasePos = lead._pos - lead._offset;

        bool advance_head = false;
        for (size_t j = 1; j < _postings.size(); ++j) {
            PostingsAndPosition& posting = _postings[j];
            int32_t expectedPos = phrasePos + posting._offset;
            // advance up to the same position as the lead
            if (!advance_position(posting, expectedPos)) {
                return false;
            }

            if (posting._pos != expectedPos) { // we advanced too far
                if (advance_position(lead, posting._pos - posting._offset + lead._offset)) {
                    advance_head = true;
                    break;
                }
                return false;
            }
        }
        if (advance_head) {
            continue;
        }

        return true;
    }
}

bool OrderedSloppyPhraseMatcher::next_match() {
    PostingsAndPosition* prev_posting = _postings.data();
    while (prev_posting->_upTo < prev_posting->_freq) {
        prev_posting->_pos = prev_posting->_postings.nextPosition();
        prev_posting->_upTo += 1;
        if (stretch_to_order(prev_posting) && _match_width <= _allowed_slop) {
            return true;
        }
    }
    return false;
}

bool OrderedSloppyPhraseMatcher::stretch_to_order(PostingsAndPosition* prev_posting) {
    _match_width = 0;
    for (size_t i = 1; i < _postings.size(); i++) {
        PostingsAndPosition& posting = _postings[i];
        if (!advance_position(posting, prev_posting->_pos + 1)) {
            return false;
        }
        _match_width += (posting._pos - (prev_posting->_pos + 1));
        prev_posting = &posting;
    }
    return true;
}

Status MatchOperator::match(roaring::Roaring& result) {
    return _match_internal(result);
}

Status MatchOperator::parser_info(std::string& query, const std::wstring& field_name,
                                  const InvertedIndexParserType& parser_type, const InvertedIndexQueryType& query_type,
                                  InvertedIndexQueryInfo& query_info, const bool& sequential_opt) {
    parser_slop(query, query_info);
    ASSIGN_OR_RETURN(query_info.terms,
                     InvertedIndexAnalyzer::get_analyse_result(query, field_name, parser_type, query_type));
    if (sequential_opt && query_info.ordered) {
        std::vector<std::string> t_queries;
        boost::split(t_queries, query, boost::algorithm::is_any_of(" "));
        for (auto& t_query : t_queries) {
            ASSIGN_OR_RETURN(auto terms,
                             InvertedIndexAnalyzer::get_analyse_result(t_query, field_name, parser_type, query_type))
            if (terms.size() >= 2) {
                query_info.additional_terms.emplace_back(std::move(terms));
            }
        }
    }
    return Status::OK();
}

void MatchOperator::parser_slop(std::string& query, InvertedIndexQueryInfo& query_info) {
    auto is_digits = [](const std::string_view& str) {
        return std::ranges::all_of(str, [](unsigned char c) { return std::isdigit(c); });
    };

    size_t last_space_pos = query.find_last_of(' ');
    if (last_space_pos != std::string::npos) {
        size_t tilde_pos = last_space_pos + 1;
        if (tilde_pos < query.size() - 1 && query[tilde_pos] == '~') {
            size_t slop_pos = tilde_pos + 1;
            std::string_view slop_str(query.data() + slop_pos, query.size() - slop_pos);
            do {
                if (slop_str.empty()) {
                    break;
                }

                bool ordered = false;
                if (slop_str.size() == 1) {
                    if (!std::isdigit(slop_str[0])) {
                        break;
                    }
                } else {
                    if (slop_str.back() == '+') {
                        ordered = true;
                        slop_str.remove_suffix(1);
                    }
                }

                if (is_digits(slop_str)) {
                    auto result = std::from_chars(slop_str.begin(), slop_str.end(), query_info.slop);
                    if (result.ec != std::errc()) {
                        break;
                    }
                    query_info.ordered = ordered;
                    query = query.substr(0, last_space_pos);
                }
            } while (false);
        }
    }
}

void MatchTermOperator::add_terms(std::vector<std::string> terms) {
    _terms = std::move(terms);
}

Status MatchTermOperator::_match_internal(roaring::Roaring& result) {
    std::vector<Term*> terms;
    std::vector<TermDocs*> _term_docs;
    std::vector<TermIterator> iterators;

    if (_terms.empty()) {
        ASSIGN_OR_RETURN(_terms,
                         InvertedIndexAnalyzer::get_analyse_result(_search_str, _field_name, _gin_query_options));
        VLOG(10) << "No terms specified, use search_str to get analyse result.";
    }

    for (const auto& term : _terms) {
        std::wstring ws_term = boost::locale::conv::utf_to_utf<TCHAR>(term);
        Term* t = _CLNEW Term(_field_name.c_str(), ws_term.c_str());
        terms.push_back(t);
        TermDocs* term_doc = _searcher->getReader()->termDocs(t, nullptr);
        _term_docs.push_back(term_doc);
        iterators.emplace_back(term_doc);
    }

    std::ranges::sort(iterators,
                      [](const TermIterator& a, const TermIterator& b) { return a.docFreq() < b.docFreq(); });

    bool first = true;
    for (auto& iterator : iterators) {
        _filter(iterator, first, result);
        first = false;
    }
    return Status::OK();
}

void MatchTermOperator::_filter(const TermIterator& term_docs, const bool& first, roaring::Roaring& result) {
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

void MatchPhraseOperator::add(const std::vector<std::string>& terms) {
    if (terms.empty()) {
        _CLTHROWA(CL_ERR_IllegalArgument, "PhraseQuery::add: terms empty");
    }

    if (terms.size() == 1) {
        std::wstring ws_term = boost::locale::conv::utf_to_utf<TCHAR>(terms[0]);
        Term* t = _CLNEW Term(_field_name.c_str(), ws_term.c_str());
        _terms.push_back(t);
        TermDocs* term_doc = _searcher->getReader()->termDocs(t, nullptr);
        _term_docs.push_back(term_doc);
        _lead1 = TermIterator(term_doc);
        return;
    }

    std::vector<TermIterator> iterators;
    auto ensureTermPosition = [this, &iterators](const std::string& term, bool is_save_iter = true) {
        std::wstring ws_term = boost::locale::conv::utf_to_utf<TCHAR>(term);
        Term* t = _CLNEW Term(_field_name.c_str(), ws_term.c_str());
        _terms.push_back(t);
        TermPositions* term_pos = _searcher->getReader()->termPositions(t, nullptr);
        _term_docs.push_back(term_pos);
        if (is_save_iter) {
            iterators.emplace_back(term_pos);
        }
        return term_pos;
    };

    if (_slop == 0) {
        ExactPhraseMatcher matcher;
        for (size_t i = 0; i < terms.size(); i++) {
            const auto& term = terms[i];
            auto* term_pos = ensureTermPosition(term);
            matcher._postings.emplace_back(term_pos, i);
        }
        _matchers.emplace_back(matcher);
    } else {
        {
            OrderedSloppyPhraseMatcher single_matcher;
            for (size_t i = 0; i < terms.size(); i++) {
                const auto& term = terms[i];
                auto* term_pos = ensureTermPosition(term);
                single_matcher._postings.emplace_back(term_pos, i);
            }
            single_matcher._allowed_slop = _slop;
            _matchers.emplace_back(single_matcher);
        }
        {
            for (auto& additional_term : _additional_terms) {
                ExactPhraseMatcher single_matcher;
                for (size_t i = 0; i < additional_term.size(); i++) {
                    const auto& term = additional_term[i];
                    auto* term_pos = ensureTermPosition(term, false);
                    single_matcher._postings.emplace_back(term_pos, i);
                }
                _matchers.emplace_back(std::move(single_matcher));
            }
        }
    }

    std::ranges::sort(iterators,
                      [](const TermIterator& a, const TermIterator& b) { return a.docFreq() < b.docFreq(); });

    _lead1 = iterators[0];
    _lead2 = iterators[1];
    for (int32_t i = 2; i < iterators.size(); i++) {
        _others.push_back(iterators[i]);
    }
}

Status MatchPhraseOperator::search_by_bitmap(roaring::Roaring& roaring) const {
    DocRange doc_range;
    while (_lead1.readRange(&doc_range)) {
        if (doc_range.type_ == DocRangeType::kMany) {
            roaring.addMany(doc_range.doc_many_size_, doc_range.doc_many->data());
        } else {
            roaring.addRange(doc_range.doc_range.first, doc_range.doc_range.second);
        }
    }
    return Status::OK();
}

Status MatchPhraseOperator::search_by_skiplist(roaring::Roaring& roaring) {
    int32_t doc = 0;
    while ((doc = do_next(_lead1.nextDoc())) != INT32_MAX) {
        if (matches(doc)) {
            roaring.add(doc);
        }
    }
    return Status::OK();
}

int32_t MatchPhraseOperator::do_next(int32_t doc) const {
    while (true) {
        assert(doc == _lead1.docID());

        // the skip list is used to find the two smallest inverted lists
        int32_t next2 = _lead2.advance(doc);
        if (next2 != doc) {
            doc = _lead1.advance(next2);
            if (next2 != doc) {
                continue;
            }
        }

        // if both lead1 and lead2 exist, use skip list to lookup other inverted indexes
        bool advance_head = false;
        for (auto& other : _others) {
            if (other.isEmpty()) {
                continue;
            }

            if (other.docID() < doc) {
                int32_t next = other.advance(doc);
                if (next > doc) {
                    doc = _lead1.advance(next);
                    advance_head = true;
                    break;
                }
            }
        }
        if (advance_head) {
            continue;
        }

        return doc;
    }
}

bool MatchPhraseOperator::matches(int32_t doc) {
    return std::ranges::all_of(_matchers, [&doc](auto&& matcher) {
        return std::visit([&doc](auto&& m) -> bool { return m.matches(doc); }, matcher);
    });
}

MatchPhraseOperator::~MatchPhraseOperator() {
    for (auto& term_doc : _term_docs) {
        if (term_doc) {
            _CLDELETE(term_doc);
        }
    }
    for (auto& term : _terms) {
        if (term) {
            _CLDELETE(term);
        }
    }
}

Status MatchPhraseOperator::_match_internal(roaring::Roaring& result) {
    InvertedIndexQueryInfo query_info;
    RETURN_IF_ERROR(parser_info(_search_str, _field_name, _gin_query_options->getParserType(),
                                InvertedIndexQueryType::MATCH_PHRASE_QUERY, query_info,
                                _gin_query_options->enablePhraseQuerySequentialOpt()));
    _slop = query_info.slop;
    if (_slop == 0 || query_info.ordered) {
        if (query_info.ordered) {
            _additional_terms = query_info.additional_terms;
        }
        // Logic for no slop query and ordered phrase query
        add(query_info.terms);
    } else {
        // Simple slop query follows the default phrase query algorithm
        _phrase_query = std::make_unique<lucene::search::PhraseQuery>();
        for (const auto& term : query_info.terms) {
            std::wstring ws_term = boost::locale::conv::utf_to_utf<TCHAR>(term);
            auto* t = _CLNEW lucene::index::Term(_field_name.c_str(), ws_term.c_str());
            _phrase_query->add(t);
            _CLDECDELETE(t);
        }
        _phrase_query->setSlop(_slop);
    }

    if (_phrase_query) {
        _searcher->_search(_phrase_query.get(),
                           [&result](const int32_t docid, const float_t /*score*/) { result.add(docid); });
        return Status::OK();
    }

    if (_lead1.isEmpty()) {
        return Status::OK();
    }
    if (_lead2.isEmpty()) {
        return search_by_bitmap(result);
    }
    return search_by_skiplist(result);
}

void MatchAnyTermOperator::_filter(const TermIterator& term_docs, const bool& first, roaring::Roaring& result) {
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
        result |= roaring;
    }
}

Status MatchPhraseEdgeOperator::_match_internal(roaring::Roaring& result) {
    ASSIGN_OR_RETURN(const auto terms,
                     InvertedIndexAnalyzer::get_analyse_result(_search_str, _field_name, _gin_query_options));

    if (terms.size() == 1) {
        return search_one_term(terms, result);
    }
    return search_multi_term(terms, result);
}

Status MatchPhraseEdgeOperator::search_one_term(const std::vector<std::string>& _terms,
                                                roaring::Roaring& roaring) const {
    bool first = true;
    std::wstring sub_term = boost::locale::conv::utf_to_utf<TCHAR>(_terms[0]);
    find_words([this, &first, &sub_term, &roaring](Term* term) {
        std::wstring_view ws_term(term->text(), term->textLength());
        if (ws_term.find(sub_term) == std::wstring::npos) {
            return;
        }

        DocRange doc_range;
        TermDocs* term_doc = _searcher->getReader()->termDocs(term);
        roaring::Roaring result;
        while (term_doc->readRange(&doc_range)) {
            if (doc_range.type_ == DocRangeType::kMany) {
                result.addMany(doc_range.doc_many_size_, doc_range.doc_many->data());
            } else {
                result.addRange(doc_range.doc_range.first, doc_range.doc_range.second);
            }
        }
        _CLDELETE(term_doc);

        if (!first) {
            roaring.swap(result);
            first = false;
        } else {
            roaring |= result;
        }
    });
    return Status::OK();
}

Status MatchPhraseEdgeOperator::search_multi_term(const std::vector<std::string>& terms,
                                                  roaring::Roaring& roaring) const {
    std::wstring suffix_term = boost::locale::conv::utf_to_utf<TCHAR>(terms[0]);
    std::wstring prefix_term = boost::locale::conv::utf_to_utf<TCHAR>(terms.back());

    std::vector<CL_NS(index)::Term*> suffix_terms;
    std::vector<CL_NS(index)::Term*> prefix_terms;

    auto max_expansions = _gin_query_options->maxExpansions();

    find_words([this, &suffix_term, &suffix_terms, &prefix_term, &prefix_terms, &max_expansions](Term* term) {
        std::wstring_view ws_term(term->text(), term->textLength());

        if (max_expansions == 0 || suffix_terms.size() < max_expansions) {
            if (ws_term.ends_with(suffix_term)) {
                suffix_terms.push_back(_CL_POINTER(term));
            }
        }

        if (max_expansions == 0 || prefix_terms.size() < max_expansions) {
            if (ws_term.starts_with(prefix_term)) {
                prefix_terms.push_back(_CL_POINTER(term));
            }
        }
    });

    lucene::search::MultiPhraseQuery query;
    for (size_t i = 0; i < terms.size(); i++) {
        if (i == 0) {
            handle_terms(&query, _field_name, suffix_term, suffix_terms);
        } else if (i == terms.size() - 1) {
            handle_terms(&query, _field_name, prefix_term, prefix_terms);
        } else {
            std::wstring ws_term = boost::locale::conv::utf_to_utf<TCHAR>(terms[i]);
            add_default_term(&query, _field_name, ws_term);
        }
    }

    _searcher->_search(&query, [&roaring](const int32_t docid, const float_t /*score*/) { roaring.add(docid); });
    return Status::OK();
}

void MatchPhraseEdgeOperator::find_words(const std::function<void(Term*)>& cb) const {
    Term* term = nullptr;
    TermEnum* enumerator = nullptr;
    try {
        enumerator = _searcher->getReader()->terms();
        while (enumerator->next()) {
            term = enumerator->term();
            cb(term);
            _CLDECDELETE(term);
        }
    }
    _CLFINALLY({
        _CLDECDELETE(term);
        enumerator->close();
        _CLDELETE(enumerator);
    })
}

void MatchPhraseEdgeOperator::add_default_term(lucene::search::MultiPhraseQuery* query, const std::wstring& field_name,
                                               const std::wstring& ws_term) {
    Term* t = _CLNEW Term(field_name.c_str(), ws_term.c_str());
    query->add(t);
    _CLLDECDELETE(t);
}

void MatchPhraseEdgeOperator::handle_terms(lucene::search::MultiPhraseQuery* query, const std::wstring& field_name,
                                           const std::wstring& ws_term, std::vector<Term*>& checked_terms) {
    if (checked_terms.empty()) {
        add_default_term(query, field_name, ws_term);
    } else {
        query->add(checked_terms);
        for (const auto& t : checked_terms) {
            _CLLDECDELETE(t);
        }
    }
}

void MatchPhrasePrefixOperator::get_prefix_terms(IndexReader* reader, const std::wstring& field_name,
                                                 const std::string& prefix, std::vector<Term*>& prefix_terms,
                                                 int32_t max_expansions) {
    std::wstring ws_prefix = boost::locale::conv::utf_to_utf<TCHAR>(prefix);

    Term* prefix_term = _CLNEW Term(field_name.c_str(), ws_prefix.c_str());
    TermEnum* enumerator = reader->terms(prefix_term);

    Term* lastTerm = nullptr;
    try {
        int32_t count = 0;
        const TCHAR* prefixText = prefix_term->text();
        const TCHAR* prefixField = prefix_term->field();
        const TCHAR* tmp = nullptr;
        size_t i = 0;
        size_t prefixLen = prefix_term->textLength();
        do {
            lastTerm = enumerator->term();
            if (lastTerm != nullptr && lastTerm->field() == prefixField) {
                size_t termLen = lastTerm->textLength();
                if (prefixLen > termLen) {
                    break;
                }

                tmp = lastTerm->text();

                for (i = prefixLen - 1; i != -1; --i) {
                    if (tmp[i] != prefixText[i]) {
                        tmp = nullptr;
                        break;
                    }
                }
                if (tmp == nullptr) {
                    break;
                }

                if (max_expansions > 0 && count >= max_expansions) {
                    break;
                }

                Term* t = _CLNEW Term(field_name.c_str(), tmp);
                prefix_terms.push_back(t);
                count++;
            } else {
                break;
            }
            _CLDECDELETE(lastTerm);
        } while (enumerator->next());
    }
    _CLFINALLY({
        enumerator->close();
        _CLDELETE(enumerator);
        _CLDECDELETE(lastTerm);
        _CLDECDELETE(prefix_term);
    });
}

Status MatchPhrasePrefixOperator::_match_internal(roaring::Roaring& result) {
    lucene::search::MultiPhraseQuery _query;
    ASSIGN_OR_RETURN(const auto terms,
                     InvertedIndexAnalyzer::get_analyse_result(_search_str, _field_name, _gin_query_options));

    for (size_t i = 0; i < terms.size(); i++) {
        if (i < terms.size() - 1) {
            std::wstring search_word = boost::locale::conv::utf_to_utf<TCHAR>(terms[i]);
            Term* t = _CLNEW Term(_field_name.c_str(), search_word.c_str());
            _query.add(t);
            _CLLDECDELETE(t);
        } else {
            std::vector<CL_NS(index)::Term*> prefix_terms;
            get_prefix_terms(_searcher->getReader(), _field_name, terms[i], prefix_terms,
                             _gin_query_options->maxExpansions());
            if (prefix_terms.empty()) {
                std::wstring ws_term = boost::locale::conv::utf_to_utf<TCHAR>(terms[i]);
                Term* t = _CLNEW Term(_field_name.c_str(), ws_term.c_str());
                prefix_terms.push_back(t);
            }
            _query.add(prefix_terms);
            for (auto& t : prefix_terms) {
                _CLLDECDELETE(t);
            }
        }
    }

    _searcher->_search(&_query, [&result](const int32_t docid, const float_t /*score*/) { result.add(docid); });
    return Status::OK();
}

Status MatchRegexpOperator::_match_internal(roaring::Roaring& result) {
    hs_database_t* database = nullptr;
    hs_compile_error_t* compile_err = nullptr;
    hs_scratch_t* scratch = nullptr;

    if (hs_compile(_pattern.data(), HS_FLAG_DOTALL | HS_FLAG_ALLOWEMPTY | HS_FLAG_UTF8, HS_MODE_BLOCK, nullptr,
                   &database, &compile_err) != HS_SUCCESS) {
        LOG(ERROR) << "hyperscan compilation failed: " << compile_err->message;
        hs_free_compile_error(compile_err);
        return Status::InternalError(fmt::format("hyperscan compilation failed: {}", compile_err->message));
    }

    if (hs_alloc_scratch(database, &scratch) != HS_SUCCESS) {
        LOG(ERROR) << "hyperscan could not allocate scratch space.";
        hs_free_database(database);
        return Status::InternalError("hyperscan could not allocate scratch space.");
    }

    auto on_match = [](unsigned int id, unsigned long long from, unsigned long long to, unsigned int flags,
                       void* context) -> int {
        *static_cast<bool*>(context) = true;
        return 0;
    };

    Term* term = nullptr;
    TermEnum* enumerator = nullptr;
    std::vector<std::string> terms;

    try {
        int32_t count = 0;
        enumerator = _searcher->getReader()->terms();
        while (enumerator->next()) {
            term = enumerator->term();
            std::string input = lucene_wcstoutf8string(term->text(), term->textLength());

            bool is_match = false;
            if (hs_scan(database, input.data(), input.size(), 0, scratch, on_match, &is_match) != HS_SUCCESS) {
                LOG(ERROR) << "hyperscan match failed: " << input;
                break;
            }

            if (is_match) {
                if (_gin_query_options->maxExpansions() > 0 && count >= _gin_query_options->maxExpansions()) {
                    break;
                }

                terms.emplace_back(std::move(input));
                count++;
            }
            _CLDECDELETE(term);
        }
    }
    _CLFINALLY({
        _CLDECDELETE(term);
        enumerator->close();
        _CLDELETE(enumerator);

        hs_free_scratch(scratch);
        hs_free_database(database);
    })

    if (terms.empty()) {
        return Status::OK();
    }

    MatchAnyTermOperator query(_ctx);
    query.add_terms(terms);
    return query.match(result);
}

} // namespace starrocks