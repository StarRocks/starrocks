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
#include <CLucene/search/MultiPhraseQuery.h>
#include <CLucene/search/query/TermIterator.h>
#include <CLucene/search/query/TermPositionIterator.h>

#include <utility>
#include <variant>

#include "common/status.h"
#include "roaring/roaring.hh"
#include "storage/index/inverted/inverted_index_common.h"

namespace starrocks {

class GinQueryOptions;

struct MatchOperatorContext {
    GinQueryOptions* gin_query_options = nullptr;
    lucene::search::IndexSearcher* searcher = nullptr;
    std::string column_name;
    std::string search_str;
    bool inclusive = false;
};

struct InvertedIndexQueryInfo {
    std::vector<std::string> terms;
    std::vector<std::vector<std::string>> additional_terms;
    int32_t slop = 0;
    bool ordered = false;

    std::string to_string() {
        std::string s;
        s += std::to_string(terms.size()) + ", ";
        s += std::to_string(additional_terms.size()) + ", ";
        s += std::to_string(slop) + ", ";
        s += std::to_string(ordered);
        return s;
    }
};

class PostingsAndPosition {
public:
    PostingsAndPosition(const TermPositionIterator& postings, int32_t offset) : _postings(postings), _offset(offset) {}

    TermPositionIterator _postings;
    int32_t _offset = 0;
    int32_t _freq = 0;
    int32_t _upTo = 0;
    int32_t _pos = 0;
};

template <typename Derived>
class PhraseMatcherBase {
public:
    // Handle position information for different types of phrase queries
    bool matches(int32_t doc);

private:
    void reset(int32_t doc);

protected:
    bool advance_position(PostingsAndPosition& posting, int32_t target);

public:
    std::vector<PostingsAndPosition> _postings;
};

class ExactPhraseMatcher : public PhraseMatcherBase<ExactPhraseMatcher> {
public:
    bool next_match();
};

class OrderedSloppyPhraseMatcher : public PhraseMatcherBase<OrderedSloppyPhraseMatcher> {
public:
    bool next_match();

private:
    bool stretch_to_order(PostingsAndPosition* prev_posting);

public:
    int32_t _allowed_slop = 0;

private:
    int32_t _match_width = -1;
};

using Matcher = std::variant<ExactPhraseMatcher, OrderedSloppyPhraseMatcher>;

// MatchOperator is the base operator which wraps index search operations
// and it would be the minimum cache unit in the searching of inverted index.
class MatchOperator {
public:
    explicit MatchOperator(MatchOperatorContext* ctx)
            : _ctx(ctx),
              _gin_query_options(ctx->gin_query_options),
              _searcher(ctx->searcher),
              _field_name(std::wstring(ctx->column_name.begin(), ctx->column_name.end())) {}
    virtual ~MatchOperator() = default;
    Status match(roaring::Roaring& result);

protected:
    virtual Status _match_internal(roaring::Roaring& result) = 0;

    static void parser_slop(std::string& query, InvertedIndexQueryInfo& query_info);
    static Status parser_info(std::string& query, const std::wstring& field_name,
                              const InvertedIndexParserType& parser_type, const InvertedIndexQueryType& query_type,
                              InvertedIndexQueryInfo& query_info, const bool& sequential_opt);

    MatchOperatorContext* _ctx;
    GinQueryOptions* _gin_query_options;
    lucene::search::IndexSearcher* _searcher;
    std::wstring _field_name;
};

class MatchTermOperator : public MatchOperator {
public:
    explicit MatchTermOperator(MatchOperatorContext* ctx)
            : MatchOperator(ctx), _search_str(std::move(ctx->search_str)) {}

    void add_terms(std::vector<std::string> terms);

protected:
    Status _match_internal(roaring::Roaring& result) override;

    virtual void _filter(const TermIterator& term_docs, const bool& first, roaring::Roaring& result);

private:
    std::string _search_str;
    std::vector<std::string> _terms{};
};

class MatchRangeOperator : public MatchOperator {
public:
    explicit MatchRangeOperator(MatchOperatorContext* ctx)
            : MatchOperator(ctx), _bound(std::move(ctx->search_str)), _inclusive(ctx->inclusive) {}

protected:
    virtual std::unique_ptr<lucene::search::RangeQuery> create_query(lucene::index::Term* term) = 0;
    Status _match_internal(roaring::Roaring& result) override;

    std::string _bound;
    bool _inclusive;
};

class MatchGreatThanOperator final : public MatchRangeOperator {
public:
    explicit MatchGreatThanOperator(MatchOperatorContext* ctx) : MatchRangeOperator(ctx) {}

protected:
    std::unique_ptr<lucene::search::RangeQuery> create_query(lucene::index::Term* term) override {
        return std::make_unique<lucene::search::RangeQuery>(term, nullptr, _inclusive);
    }
};

class MatchLessThanOperator final : public MatchRangeOperator {
public:
    explicit MatchLessThanOperator(MatchOperatorContext* ctx) : MatchRangeOperator(ctx) {}

protected:
    std::unique_ptr<lucene::search::RangeQuery> create_query(Term* term) override {
        return std::make_unique<lucene::search::RangeQuery>(nullptr, term, _inclusive);
    }
};

class MatchWildcardOperator final : public MatchOperator {
public:
    explicit MatchWildcardOperator(MatchOperatorContext* ctx)
            : MatchOperator(ctx), _wildcard(std::move(ctx->search_str)) {}

protected:
    Status _match_internal(roaring::Roaring& result) override;

private:
    std::string _wildcard;
};

class MatchPhraseOperator final : public MatchOperator {
public:
    explicit MatchPhraseOperator(MatchOperatorContext* ctx)
            : MatchOperator(ctx), _search_str(std::move(ctx->search_str)) {}
    ~MatchPhraseOperator() override;

protected:
    Status _match_internal(roaring::Roaring& result) override;

private:
    void add(const std::vector<std::string>& terms);

    // Use bitmap for merging inverted lists
    Status search_by_bitmap(roaring::Roaring& roaring) const;
    // Use skiplist for merging inverted lists
    Status search_by_skiplist(roaring::Roaring& roaring);

    int32_t do_next(int32_t doc) const;
    bool matches(int32_t doc);

    std::string _search_str;
    int _slop = 0;

    TermIterator _lead1;
    TermIterator _lead2;
    std::vector<TermIterator> _others;

    std::vector<PostingsAndPosition> _postings;

    std::vector<Term*> _terms;
    std::vector<TermDocs*> _term_docs;

    std::vector<std::vector<std::string>> _additional_terms;
    std::unique_ptr<lucene::search::PhraseQuery> _phrase_query = nullptr;
    std::vector<Matcher> _matchers;
};

class MatchAnyTermOperator final : public MatchTermOperator {
public:
    explicit MatchAnyTermOperator(MatchOperatorContext* ctx) : MatchTermOperator(ctx) {}

protected:
    void _filter(const TermIterator& term_docs, const bool& first, roaring::Roaring& result) override;
};

class MatchPhraseEdgeOperator final : public MatchOperator {
public:
    explicit MatchPhraseEdgeOperator(MatchOperatorContext* ctx) : MatchOperator(ctx), _search_str(ctx->search_str) {}

protected:
    Status _match_internal(roaring::Roaring& result) override;

private:
    Status search_one_term(const std::vector<std::string>& terms, roaring::Roaring& roaring) const;
    Status search_multi_term(const std::vector<std::string>& terms, roaring::Roaring& roaring) const;

    void find_words(const std::function<void(Term*)>& cb) const;
    static void add_default_term(lucene::search::MultiPhraseQuery* query, const std::wstring& field_name,
                                 const std::wstring& ws_term);
    static void handle_terms(lucene::search::MultiPhraseQuery* query, const std::wstring& field_name,
                             const std::wstring& ws_term, std::vector<Term*>& checked_terms);

    std::string _search_str;
};

class MatchPhrasePrefixOperator final : public MatchOperator {
public:
    explicit MatchPhrasePrefixOperator(MatchOperatorContext* ctx) : MatchOperator(ctx), _search_str(ctx->search_str) {}

protected:
    Status _match_internal(roaring::Roaring& result) override;

private:
    static void get_prefix_terms(IndexReader* reader, const std::wstring& field_name, const std::string& prefix,
                                 std::vector<CL_NS(index)::Term*>& prefix_terms, int32_t max_expansions);

    std::string _search_str;
};

class MatchRegexpOperator final : public MatchOperator {
public:
    explicit MatchRegexpOperator(MatchOperatorContext* ctx)
            : MatchOperator(ctx), _ctx(ctx), _pattern(ctx->search_str) {}

protected:
    Status _match_internal(roaring::Roaring& result) override;

private:
    MatchOperatorContext* _ctx;
    std::string _pattern;
};

} // namespace starrocks