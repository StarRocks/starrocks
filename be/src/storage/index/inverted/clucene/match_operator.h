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
#include <utility>

#include "CLucene.h"
#include "common/status.h"
#include "roaring/roaring.hh"
#include "storage/index/inverted/inverted_index_common.h"

namespace starrocks {

class InvertedIndexCtx;

// MatchOperator is the base operator which wraps index search operations
// and it would be the minimum cache unit in the searching of inverted index.
class MatchOperator {
public:
    MatchOperator(InvertedIndexCtx* ctx, lucene::search::IndexSearcher* searcher, lucene::store::Directory* dir,
                  std::wstring field_name)
            : _inverted_index_ctx(ctx), _searcher(searcher), _dir(dir), _field_name(std::move(field_name)) {}
    virtual ~MatchOperator() = default;
    Status match(roaring::Roaring& result);

protected:
    virtual Status _match_internal(roaring::Roaring& result) = 0;

    InvertedIndexCtx* _inverted_index_ctx;
    lucene::search::IndexSearcher* _searcher;
    lucene::store::Directory* _dir;
    std::wstring _field_name;
};

class MatchTermOperator : public MatchOperator {
public:
    MatchTermOperator(InvertedIndexCtx* ctx, lucene::search::IndexSearcher* searcher, lucene::store::Directory* dir,
                      std::wstring field_name, std::string search_str)
            : MatchOperator(ctx, searcher, dir, std::move(field_name)), _search_str(std::move(search_str)) {}

protected:
    std::string _search_str;

    Status _match_internal(roaring::Roaring& result) override;
};

class MatchRangeOperator : public MatchOperator {
public:
    MatchRangeOperator(InvertedIndexCtx* ctx, lucene::search::IndexSearcher* searcher, lucene::store::Directory* dir,
                       std::wstring field_name, std::string bound, bool inclusive)
            : MatchOperator(ctx, searcher, dir, std::move(field_name)),
              _bound(std::move(bound)),
              _inclusive(inclusive) {}

protected:
    virtual std::unique_ptr<lucene::search::RangeQuery> create_query(lucene::index::Term* term) = 0;
    Status _match_internal(roaring::Roaring& result) override;

    std::string _bound;
    bool _inclusive;
};

class MatchGreatThanOperator final : public MatchRangeOperator {
public:
    MatchGreatThanOperator(InvertedIndexCtx* ctx, lucene::search::IndexSearcher* searcher,
                           lucene::store::Directory* dir, std::wstring field_name, std::string bound, bool inclusive)
            : MatchRangeOperator(ctx, searcher, dir, std::move(field_name), std::move(bound), inclusive) {}

protected:
    std::unique_ptr<lucene::search::RangeQuery> create_query(lucene::index::Term* term) override {
        return std::make_unique<lucene::search::RangeQuery>(term, nullptr, _inclusive);
    }
};

class MatchLessThanOperator final : public MatchRangeOperator {
public:
    MatchLessThanOperator(InvertedIndexCtx* ctx, lucene::search::IndexSearcher* searcher, lucene::store::Directory* dir,
                          std::wstring field_name, std::string bound, bool inclusive)
            : MatchRangeOperator(ctx, searcher, dir, std::move(field_name), std::move(bound), inclusive) {}

protected:
    std::unique_ptr<lucene::search::RangeQuery> create_query(lucene::index::Term* term) override {
        return std::make_unique<lucene::search::RangeQuery>(nullptr, term, _inclusive);
    }
};

class MatchWildcardOperator final : public MatchOperator {
public:
    MatchWildcardOperator(InvertedIndexCtx* ctx, lucene::search::IndexSearcher* searcher, lucene::store::Directory* dir,
                          std::wstring field_name, std::string wildard)
            : MatchOperator(ctx, searcher, dir, std::move(field_name)), _wildcard(std::move(wildard)) {}

protected:
    Status _match_internal(roaring::Roaring& result) override;

private:
    std::string _wildcard;
};

class MatchPhraseOperator final : public MatchOperator {
public:
    MatchPhraseOperator(InvertedIndexCtx* ctx, lucene::search::IndexSearcher* searcher, lucene::store::Directory* dir,
                        std::wstring field_name, std::string query_str, int slop, InvertedIndexParserType parser_type)
            : MatchOperator(ctx, searcher, dir, std::move(field_name)),
              _query_str(std::move(query_str)),
              _slop(slop),
              _parser_type(parser_type) {}

protected:
    Status _match_internal(roaring::Roaring& result) override;

private:
    std::string _query_str;
    int _slop;
    InvertedIndexParserType _parser_type;
};

} // namespace starrocks