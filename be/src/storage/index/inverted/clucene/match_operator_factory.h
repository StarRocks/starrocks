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
#include "common/statusor.h"
#include "match_operator.h"

namespace starrocks {

class MatchOperatorFactory {
public:
    template <typename... Args>
    static StatusOr<std::unique_ptr<MatchOperator>> create(const InvertedIndexQueryType& query_type, Args&&... args) {
        switch (query_type) {
        case InvertedIndexQueryType::MATCH_ALL_QUERY:
        case InvertedIndexQueryType::EQUAL_QUERY:
            return std::make_unique<MatchTermOperator>(std::forward<Args>(args)...);
        case InvertedIndexQueryType::MATCH_PHRASE_QUERY:
            // in phrase query
            return std::make_unique<MatchPhraseOperator>(std::forward<Args>(args)...);
        case InvertedIndexQueryType::LESS_THAN_QUERY:
            return std::make_unique<MatchLessThanOperator>(std::forward<Args>(args)...);
        case InvertedIndexQueryType::LESS_EQUAL_QUERY:
            return std::make_unique<MatchLessThanOperator>(std::forward<Args>(args)...);
        case InvertedIndexQueryType::GREATER_THAN_QUERY:
            return std::make_unique<MatchGreatThanOperator>(std::forward<Args>(args)...);
        case InvertedIndexQueryType::GREATER_EQUAL_QUERY:
            return std::make_unique<MatchGreatThanOperator>(std::forward<Args>(args)...);
        case InvertedIndexQueryType::MATCH_WILDCARD_QUERY:
            return std::make_unique<MatchWildcardOperator>(std::forward<Args>(args)...);
        case InvertedIndexQueryType::MATCH_ANY_QUERY:
            return std::make_unique<MatchAnyTermOperator>(std::forward<Args>(args)...);
        case InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY:
            return std::make_unique<MatchPhraseEdgeOperator>(std::forward<Args>(args)...);
        case InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY:
            return std::make_unique<MatchPhrasePrefixOperator>(std::forward<Args>(args)...);
        case InvertedIndexQueryType::MATCH_REGEXP_QUERY:
            return std::make_unique<MatchRegexpOperator>(std::forward<Args>(args)...);
        default:
            return Status::InvalidArgument("Unknown query type");
        }
    }
};

} // namespace starrocks
