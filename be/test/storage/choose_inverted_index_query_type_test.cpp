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

#include <gtest/gtest.h>

#include "storage/column_expr_predicate.h"

namespace starrocks {

// Unit-tests the extracted query-type decision used by inverted-index pushdown,
// including the new fallback (std::nullopt) for LIKE patterns containing '_'.
TEST(ChooseInvertedIndexQueryTypeTest, all_branches) {
    using QT = InvertedIndexQueryType;
    // For valid_like cases valid_match is false, so `op` is ignored; pass any value.
    const auto IGN = TExprOpcode::MATCH_ANY;

    // A MATCH_ANY match query maps to MATCH_ANY_QUERY.
    EXPECT_EQ(QT::MATCH_ANY_QUERY, choose_inverted_index_query_type(false, true, TExprOpcode::MATCH_ANY, "x").value());

    // LIKE patterns containing '_' are not pushed down (the fix); they fall back to the LIKE engine.
    EXPECT_FALSE(choose_inverted_index_query_type(true, false, IGN, "foo_bar").has_value());
    EXPECT_FALSE(choose_inverted_index_query_type(true, false, IGN, "中_文").has_value());
    EXPECT_FALSE(choose_inverted_index_query_type(true, false, IGN, "a_b%c").has_value());

    // A '%' or '*' (with no '_') still pushes down as a wildcard query on this branch.
    EXPECT_EQ(QT::MATCH_WILDCARD_QUERY, choose_inverted_index_query_type(true, false, IGN, "foo%bar").value());
    EXPECT_EQ(QT::MATCH_WILDCARD_QUERY, choose_inverted_index_query_type(true, false, IGN, "foo*bar").value());
    // No wildcard and no '_' is an exact-match query.
    EXPECT_EQ(QT::EQUAL_QUERY, choose_inverted_index_query_type(true, false, IGN, "foobar").value());
}

} // namespace starrocks
