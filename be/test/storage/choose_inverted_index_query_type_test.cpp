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

    // LIKE patterns containing '_' are not pushed down (the fix); they fall back to the LIKE engine.
    EXPECT_FALSE(choose_inverted_index_query_type(true, "foo_bar").has_value());
    EXPECT_FALSE(choose_inverted_index_query_type(true, "中_文").has_value());
    EXPECT_FALSE(choose_inverted_index_query_type(true, "a_b%c").has_value());

    // A '%' or '*' (with no '_') still pushes down as a wildcard query on this branch.
    EXPECT_EQ(QT::MATCH_WILDCARD_QUERY, choose_inverted_index_query_type(true, "foo%bar").value());
    EXPECT_EQ(QT::MATCH_WILDCARD_QUERY, choose_inverted_index_query_type(false, "foo*bar").value());
    // No wildcard and no '_' is an exact-match query.
    EXPECT_EQ(QT::EQUAL_QUERY, choose_inverted_index_query_type(true, "foobar").value());
}

} // namespace starrocks
