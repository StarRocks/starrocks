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

#include "storage/delete_predicates.h"

#include <gtest/gtest.h>

#include "storage/column_predicate.h"
#include "storage/conjunctive_predicates.h"

namespace starrocks {

using PredicatePtr = std::unique_ptr<ColumnPredicate>;

TEST(DeletePredicatesTest, test_add_empty_preds) {
    DeletePredicates delete_predicates;
    EXPECT_TRUE(delete_predicates.get_predicates(0).empty());

    {
        ConjunctivePredicates conjuncts;
        delete_predicates.add(1, conjuncts);
        // Nothing added
        EXPECT_TRUE(delete_predicates.get_predicates(0).empty());
    }
    {
        PredicatePtr p0(new_column_null_predicate(get_type_info(TYPE_INT), 0, false));
        ConjunctivePredicates conjuncts;

        conjuncts.add(p0.get());
        delete_predicates.add(1, conjuncts);

        auto dis_delete_predicates = delete_predicates.get_predicates(0);
        auto& conjuncts_arr = dis_delete_predicates.predicate_list();
        EXPECT_EQ(1U, conjuncts_arr.size());
        EXPECT_EQ(1U, conjuncts_arr[0].vec_preds().size());
        EXPECT_EQ(p0.get(), conjuncts_arr[0].vec_preds()[0]);
    }
}

} // namespace starrocks
