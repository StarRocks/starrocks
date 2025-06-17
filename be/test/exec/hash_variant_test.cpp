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

#include "exec/aggregate/agg_hash_variant.h"
#include "types/logical_type.h"

namespace starrocks {
TEST(HashVariantResolverTest, unary_assert) {
#define TYPE_RESULT(phase, type, nullable) \
    HashVariantResolver<AggHashMapVariant>::instance().get_unary_type(phase, type, nullable)

    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_TINYINT, true), AggHashMapVariant::Type::phase1_null_int8);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_SMALLINT, true), AggHashMapVariant::Type::phase1_null_int16);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_INT, true), AggHashMapVariant::Type::phase1_null_int32);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_BIGINT, true), AggHashMapVariant::Type::phase1_null_int64);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_LARGEINT, true), AggHashMapVariant::Type::phase1_null_int128);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_VARCHAR, true), AggHashMapVariant::Type::phase1_null_string);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_DATE, true), AggHashMapVariant::Type::phase1_null_date);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_DATETIME, true), AggHashMapVariant::Type::phase1_null_timestamp);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_DECIMAL32, true), AggHashMapVariant::Type::phase1_null_decimal32);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_DECIMAL64, true), AggHashMapVariant::Type::phase1_null_decimal64);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_DECIMAL128, true), AggHashMapVariant::Type::phase1_null_decimal128);

    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_TINYINT, false), AggHashMapVariant::Type::phase1_int8);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_SMALLINT, false), AggHashMapVariant::Type::phase1_int16);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_INT, false), AggHashMapVariant::Type::phase1_int32);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_BIGINT, false), AggHashMapVariant::Type::phase1_int64);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_LARGEINT, false), AggHashMapVariant::Type::phase1_int128);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_VARCHAR, false), AggHashMapVariant::Type::phase1_string);

    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_FLOAT, true), AggHashMapVariant::Type::phase1_slice);
    EXPECT_EQ(TYPE_RESULT(AggrPhase2, TYPE_FLOAT, true), AggHashMapVariant::Type::phase2_slice);
}

} // namespace starrocks