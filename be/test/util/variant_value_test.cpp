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

#include "util/variant_value.h"


#include <gtest/gtest.h>
namespace starrocks {

class VariantValueTest : public ::testing::Test {

};

TEST_F(VariantValueTest, NullToJson) {
    uint8_t null_chars[] = {static_cast<uint8_t>(VariantPrimitiveType::NULL_TYPE) << 2};
    std::string_view null_value(reinterpret_cast<const char*>(null_chars), 1);
    VariantValue v(VariantMetadata::kEmptyMetadata, null_value);
    auto json = v.to_json();
    ASSERT_TRUE(json.ok());
    EXPECT_EQ("null", *json);
}

}