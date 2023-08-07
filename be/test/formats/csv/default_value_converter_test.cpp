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

#include "column/column_helper.h"
#include "formats/csv/converter.h"
#include "runtime/types.h"

namespace starrocks::csv {

TEST(DefaultValueConverterTest, test_hive_array_nested_with_map) {
    // ARRAY<TINYINT>
    TypeDescriptor t(TYPE_ARRAY);
    t.children.emplace_back(TYPE_MAP);
    t.children[0].children.emplace_back(TYPE_INT);
    t.children[0].children.emplace_back(TYPE_INT);

    auto conv = csv::get_hive_converter(t, true);
    auto col = ColumnHelper::create_column(t, true);

    Converter::Options opts;
    opts.invalid_field_as_null = true;

    EXPECT_TRUE(conv->read_string(col.get(), "[MAP]", opts));
    EXPECT_TRUE(col->get(0).is_null());

    opts.invalid_field_as_null = false;
    EXPECT_FALSE(conv->read_string(col.get(), "[MAP]", opts));
}

TEST(DefaultValueConverterTest, test_array_nested_with_map) {
    // ARRAY<TINYINT>
    TypeDescriptor t(TYPE_ARRAY);
    t.children.emplace_back(TYPE_MAP);
    t.children[0].children.emplace_back(TYPE_INT);
    t.children[0].children.emplace_back(TYPE_INT);

    auto conv = csv::get_converter(t, true);
    EXPECT_TRUE(nullptr != conv);
}

} // namespace starrocks::csv