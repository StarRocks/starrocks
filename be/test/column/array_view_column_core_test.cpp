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

#include "column/array_column.h"
#include "column/array_view_column.h"

namespace starrocks {

TEST(ArrayViewColumnCoreTest, RoundTripAndCompare) {
    auto array_col = ArrayColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                         UInt32Column::create());
    array_col->append_datum(DatumArray{Datum(int32_t(1)), Datum(int32_t(2))});
    array_col->append_datum(DatumArray{Datum(int32_t(3))});

    auto view_col = ArrayViewColumn::from_array_column(array_col);
    ASSERT_TRUE(view_col->is_array_view());
    ASSERT_EQ(2, view_col->size());

    auto restored = ArrayViewColumn::to_array_column(view_col);
    ASSERT_TRUE(restored->is_array());
    EXPECT_EQ("[[1,2], [3]]", restored->debug_string());

    auto* view = down_cast<ArrayViewColumn*>(view_col.get());
    EXPECT_EQ(0, view->compare_at(0, 0, *view_col, 1));
    EXPECT_LT(view->compare_at(0, 1, *view_col, 1), 0);
}

} // namespace starrocks
