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

#include "column/nullable_column.h"
#include "column/struct_column.h"

namespace starrocks {

TEST(StructColumnCoreTest, SerializeAndSelectiveAppend) {
    MutableColumns fields;
    fields.emplace_back(NullableColumn::create(Int32Column::create(), NullColumn::create()));
    fields.emplace_back(NullableColumn::create(BinaryColumn::create(), NullColumn::create()));

    auto col = StructColumn::create(std::move(fields), {"id", "name"});
    col->append_datum(DatumStruct{Datum(int32_t(1)), Datum(Slice("a"))});
    col->append_datum(DatumStruct{Datum(int32_t(2)), Datum(Slice("b"))});

    ASSERT_EQ(2, col->size());

    std::vector<uint8_t> encoded(col->serialize_size(0));
    col->serialize(0, encoded.data());

    auto restored_holder = col->clone_empty();
    auto* restored = down_cast<StructColumn*>(restored_holder.get());
    restored->deserialize_and_append(encoded.data());

    ASSERT_EQ(1, restored->size());
    EXPECT_EQ("{id:1,name:'a'}", restored->debug_item(0));

    auto dst_holder = col->clone_empty();
    auto* dst = down_cast<StructColumn*>(dst_holder.get());
    const uint32_t indexes[] = {1, 0};
    dst->append_selective(*col, indexes, 0, 2);

    ASSERT_EQ(2, dst->size());
    EXPECT_EQ("{id:2,name:'b'}", dst->debug_item(0));
    EXPECT_EQ("{id:1,name:'a'}", dst->debug_item(1));
}

} // namespace starrocks
