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

#include <memory>
#include <string>
#include <vector>

#include "column/binary_column.h"
#include "column/decimalv3_column.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "column/type_converter.h"
#include "types/json_value.h"
#include "types/type_info.h"

namespace starrocks {

class TypeConverterTestAllocator {
public:
    TypeInfoAllocator make_allocator() { return TypeInfoAllocator{this, allocate}; }

private:
    static uint8_t* allocate(void* ctx, size_t size) {
        auto* self = static_cast<TypeConverterTestAllocator*>(ctx);
        auto buffer = std::make_unique<uint8_t[]>(size);
        uint8_t* result = buffer.get();
        self->_buffers.emplace_back(std::move(buffer));
        return result;
    }

    std::vector<std::unique_ptr<uint8_t[]>> _buffers;
};

TEST(TypeConverterCoreTest, NullableIntToVarcharUsesAllocator) {
    TypeConverterTestAllocator allocator_holder;
    TypeInfoAllocator allocator = allocator_holder.make_allocator();

    auto src = NullableColumn::create(Int32Column::create(), NullColumn::create());
    src->append_datum(Datum(static_cast<int32_t>(123)));
    src->append_nulls(1);

    auto dst = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    auto src_type = get_type_info(TYPE_INT);
    auto dst_type = get_type_info(TYPE_VARCHAR);
    const TypeConverter* converter = get_type_converter(TYPE_INT, TYPE_VARCHAR);
    ASSERT_NE(nullptr, converter);

    auto status = converter->convert_column(src_type.get(), *src, dst_type.get(), dst.get(), &allocator);
    ASSERT_TRUE(status.ok()) << status.to_string();

    EXPECT_EQ("123", dst->get(0).get_slice().to_string());
    EXPECT_TRUE(dst->get(1).is_null());
}

TEST(TypeConverterCoreTest, NullableVarcharToInt) {
    TypeConverterTestAllocator allocator_holder;
    TypeInfoAllocator allocator = allocator_holder.make_allocator();

    auto src = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    src->append_datum(Datum(Slice("456")));
    src->append_nulls(1);

    auto dst = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto src_type = get_type_info(TYPE_VARCHAR);
    auto dst_type = get_type_info(TYPE_INT);
    const TypeConverter* converter = get_type_converter(TYPE_VARCHAR, TYPE_INT);
    ASSERT_NE(nullptr, converter);

    auto status = converter->convert_column(src_type.get(), *src, dst_type.get(), dst.get(), &allocator);
    ASSERT_TRUE(status.ok()) << status.to_string();

    EXPECT_EQ(456, dst->get(0).get_int32());
    EXPECT_TRUE(dst->get(1).is_null());
}

TEST(TypeConverterCoreTest, VarcharJsonRoundTrip) {
    TypeConverterTestAllocator allocator_holder;
    TypeInfoAllocator allocator = allocator_holder.make_allocator();

    auto varchar_type = get_type_info(TYPE_VARCHAR);
    auto json_type = get_type_info(TYPE_JSON);

    auto json_dst = JsonColumn::create();
    auto string_src = BinaryColumn::create();
    string_src->append(R"({"a": 1})");
    const TypeConverter* to_json = get_type_converter(TYPE_VARCHAR, TYPE_JSON);
    ASSERT_NE(nullptr, to_json);
    auto status = to_json->convert_column(varchar_type.get(), *string_src, json_type.get(), json_dst.get(), &allocator);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(R"({"a": 1})", json_dst->get(0).get_json()->to_string_uncheck());

    auto string_dst = BinaryColumn::create();
    const TypeConverter* to_varchar = get_type_converter(TYPE_JSON, TYPE_VARCHAR);
    ASSERT_NE(nullptr, to_varchar);
    status = to_varchar->convert_column(json_type.get(), *json_dst, varchar_type.get(), string_dst.get(), &allocator);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(R"({"a": 1})", string_dst->get_slice(0).to_string());
}

TEST(TypeConverterCoreTest, DecimalV3WideningConversion) {
    auto src = Decimal32Column::create(9, 2, 0);
    src->append_datum(Datum(static_cast<int32_t>(12345)));

    auto dst = Decimal64Column::create(18, 2, 0);
    auto src_type = get_type_info(TYPE_DECIMAL32, 9, 2);
    auto dst_type = get_type_info(TYPE_DECIMAL64, 18, 2);
    const TypeConverter* converter = get_type_converter(TYPE_DECIMAL32, TYPE_DECIMAL64);
    ASSERT_NE(nullptr, converter);

    auto status = converter->convert_column(src_type.get(), *src, dst_type.get(), dst.get(), nullptr);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(12345, dst->get(0).get_int64());
}

} // namespace starrocks
