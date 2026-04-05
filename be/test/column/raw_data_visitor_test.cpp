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

#include "column/raw_data_visitor.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "column/adaptive_nullable_column.h"
#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/const_column.h"
#include "column/decimalv3_column.h"
#include "column/json_column.h"
#include "testutil/column_test_helper.h"
#include "types/datum.h"

namespace starrocks {

TEST(MutableRawDataVisitorTest, VisitInt32Column) {
    MutableRawDataVisitor visitor;
    auto col = ColumnTestHelper::build_column<int32_t>({42, 7});

    ASSERT_OK(col->accept_mutable(&visitor));

    const auto* data = reinterpret_cast<const int32_t*>(visitor.result());
    EXPECT_EQ(data[0], 42);
    EXPECT_EQ(data[1], 7);
}

TEST(MutableRawDataVisitorTest, VisitDecimal32Column) {
    MutableRawDataVisitor visitor;
    auto col = Decimal32Column::create(9, 2);
    col->append(int32_t{100});
    col->append(int32_t{200});

    ASSERT_OK(col->accept_mutable(&visitor));

    const auto* data = reinterpret_cast<const int32_t*>(visitor.result());
    EXPECT_EQ(data[0], 100);
    EXPECT_EQ(data[1], 200);
}

TEST(MutableRawDataVisitorTest, VisitArrayColumn) {
    MutableRawDataVisitor visitor;
    // Array of int32: [[42, 7]]
    auto elements = ColumnTestHelper::build_column<int32_t>({42, 7});
    auto offsets = ColumnTestHelper::build_column<uint32_t>({0, 2});
    auto col = ArrayColumn::create(std::move(elements), std::move(offsets));

    ASSERT_OK(col->accept_mutable(&visitor));

    // result points to the elements column's raw data
    const auto* data = reinterpret_cast<const int32_t*>(visitor.result());
    EXPECT_EQ(data[0], 42);
    EXPECT_EQ(data[1], 7);
}

TEST(MutableRawDataVisitorTest, VisitConstColumn) {
    MutableRawDataVisitor visitor;
    auto inner = ColumnTestHelper::build_column<int32_t>({42});
    auto col = ConstColumn::create(std::move(inner), 4);

    ASSERT_OK(col->accept_mutable(&visitor));
    ASSERT_NE(visitor.result(), nullptr);

    const auto* data = reinterpret_cast<const int32_t*>(visitor.result());
    EXPECT_EQ(data[0], 42);
}

TEST(MutableRawDataVisitorTest, VisitNullableColumn) {
    MutableRawDataVisitor visitor;
    auto col = ColumnTestHelper::build_nullable_column<int32_t>({42, 7});

    ASSERT_OK(col->accept_mutable(&visitor));
    ASSERT_NE(visitor.result(), nullptr);

    const auto* data = reinterpret_cast<const int32_t*>(visitor.result());
    EXPECT_EQ(data[0], 42);
    EXPECT_EQ(data[1], 7);
}

TEST(MutableRawDataVisitorTest, FallbackNotSupported) {
    MutableRawDataVisitor visitor;
    auto col = BinaryColumn::create();
    col->append(Slice("hello"));

    auto st = col->accept_mutable(&visitor);
    EXPECT_TRUE(st.is_not_supported());
    EXPECT_TRUE(st.message().find(col->get_name()) != std::string::npos);
}

TEST(MutableRawDataVisitorTest, VisitAdaptiveNullableColumn) {
    MutableRawDataVisitor visitor;
    auto col = AdaptiveNullableColumn::create(Int32Column::create(), NullColumn::create());
    col->append_datum(Datum(static_cast<int32_t>(42)));
    col->append_datum(Datum(static_cast<int32_t>(7)));

    ASSERT_OK(col->accept_mutable(&visitor));

    const auto* data = reinterpret_cast<const int32_t*>(visitor.result());
    EXPECT_EQ(data[0], 42);
    EXPECT_EQ(data[1], 7);
}

// ---- RawBytesVisitor tests ----

TEST(RawBytesVisitorTest, VisitInt32Column) {
    RawBytesVisitor visitor;
    auto col = ColumnTestHelper::build_column<int32_t>({42, 7});

    ASSERT_OK(col->accept(&visitor));

    const auto* data = reinterpret_cast<const int32_t*>(visitor.result());
    EXPECT_EQ(data[0], 42);
    EXPECT_EQ(data[1], 7);
}

TEST(RawBytesVisitorTest, VisitDecimal32Column) {
    RawBytesVisitor visitor;
    auto col = Decimal32Column::create(9, 2);
    col->append(int32_t{100});
    col->append(int32_t{200});

    ASSERT_OK(col->accept(&visitor));

    const auto* data = reinterpret_cast<const int32_t*>(visitor.result());
    EXPECT_EQ(data[0], 100);
    EXPECT_EQ(data[1], 200);
}

// Key test: for BinaryColumn, raw_bytes() returns the flat bytes buffer,
// NOT the Slice* cache that raw_data() returns.
TEST(RawBytesVisitorTest, VisitBinaryColumn) {
    RawBytesVisitor visitor;
    auto col = BinaryColumn::create();
    col->append(Slice("hello"));
    col->append(Slice("world"));

    ASSERT_OK(col->accept(&visitor));
    ASSERT_NE(visitor.result(), nullptr);

    // result points to the contiguous bytes buffer: "helloworld"
    const auto* bytes = visitor.result();
    EXPECT_EQ(memcmp(bytes, "hello", 5), 0);
    EXPECT_EQ(memcmp(bytes + 5, "world", 5), 0);

    // Confirm it is NOT the Slice cache (raw_data() pointer)
    EXPECT_NE(visitor.result(), col->raw_data());
}

// LargeBinaryColumn is BinaryColumnBase<uint64_t>; same flat-bytes semantics as BinaryColumn.
TEST(RawBytesVisitorTest, VisitLargeBinaryColumn) {
    RawBytesVisitor visitor;
    auto col = LargeBinaryColumn::create();
    col->append(Slice("hello"));
    col->append(Slice("world"));

    ASSERT_OK(col->accept(&visitor));
    ASSERT_NE(visitor.result(), nullptr);

    const auto* bytes = visitor.result();
    EXPECT_EQ(memcmp(bytes, "hello", 5), 0);
    EXPECT_EQ(memcmp(bytes + 5, "world", 5), 0);

    // Confirm it is NOT the Slice cache (raw_data() pointer)
    EXPECT_NE(visitor.result(), col->raw_data());
}

TEST(RawBytesVisitorTest, VisitNullableColumn) {
    RawBytesVisitor visitor;
    auto col = ColumnTestHelper::build_nullable_column<int32_t>({42, 7});

    ASSERT_OK(col->accept(&visitor));
    ASSERT_NE(visitor.result(), nullptr);

    const auto* data = reinterpret_cast<const int32_t*>(visitor.result());
    EXPECT_EQ(data[0], 42);
    EXPECT_EQ(data[1], 7);
}

TEST(RawBytesVisitorTest, VisitConstColumn) {
    RawBytesVisitor visitor;
    auto inner = ColumnTestHelper::build_column<int32_t>({42});
    auto col = ConstColumn::create(std::move(inner), 4);

    ASSERT_OK(col->accept(&visitor));
    ASSERT_NE(visitor.result(), nullptr);

    const auto* data = reinterpret_cast<const int32_t*>(visitor.result());
    EXPECT_EQ(data[0], 42);
}

TEST(RawBytesVisitorTest, VisitArrayColumn) {
    RawBytesVisitor visitor;
    // Array of int32: [[42, 7]]
    auto elements = ColumnTestHelper::build_column<int32_t>({42, 7});
    auto offsets = ColumnTestHelper::build_column<uint32_t>({0, 2});
    auto col = ArrayColumn::create(std::move(elements), std::move(offsets));

    ASSERT_OK(col->accept(&visitor));

    // result points to the elements column's raw data
    const auto* data = reinterpret_cast<const int32_t*>(visitor.result());
    EXPECT_EQ(data[0], 42);
    EXPECT_EQ(data[1], 7);
}

TEST(RawBytesVisitorTest, VisitAdaptiveNullableColumn) {
    RawBytesVisitor visitor;
    auto col = AdaptiveNullableColumn::create(Int32Column::create(), NullColumn::create());
    col->append_datum(Datum(static_cast<int32_t>(42)));
    col->append_datum(Datum(static_cast<int32_t>(7)));

    ASSERT_OK(col->accept(&visitor));

    const auto* data = reinterpret_cast<const int32_t*>(visitor.result());
    EXPECT_EQ(data[0], 42);
    EXPECT_EQ(data[1], 7);
}

TEST(RawBytesVisitorTest, FallbackNotSupported) {
    RawBytesVisitor visitor;
    auto col = JsonColumn::create();

    auto st = col->accept(&visitor);
    EXPECT_TRUE(st.is_not_supported());
    EXPECT_TRUE(st.message().find(col->get_name()) != std::string::npos);
}

} // namespace starrocks