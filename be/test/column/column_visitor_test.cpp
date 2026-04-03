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

#include "column/column_visitor.h"

#include <gtest/gtest.h>

#include "column/adaptive_nullable_column.h"
#include "column/column_visitor_adapter.h"
#include "column/column_visitor_mutable.h"

namespace starrocks {

TEST(ColumnVisitorTest, ConstructAndDestruct) {
    ColumnVisitor visitor;
    ColumnVisitorMutable mutable_visitor;
    EXPECT_NE(&visitor, nullptr);
    EXPECT_NE(&mutable_visitor, nullptr);
}

TEST(ColumnVisitorTest, ResolveVisitorMethodPointers) {
    using VisitConstInt8 = Status (ColumnVisitor::*)(const Int8Column&);
    using VisitConstInt32 = Status (ColumnVisitor::*)(const Int32Column&);
    using VisitMutInt8 = Status (ColumnVisitorMutable::*)(Int8Column*);
    using VisitMutInt32 = Status (ColumnVisitorMutable::*)(Int32Column*);

    auto const_int8 = static_cast<VisitConstInt8>(&ColumnVisitor::visit);
    auto const_int32 = static_cast<VisitConstInt32>(&ColumnVisitor::visit);
    auto mut_int8 = static_cast<VisitMutInt8>(&ColumnVisitorMutable::visit);
    auto mut_int32 = static_cast<VisitMutInt32>(&ColumnVisitorMutable::visit);

    EXPECT_NE(const_int8, nullptr);
    EXPECT_NE(const_int32, nullptr);
    EXPECT_NE(mut_int8, nullptr);
    EXPECT_NE(mut_int32, nullptr);
}

class DemoVisitor final : public ColumnVisitorAdapter<DemoVisitor> {
public:
    DemoVisitor() : ColumnVisitorAdapter(this) {}

    // Catch-all for column types we don't care about in this test.
    template <typename T>
    static Status do_visit(const T&) {
        return Status::NotSupported("unexpected column type");
    }

    Status do_visit(const AdaptiveNullableColumn&) {
        visited_adaptive = true;
        return Status::OK();
    }

    bool result() const { return visited_adaptive; }

private:
    bool visited_adaptive = false;
};

class DemoMutableVisitor final : public ColumnVisitorMutableAdapter<DemoMutableVisitor> {
public:
    DemoMutableVisitor() : ColumnVisitorMutableAdapter(this) {}

    // Catch-all for column types we don't care about in this test.
    template <typename T>
    static Status do_visit(T*) {
        return Status::NotSupported("unexpected column type");
    }

    Status do_visit(AdaptiveNullableColumn* col) {
        col->append_nulls(1);
        visited_adaptive = true;
        return Status::OK();
    }

    bool result() const { return visited_adaptive; }

private:
    bool visited_adaptive = false;
};

TEST(ColumnVisitorAdapterTest, VisitAdaptiveNullableColumnMutable) {
    DemoMutableVisitor visitor;

    auto col = AdaptiveNullableColumn::create(Int32Column::create(), NullColumn::create());
    col->append_nulls(2);
    size_t size_before = col->size();

    ASSERT_TRUE(col->accept_mutable(&visitor).ok());
    EXPECT_TRUE(visitor.result());
    EXPECT_EQ(col->size(), size_before + 1);
}

TEST(ColumnVisitorAdapterTest, VisitAdaptiveNullableColumn) {
    DemoVisitor visitor;

    auto col = AdaptiveNullableColumn::create(Int32Column::create(), NullColumn::create());
    col->append_nulls(3);

    ASSERT_TRUE(col->accept(&visitor).ok());
    EXPECT_TRUE(visitor.result());
}

} // namespace starrocks
