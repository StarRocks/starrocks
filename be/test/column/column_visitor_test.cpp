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

} // namespace starrocks
