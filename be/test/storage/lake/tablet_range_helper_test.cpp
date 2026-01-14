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

#include "storage/lake/tablet_range_helper.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "runtime/types.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"

namespace starrocks::lake {

TEST(TabletRangeHelperTest, test_create_sst_seek_range_from) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(PRIMARY_KEYS);

    // c0, c1 are keys
    auto c0 = schema_pb.add_column();
    c0->set_name("c0");
    c0->set_type("INT");
    c0->set_is_key(true);
    c0->set_is_nullable(false);

    auto c1 = schema_pb.add_column();
    c1->set_name("c1");
    c1->set_type("INT");
    c1->set_is_key(true);
    c1->set_is_nullable(false);

    auto c2 = schema_pb.add_column();
    c2->set_name("c2");
    c2->set_type("INT");
    c2->set_is_key(false);
    c2->set_is_nullable(true);

    // Case 1: sort keys are the same as pk keys (c0, c1) -> index (0, 1)
    schema_pb.clear_sort_key_idxes();
    schema_pb.add_sort_key_idxes(0);
    schema_pb.add_sort_key_idxes(1);

    auto tablet_schema = TabletSchema::create(schema_pb);

    TabletRangePB range_pb;
    auto lower = range_pb.mutable_lower_bound();
    auto v0 = lower->add_values();
    TypeDescriptor type_int(TYPE_INT);
    v0->mutable_type()->CopyFrom(type_int.to_protobuf());
    v0->set_value("1");
    auto v1 = lower->add_values();
    v1->mutable_type()->CopyFrom(type_int.to_protobuf());
    v1->set_value("2");
    range_pb.set_lower_bound_included(true);

    auto res = TabletRangeHelper::create_sst_seek_range_from(range_pb, tablet_schema);
    ASSERT_OK(res.status());
    ASSERT_FALSE(res.value().seek_key.empty());

    // Case 2: different order -> Should return InternalError
    schema_pb.clear_sort_key_idxes();
    schema_pb.add_sort_key_idxes(1);
    schema_pb.add_sort_key_idxes(0);
    auto tablet_schema_wrong = TabletSchema::create(schema_pb);
    auto res2 = TabletRangeHelper::create_sst_seek_range_from(range_pb, tablet_schema_wrong);
    ASSERT_FALSE(res2.ok());
    ASSERT_TRUE(res2.status().is_internal_error());
    ASSERT_THAT(res2.status().to_string(), testing::HasSubstr("Sort key index 0 must be 0, but is 1"));
}

} // namespace starrocks::lake
