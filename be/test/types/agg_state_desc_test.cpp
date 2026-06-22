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

#include "types/agg_state_desc.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace starrocks {

namespace {

AggStateDesc make_desc() {
    std::vector<TypeDescriptor> arg_types = {TypeDescriptor::from_logical_type(TYPE_SMALLINT)};
    auto return_type = TypeDescriptor::from_logical_type(TYPE_DOUBLE);
    return AggStateDesc("avg", return_type, arg_types, true, 3);
}

void expect_equal(const AggStateDesc& actual, const AggStateDesc& expected) {
    EXPECT_EQ(expected.get_func_name(), actual.get_func_name());
    EXPECT_EQ(expected.get_return_type(), actual.get_return_type());
    EXPECT_EQ(expected.get_arg_types(), actual.get_arg_types());
    EXPECT_EQ(expected.is_result_nullable(), actual.is_result_nullable());
    EXPECT_EQ(expected.get_func_version(), actual.get_func_version());
}

} // namespace

TEST(AggStateDescTest, basicAccessorsAndCopies) {
    auto desc = make_desc();
    EXPECT_EQ("avg", desc.get_func_name());
    EXPECT_TRUE(desc.is_result_nullable());
    EXPECT_EQ(3, desc.get_func_version());
    EXPECT_NE(std::string::npos, desc.debug_string().find("avg"));

    auto copied = desc;
    expect_equal(copied, desc);

    auto assigned = AggStateDesc("avg", TypeDescriptor::from_logical_type(TYPE_DOUBLE),
                                 {TypeDescriptor::from_logical_type(TYPE_SMALLINT)}, false, 1);
    assigned = desc;
    expect_equal(assigned, desc);
}

TEST(AggStateDescTest, thriftRoundTrip) {
    auto desc = make_desc();
    TAggStateDesc thrift_desc;
    desc.to_thrift(&thrift_desc);

    auto restored = AggStateDesc::from_thrift(thrift_desc);
    expect_equal(restored, desc);
}

TEST(AggStateDescTest, protobufRoundTrip) {
    auto desc = make_desc();
    AggStateDescPB protobuf_desc;
    desc.to_protobuf(&protobuf_desc);

    auto restored = AggStateDesc::from_protobuf(protobuf_desc);
    expect_equal(restored, desc);
}

TEST(AggStateDescTest, thriftToProtobuf) {
    auto desc = make_desc();
    TAggStateDesc thrift_desc;
    desc.to_thrift(&thrift_desc);

    AggStateDescPB protobuf_desc;
    AggStateDesc::thrift_to_protobuf(thrift_desc, &protobuf_desc);

    auto restored = AggStateDesc::from_protobuf(protobuf_desc);
    expect_equal(restored, desc);
}

} // namespace starrocks
