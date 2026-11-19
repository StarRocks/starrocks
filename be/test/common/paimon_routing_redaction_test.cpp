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

#include "common/util/thrift_util.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/QueryPlanExtra_types.h"

namespace starrocks {

TEST(PaimonRoutingRedactionTest, MasksNativeSinkCredentialsAndPreservesTransmissionCopy) {
    TCloudConfiguration cloud;
    cloud.__set_cloud_properties({{"access_key", "secret-key"}});
    TPaimonTableSink sink;
    sink.__set_cloud_configuration(cloud);
    sink.__set_options({{"credential", "secret-option"}});
    sink.__set_schema_json("secret-schema-option");
    sink.__set_location("s3://bucket/path?token=secret-location");
    sink.__set_table_identity("secret-identity");
    sink.__set_static_partition({{"p", "secret-partition"}});
    sink.__set_commit_user("secret-user");
    sink.__set_operation_id("secret-operation");
    TDataSink output;
    output.__set_type(TDataSinkType::PAIMON_TABLE_SINK);
    output.__set_paimon_table_sink(sink);
    TPlanFragment fragment;
    fragment.__set_output_sink(output);
    auto masked_fragment = fragment;
    auto& masked = masked_fragment.output_sink.paimon_table_sink;
    masked.__set_cloud_configuration(TCloudConfiguration{});
    masked.__set_options({});
    masked.__set_schema_json("******");
    masked.__set_location("******");
    masked.__set_table_identity("******");
    masked.__set_static_partition({});
    masked.__set_commit_user("******");
    masked.__set_operation_id("******");
    EXPECT_EQ(apache::thrift::ThriftDebugString(masked_fragment), thrift_plan_debug_string(fragment));
    EXPECT_EQ(sink, fragment.output_sink.paimon_table_sink);
    TExecPlanFragmentParams request;
    request.__set_fragment(fragment);
    auto masked_request = request;
    masked_request.fragment = masked_fragment;
    EXPECT_EQ(apache::thrift::ThriftDebugString(masked_request), thrift_plan_debug_string(request));
    EXPECT_EQ(sink, request.fragment.output_sink.paimon_table_sink);
}

} // namespace starrocks
