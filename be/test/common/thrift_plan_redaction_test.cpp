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

TEST(ThriftPlanRedactionTest, MasksDiagnosticCopiesBeforeThriftEncoding) {
    for (const std::string& secret : {std::string("test-key"), std::string("test\n\x01\"key")}) {
        TAIProvider config;
        config.__set_id("provider:test-id");
        config.__set_type(TAIProviderType::CHAT);
        config.__set_protocol(TAIProviderProtocol::OPENAI);
        config.__set_params({{"endpoint", "https://models.example.test/v1/inference"}, {"api_key", secret}});
        TAIProjectNode ai;
        ai.__set_ai_provider_mapping({{"provider:test-id", config}});
        TPlanNode node;
        node.__set_ai_project_node(ai);
        TPlan plan;
        plan.__set_nodes({node});
        TPlanFragment fragment;
        fragment.__set_plan(plan);
        TExecPlanFragmentParams request;
        request.__set_fragment(fragment);
        TQueryPlanInfo info;
        info.__set_plan_fragment(fragment);

        TPlanNode masked_node(node);
        auto& masked = masked_node.ai_project_node.ai_provider_mapping.at("provider:test-id");
        masked.params["api_key"] = "******";
        TPlanFragment masked_fragment(fragment);
        masked_fragment.plan.nodes[0] = masked_node;
        TExecPlanFragmentParams masked_request(request);
        masked_request.fragment = masked_fragment;
        TQueryPlanInfo masked_info(info);
        masked_info.plan_fragment = masked_fragment;

        // Compare the complete serialized diagnostic form, not merely the raw secret substring.
        EXPECT_EQ(apache::thrift::ThriftDebugString(masked_node), thrift_plan_debug_string(node));
        EXPECT_EQ(apache::thrift::ThriftDebugString(masked_fragment), thrift_plan_debug_string(fragment));
        EXPECT_EQ(apache::thrift::ThriftDebugString(masked_request), thrift_plan_debug_string(request));
        EXPECT_EQ(apache::thrift::ThriftDebugString(masked_info), thrift_plan_debug_string(info));
        EXPECT_EQ(secret, request.fragment.plan.nodes[0]
                                  .ai_project_node.ai_provider_mapping.at("provider:test-id")
                                  .params.at("api_key"));
        EXPECT_EQ(secret, info.plan_fragment.plan.nodes[0]
                                  .ai_project_node.ai_provider_mapping.at("provider:test-id")
                                  .params.at("api_key"));
    }
}

TEST(ThriftPlanRedactionTest, OrdinaryPlanDiagnosticsAreUnchanged) {
    TPlanFragment fragment;
    EXPECT_EQ(apache::thrift::ThriftDebugString(fragment), thrift_plan_debug_string(fragment));
}

} // namespace starrocks
