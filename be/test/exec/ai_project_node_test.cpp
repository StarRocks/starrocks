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

#include "exec/ai_project_node.h"

#include <gtest/gtest.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "common/config_exec_fwd.h"
#include "exec/exec_factory.h"
#include "exec_primitive/exec_node.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "types/type_descriptor.h"

namespace starrocks {
namespace {

constexpr int64_t kAICompletePromptFid = 200100;
constexpr TSlotId kPromptSlotId = 1;
constexpr TSlotId kCommonSlotId = 2;
constexpr TSlotId kAnswerSlotId = 3;
constexpr std::string_view kSystemChatConfigId = "__system_chat__";
constexpr std::string_view kEndpointSentinel = "https://unit.test.invalid/v1/chat/completions";
constexpr std::string_view kModelSentinel = "unit-test-model";
constexpr std::string_view kSecretSentinel = "must-not-appear-in-plan-or-error";
constexpr std::string_view kArbitraryCredentialEnvironment = "UNIT_TEST_ARBITRARY_AI_CREDENTIAL_ENV";

TypeDescriptor varchar_type() {
    return TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
}

TExpr make_slot_ref(TTupleId tuple_id, TSlotId slot_id) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::SLOT_REF);
    node.__set_num_children(0);
    node.__set_type(varchar_type().to_thrift());
    node.__set_is_nullable(true);

    TSlotRef slot;
    slot.__set_tuple_id(tuple_id);
    slot.__set_slot_id(slot_id);
    node.__set_slot_ref(slot);

    TExpr expression;
    expression.nodes.emplace_back(std::move(node));
    return expression;
}

TExpr make_ai_complete(TTupleId tuple_id, TSlotId prompt_slot,
                       std::string config_id = std::string(kSystemChatConfigId)) {
    TFunctionName name;
    name.__set_function_name("ai_complete");

    TFunction function;
    function.__set_name(name);
    function.__set_binary_type(TFunctionBinaryType::AI);
    function.__set_arg_types({varchar_type().to_thrift()});
    function.__set_ret_type(varchar_type().to_thrift());
    function.__set_has_var_args(false);
    function.__set_fid(kAICompletePromptFid);
    function.__set_ai_model_source(TAIModelSource::SYSTEM);

    TExprNode call;
    call.__set_node_type(TExprNodeType::FUNCTION_CALL);
    call.__set_num_children(1);
    call.__set_type(varchar_type().to_thrift());
    call.__set_is_nullable(true);
    call.__set_fn(function);
    call.__set_ai_model_config_id(std::move(config_id));

    TExpr expression;
    expression.nodes.emplace_back(std::move(call));
    expression.nodes.emplace_back(make_slot_ref(tuple_id, prompt_slot).nodes.front());
    return expression;
}

TAIModelConfiguration make_system_chat_config(std::string provider = "openai_compatible") {
    TAIEndpointConfig chat;
    chat.__set_endpoint(std::string(kEndpointSentinel));
    chat.__set_model(std::string(kModelSentinel));
    chat.__set_provider(std::move(provider));

    TAIModelConfiguration config;
    config.__set_chat(std::move(chat));
    return config;
}

TPlanNode make_base_node(TPlanNodeType::type type, TTupleId tuple_id, int32_t node_id, int32_t num_children) {
    TPlanNode node;
    node.__set_node_id(node_id);
    node.__set_node_type(type);
    node.__set_num_children(num_children);
    node.__set_limit(-1);
    node.row_tuples.emplace_back(tuple_id);
    return node;
}

TPlan make_valid_ai_plan(TTupleId tuple_id) {
    TAIProjectNode project;
    project.__set_slot_map({
            {kPromptSlotId, make_slot_ref(tuple_id, kPromptSlotId)},
            {kAnswerSlotId, make_ai_complete(tuple_id, kCommonSlotId)},
    });
    project.__set_common_slot_map({
            {kCommonSlotId, make_slot_ref(tuple_id, kPromptSlotId)},
    });
    project.__set_ai_model_configs({
            {std::string(kSystemChatConfigId), make_system_chat_config()},
    });

    TPlanNode ai = make_base_node(TPlanNodeType::AI_PROJECT_NODE, tuple_id, 10, 1);
    ai.__set_ai_project_node(std::move(project));

    TPlan plan;
    plan.nodes.emplace_back(std::move(ai));
    plan.nodes.emplace_back(make_base_node(TPlanNodeType::EMPTY_SET_NODE, tuple_id, 11, 0));
    return plan;
}

TPlan binary_round_trip(TPlan plan, std::string* serialized = nullptr) {
    auto buffer = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
    auto protocol = std::make_shared<apache::thrift::protocol::TBinaryProtocol>(buffer);
    plan.write(protocol.get());

    const std::string bytes = buffer->getBufferAsString();
    if (serialized != nullptr) {
        *serialized = bytes;
    }
    auto read_buffer = std::make_shared<apache::thrift::transport::TMemoryBuffer>(
            reinterpret_cast<uint8_t*>(const_cast<char*>(bytes.data())), static_cast<uint32_t>(bytes.size()),
            apache::thrift::transport::TMemoryBuffer::COPY);
    auto read_protocol = std::make_shared<apache::thrift::protocol::TBinaryProtocol>(read_buffer);

    TPlan decoded;
    decoded.read(read_protocol.get());
    return decoded;
}

class AIProjectNodeTest : public ::testing::Test {
protected:
    void SetUp() override {
        TDescriptorTableBuilder table;
        TTupleDescriptorBuilder tuple;
        tuple.add_slot(TSlotDescriptorBuilder()
                               .id(kPromptSlotId)
                               .type(varchar_type())
                               .nullable(true)
                               .column_name("prompt")
                               .build());
        tuple.add_slot(TSlotDescriptorBuilder()
                               .id(kAnswerSlotId)
                               .type(varchar_type())
                               .nullable(true)
                               .column_name("answer")
                               .build());
        tuple.build(&table);

        ASSERT_OK(DescriptorTbl::create(&_runtime_state, &_descriptor_pool, table.desc_tbl(), &_descriptors,
                                        config::vector_chunk_size));
        _runtime_state.set_desc_tbl(_descriptors);

        std::vector<TupleDescriptor*> tuple_descriptors;
        _descriptors->get_tuple_descs(&tuple_descriptors);
        ASSERT_EQ(1, tuple_descriptors.size());
        _tuple_id = tuple_descriptors.front()->id();
    }

    RuntimeState _runtime_state{TQueryGlobals{}};
    ObjectPool _descriptor_pool;
    DescriptorTbl* _descriptors = nullptr;
    TTupleId _tuple_id = 0;
};

TEST_F(AIProjectNodeTest, ExecFactoryDeserializesExactThriftContract) {
    std::string serialized;
    TPlan plan = binary_round_trip(make_valid_ai_plan(_tuple_id), &serialized);
    ASSERT_EQ(2, plan.nodes.size());

    const TPlanNode& wire_node = plan.nodes.front();
    ASSERT_EQ(TPlanNodeType::AI_PROJECT_NODE, wire_node.node_type);
    ASSERT_TRUE(wire_node.__isset.ai_project_node);
    ASSERT_EQ(2, wire_node.ai_project_node.slot_map.size());
    ASSERT_EQ(1, wire_node.ai_project_node.common_slot_map.size());
    ASSERT_EQ(1, wire_node.ai_project_node.ai_model_configs.size());
    ASSERT_EQ(std::string(kSystemChatConfigId),
              wire_node.ai_project_node.slot_map.at(kAnswerSlotId).nodes.front().ai_model_config_id);

    const std::string wire_debug = wire_node.ai_project_node.ai_model_configs.begin()->second.chat.endpoint;
    ASSERT_EQ(kEndpointSentinel, wire_debug);
    ASSERT_EQ(std::string::npos, serialized.find(kSecretSentinel));
    ASSERT_EQ(std::string::npos, serialized.find(kArbitraryCredentialEnvironment));

    ObjectPool plan_pool;
    ExecNode* root = nullptr;
    ASSERT_OK(ExecFactory::create_tree(&_runtime_state, &plan_pool, plan, *_descriptors, &root));
    ASSERT_NE(nullptr, root);
    EXPECT_EQ(TPlanNodeType::AI_PROJECT_NODE, root->type());
    EXPECT_NE(nullptr, dynamic_cast<AIProjectNode*>(root));
    ASSERT_EQ(1, root->children().size());
    EXPECT_EQ(TPlanNodeType::EMPTY_SET_NODE, root->children().front()->type());
    const std::string debug = root->debug_string();
    EXPECT_EQ(std::string::npos, debug.find(kSecretSentinel));
    EXPECT_EQ(std::string::npos, debug.find(kArbitraryCredentialEnvironment));
    root->close(&_runtime_state);
}

TEST_F(AIProjectNodeTest, MissingOptionalCommonSlotMapIsTreatedAsEmpty) {
    TPlan plan = make_valid_ai_plan(_tuple_id);
    TAIProjectNode& project = plan.nodes.front().ai_project_node;
    project.slot_map[kAnswerSlotId] = make_ai_complete(_tuple_id, kPromptSlotId);
    project.common_slot_map.clear();
    project.__isset.common_slot_map = false;
    plan = binary_round_trip(std::move(plan));

    ASSERT_FALSE(plan.nodes.front().ai_project_node.__isset.common_slot_map);
    ASSERT_TRUE(plan.nodes.front().ai_project_node.common_slot_map.empty());

    ObjectPool plan_pool;
    ExecNode* root = nullptr;
    ASSERT_OK(ExecFactory::create_tree(&_runtime_state, &plan_pool, plan, *_descriptors, &root));
    ASSERT_NE(nullptr, root);
    EXPECT_NE(nullptr, dynamic_cast<AIProjectNode*>(root));
    root->close(&_runtime_state);
}

TEST(AIProjectNodeRecordDescriptorTest, ResolvesProjectionSlotsAcrossTupleBoundaries) {
    TDescriptorTableBuilder table;
    TTupleDescriptorBuilder input_tuple;
    input_tuple.add_slot(TSlotDescriptorBuilder()
                                 .id(kPromptSlotId)
                                 .type(varchar_type())
                                 .nullable(true)
                                 .column_name("prompt")
                                 .build());
    input_tuple.build(&table);

    TTupleDescriptorBuilder projected_tuple;
    projected_tuple.add_slot(TSlotDescriptorBuilder()
                                     .id(kAnswerSlotId)
                                     .type(varchar_type())
                                     .nullable(true)
                                     .column_name("answer")
                                     .build());
    projected_tuple.build(&table);

    RuntimeState state{TQueryGlobals{}};
    ObjectPool descriptor_pool;
    DescriptorTbl* descriptors = nullptr;
    ASSERT_OK(
            DescriptorTbl::create(&state, &descriptor_pool, table.desc_tbl(), &descriptors, config::vector_chunk_size));
    state.set_desc_tbl(descriptors);

    std::vector<TupleDescriptor*> tuple_descriptors;
    descriptors->get_tuple_descs(&tuple_descriptors);
    ASSERT_EQ(2, tuple_descriptors.size());
    const TupleDescriptor* input_descriptor = nullptr;
    const TupleDescriptor* projected_descriptor = nullptr;
    for (const TupleDescriptor* tuple_descriptor : tuple_descriptors) {
        if (tuple_descriptor->get_slot_by_id(kPromptSlotId) != nullptr) {
            input_descriptor = tuple_descriptor;
        }
        if (tuple_descriptor->get_slot_by_id(kAnswerSlotId) != nullptr) {
            projected_descriptor = tuple_descriptor;
        }
    }
    ASSERT_NE(nullptr, input_descriptor);
    ASSERT_NE(nullptr, projected_descriptor);
    const TTupleId input_tuple_id = input_descriptor->id();
    const TTupleId projected_tuple_id = projected_descriptor->id();

    TAIProjectNode project;
    project.__set_slot_map({
            {kPromptSlotId, make_slot_ref(input_tuple_id, kPromptSlotId)},
            {kAnswerSlotId, make_ai_complete(projected_tuple_id, kCommonSlotId)},
    });
    project.__set_common_slot_map({
            {kCommonSlotId, make_slot_ref(input_tuple_id, kPromptSlotId)},
    });
    project.__set_ai_model_configs({
            {std::string(kSystemChatConfigId), make_system_chat_config()},
    });

    TPlanNode ai = make_base_node(TPlanNodeType::AI_PROJECT_NODE, input_tuple_id, 10, 1);
    ai.row_tuples.emplace_back(projected_tuple_id);
    ai.__set_ai_project_node(std::move(project));

    TPlan plan;
    plan.nodes.emplace_back(std::move(ai));
    plan.nodes.emplace_back(make_base_node(TPlanNodeType::EMPTY_SET_NODE, input_tuple_id, 11, 0));
    plan = binary_round_trip(std::move(plan));

    ObjectPool plan_pool;
    ExecNode* root = nullptr;
    ASSERT_OK(ExecFactory::create_tree(&state, &plan_pool, plan, *descriptors, &root));
    ASSERT_NE(nullptr, root);
    EXPECT_NE(nullptr, dynamic_cast<AIProjectNode*>(root));
    root->close(&state);
}

TEST_F(AIProjectNodeTest, RejectsMalformedThriftContractsBeforeCreatingATree) {
    struct Case {
        const char* name;
        std::function<void(TPlanNode&)> mutate;
    };

    const std::vector<Case> cases{
            {"missing ai_project_node",
             [](TPlanNode& node) {
                 node.__isset.ai_project_node = false;
                 node.ai_project_node = TAIProjectNode{};
             }},
            {"missing slot_map",
             [](TPlanNode& node) {
                 node.ai_project_node.slot_map.clear();
                 node.ai_project_node.__isset.slot_map = false;
             }},
            {"no AI output", [](TPlanNode& node) { node.ai_project_node.slot_map.erase(kAnswerSlotId); }},
            {"output and common slots overlap",
             [](TPlanNode& node) {
                 node.ai_project_node.common_slot_map[kPromptSlotId] =
                         make_slot_ref(node.row_tuples.front(), kPromptSlotId);
             }},
            {"pass-through changes slot identity",
             [](TPlanNode& node) {
                 node.ai_project_node.slot_map[kPromptSlotId] = make_slot_ref(node.row_tuples.front(), kCommonSlotId);
             }},
            {"common expression contains AI",
             [](TPlanNode& node) {
                 node.ai_project_node.common_slot_map[kCommonSlotId] =
                         make_ai_complete(node.row_tuples.front(), kPromptSlotId);
             }},
            {"common expression depends on AI output",
             [](TPlanNode& node) {
                 node.ai_project_node.common_slot_map[kCommonSlotId] =
                         make_slot_ref(node.row_tuples.front(), kAnswerSlotId);
             }},
            {"referenced config is missing", [](TPlanNode& node) { node.ai_project_node.ai_model_configs.clear(); }},
            {"AI call references unknown config",
             [](TPlanNode& node) {
                 node.ai_project_node.slot_map[kAnswerSlotId].nodes.front().__set_ai_model_config_id("unknown");
             }},
            {"AI output depends on a current AI output",
             [](TPlanNode& node) {
                 node.ai_project_node.slot_map[kAnswerSlotId] =
                         make_ai_complete(node.row_tuples.front(), kAnswerSlotId);
             }},
            {"chat config is missing",
             [](TPlanNode& node) {
                 node.ai_project_node.ai_model_configs[std::string(kSystemChatConfigId)] = TAIModelConfiguration{};
             }},
            {"provider is unsupported",
             [](TPlanNode& node) {
                 node.ai_project_node.ai_model_configs[std::string(kSystemChatConfigId)] =
                         make_system_chat_config("unsupported");
             }},
    };

    for (const Case& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        TPlan plan = make_valid_ai_plan(_tuple_id);
        test_case.mutate(plan.nodes.front());
        plan = binary_round_trip(std::move(plan));

        ObjectPool plan_pool;
        ExecNode* root = nullptr;
        const Status status = ExecFactory::create_tree(&_runtime_state, &plan_pool, plan, *_descriptors, &root);
        if (root != nullptr) {
            root->close(&_runtime_state);
        }
        ASSERT_TRUE(status.is_invalid_argument()) << status;
        EXPECT_EQ(nullptr, root);
        EXPECT_EQ(std::string::npos, status.to_string().find(kEndpointSentinel));
        EXPECT_EQ(std::string::npos, status.to_string().find(kModelSentinel));
        EXPECT_EQ(std::string::npos, status.to_string().find(kSecretSentinel));
        EXPECT_EQ(std::string::npos, status.to_string().find(kArbitraryCredentialEnvironment));
    }
}

} // namespace
} // namespace starrocks
