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

#include "exprs/ai/ai_function_call_expr.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "base/string/slice.h"
#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/file_column.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/expr_factory.h"
#include "gen_cpp/Exprs_types.h"
#include "platform/llm/ai_provider.h"
#include "platform/llm/openai_compatible_provider.h"
#include "runtime/runtime_state.h"
#include "types/json_value.h"
#include "types/type_descriptor.h"

namespace starrocks {
namespace {

constexpr int64_t kPromptFid = 200000;
constexpr std::string_view kDefaultConfigId = "provider:default_chat";

// Use an explicitly typed expression node: Expr(TypeDescriptor) constructs a
// literal node and intentionally does not accept every column type (e.g. FILE).
TExprNode counting_column_node(const TypeDescriptor& type) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::SLOT_REF);
    node.__set_type(type.to_thrift());
    node.__set_num_children(0);
    node.__set_is_nullable(true);
    return node;
}

class CountingColumnExpr final : public Expr {
public:
    CountingColumnExpr(TypeDescriptor type, ColumnPtr column, bool constant_expression = false)
            : Expr(counting_column_node(type)), _column(std::move(column)), _constant_expression(constant_expression) {}

    Expr* clone(ObjectPool* pool) const override {
        return pool->add(new CountingColumnExpr(type(), _column, _constant_expression));
    }

    bool is_constant() const override { return _constant_expression || _column->is_constant(); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext*, Chunk*) override {
        ++_evaluation_count;
        return _column;
    }

    int evaluation_count() const { return _evaluation_count; }

private:
    ColumnPtr _column;
    bool _constant_expression;
    int _evaluation_count = 0;
};

class MarkerExpr final : public Expr {
public:
    explicit MarkerExpr(const TExprNode& node) : Expr(node) {}

    Expr* clone(ObjectPool* pool) const override { return pool->add(new MarkerExpr(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext*, Chunk*) override {
        return ColumnHelper::create_const_column<TYPE_INT>(1, 1);
    }
};

int g_post_hook_calls = 0;

Status post_hook_handle_all(ObjectPool* pool, const TExprNode& node, Expr** expr, RuntimeState*) {
    ++g_post_hook_calls;
    *expr = pool->add(new MarkerExpr(node));
    return Status::OK();
}

class CapturingLogSink final : public google::LogSink {
public:
    CapturingLogSink() { google::AddLogSink(this); }

    ~CapturingLogSink() override { google::RemoveLogSink(this); }

    void send(google::LogSeverity, const char*, const char*, int, const google::LogMessageTime&, const char* message,
              size_t message_length) override {
        std::lock_guard lock(_mutex);
        _messages.append(message, message_length);
        _messages.push_back('\n');
    }

    std::string messages() const {
        std::lock_guard lock(_mutex);
        return _messages;
    }

private:
    mutable std::mutex _mutex;
    std::string _messages;
};

TypeDescriptor varchar_type() {
    return TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
}

TypeDescriptor options_type() {
    return TypeDescriptor(TYPE_JSON);
}

std::vector<TypeDescriptor> signature_argument_types(int64_t fid) {
    EXPECT_EQ(kPromptFid, fid);
    return {varchar_type(), varchar_type(), varchar_type(), options_type()};
}

TExprNode make_string_literal_node(const TypeDescriptor& type, std::string value);

TExprNode default_options_node() {
    TExprNode node;
    node.__set_node_type(TExprNodeType::NULL_LITERAL);
    node.__set_num_children(0);
    node.__set_type(options_type().to_thrift());
    node.__set_is_nullable(true);
    return node;
}

TExprNode make_slot_ref(const TypeDescriptor& type, int slot_id) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::SLOT_REF);
    node.__set_num_children(0);
    node.__set_type(type.to_thrift());
    node.__set_is_nullable(true);

    TSlotRef slot;
    slot.__set_tuple_id(0);
    slot.__set_slot_id(slot_id);
    node.__set_slot_ref(slot);
    return node;
}

TExpr make_ai_expression(int64_t fid) {
    const auto arguments = signature_argument_types(fid);

    TFunctionName name;
    name.__set_function_name("ai_complete");

    std::vector<TTypeDesc> argument_types;
    argument_types.reserve(arguments.size());
    for (const auto& type : arguments) {
        argument_types.emplace_back(type.to_thrift());
    }

    TFunction function;
    function.__set_name(name);
    function.__set_binary_type(TFunctionBinaryType::AI);
    function.__set_arg_types(argument_types);
    function.__set_ret_type(varchar_type().to_thrift());
    function.__set_has_var_args(false);
    function.__set_fid(fid);

    TExprNode call;
    call.__set_node_type(TExprNodeType::FUNCTION_CALL);
    call.__set_num_children(static_cast<int32_t>(arguments.size()));
    call.__set_type(varchar_type().to_thrift());
    call.__set_is_nullable(true);
    call.__set_fn(function);
    call.__set_ai_provider_id(std::string(kDefaultConfigId));

    TExpr expression;
    expression.nodes.emplace_back(std::move(call));
    for (size_t i = 0; i < arguments.size(); ++i) {
        if (i == 1 || i == 2) {
            expression.nodes.emplace_back(make_string_literal_node(varchar_type(), ""));
        } else if (i == 3) {
            expression.nodes.emplace_back(default_options_node());
        } else {
            expression.nodes.emplace_back(make_slot_ref(arguments[i], static_cast<int>(i + 1)));
        }
    }
    return expression;
}

TExpr wrap_in_builtin_call(TExpr expression) {
    TFunctionName name;
    name.__set_function_name("ordinary_wrapper");

    TFunction function;
    function.__set_name(name);
    function.__set_binary_type(TFunctionBinaryType::BUILTIN);
    function.__set_arg_types({varchar_type().to_thrift()});
    function.__set_ret_type(varchar_type().to_thrift());
    function.__set_has_var_args(false);

    TExprNode call;
    call.__set_node_type(TExprNodeType::FUNCTION_CALL);
    call.__set_num_children(1);
    call.__set_type(varchar_type().to_thrift());
    call.__set_is_nullable(true);
    call.__set_fn(function);
    expression.nodes.insert(expression.nodes.begin(), std::move(call));
    return expression;
}

TExprNode make_string_literal_node(const TypeDescriptor& type, std::string value) {
    TExprNode literal;
    literal.__set_node_type(TExprNodeType::STRING_LITERAL);
    literal.__set_num_children(0);
    literal.__set_type(type.to_thrift());
    literal.__set_is_nullable(false);
    TStringLiteral string_literal;
    string_literal.__set_value(std::move(value));
    literal.__set_string_literal(string_literal);
    return literal;
}

void clear_ai_markers(TExprNode* node) {
    node->fn.binary_type = TFunctionBinaryType::BUILTIN;
    node->fn.name.function_name = "ordinary_function";
    node->fn.__isset.fid = false;
    node->__isset.ai_provider_id = false;
}

StatusOr<AIFunctionCallExpr*> create_ai_expression(ObjectPool* pool, int64_t fid) {
    Expr* root = nullptr;
    RETURN_IF_ERROR(ExprFactory::create_expr_tree(pool, make_ai_expression(fid), &root, nullptr));
    auto* ai = dynamic_cast<AIFunctionCallExpr*>(root);
    if (ai == nullptr) {
        return Status::InternalError("factory did not create an AI function expression");
    }
    return ai;
}

ColumnPtr make_varchar_column(std::initializer_list<std::optional<std::string>> values) {
    ColumnBuilder<TYPE_VARCHAR> builder(static_cast<int32_t>(values.size()));
    for (const auto& value : values) {
        if (value.has_value()) {
            builder.append(Slice(*value));
        } else {
            builder.append_null();
        }
    }
    return builder.build(false);
}

ColumnPtr make_const_options(size_t rows, bool top_level_null = false) {
    auto json = JsonColumn::create();
    json->append(JsonValue::parse(R"({"temperature":0.25})").value());
    if (!top_level_null) {
        return ConstColumn::create(std::move(json), rows);
    }

    auto nulls = NullColumn::create();
    nulls->append(1);
    auto nullable = NullableColumn::create(std::move(json), std::move(nulls));
    return ConstColumn::create(std::move(nullable), rows);
}

ColumnPtr make_non_const_options() {
    auto json = JsonColumn::create();
    json->append(JsonValue::parse(R"({"temperature":0.25})").value());
    return json;
}

ChunkPtr make_chunk(size_t rows) {
    auto row_ids = Int32Column::create();
    for (size_t i = 0; i < rows; ++i) {
        row_ids->append(static_cast<int32_t>(i));
    }
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(row_ids), 0);
    return chunk;
}

std::vector<CountingColumnExpr*> replace_children(
        ObjectPool* pool, AIFunctionCallExpr* expression,
        std::initializer_list<std::pair<TypeDescriptor, ColumnPtr>> children) {
    expression->mutable_children().clear();
    std::vector<CountingColumnExpr*> result;
    result.reserve(children.size());
    for (const auto& [type, column] : children) {
        auto* child = pool->add(new CountingColumnExpr(type, column));
        expression->add_child(child);
        result.emplace_back(child);
    }
    return result;
}

void append_default_selectors(ObjectPool* pool, AIFunctionCallExpr* expression, bool options = false) {
    for (int i = 0; i < 2; ++i) {
        expression->add_child(pool->add(
                new CountingColumnExpr(varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>("", 1))));
    }
    if (options) {
        expression->add_child(pool->add(new CountingColumnExpr(options_type(), make_const_options(1, true))));
    }
}

std::vector<CountingColumnExpr*> replace_inputs(ObjectPool* pool, AIFunctionCallExpr* expression,
                                                std::initializer_list<std::pair<TypeDescriptor, ColumnPtr>> inputs,
                                                bool options = false) {
    auto children = replace_children(pool, expression, inputs);
    CountingColumnExpr* supplied_options = nullptr;
    if (!inputs.size()) return children;
    if ((inputs.end() - 1)->first.type == TYPE_JSON) {
        supplied_options = children.back();
        expression->mutable_children().pop_back();
    }
    append_default_selectors(pool, expression, options && supplied_options == nullptr);
    if (supplied_options != nullptr) expression->add_child(supplied_options);
    return children;
}

std::vector<CountingColumnExpr*> replace_complete_inputs(
        ObjectPool* pool, AIFunctionCallExpr* expression,
        std::initializer_list<std::pair<TypeDescriptor, ColumnPtr>> inputs) {
    return replace_inputs(pool, expression, inputs, true);
}

std::string request_body(const AIFunctionInputBatch& batch, const AIFunctionRowInput& row) {
    OpenAICompatibleProvider provider;
    auto request = provider.build_request(AIChatRequest{
            .endpoint = "https://provider.example/v1/chat/completions",
            .model = row.model,
            .api_key = "test-key",
            .prompt = row.prompt,
            .options = batch.options.get(),
    });
    EXPECT_TRUE(request.ok()) << request.status().message();
    return request.ok() ? std::move(request).value().body : std::string();
}

size_t count_occurrences(std::string_view haystack, std::string_view needle) {
    size_t count = 0;
    size_t offset = 0;
    while ((offset = haystack.find(needle, offset)) != std::string_view::npos) {
        ++count;
        offset += needle.size();
    }
    return count;
}

class AIFunctionCallExprTest : public ::testing::Test {
protected:
    void TearDown() override { ExprFactory::set_non_core_create_post_hook(nullptr); }
};

TEST_F(AIFunctionCallExprTest, FactoryDispatchesAIBeforeThePostHook) {
    for (TExprNodeType::type node_type : {TExprNodeType::FUNCTION_CALL, TExprNodeType::COMPUTE_FUNCTION_CALL}) {
        for (const char* function_name : {"ai_complete", "AI_COMPLETE", "Ai_Complete"}) {
            SCOPED_TRACE(function_name);
            SCOPED_TRACE(node_type);
            g_post_hook_calls = 0;
            ExprFactory::set_non_core_create_post_hook(post_hook_handle_all);

            TExpr expression = make_ai_expression(kPromptFid);
            expression.nodes.front().node_type = node_type;
            expression.nodes.front().fn.name.function_name = function_name;
            ObjectPool pool;
            Expr* root = nullptr;
            Status status = ExprFactory::create_expr_tree(&pool, expression, &root, nullptr);

            ASSERT_TRUE(status.ok()) << status.message();
            EXPECT_EQ(0, g_post_hook_calls);
            auto* ai = dynamic_cast<AIFunctionCallExpr*>(root);
            ASSERT_NE(nullptr, ai);
            EXPECT_EQ(kDefaultConfigId, ai->provider_id());
        }
    }
}

TEST_F(AIFunctionCallExprTest, FactoryRejectsMalformedAIThriftWithoutDelegatingToThePostHook) {
    using Mutation = std::function<void(TExprNode&)>;
    const std::vector<std::string> sensitive_sentinels = {"sensitive-ai-function-name", "sensitive-ai-config",
                                                          "sensitive-literal-prompt"};
    const std::vector<std::pair<std::string, Mutation>> malformed = {
            {"wrong name", [](TExprNode& node) { node.fn.name.function_name = "sensitive-ai-function-name"; }},
            {"missing fid", [](TExprNode& node) { node.fn.__isset.fid = false; }},
            {"unknown fid", [](TExprNode& node) { node.fn.fid = 299999; }},
            {"varargs", [](TExprNode& node) { node.fn.has_var_args = true; }},
            {"missing config", [](TExprNode& node) { node.__isset.ai_provider_id = false; }},
            {"wrong config", [](TExprNode& node) { node.ai_provider_id = "invalid\nconfig"; }},
            {"missing nullable", [](TExprNode& node) { node.__isset.is_nullable = false; }},
            {"non-nullable", [](TExprNode& node) { node.__set_is_nullable(false); }},
            {"wrong node return", [](TExprNode& node) { node.type = TypeDescriptor(TYPE_INT).to_thrift(); }},
            {"wrong function return", [](TExprNode& node) { node.fn.ret_type = TypeDescriptor(TYPE_INT).to_thrift(); }},
            {"wrong arity", [](TExprNode& node) { node.num_children = 2; }},
            {"wrong argument count",
             [](TExprNode& node) { node.fn.arg_types.emplace_back(varchar_type().to_thrift()); }},
            {"wrong argument type",
             [](TExprNode& node) { node.fn.arg_types[0] = TypeDescriptor(TYPE_INT).to_thrift(); }},
            {"mismatched fid", [](TExprNode& node) { node.fn.fid = 200004; }},
    };

    for (const auto& [name, mutate] : malformed) {
        SCOPED_TRACE(name);
        TExpr expression = make_ai_expression(kPromptFid);
        expression.nodes[1].__isset.slot_ref = false;
        expression.nodes[1].__set_node_type(TExprNodeType::STRING_LITERAL);
        TStringLiteral prompt;
        prompt.__set_value(sensitive_sentinels[2]);
        expression.nodes[1].__set_string_literal(prompt);
        mutate(expression.nodes.front());

        CapturingLogSink log_sink;
        g_post_hook_calls = 0;
        ExprFactory::set_non_core_create_post_hook(post_hook_handle_all);
        ObjectPool pool;
        Expr* root = nullptr;
        Status status = ExprFactory::create_expr_tree(&pool, expression, &root, nullptr);

        EXPECT_FALSE(status.ok());
        EXPECT_EQ(0, g_post_hook_calls);
        EXPECT_EQ(nullptr, root);
        EXPECT_EQ("Invalid AI function expression", status.message());
        for (const auto& sentinel : sensitive_sentinels) {
            EXPECT_EQ(std::string::npos, std::string(status.message()).find(sentinel));
        }
        const std::string messages = log_sink.messages();
        EXPECT_EQ(1, count_occurrences(messages, "Could not construct AI expression tree"));
        EXPECT_EQ(std::string::npos, messages.find("Invalid AI function expression"));
        for (const auto& sentinel : sensitive_sentinels) {
            EXPECT_EQ(std::string::npos, messages.find(sentinel));
        }
    }
}

TEST_F(AIFunctionCallExprTest, FactoryRedactsMalformedNestedAIThrift) {
    const std::vector<std::string> sensitive_sentinels = {"nested-sensitive-content", "nested-sensitive-service",
                                                          "nested-sensitive-literal-prompt"};
    TExpr expression = wrap_in_builtin_call(make_ai_expression(kPromptFid));
    TExprNode& ai_call = expression.nodes[1];
    ai_call.ai_provider_id = "invalid-config";
    ai_call.fn.__set_content(sensitive_sentinels[0]);
    ai_call.fn.__set_service_url(sensitive_sentinels[1]);
    expression.nodes[2].__isset.slot_ref = false;
    expression.nodes[2].__set_node_type(TExprNodeType::STRING_LITERAL);
    TStringLiteral prompt;
    prompt.__set_value(sensitive_sentinels[2]);
    expression.nodes[2].__set_string_literal(prompt);

    CapturingLogSink log_sink;
    ObjectPool pool;
    Expr* root = nullptr;
    Status status = ExprFactory::create_expr_tree(&pool, expression, &root, nullptr);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(nullptr, root);
    EXPECT_NE(std::string::npos, std::string(status.message()).find("Invalid AI function expression"));
    const std::string messages = log_sink.messages();
    EXPECT_NE(std::string::npos, messages.find("Could not construct AI expression tree"));
    EXPECT_EQ(std::string::npos, messages.find("Invalid AI function expression"));
    for (const auto& sentinel : sensitive_sentinels) {
        EXPECT_EQ(std::string::npos, std::string(status.message()).find(sentinel));
        EXPECT_EQ(std::string::npos, messages.find(sentinel));
    }
}

TEST_F(AIFunctionCallExprTest, ValidAIRootRedactsChildConstructionFailure) {
    const std::string sentinel = "valid-ai-root-sensitive-child";
    TExpr expression = make_ai_expression(kPromptFid);
    TExprNode& child = expression.nodes[1];
    child.__isset.slot_ref = false;
    TStringLiteral poison;
    poison.__set_value(sentinel);
    child.__set_string_literal(poison);

    CapturingLogSink log_sink;
    ObjectPool pool;
    Expr* root = reinterpret_cast<Expr*>(1);
    Status status = ExprFactory::create_expr_tree(&pool, expression, &root, nullptr);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(nullptr, root);
    EXPECT_EQ("Invalid AI function expression", status.message());
    const std::string messages = log_sink.messages();
    EXPECT_EQ(1, count_occurrences(messages, "Could not construct AI expression tree"));
    EXPECT_EQ(std::string::npos, messages.find(sentinel));
}

TEST_F(AIFunctionCallExprTest, FactoryRedactsMalformedTreesContainingAIFunctions) {
    for (bool nested : {false, true}) {
        SCOPED_TRACE(nested ? "nested" : "root");
        const std::string sentinel = nested ? "nested-sensitive-prompt" : "root-sensitive-prompt";
        TExpr expression = make_ai_expression(kPromptFid);
        expression.nodes[1].__isset.slot_ref = false;
        expression.nodes[1].__set_node_type(TExprNodeType::STRING_LITERAL);
        TStringLiteral prompt;
        prompt.__set_value(sentinel);
        expression.nodes[1].__set_string_literal(prompt);
        if (nested) {
            expression = wrap_in_builtin_call(std::move(expression));
        }
        // Keep the AI function metadata valid; the extra node makes the tree malformed.
        expression.nodes.emplace_back(make_slot_ref(varchar_type(), 99));

        CapturingLogSink log_sink;
        ObjectPool pool;
        Expr* root = nullptr;
        Status status = ExprFactory::create_expr_tree(&pool, expression, &root, nullptr);

        EXPECT_FALSE(status.ok());
        EXPECT_EQ(nullptr, root);
        EXPECT_EQ("Invalid AI function expression", status.message());
        EXPECT_EQ(std::string::npos, std::string(status.message()).find(sentinel));
        const std::string messages = log_sink.messages();
        EXPECT_EQ(1, count_occurrences(messages, "Could not construct AI expression tree"));
        EXPECT_EQ(std::string::npos, messages.find("Invalid AI function expression"));
        EXPECT_EQ(std::string::npos, messages.find(sentinel));
    }
}

TEST_F(AIFunctionCallExprTest, FunctionNameAndIdAloneAreNotAIMarkers) {
    for (bool use_ai_function_id : {false, true}) {
        SCOPED_TRACE(use_ai_function_id ? "function id" : "function name");
        TExpr expression = make_ai_expression(kPromptFid);
        clear_ai_markers(&expression.nodes.front());
        if (use_ai_function_id) {
            expression.nodes.front().fn.name.function_name = "ordinary_function";
            expression.nodes.front().fn.__set_fid(kPromptFid);
        } else {
            expression.nodes.front().fn.name.function_name = "ai_complete";
        }

        ObjectPool pool;
        Expr* root = nullptr;
        Status status = ExprFactory::create_expr_tree(&pool, expression, &root, nullptr);

        ASSERT_TRUE(status.ok()) << status.message();
        ASSERT_NE(nullptr, root);
        EXPECT_EQ(nullptr, dynamic_cast<AIFunctionCallExpr*>(root));
    }
}

TEST_F(AIFunctionCallExprTest, FactoryClearsPresetOutputsForPartiallyReconstructedAITrees) {
    TExpr expression = make_ai_expression(kPromptFid);
    expression.nodes.emplace_back(make_slot_ref(varchar_type(), 99));

    CapturingLogSink log_sink;
    ObjectPool pool;
    Expr* root = reinterpret_cast<Expr*>(1);
    Status status = ExprFactory::create_expr_tree(&pool, expression, &root, nullptr);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(nullptr, root);
    EXPECT_EQ("Invalid AI function expression", status.message());
    EXPECT_EQ(1, count_occurrences(log_sink.messages(), "Could not construct AI expression tree"));

    ExprContext* context = reinterpret_cast<ExprContext*>(1);
    status = ExprFactory::create_expr_tree(&pool, expression, &context, nullptr);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(nullptr, context);
    EXPECT_EQ("Invalid AI function expression", status.message());
}

TEST_F(AIFunctionCallExprTest, FactoryRejectsAndRedactsDisallowedAIProviderFields) {
    using Mutation = std::function<void(TFunction&, const std::string&)>;
    const std::vector<std::pair<std::string, Mutation>> test_cases = {
            {"hdfs_location", [](TFunction& fn, const std::string& value) { fn.__set_hdfs_location(value); }},
            {"content", [](TFunction& fn, const std::string& value) { fn.__set_content(value); }},
            {"service_url", [](TFunction& fn, const std::string& value) { fn.__set_service_url(value); }},
            {"cloud_configuration",
             [](TFunction& fn, const std::string& value) {
                 TCloudConfiguration cloud;
                 cloud.__set_cloud_properties(std::map<std::string, std::string>{{"secret", value}});
                 fn.__set_cloud_configuration(cloud);
             }},
    };

    for (const auto& [name, mutate] : test_cases) {
        SCOPED_TRACE(name);
        const std::string sentinel = "disallowed-provider-field-" + name;
        TExpr expression = make_ai_expression(kPromptFid);
        mutate(expression.nodes.front().fn, sentinel);

        CapturingLogSink log_sink;
        ObjectPool pool;
        Expr* root = nullptr;
        Status status = ExprFactory::create_expr_tree(&pool, expression, &root, nullptr);

        EXPECT_FALSE(status.ok());
        EXPECT_EQ(nullptr, root);
        EXPECT_NE(std::string::npos, std::string(status.message()).find("Invalid AI function expression"));
        EXPECT_EQ(std::string::npos, std::string(status.message()).find(sentinel));
        const std::string messages = log_sink.messages();
        EXPECT_NE(std::string::npos, messages.find("Could not construct AI expression tree"));
        EXPECT_EQ(std::string::npos, messages.find("Invalid AI function expression"));
        EXPECT_EQ(std::string::npos, messages.find(sentinel));
    }
}

TEST_F(AIFunctionCallExprTest, FactoryRejectsArgumentTypesThatDoNotMatchEachFid) {
    struct TestCase {
        int64_t fid;
        size_t argument_index;
    };
    const std::vector<TestCase> test_cases = {
            {kPromptFid, 0},
            {kPromptFid, 1},
            {kPromptFid, 2},
            {kPromptFid, 3},
    };

    for (const auto& test_case : test_cases) {
        SCOPED_TRACE(test_case.fid);
        TExpr expression = make_ai_expression(test_case.fid);
        expression.nodes.front().fn.arg_types[test_case.argument_index] = TypeDescriptor(TYPE_INT).to_thrift();

        ObjectPool pool;
        Expr* root = nullptr;
        Status status = ExprFactory::create_expr_tree(&pool, expression, &root, nullptr);

        EXPECT_FALSE(status.ok());
        EXPECT_EQ(nullptr, root);
        EXPECT_NE(std::string::npos, std::string(status.message()).find("Invalid AI function expression"));
    }
}

TEST_F(AIFunctionCallExprTest, PrepareRejectsFlattenedChildTypesThatDisagreeWithTheValidatedSignature) {
    struct TestCase {
        int64_t fid;
        size_t argument_index;
    };
    const std::vector<TestCase> test_cases = {
            {kPromptFid, 0},
            {kPromptFid, 1},
            {kPromptFid, 2},
            {kPromptFid, 3},
    };

    for (const auto& test_case : test_cases) {
        SCOPED_TRACE(test_case.fid);
        TExpr expression = make_ai_expression(test_case.fid);
        expression.nodes[test_case.argument_index + 1] =
                make_slot_ref(TypeDescriptor(TYPE_INT), test_case.argument_index + 1);

        ObjectPool pool;
        Expr* root = nullptr;
        Status factory_status = ExprFactory::create_expr_tree(&pool, expression, &root, nullptr);
        ASSERT_TRUE(factory_status.ok()) << factory_status.message();
        ASSERT_NE(nullptr, dynamic_cast<AIFunctionCallExpr*>(root));

        RuntimeState state;
        ExprContext context(root);
        Status prepare_status = context.prepare(&state);
        EXPECT_FALSE(prepare_status.ok());
        EXPECT_NE(std::string::npos, std::string(prepare_status.message()).find("Invalid AI function expression"));
    }
}

TEST_F(AIFunctionCallExprTest, PrepareAcceptsCurrentMainCompatibleStringChildTypes) {
    struct TestCase {
        std::string name;
        TypeDescriptor declared_type;
        TypeDescriptor child_type;
    };
    const std::vector<TestCase> compatible_types = {
            {"narrow varchar child", varchar_type(), TypeDescriptor::create_varchar_type(32)},
            {"default string child", varchar_type(), TypeDescriptor::create_varchar_type(65533)},
            {"char child", varchar_type(), TypeDescriptor::create_char_type(16)},
            {"narrow wire declaration", TypeDescriptor::create_varchar_type(8),
             TypeDescriptor::create_varchar_type(65533)},
    };

    for (const auto& test_case : compatible_types) {
        SCOPED_TRACE(test_case.name);
        TExpr expression = make_ai_expression(kPromptFid);
        expression.nodes.front().fn.arg_types[0] = test_case.declared_type.to_thrift();
        expression.nodes[1].type = test_case.child_type.to_thrift();

        ObjectPool pool;
        Expr* root = nullptr;
        Status factory_status = ExprFactory::create_expr_tree(&pool, expression, &root, nullptr);
        ASSERT_TRUE(factory_status.ok()) << factory_status.message();

        RuntimeState state;
        ExprContext context(root);
        Status prepare_status = context.prepare(&state);
        EXPECT_TRUE(prepare_status.ok()) << prepare_status.message();
    }
}

TEST_F(AIFunctionCallExprTest, PrepareRejectsConcreteOptionTypesThatDisagreeWithTheValidatedFunctionType) {
    TExpr expression = make_ai_expression(kPromptFid);
    expression.nodes[4] = make_slot_ref(TypeDescriptor::create_map_type(varchar_type(), TypeDescriptor(TYPE_INT)), 4);

    ObjectPool pool;
    Expr* root = nullptr;
    Status factory_status = ExprFactory::create_expr_tree(&pool, expression, &root, nullptr);
    ASSERT_TRUE(factory_status.ok()) << factory_status.message();
    ASSERT_NE(nullptr, dynamic_cast<AIFunctionCallExpr*>(root));

    RuntimeState state;
    ExprContext context(root);
    Status prepare_status = context.prepare(&state);
    EXPECT_FALSE(prepare_status.ok());
    EXPECT_EQ("Invalid AI function expression", prepare_status.message());
}

TEST_F(AIFunctionCallExprTest, CloneRetainsOnlyValidatedIdentityAndNeverBecomesConstant) {
    ObjectPool pool;
    TExpr thrift_expression = make_ai_expression(kPromptFid);
    thrift_expression.nodes.front().fn.__set_comment("poison-comment");
    thrift_expression.nodes.front().fn.__set_signature("poison-signature");
    thrift_expression.nodes.front().fn.__set_checksum("poison-checksum");
    thrift_expression.nodes.front().fn.__set_input_type("poison-input-type");
    Expr* root = nullptr;
    ASSERT_TRUE(ExprFactory::create_expr_tree(&pool, thrift_expression, &root, nullptr).ok());
    auto* expression = dynamic_cast<AIFunctionCallExpr*>(root);
    ASSERT_NE(nullptr, expression);

    EXPECT_EQ(kDefaultConfigId, expression->provider_id());
    EXPECT_FALSE(expression->is_constant());
    EXPECT_FALSE(expression->has_fn_ctx());
    EXPECT_TRUE(expression->fn().name.function_name.empty());
    EXPECT_FALSE(expression->fn().__isset.hdfs_location);
    EXPECT_FALSE(expression->fn().__isset.content);
    EXPECT_FALSE(expression->fn().__isset.service_url);
    EXPECT_FALSE(expression->fn().__isset.comment);
    EXPECT_FALSE(expression->fn().__isset.signature);
    EXPECT_FALSE(expression->fn().__isset.checksum);
    EXPECT_FALSE(expression->fn().__isset.input_type);

    ObjectPool clone_pool;
    auto* clone = dynamic_cast<AIFunctionCallExpr*>(Expr::copy(&clone_pool, expression));
    ASSERT_NE(nullptr, clone);
    EXPECT_EQ(kDefaultConfigId, clone->provider_id());
    EXPECT_EQ(expression->get_num_children(), clone->get_num_children());
    EXPECT_FALSE(clone->is_constant());
    EXPECT_FALSE(clone->has_fn_ctx());
    EXPECT_TRUE(clone->fn().name.function_name.empty());
    EXPECT_FALSE(clone->fn().__isset.hdfs_location);
    EXPECT_FALSE(clone->fn().__isset.content);
    EXPECT_FALSE(clone->fn().__isset.service_url);
    EXPECT_FALSE(clone->fn().__isset.comment);
    EXPECT_FALSE(clone->fn().__isset.signature);
    EXPECT_FALSE(clone->fn().__isset.checksum);
    EXPECT_FALSE(clone->fn().__isset.input_type);
}

TEST_F(AIFunctionCallExprTest, OrdinarySynchronousEvaluationFailsClosed) {
    ObjectPool pool;
    auto result = create_ai_expression(&pool, kPromptFid);
    ASSERT_TRUE(result.ok()) << result.status().message();

    auto evaluated = result.value()->evaluate_checked(nullptr, nullptr);

    ASSERT_FALSE(evaluated.ok());
    EXPECT_NE(std::string::npos, std::string(evaluated.status().message()).find("AIProject asynchronous execution"));

    RuntimeState state;
    ExprContext context(result.value());
    ASSERT_TRUE(context.prepare(&state).ok());
    ASSERT_TRUE(context.open(&state).ok());
    auto chunk = make_chunk(1);
    evaluated = context.evaluate(result.value(), chunk.get());
    ASSERT_FALSE(evaluated.ok());
    EXPECT_NE(std::string::npos, std::string(evaluated.status().message()).find("AIProject asynchronous execution"));
}

TEST_F(AIFunctionCallExprTest, PromptRowsUseTheValidatedDefaultModelAndPreserveNullAndEmptySemantics) {
    ObjectPool pool;
    auto expression_result = create_ai_expression(&pool, kPromptFid);
    ASSERT_TRUE(expression_result.ok()) << expression_result.status().message();
    auto* expression = expression_result.value();
    auto children = replace_complete_inputs(
            &pool, expression, {{varchar_type(), make_varchar_column({std::string("first"), std::nullopt, ""})}});

    RuntimeState state;
    ExprContext context(expression);
    ASSERT_TRUE(context.prepare(&state).ok());
    ASSERT_TRUE(context.open(&state).ok());
    auto chunk = make_chunk(3);
    auto batch_result = expression->build_input_batch(&context, chunk.get(), "default-model");

    ASSERT_TRUE(batch_result.ok()) << batch_result.status().message();
    const auto& rows = batch_result->rows;
    ASSERT_EQ(3, rows.size());
    EXPECT_EQ(AIFunctionRowAction::DISPATCH, rows[0].action);
    EXPECT_EQ("default-model", rows[0].model);
    EXPECT_EQ("first", rows[0].prompt);
    EXPECT_EQ(AIFunctionRowAction::SQL_NULL, rows[1].action);
    EXPECT_EQ(AIFunctionRowAction::DISPATCH, rows[2].action);
    EXPECT_EQ("default-model", rows[2].model);
    EXPECT_TRUE(rows[2].prompt.empty());
    ASSERT_NE(nullptr, batch_result->options);
    EXPECT_TRUE(batch_result->options->members().empty());
    EXPECT_EQ(1, children[0]->evaluation_count());
}

TEST_F(AIFunctionCallExprTest, ExplicitModelAndPromptRowsRemainPositionallyStableAndOwning) {
    AIFunctionInputBatch owned;
    {
        ObjectPool pool;
        auto created = create_ai_expression(&pool, kPromptFid);
        ASSERT_TRUE(created.ok());
        auto* expression = created.value();
        replace_children(&pool, expression,
                         {{varchar_type(), make_varchar_column({"first", std::nullopt, ""})},
                          {varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>("", 3)},
                          {varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>("  model  ", 3)},
                          {options_type(), make_const_options(3, true)}});
        RuntimeState state;
        ExprContext context(expression);
        ASSERT_TRUE(context.prepare(&state).ok());
        ASSERT_TRUE(context.open(&state).ok());
        auto batch = expression->build_input_batch(&context, make_chunk(3).get(), "default");
        ASSERT_TRUE(batch.ok()) << batch.status();
        owned = std::move(batch).value();
        context.close(&state);
    }
    ASSERT_EQ(3, owned.rows.size());
    EXPECT_EQ("first", owned.rows[0].prompt);
    EXPECT_EQ("  model  ", owned.rows[0].model);
    EXPECT_EQ(AIFunctionRowAction::SQL_NULL, owned.rows[1].action);
    EXPECT_EQ(AIFunctionRowAction::DISPATCH, owned.rows[2].action);
    EXPECT_TRUE(owned.rows[2].prompt.empty());
}

TEST_F(AIFunctionCallExprTest, PromptOptionsAreConstantPreparedOnceAndSharedByTheBatch) {
    AIFunctionInputBatch owned_batch;
    {
        ObjectPool pool;
        auto expression_result = create_ai_expression(&pool, kPromptFid);
        ASSERT_TRUE(expression_result.ok()) << expression_result.status().message();
        auto* expression = expression_result.value();
        auto children = replace_complete_inputs(
                &pool, expression,
                {{varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>("same prompt", 3)},
                 {options_type(), make_const_options(3)}});

        RuntimeState state;
        ExprContext context(expression);
        ASSERT_TRUE(context.prepare(&state).ok());
        ASSERT_TRUE(context.open(&state).ok());
        auto chunk = make_chunk(3);
        auto batch_result = expression->build_input_batch(&context, chunk.get(), "default-model");

        ASSERT_TRUE(batch_result.ok()) << batch_result.status().message();
        ASSERT_NE(nullptr, batch_result->options);
        ASSERT_EQ(3, batch_result->rows.size());
        for (const auto& row : batch_result->rows) {
            EXPECT_EQ(AIFunctionRowAction::DISPATCH, row.action);
            EXPECT_EQ("default-model", row.model);
            EXPECT_EQ("same prompt", row.prompt);
        }
        EXPECT_EQ(1, children[0]->evaluation_count());
        EXPECT_EQ(1, children[1]->evaluation_count());
        owned_batch = std::move(batch_result).value();
    }

    ASSERT_NE(nullptr, owned_batch.options);
    for (const auto& row : owned_batch.rows) {
        EXPECT_NE(std::string::npos, request_body(owned_batch, row).find(R"("temperature":0.25)"));
    }
}

TEST_F(AIFunctionCallExprTest, FactoryRejectsNonJsonOptions) {
    for (const auto& type : {varchar_type(), TypeDescriptor::create_map_type(varchar_type(), varchar_type()),
                             TypeDescriptor::create_map_type(TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_DOUBLE))}) {
        ObjectPool pool;
        auto wire = make_ai_expression(kPromptFid);
        wire.nodes.front().__set_num_children(4);
        wire.nodes.front().fn.__set_arg_types(
                {varchar_type().to_thrift(), varchar_type().to_thrift(), varchar_type().to_thrift(), type.to_thrift()});
        Expr* root = nullptr;
        Status status = ExprFactory::create_expr_tree(&pool, wire, &root, nullptr);
        EXPECT_FALSE(status.ok());
        EXPECT_EQ(nullptr, root);
        EXPECT_EQ("Invalid AI function expression", status.message());
    }
}

TEST_F(AIFunctionCallExprTest, ConstantJsonExpressionIsPreparedOnceForTheBatch) {
    ObjectPool pool;
    auto wire = make_ai_expression(kPromptFid);
    wire.nodes.front().__set_num_children(4);
    wire.nodes.front().fn.__set_arg_types({varchar_type().to_thrift(), varchar_type().to_thrift(),
                                           varchar_type().to_thrift(), options_type().to_thrift()});
    auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
    ASSERT_TRUE(created.ok()) << created.status();
    auto* expression = created.value();
    replace_children(&pool, expression,
                     {{varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>("prompt", 3)},
                      {varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>("", 3)},
                      {varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>("", 3)}});
    auto json = JsonColumn::create();
    auto value = JsonValue::parse(R"({"guardrails":true,"temperature":0.5})");
    ASSERT_TRUE(value.ok());
    for (int i = 0; i < 3; ++i) {
        json->append(value.value());
    }
    // Constant SQL expressions need not evaluate to a ConstColumn.
    auto* options = pool.add(new CountingColumnExpr(options_type(), std::move(json), true));
    expression->add_child(options);
    RuntimeState state;
    ExprContext context(expression);
    ASSERT_TRUE(context.prepare(&state).ok());
    ASSERT_TRUE(context.open(&state).ok());
    auto chunk = make_chunk(3);
    auto batch = expression->build_input_batch(&context, chunk.get(), "default-model");
    ASSERT_TRUE(batch.ok()) << batch.status();
    EXPECT_EQ(1, options->evaluation_count());
    ASSERT_EQ(3, batch->rows.size());
    ASSERT_NE(nullptr, batch->options);
    for (const auto& row : batch->rows) {
        auto body = request_body(*batch, row);
        EXPECT_NE(std::string::npos, body.find(R"("guardrails":true)"));
        EXPECT_NE(std::string::npos, body.find(R"("temperature":0.5)"));
    }
}

TEST_F(AIFunctionCallExprTest, ProviderPrecedesModelInCanonicalArguments) {
    for (const std::string model : {"chosen-model", ""}) {
        ObjectPool pool;
        TExpr wire = make_ai_expression(kPromptFid);
        wire.nodes.front().__set_num_children(4);
        wire.nodes.front().fn.__set_arg_types({varchar_type().to_thrift(), varchar_type().to_thrift(),
                                               varchar_type().to_thrift(), options_type().to_thrift()});
        auto expression_result = AIFunctionCallExpr::create(&pool, wire.nodes.front());
        ASSERT_TRUE(expression_result.ok()) << expression_result.status();
        auto* expression = expression_result.value();
        replace_children(&pool, expression,
                         {{varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>("hello", 1)},
                          {varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>("provider-name", 1)},
                          {varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>(model, 1)},
                          {options_type(), make_const_options(1)}});
        RuntimeState state;
        ExprContext context(expression);
        ASSERT_TRUE(context.prepare(&state).ok());
        ASSERT_TRUE(context.open(&state).ok());
        auto chunk = make_chunk(1);
        auto batch = expression->build_input_batch(&context, chunk.get(), "provider-default-model");
        ASSERT_TRUE(batch.ok()) << batch.status();
        ASSERT_EQ(1, batch->rows.size());
        EXPECT_EQ(AIFunctionRowAction::DISPATCH, batch->rows[0].action);
        EXPECT_EQ("hello", batch->rows[0].prompt);
        EXPECT_EQ(model.empty() ? "provider-default-model" : model, batch->rows[0].model);
    }
}

TEST_F(AIFunctionCallExprTest, NormalizedSelectorsAndNullOptionsUseDefaults) {
    ObjectPool pool;
    TExpr wire = make_ai_expression(kPromptFid);
    wire.nodes.front().__set_num_children(4);
    wire.nodes.front().fn.__set_arg_types({varchar_type().to_thrift(), varchar_type().to_thrift(),
                                           varchar_type().to_thrift(), options_type().to_thrift()});
    auto expression_result = AIFunctionCallExpr::create(&pool, wire.nodes.front());
    ASSERT_TRUE(expression_result.ok()) << expression_result.status().message();
    auto* expression = expression_result.value();
    // FE normalizes NULL provider/model arguments to empty string defaults.
    replace_children(&pool, expression,
                     {{varchar_type(), make_varchar_column({std::string("prompt")})},
                      {varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>("", 1)},
                      {varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>("", 1)},
                      {options_type(), make_const_options(1, true)}});

    RuntimeState state;
    ExprContext context(expression);
    ASSERT_TRUE(context.prepare(&state).ok());
    ASSERT_TRUE(context.open(&state).ok());
    auto chunk = make_chunk(1);
    auto batch_result = expression->build_input_batch(&context, chunk.get(), "default-model");

    ASSERT_TRUE(batch_result.ok()) << batch_result.status().message();
    ASSERT_EQ(1, batch_result->rows.size());
    EXPECT_EQ(AIFunctionRowAction::DISPATCH, batch_result->rows[0].action);
    EXPECT_EQ("prompt", batch_result->rows[0].prompt);
    EXPECT_EQ("default-model", batch_result->rows[0].model);
    ASSERT_NE(nullptr, batch_result->options);
    EXPECT_TRUE(batch_result->options->members().empty());
}

TEST_F(AIFunctionCallExprTest, NonConstantOptionsAreRejectedAfterEachChildIsEvaluatedOnce) {
    ObjectPool pool;
    auto expression_result = create_ai_expression(&pool, kPromptFid);
    ASSERT_TRUE(expression_result.ok()) << expression_result.status().message();
    auto* expression = expression_result.value();
    auto children = replace_complete_inputs(&pool, expression,
                                            {{varchar_type(), make_varchar_column({std::string("prompt")})},
                                             {options_type(), make_non_const_options()}});

    RuntimeState state;
    ExprContext context(expression);
    ASSERT_TRUE(context.prepare(&state).ok());
    ASSERT_TRUE(context.open(&state).ok());
    auto chunk = make_chunk(1);
    auto batch_result = expression->build_input_batch(&context, chunk.get(), "default-model");

    ASSERT_FALSE(batch_result.ok());
    EXPECT_NE(std::string::npos, std::string(batch_result.status().message()).find("constant"));
    EXPECT_EQ(1, children[0]->evaluation_count());
    EXPECT_EQ(1, children[1]->evaluation_count());
}

TEST_F(AIFunctionCallExprTest, NonConstantChildCardinalityMustMatchTheInputChunk) {
    for (size_t child_rows : {1, 3}) {
        SCOPED_TRACE(child_rows);
        ObjectPool pool;
        auto expression_result = create_ai_expression(&pool, kPromptFid);
        ASSERT_TRUE(expression_result.ok()) << expression_result.status().message();
        auto* expression = expression_result.value();
        auto children = replace_complete_inputs(
                &pool, expression,
                {{varchar_type(), child_rows == 1 ? make_varchar_column({std::string("short")})
                                                  : make_varchar_column({std::string("long-0"), std::string("long-1"),
                                                                         std::string("long-2")})}});

        RuntimeState state;
        ExprContext context(expression);
        ASSERT_TRUE(context.prepare(&state).ok());
        ASSERT_TRUE(context.open(&state).ok());
        auto chunk = make_chunk(2);
        auto batch_result = expression->build_input_batch(&context, chunk.get(), "default-model");

        EXPECT_FALSE(batch_result.ok());
        EXPECT_EQ("Invalid AI function expression", batch_result.status().message());
        EXPECT_EQ(1, children[0]->evaluation_count());
    }
}

TEST_F(AIFunctionCallExprTest, ConstantModelSelectionPreservesNullInputRows) {
    for (const std::string model : {"", "explicit-model"}) {
        ObjectPool pool;
        auto created = create_ai_expression(&pool, kPromptFid);
        ASSERT_TRUE(created.ok());
        auto* expression = created.value();
        auto children = replace_children(&pool, expression,
                                         {{varchar_type(), make_varchar_column({"prompt", std::nullopt})},
                                          {varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>("", 2)},
                                          {varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>(model, 2)},
                                          {options_type(), make_const_options(2)}});
        RuntimeState state;
        ExprContext context(expression);
        ASSERT_TRUE(context.prepare(&state).ok());
        ASSERT_TRUE(context.open(&state).ok());
        auto batch = expression->build_input_batch(&context, make_chunk(2).get(), "default");
        ASSERT_TRUE(batch.ok()) << batch.status();
        ASSERT_EQ(2, batch->rows.size());
        EXPECT_EQ(AIFunctionRowAction::DISPATCH, batch->rows[0].action);
        EXPECT_EQ(model.empty() ? "default" : model, batch->rows[0].model);
        EXPECT_EQ(AIFunctionRowAction::SQL_NULL, batch->rows[1].action);
        for (auto* child : children) EXPECT_EQ(1, child->evaluation_count());
        context.close(&state);
    }
}

TExpr make_extended_ai_expression(int64_t fid, const std::string& name, const TypeDescriptor& result_type,
                                  const std::vector<TypeDescriptor>& arguments, bool provider = false) {
    TExpr expression = make_ai_expression(kPromptFid);
    auto& call = expression.nodes.front();
    call.fn.__set_fid(fid);
    call.fn.name.__set_function_name(name);
    call.fn.__set_ret_type(result_type.to_thrift());
    call.__set_type(result_type.to_thrift());
    auto canonical_arguments = arguments;
    const size_t count = fid == 200001 || fid == 200009 ? 5 : fid == 200004 || fid == 200007 || fid == 200011 ? 3 : 4;
    if (canonical_arguments.size() < count) {
        canonical_arguments.push_back(varchar_type());
        canonical_arguments.push_back(varchar_type());
        if (fid == 200000 || fid == 200001 || fid == 200002 || fid == 200003) {
            canonical_arguments.push_back(options_type());
        }
    }
    call.fn.arg_types.clear();
    for (const auto& argument : canonical_arguments) {
        call.fn.arg_types.push_back(argument.to_thrift());
    }
    call.__set_num_children(canonical_arguments.size());
    call.__set_ai_provider_id(provider ? "provider:00000000-0000-0000-0000-000000000102"
                                       : result_type.type == TYPE_ARRAY ? "provider:default_embedding"
                                                                        : "provider:default_chat");
    expression.nodes.resize(1);
    for (size_t i = 0; i < canonical_arguments.size(); ++i) {
        if (i < arguments.size()) {
            expression.nodes.push_back(make_slot_ref(arguments[i], i + 1));
        } else if (canonical_arguments[i].type == TYPE_JSON) {
            expression.nodes.push_back(default_options_node());
        } else {
            expression.nodes.push_back(make_string_literal_node(varchar_type(), ""));
        }
    }
    return expression;
}

struct AIOverloadCase {
    int64_t fid;
    std::string name;
    TypeDescriptor result;
    std::vector<TypeDescriptor> arguments;
    AIFunctionResultKind result_kind;
    int model_argument = -1;
    int provider_argument = -1;
};

std::vector<AIOverloadCase> ai_overload_cases() {
    const TypeDescriptor text = varchar_type();
    const TypeDescriptor strings = TypeDescriptor::create_array_type(text);
    const TypeDescriptor embedding = TypeDescriptor::create_array_type(TypeDescriptor(TYPE_FLOAT));
    const TypeDescriptor json(TYPE_JSON);
    const TypeDescriptor similarity(TYPE_FLOAT);
    const TypeDescriptor boolean(TYPE_BOOLEAN);
    const TypeDescriptor options = options_type();
    const TypeDescriptor file(TYPE_FILE);
    using Result = AIFunctionResultKind;
    return {
            {200000, "ai_complete", text, {text, text, text, options}, Result::STRING, 2, 1},
            {200001, "ai_complete", text, {text, file, text, text, options}, Result::STRING, 3, 2},
            {200004, "ai_sentiment", text, {text, text, text}, Result::SENTIMENT, 2, 1},
            {200005, "ai_classify", json, {text, strings, text, text}, Result::JSON, 3, 2},
            {200006, "ai_extract", json, {text, strings, text, text}, Result::JSON, 3, 2},
            {200007, "ai_fix_grammar", text, {text, text, text}, Result::STRING, 2, 1},
            {200008, "ai_redact", text, {text, strings, text, text}, Result::STRING, 3, 2},
            {200009, "ai_translate", text, {text, text, text, text, text}, Result::STRING, 4, 3},
            {200010, "ai_similarity", similarity, {text, text, text, text}, Result::SIMILARITY, 3, 2},
            {200011, "ai_summarize", text, {text, text, text}, Result::STRING, 2, 1},
            {200012, "ai_filter", boolean, {text, text, text, text}, Result::BOOLEAN, 3, 2},
            {200002, "ai_embed", embedding, {text, text, text, options}, Result::EMBEDDING, 2, 1},
            {200003, "ai_embed", embedding, {file, text, text, options}, Result::EMBEDDING, 2, 1},
    };
}

TEST_F(AIFunctionCallExprTest, EveryRegisteredOverloadRetainsValidatedSemanticsWhenCloned) {
    const auto cases = ai_overload_cases();
    ASSERT_EQ(13, cases.size());
    for (const auto& test : cases) {
        SCOPED_TRACE(test.fid);
        TExpr wire = make_extended_ai_expression(test.fid, test.name, test.result, test.arguments,
                                                 test.provider_argument >= 0);
        ObjectPool pool;
        auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
        ASSERT_TRUE(created.ok()) << created.status();
        auto* clone = down_cast<AIFunctionCallExpr*>(created.value()->clone(&pool));
        for (auto* expression : {created.value(), clone}) {
            EXPECT_EQ(test.result_kind, expression->result_kind());
            EXPECT_EQ(
                    test.result_kind == AIFunctionResultKind::EMBEDDING ? AICapability::EMBEDDING : AICapability::CHAT,
                    expression->capability());
            EXPECT_EQ(wire.nodes.front().ai_provider_id, expression->provider_id());
            EXPECT_FALSE(expression->is_constant());
            EXPECT_TRUE(expression->fn().name.function_name.empty());
        }
        EXPECT_TRUE(AIFunctionCallExpr::is_ai_function_id(test.fid));
        EXPECT_TRUE(AIFunctionCallExpr::is_ai_function_name(test.name));

        TExprNode invalid = wire.nodes.front();
        invalid.fn.name.function_name = test.name == "ai_complete" ? "ai_embed" : "ai_complete";
        EXPECT_FALSE(AIFunctionCallExpr::create(&pool, invalid).ok());
        invalid = wire.nodes.front();
        invalid.__set_type(TypeDescriptor(TYPE_DOUBLE).to_thrift());
        EXPECT_FALSE(AIFunctionCallExpr::create(&pool, invalid).ok());
        for (size_t argument = 0; argument < test.arguments.size(); ++argument) {
            SCOPED_TRACE(argument);
            invalid = wire.nodes.front();
            invalid.fn.arg_types[argument] = TypeDescriptor(TYPE_INT).to_thrift();
            EXPECT_FALSE(AIFunctionCallExpr::create(&pool, invalid).ok());
        }
    }
    // Unassigned function IDs are not AIProject functions.
    for (int64_t fid : {199999, 200013, 200014, 200100, 200101, 200102, 200103, 200104, 200108, 200109, 200110, 200111,
                        200112, 200113, 200114, 200115, 200116, 200118, 200120, 200122, 200124, 200126, 200128, 200129,
                        200130, 200134, 200138, 200139, 200140, 200141, 200142, 200143, 200144, 200150, 200164}) {
        EXPECT_FALSE(AIFunctionCallExpr::is_ai_function_id(fid)) << fid;
    }
    EXPECT_FALSE(AIFunctionCallExpr::is_ai_function_name("ai_query"));
}

TEST_F(AIFunctionCallExprTest, FileInputsPreserveNullsAndOwnInlineBytes) {
    const std::string png("\x89PNG\r\n\x1a\n", 8);
    auto files = NullableColumn::create(FileColumn::create(), NullColumn::create());
    files->append_datum(FileDatumBuilder::make({}, {}, {}, {}, {}, Slice(png)));
    files->append_nulls(1);
    files->append_datum(FileDatumBuilder::make(Slice("s3://private/object"), 0, -1, {}, {}, {}));
    ObjectPool pool;
    auto wire = make_extended_ai_expression(200001, "ai_complete", varchar_type(),
                                            {varchar_type(), TypeDescriptor(TYPE_FILE)});
    auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
    ASSERT_TRUE(created.ok()) << created.status();
    auto* expression = created.value();
    auto children =
            replace_complete_inputs(&pool, expression,
                                    {{varchar_type(), make_varchar_column({"describe", "unused", std::nullopt})},
                                     {TypeDescriptor(TYPE_FILE), files}});
    RuntimeState state;
    ExprContext context(expression);
    ASSERT_TRUE(context.prepare(&state).ok());
    ASSERT_TRUE(context.open(&state).ok());
    auto chunk = make_chunk(3);
    auto result = expression->build_input_batch(&context, chunk.get(), "vision");
    ASSERT_TRUE(result.ok()) << result.status();
    auto batch = std::move(result).value();
    ASSERT_EQ(3, batch.rows.size());
    EXPECT_EQ(AIFunctionRowAction::DISPATCH, batch.rows[0].action);
    EXPECT_EQ("describe", batch.rows[0].prompt);
    ASSERT_TRUE(batch.rows[0].media.has_value());
    EXPECT_EQ("image/png", batch.rows[0].media->mime_type);
    EXPECT_EQ(png, batch.rows[0].media->bytes);
    EXPECT_EQ(AIFunctionRowAction::SQL_NULL, batch.rows[1].action);
    // A NULL prompt short-circuits even a reference FILE without attempting to read it.
    EXPECT_EQ(AIFunctionRowAction::SQL_NULL, batch.rows[2].action);
    for (auto* child : children) {
        EXPECT_EQ(1, child->evaluation_count());
    }
    files->reset_column();
    EXPECT_EQ(png, batch.rows[0].media->bytes);
}

TEST_F(AIFunctionCallExprTest, FileEmbeddingIsNotAStringCastAndEnforcesInputLimit) {
    const std::string png("\x89PNG\r\n\x1a\n", 8);
    auto files = FileColumn::create();
    files->append_datum(FileDatumBuilder::make({}, {}, {}, Slice("image/png"), {}, Slice(png)));
    ObjectPool pool;
    auto wire = make_extended_ai_expression(200003, "ai_embed",
                                            TypeDescriptor::create_array_type(TypeDescriptor(TYPE_FLOAT)),
                                            {TypeDescriptor(TYPE_FILE)});
    auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
    ASSERT_TRUE(created.ok()) << created.status();
    auto* expression = created.value();
    replace_complete_inputs(&pool, expression, {{TypeDescriptor(TYPE_FILE), ConstColumn::create(files, 2)}});
    RuntimeState state;
    ExprContext context(expression);
    ASSERT_TRUE(context.prepare(&state).ok());
    ASSERT_TRUE(context.open(&state).ok());
    auto chunk = make_chunk(2);
    auto result = expression->build_input_batch(&context, chunk.get(), "embed", png.size());
    ASSERT_TRUE(result.ok()) << result.status();
    auto batch = std::move(result).value();
    ASSERT_EQ(2, batch.rows.size());
    EXPECT_TRUE(batch.rows[0].prompt.empty());
    EXPECT_EQ(png, batch.rows[1].media->bytes);
    auto limited = expression->build_input_batch(&context, chunk.get(), "embed", png.size() - 1);
    ASSERT_TRUE(limited.ok()) << limited.status();
    EXPECT_EQ(AIFunctionRowAction::TERMINAL_ROW_FAILURE, limited->rows[0].action);
    EXPECT_FALSE(limited->rows[0].media.has_value());
    wire.nodes.front().fn.arg_types[0] = varchar_type().to_thrift();
    EXPECT_FALSE(AIFunctionCallExpr::create(&pool, wire.nodes.front()).ok());
}

TEST_F(AIFunctionCallExprTest, FileReferencesAreRejectedWithoutDisclosingLocation) {
    auto files = FileColumn::create();
    files->append_datum(FileDatumBuilder::make(Slice("s3://private/object?credential=secret"), 12, -1, {}, {}, {}));
    ObjectPool pool;
    auto wire = make_extended_ai_expression(200001, "ai_complete", varchar_type(),
                                            {varchar_type(), TypeDescriptor(TYPE_FILE)});
    auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
    ASSERT_TRUE(created.ok()) << created.status();
    auto* expression = created.value();
    replace_complete_inputs(&pool, expression,
                            {{varchar_type(), make_varchar_column({"describe"})}, {TypeDescriptor(TYPE_FILE), files}});
    RuntimeState state;
    ExprContext context(expression);
    ASSERT_TRUE(context.prepare(&state).ok());
    ASSERT_TRUE(context.open(&state).ok());
    auto chunk = make_chunk(1);
    auto batch = expression->build_input_batch(&context, chunk.get(), "vision");
    ASSERT_FALSE(batch.ok());
    EXPECT_TRUE(batch.status().is_not_supported());
    EXPECT_EQ(std::string::npos, batch.status().message().find("private"));
    EXPECT_EQ(std::string::npos, batch.status().message().find("secret"));
}

TEST_F(AIFunctionCallExprTest, MalformedInlineFilesFailOnlyTheirOwnRows) {
    const std::string png("\x89PNG\r\n\x1a\n", 8);
    auto files = FileColumn::create();
    files->append_datum(FileDatumBuilder::make(Slice("s3://private/object"), {}, {}, {}, {}, Slice(png)));
    files->append_datum(FileDatumBuilder::make({}, 0, {}, {}, {}, Slice(png)));
    files->append_datum(FileDatumBuilder::make({}, {}, -1, {}, {}, Slice(png)));
    files->append_datum(FileDatumBuilder::make({}, {}, {}, {}, {}, {}));
    files->append_datum(FileDatumBuilder::make({}, {}, {}, Slice("image/jpeg"), {}, Slice(png)));
    files->append_datum(FileDatumBuilder::make({}, {}, {}, {}, {}, Slice("not-an-image")));
    files->append_datum(FileDatumBuilder::make({}, {}, {}, {}, {}, Slice(png)));
    ObjectPool pool;
    auto wire = make_extended_ai_expression(200003, "ai_embed",
                                            TypeDescriptor::create_array_type(TypeDescriptor(TYPE_FLOAT)),
                                            {TypeDescriptor(TYPE_FILE)});
    auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
    ASSERT_TRUE(created.ok()) << created.status();
    auto* expression = created.value();
    replace_complete_inputs(&pool, expression, {{TypeDescriptor(TYPE_FILE), files}});
    RuntimeState state;
    ExprContext context(expression);
    ASSERT_TRUE(context.prepare(&state).ok());
    ASSERT_TRUE(context.open(&state).ok());
    auto chunk = make_chunk(7);
    auto batch = expression->build_input_batch(&context, chunk.get(), "embed");
    ASSERT_TRUE(batch.ok()) << batch.status();
    ASSERT_EQ(7, batch->rows.size());
    for (size_t row = 0; row < 6; ++row) {
        SCOPED_TRACE(row);
        EXPECT_EQ(AIFunctionRowAction::TERMINAL_ROW_FAILURE, batch->rows[row].action);
        EXPECT_FALSE(batch->rows[row].media.has_value());
    }
    EXPECT_EQ(AIFunctionRowAction::DISPATCH, batch->rows[6].action);
    ASSERT_TRUE(batch->rows[6].media.has_value());
    EXPECT_EQ(png, batch->rows[6].media->bytes);
}

TEST_F(AIFunctionCallExprTest, FilterPromptRequestsUnquotedBooleanTokens) {
    for (bool explicit_model : {false, true}) {
        SCOPED_TRACE(explicit_model);
        ObjectPool pool;
        std::vector<TypeDescriptor> arguments(4, varchar_type());
        auto wire = make_extended_ai_expression(200012, "ai_filter", TypeDescriptor(TYPE_BOOLEAN), arguments);
        auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
        ASSERT_TRUE(created.ok()) << created.status();
        auto* expression = created.value();
        expression->add_child(pool.add(new CountingColumnExpr(varchar_type(), make_varchar_column({"sunny"}))));
        expression->add_child(pool.add(new CountingColumnExpr(varchar_type(), make_varchar_column({"is weather"}))));
        expression->add_child(pool.add(
                new CountingColumnExpr(varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>("", 1))));
        expression->add_child(pool.add(new CountingColumnExpr(
                varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>(explicit_model ? "model" : "", 1))));
        RuntimeState state;
        ExprContext context(expression);
        ASSERT_TRUE(context.prepare(&state).ok());
        ASSERT_TRUE(context.open(&state).ok());
        auto chunk = make_chunk(1);
        auto batch = expression->build_input_batch(&context, chunk.get(), "default-model");
        ASSERT_TRUE(batch.ok()) << batch.status();
        ASSERT_EQ(1, batch->rows.size());
        EXPECT_EQ(AIFunctionRowAction::DISPATCH, batch->rows[0].action);
        EXPECT_NE(std::string::npos, batch->rows[0].prompt.find("exactly true or false"));
        EXPECT_EQ(std::string::npos, batch->rows[0].prompt.find("'true'"));
        EXPECT_EQ(std::string::npos, batch->rows[0].prompt.find("'false'"));
    }
}

TEST_F(AIFunctionCallExprTest, TranslationAutoDetectsNullSourceAndUsesConstantModel) {
    ObjectPool pool;
    auto wire = make_extended_ai_expression(200009, "ai_translate", varchar_type(),
                                            std::vector<TypeDescriptor>(5, varchar_type()));
    auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
    ASSERT_TRUE(created.ok()) << created.status();
    auto* expression = created.value();
    replace_children(&pool, expression,
                     {{varchar_type(), make_varchar_column({"hello", "bonjour", "null target"})},
                      {varchar_type(), make_varchar_column({std::nullopt, "fr", "en"})},
                      {varchar_type(), make_varchar_column({"zh", "en", std::nullopt})},
                      {varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>("", 3)},
                      {varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>("model", 3)}});
    RuntimeState state;
    ExprContext context(expression);
    ASSERT_TRUE(context.prepare(&state).ok());
    ASSERT_TRUE(context.open(&state).ok());
    auto chunk = make_chunk(3);
    auto batch = expression->build_input_batch(&context, chunk.get(), "ignored");
    ASSERT_TRUE(batch.ok()) << batch.status();
    EXPECT_EQ("model", batch->rows[0].model);
    EXPECT_EQ("model", batch->rows[1].model);
    EXPECT_NE(std::string::npos, batch->rows[0].prompt.find("Auto-detect"));
    EXPECT_NE(std::string::npos, batch->rows[1].prompt.find("from fr into en"));
    EXPECT_EQ(AIFunctionRowAction::SQL_NULL, batch->rows[2].action);
}

TEST_F(AIFunctionCallExprTest, TranslationNullAndBlankPoliciesApplyAtEveryDeclaredInputOffset) {
    for (bool explicit_model : {false, true}) {
        SCOPED_TRACE(explicit_model);
        ObjectPool pool;
        auto wire = make_extended_ai_expression(200009, "ai_translate", varchar_type(),
                                                std::vector<TypeDescriptor>(5, varchar_type()));
        auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
        ASSERT_TRUE(created.ok()) << created.status();
        auto* expression = created.value();
        expression->add_child(pool.add(new CountingColumnExpr(
                varchar_type(), make_varchar_column({"hello", "hello", "hello", "hello", std::nullopt, "hello"}))));
        expression->add_child(pool.add(new CountingColumnExpr(
                varchar_type(), make_varchar_column({std::nullopt, "", "English", "English", "English", "English"}))));
        expression->add_child(pool.add(new CountingColumnExpr(
                varchar_type(), make_varchar_column({"Chinese", "Chinese", "", " \t\r\n", "Chinese", "Chinese"}))));
        expression->add_child(pool.add(
                new CountingColumnExpr(varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>("", 6))));
        expression->add_child(pool.add(new CountingColumnExpr(
                varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>(explicit_model ? "model" : "", 6))));
        RuntimeState state;
        ExprContext context(expression);
        ASSERT_TRUE(context.prepare(&state).ok());
        ASSERT_TRUE(context.open(&state).ok());
        auto chunk = make_chunk(6);
        auto batch = expression->build_input_batch(&context, chunk.get(), "snapshot-model");
        ASSERT_TRUE(batch.ok()) << batch.status();
        ASSERT_EQ(6, batch->rows.size());
        EXPECT_EQ(AIFunctionRowAction::DISPATCH, batch->rows[0].action);
        EXPECT_NE(std::string::npos, batch->rows[0].prompt.find("Auto-detect"));
        EXPECT_EQ(AIFunctionRowAction::DISPATCH, batch->rows[1].action);
        EXPECT_NE(std::string::npos, batch->rows[1].prompt.find("Auto-detect"));
        EXPECT_EQ(AIFunctionRowAction::SQL_NULL, batch->rows[2].action);
        EXPECT_EQ(AIFunctionRowAction::SQL_NULL, batch->rows[3].action);
        EXPECT_EQ(AIFunctionRowAction::SQL_NULL, batch->rows[4].action);
        EXPECT_EQ(AIFunctionRowAction::DISPATCH, batch->rows[5].action);
        EXPECT_EQ(explicit_model ? "model" : "snapshot-model", batch->rows[5].model);
    }
}

TEST_F(AIFunctionCallExprTest, ProviderSelectorIsConstantBindingNotTheProviderModel) {
    ObjectPool pool;
    auto wire = make_extended_ai_expression(200000, "ai_complete", varchar_type(),
                                            {varchar_type(), varchar_type(), varchar_type(), options_type()}, true);
    auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
    ASSERT_TRUE(created.ok()) << created.status();
    auto* expression = created.value();
    auto* selector = pool.add(new CountingColumnExpr(
            varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>("text_provider", 2)));
    expression->add_child(
            pool.add(new CountingColumnExpr(varchar_type(), make_varchar_column({"hello", std::nullopt}))));
    expression->add_child(selector);
    expression->add_child(pool.add(new CountingColumnExpr(
            varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>("explicit-model", 2))));
    expression->add_child(pool.add(new CountingColumnExpr(options_type(), make_const_options(2, true))));
    RuntimeState state;
    ExprContext context(expression);
    ASSERT_TRUE(context.prepare(&state).ok());
    ASSERT_TRUE(context.open(&state).ok());
    auto chunk = make_chunk(2);
    auto batch = expression->build_input_batch(&context, chunk.get(), "snapshot-model");
    ASSERT_TRUE(batch.ok()) << batch.status();
    EXPECT_EQ("explicit-model", batch->rows[0].model);
    EXPECT_EQ(1, selector->evaluation_count());
    EXPECT_EQ("hello", batch->rows[0].prompt);
    EXPECT_EQ(AIFunctionRowAction::SQL_NULL, batch->rows[1].action);
}

ColumnPtr make_string_array(std::initializer_list<std::optional<std::string>> values, bool constant, size_t rows = 1) {
    auto elements = ColumnHelper::create_column(varchar_type(), true);
    for (const auto& value : values) {
        if (value.has_value()) {
            elements->append_datum(Datum(Slice(*value)));
        } else {
            elements->append_nulls(1);
        }
    }
    auto offsets = UInt32Column::create();
    offsets->append(0);
    offsets->append(values.size());
    auto array = ArrayColumn::create(std::move(elements), std::move(offsets));
    if (constant) {
        return ConstColumn::create(std::move(array), rows);
    }
    return array;
}

TEST_F(AIFunctionCallExprTest, EveryOverloadAndCloneDispatchesItsDeclaredInputsAndPreservesNullRows) {
    const std::map<std::string, std::string> prompt_markers = {
            {"ai_sentiment", "Analyze the overall sentiment"},
            {"ai_classify", "Classify the following text"},
            {"ai_extract", "Extract a value for each"},
            {"ai_fix_grammar", "Fix the grammar and spelling"},
            {"ai_redact", "Redact personally identifiable information"},
            {"ai_translate", "Translate the following text"},
            {"ai_similarity", "Calculate the semantic similarity"},
            {"ai_summarize", "Summarize the following text"},
            {"ai_filter", "determine if this condition is true"},
    };
    for (const auto& test : ai_overload_cases()) {
        SCOPED_TRACE(test.fid);
        ObjectPool pool;
        TExpr wire = make_extended_ai_expression(test.fid, test.name, test.result, test.arguments,
                                                 test.provider_argument >= 0);
        auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
        ASSERT_TRUE(created.ok()) << created.status();
        const bool has_options = test.arguments.back().type == TYPE_JSON;
        for (size_t index = 0; index < test.arguments.size(); ++index) {
            ColumnPtr column;
            if (static_cast<int>(index) == test.provider_argument) {
                column = ColumnHelper::create_const_column<TYPE_VARCHAR>("test_provider", 2);
            } else if (static_cast<int>(index) == test.model_argument) {
                column = ColumnHelper::create_const_column<TYPE_VARCHAR>("explicit-model", 2);
            } else if (test.arguments[index].type == TYPE_JSON) {
                column = make_const_options(2, true);
            } else if (test.arguments[index].type == TYPE_ARRAY) {
                column = make_string_array({"category"}, true, 2);
            } else if (test.arguments[index].type == TYPE_FILE) {
                auto files = NullableColumn::create(FileColumn::create(), NullColumn::create());
                files->append_datum(FileDatumBuilder::make({}, {}, {}, {}, {}, Slice("\x89PNG\r\n\x1a\n", 8)));
                files->append_nulls(1);
                column = std::move(files);
            } else {
                column = make_varchar_column({"input-" + std::to_string(index), std::nullopt});
            }
            created.value()->add_child(pool.add(new CountingColumnExpr(test.arguments[index], std::move(column))));
        }
        auto* clone = down_cast<AIFunctionCallExpr*>(Expr::copy(&pool, created.value()));
        for (auto* expression : {created.value(), clone}) {
            RuntimeState state;
            ExprContext context(expression);
            ASSERT_TRUE(context.prepare(&state).ok());
            ASSERT_TRUE(context.open(&state).ok());
            auto chunk = make_chunk(2);
            auto batch = expression->build_input_batch(&context, chunk.get(), "snapshot-model");
            ASSERT_TRUE(batch.ok()) << batch.status();
            ASSERT_EQ(2, batch->rows.size());
            EXPECT_EQ(AIFunctionRowAction::DISPATCH, batch->rows[0].action);
            EXPECT_EQ(AIFunctionRowAction::SQL_NULL, batch->rows[1].action);
            EXPECT_EQ(test.model_argument >= 0 ? "explicit-model" : "snapshot-model", batch->rows[0].model);
            const bool has_file = std::any_of(test.arguments.begin(), test.arguments.end(),
                                              [](const TypeDescriptor& type) { return type.type == TYPE_FILE; });
            EXPECT_EQ(has_file, batch->rows[0].media.has_value());
            EXPECT_FALSE(batch->rows[1].media.has_value());
            for (size_t index = 0; index < test.arguments.size(); ++index) {
                if (static_cast<int>(index) != test.model_argument &&
                    static_cast<int>(index) != test.provider_argument && test.arguments[index].type == TYPE_VARCHAR) {
                    EXPECT_NE(std::string::npos, batch->rows[0].prompt.find("input-" + std::to_string(index)));
                }
            }
            auto marker = prompt_markers.find(test.name);
            if (marker != prompt_markers.end()) {
                EXPECT_NE(std::string::npos, batch->rows[0].prompt.find(marker->second));
            }
            if (has_options) {
                ASSERT_NE(nullptr, batch->options);
                EXPECT_TRUE(batch->options->members().empty());
            } else {
                EXPECT_EQ(nullptr, batch->options);
            }
        }
    }
}

TEST_F(AIFunctionCallExprTest, CategoryAndKeyArraysAreValidatedAtOpen) {
    const auto array_type = TypeDescriptor::create_array_type(varchar_type());
    const std::vector<std::pair<std::string, ColumnPtr>> invalid_arrays = {
            {"nonconstant", make_string_array({"category"}, false)},
            {"empty array", make_string_array({}, true)},
            {"SQL NULL", ColumnHelper::create_const_null_column(1)},
            {"NULL element", make_string_array({"category", std::nullopt}, true)},
            {"empty element", make_string_array({"category", ""}, true)},
            {"blank element", make_string_array({"category", " \t\r\n"}, true)},
            {"Unicode blank element", make_string_array({"category", "\u2003\u3000"}, true)},
            {"Java whitespace control element", make_string_array({"category", "\x1c\x1d\x1e\x1f"}, true)},
    };
    for (const auto& test : ai_overload_cases()) {
        if (test.arguments.size() < 2 || test.arguments[1].type != TYPE_ARRAY) {
            continue;
        }
        SCOPED_TRACE(test.fid);
        for (const auto& [name, categories] : invalid_arrays) {
            SCOPED_TRACE(name);
            ObjectPool pool;
            auto wire = make_extended_ai_expression(test.fid, test.name, test.result, test.arguments);
            auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
            ASSERT_TRUE(created.ok()) << created.status();
            auto* expression = created.value();
            replace_inputs(&pool, expression,
                           {{varchar_type(), make_varchar_column({"text"})}, {array_type, categories}});

            RuntimeState state;
            ExprContext context(expression);
            ASSERT_TRUE(context.prepare(&state).ok());
            Status status = context.open(&state);
            EXPECT_TRUE(status.is_invalid_argument()) << status;
            context.close(&state);
        }
    }
}

TEST_F(AIFunctionCallExprTest, BatchBuilderRetainsArrayValidation) {
    const auto array_type = TypeDescriptor::create_array_type(varchar_type());
    for (auto categories : {make_string_array({}, true), make_string_array({"category"}, false)}) {
        ObjectPool pool;
        auto wire = make_extended_ai_expression(200005, "ai_classify", TypeDescriptor(TYPE_JSON),
                                                {varchar_type(), array_type});
        auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
        ASSERT_TRUE(created.ok()) << created.status();
        auto* expression = created.value();
        replace_inputs(&pool, expression,
                       {{varchar_type(), make_varchar_column({"great support"})},
                        {array_type, make_string_array({"category"}, true)}});
        RuntimeState state;
        ExprContext context(expression);
        ASSERT_TRUE(context.prepare(&state).ok());
        ASSERT_TRUE(context.open(&state).ok());

        // Supply an invalid materialized input after open to exercise the batch-builder backstop independently.
        expression->mutable_children()[1] = pool.add(new CountingColumnExpr(array_type, std::move(categories)));
        auto chunk = make_chunk(1);
        auto batch = expression->build_input_batch(&context, chunk.get(), "default-model");
        EXPECT_TRUE(batch.status().is_invalid_argument()) << batch.status();
        context.close(&state);
    }
}

TEST_F(AIFunctionCallExprTest, SemanticConstantArrayDoesNotRequireConstColumnStorage) {
    class ConstantArrayExpr final : public Expr {
    public:
        explicit ConstantArrayExpr(ColumnPtr column)
                : Expr(TypeDescriptor::create_array_type(varchar_type())), _column(std::move(column)) {}
        bool is_constant() const override { return true; }
        Expr* clone(ObjectPool* pool) const override { return pool->add(new ConstantArrayExpr(_column)); }
        StatusOr<ColumnPtr> evaluate_checked(ExprContext*, Chunk*) override {
            ++evaluation_count;
            return _column;
        }

        int evaluation_count = 0;

    private:
        ColumnPtr _column;
    };
    ObjectPool pool;
    const auto array_type = TypeDescriptor::create_array_type(varchar_type());
    auto wire =
            make_extended_ai_expression(200005, "ai_classify", TypeDescriptor(TYPE_JSON), {varchar_type(), array_type});
    auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
    ASSERT_TRUE(created.ok());
    auto* prompt = pool.add(new CountingColumnExpr(varchar_type(), make_varchar_column({"hello"})));
    auto* categories = pool.add(new ConstantArrayExpr(make_string_array({"greeting", "other"}, false)));
    created.value()->add_child(prompt);
    created.value()->add_child(categories);
    append_default_selectors(&pool, created.value());
    RuntimeState state;
    ExprContext context(created.value());
    ASSERT_TRUE(context.prepare(&state).ok());
    EXPECT_EQ(0, categories->evaluation_count);
    ASSERT_TRUE(context.open(&state).ok());
    EXPECT_EQ(1, categories->evaluation_count);
    EXPECT_EQ(0, prompt->evaluation_count());
    auto constant = categories->evaluate_const(&context);
    ASSERT_TRUE(constant.ok()) << constant.status();
    ASSERT_NE(nullptr, constant.value());
    EXPECT_FALSE(constant.value()->is_constant());
    EXPECT_TRUE(constant.value()->is_array());

    ExprContext* clone = nullptr;
    ASSERT_TRUE(context.clone(&state, &pool, &clone).ok());
    ASSERT_TRUE(clone->open(&state).ok());
    EXPECT_EQ(1, categories->evaluation_count);
    EXPECT_EQ(0, prompt->evaluation_count());
    auto chunk = make_chunk(1);
    auto batch = created.value()->build_input_batch(clone, chunk.get(), "default-model");
    ASSERT_TRUE(batch.ok()) << batch.status();
    EXPECT_NE(std::string::npos, batch->rows[0].prompt.find("greeting"));
    EXPECT_NE(std::string::npos, batch->rows[0].prompt.find("hello"));
    clone->close(&state);
    context.close(&state);
}

TEST_F(AIFunctionCallExprTest, OpenPropagatesConstantArrayEvaluationErrors) {
    class FailingArrayExpr final : public Expr {
    public:
        FailingArrayExpr() : Expr(TypeDescriptor::create_array_type(varchar_type())) {}
        bool is_constant() const override { return true; }
        Expr* clone(ObjectPool* pool) const override { return pool->add(new FailingArrayExpr()); }
        StatusOr<ColumnPtr> evaluate_checked(ExprContext*, Chunk*) override {
            return Status::InternalError("constant array evaluation failed");
        }
    };
    ObjectPool pool;
    auto wire = make_extended_ai_expression(200005, "ai_classify", TypeDescriptor(TYPE_JSON),
                                            {varchar_type(), TypeDescriptor::create_array_type(varchar_type())});
    auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
    ASSERT_TRUE(created.ok()) << created.status();
    created.value()->add_child(pool.add(new CountingColumnExpr(varchar_type(), make_varchar_column({"hello"}))));
    created.value()->add_child(pool.add(new FailingArrayExpr()));
    append_default_selectors(&pool, created.value());
    RuntimeState state;
    ExprContext context(created.value());
    ASSERT_TRUE(context.prepare(&state).ok());
    Status status = context.open(&state);
    EXPECT_TRUE(status.is_internal_error()) << status;
    EXPECT_EQ("constant array evaluation failed", status.message());
    context.close(&state);
}

TEST_F(AIFunctionCallExprTest, OpenValidatesNativeStringAndJsonArrayCasts) {
    struct TestCase {
        std::string input;
        bool from_json;
        std::optional<std::string> expected_array;
    };
    std::vector<TestCase> test_cases = {
            {"[a,b]", false, R"(["a", "b"])"},
            {"[null]", false, R"(["null"])"},
            {"['null']", false, R"(["null"])"},
            {"[]", false, std::nullopt},
            {"not an array", false, std::nullopt},
            {"[' ']", false, std::nullopt},
            {R"(["a","b"])", true, R"(["a", "b"])"},
            {R"(["null"])", true, R"(["null"])"},
            {"[null]", true, std::nullopt},
            {"null", true, std::nullopt},
            {"[]", true, std::nullopt},
    };
    // Keep the same whitespace contract as FE String.isBlank(), including the no-break-space exclusions.
    const std::vector<std::string> blank_elements = {
            "",       "\t",     "\n",     "\v",     "\f",     "\r",     "\x1c",   "\x1d",   "\x1e",
            "\x1f",   " ",      "\u1680", "\u2000", "\u2001", "\u2002", "\u2003", "\u2004", "\u2005",
            "\u2006", "\u2008", "\u2009", "\u200a", "\u2028", "\u2029", "\u205f", "\u3000", " \t\u2003\u3000"};
    const std::vector<std::string> nonblank_elements = {
            "\u00a0", "\u2007", "\u202f", "\u200b", "\ufeff", "\u2003category\u3000", "\u2003\U0001f600\u3000"};
    for (bool blank : {true, false}) {
        for (const std::string& element : blank ? blank_elements : nonblank_elements) {
            auto quoted = JsonValue::from_string(Slice(element)).to_string();
            ASSERT_TRUE(quoted.ok()) << quoted.status();
            const std::string json_array = "[" + quoted.value() + "]";
            const auto expected = blank ? std::nullopt : std::make_optional(json_array);
            // STRING casts preserve raw characters; JSON casts decode escaped control characters.
            test_cases.push_back({"[\"" + element + "\"]", false, expected});
            test_cases.push_back({json_array, true, expected});
        }
    }
    const auto array_type = TypeDescriptor::create_array_type(varchar_type());
    auto cast_node = [](const TypeDescriptor& from, const TypeDescriptor& to) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::CAST_EXPR);
        node.__set_opcode(TExprOpcode::CAST);
        node.__set_num_children(1);
        node.__set_type(to.to_thrift());
        node.__set_child_type_desc(from.to_thrift());
        node.__set_child_type(to_thrift(from.type));
        node.__set_is_nullable(true);
        return node;
    };
    for (const auto& test : test_cases) {
        SCOPED_TRACE(test.input);
        SCOPED_TRACE(test.from_json);
        auto wire = make_extended_ai_expression(200006, "ai_extract", TypeDescriptor(TYPE_JSON),
                                                {varchar_type(), array_type, varchar_type(), varchar_type()});
        wire.nodes.resize(2); // Keep the call and input slot; append the array expression and selectors.
        wire.nodes.emplace_back(cast_node(test.from_json ? TypeDescriptor(TYPE_JSON) : varchar_type(), array_type));
        if (test.from_json) {
            wire.nodes.emplace_back(cast_node(varchar_type(), TypeDescriptor(TYPE_JSON)));
        }
        wire.nodes.emplace_back(make_string_literal_node(varchar_type(), test.input));
        wire.nodes.emplace_back(make_string_literal_node(varchar_type(), ""));
        wire.nodes.emplace_back(make_string_literal_node(varchar_type(), "explicit-model"));
        ObjectPool pool;
        Expr* root = nullptr;
        ASSERT_TRUE(ExprFactory::create_expr_tree(&pool, wire, &root, nullptr).ok());
        auto* expression = dynamic_cast<AIFunctionCallExpr*>(root);
        ASSERT_NE(nullptr, expression);
        RuntimeState state;
        ExprContext context(expression);
        ASSERT_TRUE(context.prepare(&state).ok());
        Status status = context.open(&state);
        if (test.expected_array.has_value()) {
            ASSERT_TRUE(status.ok()) << status;
            auto chunk = make_chunk(1);
            chunk->append_column(make_varchar_column({"hello"}), 1);
            auto batch = expression->build_input_batch(&context, chunk.get(), "unused-default");
            ASSERT_TRUE(batch.ok()) << batch.status();
            EXPECT_EQ("explicit-model", batch->rows[0].model);
            EXPECT_NE(std::string::npos, batch->rows[0].prompt.find(*test.expected_array));
        } else {
            EXPECT_TRUE(status.is_invalid_argument()) << status;
        }
        context.close(&state);
    }
}

TEST_F(AIFunctionCallExprTest, EmbeddingOptionsCannotOverrideProviderInput) {
    ObjectPool pool;
    auto wire = make_extended_ai_expression(200002, "ai_embed",
                                            TypeDescriptor::create_array_type(TypeDescriptor(TYPE_FLOAT)),
                                            {varchar_type(), varchar_type(), varchar_type(), options_type()});
    auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
    ASSERT_TRUE(created.ok()) << created.status();
    auto* expression = created.value();
    auto json = JsonColumn::create();
    json->append(JsonValue::parse(R"({"input":123})").value());
    auto options = ConstColumn::create(std::move(json), 1);
    replace_children(&pool, expression,
                     {{varchar_type(), make_varchar_column({"text"})},
                      {varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>("", 1)},
                      {varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>("", 1)},
                      {options_type(), options}});
    RuntimeState state;
    ExprContext context(expression);
    ASSERT_TRUE(context.prepare(&state).ok());
    ASSERT_TRUE(context.open(&state).ok());
    auto chunk = make_chunk(1);
    EXPECT_FALSE(expression->build_input_batch(&context, chunk.get(), "embedding-model").ok());
}

TEST_F(AIFunctionCallExprTest, CapabilityAndResultKindMatchTheDescriptor) {
    ObjectPool pool;
    const auto embedding_type = TypeDescriptor::create_array_type(TypeDescriptor(TYPE_FLOAT));
    auto embedding = make_extended_ai_expression(200002, "ai_embed", embedding_type, {varchar_type()});
    auto created = AIFunctionCallExpr::create(&pool, embedding.nodes.front());
    ASSERT_TRUE(created.ok());
    EXPECT_EQ(AICapability::EMBEDDING, created.value()->capability());
    EXPECT_EQ(AIFunctionResultKind::EMBEDDING, created.value()->result_kind());
    auto* clone = down_cast<AIFunctionCallExpr*>(created.value()->clone(&pool));
    EXPECT_EQ(created.value()->capability(), clone->capability());
    EXPECT_EQ(created.value()->result_kind(), clone->result_kind());
    EXPECT_EQ(created.value()->provider_id(), clone->provider_id());
    auto translate = make_extended_ai_expression(200009, "ai_translate", varchar_type(),
                                                 std::vector<TypeDescriptor>(5, varchar_type()));
    created = AIFunctionCallExpr::create(&pool, translate.nodes.front());
    ASSERT_TRUE(created.ok());
    EXPECT_EQ(AICapability::CHAT, created.value()->capability());
}

TEST_F(AIFunctionCallExprTest, AIProviderBindingRejectsNonconstantOrBlankSelector) {
    for (bool constant : {false, true}) {
        ObjectPool pool;
        auto wire = make_extended_ai_expression(200000, "ai_complete", varchar_type(),
                                                {varchar_type(), varchar_type(), varchar_type(), options_type()}, true);
        auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
        ASSERT_TRUE(created.ok());
        ColumnPtr selector = constant ? ColumnHelper::create_const_column<TYPE_VARCHAR>(" \t", 1)
                                      : make_varchar_column({"text_provider"});
        replace_children(&pool, created.value(),
                         {{varchar_type(), make_varchar_column({"hello"})},
                          {varchar_type(), selector},
                          {varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>("model", 1)},
                          {options_type(), make_const_options(1, true)}});
        RuntimeState state;
        ExprContext context(created.value());
        ASSERT_TRUE(context.prepare(&state).ok());
        ASSERT_TRUE(context.open(&state).ok());
        auto chunk = make_chunk(1);
        EXPECT_FALSE(created.value()->build_input_batch(&context, chunk.get(), "snapshot-model").ok());
    }
}

TEST_F(AIFunctionCallExprTest, ProviderCorrelationKeyIsExplicit) {
    ObjectPool pool;
    auto wire = make_extended_ai_expression(200000, "ai_complete", varchar_type(),
                                            {varchar_type(), varchar_type(), varchar_type(), options_type()}, true);
    auto& node = wire.nodes.front();
    node.__set_ai_provider_id("opaque-correlation-key");
    auto created = AIFunctionCallExpr::create(&pool, node);
    ASSERT_TRUE(created.ok()) << created.status();
    for (const auto key : {"", " \t", "key\n"}) {
        node.__set_ai_provider_id(key);
        EXPECT_FALSE(AIFunctionCallExpr::create(&pool, node).ok()) << key;
    }
}

} // namespace
} // namespace starrocks
