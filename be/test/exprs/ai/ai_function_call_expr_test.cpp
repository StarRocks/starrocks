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
#include "column/fixed_length_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/expr_factory.h"
#include "gen_cpp/Exprs_types.h"
#include "platform/llm/ai_provider.h"
#include "platform/llm/openai_compatible_provider.h"
#include "runtime/runtime_state.h"
#include "types/type_descriptor.h"

namespace starrocks {
namespace {

constexpr int64_t kPromptFid = 200100;
constexpr int64_t kPromptOptionsFid = 200101;
constexpr int64_t kModelPromptFid = 200102;
constexpr int64_t kModelPromptOptionsFid = 200103;
constexpr std::string_view kSystemConfigId = "__system_chat__";

class CountingColumnExpr final : public Expr {
public:
    CountingColumnExpr(TypeDescriptor type, ColumnPtr column) : Expr(std::move(type)), _column(std::move(column)) {}

    Expr* clone(ObjectPool* pool) const override { return pool->add(new CountingColumnExpr(type(), _column)); }

    bool is_constant() const override { return _column->is_constant(); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext*, Chunk*) override {
        ++_evaluation_count;
        return _column;
    }

    int evaluation_count() const { return _evaluation_count; }

private:
    ColumnPtr _column;
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
    return TypeDescriptor::create_map_type(varchar_type(), TypeDescriptor(TYPE_DOUBLE));
}

TypeDescriptor wire_normalized_untyped_options_type() {
    return TypeDescriptor::create_map_type(TypeDescriptor(TYPE_BOOLEAN), TypeDescriptor(TYPE_BOOLEAN));
}

std::vector<TypeDescriptor> signature_argument_types(int64_t fid) {
    switch (fid) {
    case kPromptFid:
        return {varchar_type()};
    case kPromptOptionsFid:
        return {varchar_type(), options_type()};
    case kModelPromptFid:
        return {varchar_type(), varchar_type()};
    case kModelPromptOptionsFid:
        return {varchar_type(), varchar_type(), options_type()};
    default:
        ADD_FAILURE() << "unexpected test fid " << fid;
        return {};
    }
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
    function.__set_ai_model_source(TAIModelSource::SYSTEM);

    TExprNode call;
    call.__set_node_type(TExprNodeType::FUNCTION_CALL);
    call.__set_num_children(static_cast<int32_t>(arguments.size()));
    call.__set_type(varchar_type().to_thrift());
    call.__set_is_nullable(true);
    call.__set_fn(function);
    call.__set_ai_model_config_id(std::string(kSystemConfigId));

    TExpr expression;
    expression.nodes.emplace_back(std::move(call));
    for (size_t i = 0; i < arguments.size(); ++i) {
        expression.nodes.emplace_back(make_slot_ref(arguments[i], static_cast<int>(i + 1)));
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

TExpr make_string_expression(std::string value) {
    TExpr expression;
    expression.nodes.emplace_back(make_string_literal_node(varchar_type(), std::move(value)));
    return expression;
}

TExpr make_ai_expression_with_literal_options() {
    TExpr expression = make_ai_expression(kPromptOptionsFid);
    TExprNode call = std::move(expression.nodes.front());
    expression.nodes.clear();
    expression.nodes.emplace_back(std::move(call));
    expression.nodes.emplace_back(make_string_literal_node(varchar_type(), "prompt"));

    TExprNode map;
    map.__set_node_type(TExprNodeType::MAP_EXPR);
    map.__set_num_children(2);
    map.__set_type(options_type().to_thrift());
    map.__set_is_nullable(false);
    expression.nodes.emplace_back(std::move(map));
    expression.nodes.emplace_back(make_string_literal_node(varchar_type(), "temperature"));

    TExprNode value;
    value.__set_node_type(TExprNodeType::FLOAT_LITERAL);
    value.__set_num_children(0);
    value.__set_type(TypeDescriptor(TYPE_DOUBLE).to_thrift());
    value.__set_is_nullable(false);
    TFloatLiteral float_literal;
    float_literal.__set_value(0.5);
    value.__set_float_literal(float_literal);
    expression.nodes.emplace_back(std::move(value));
    return expression;
}

void clear_ai_markers(TExprNode* node) {
    node->fn.binary_type = TFunctionBinaryType::BUILTIN;
    node->fn.name.function_name = "ordinary_function";
    node->fn.__isset.ai_model_source = false;
    node->fn.__isset.fid = false;
    node->__isset.ai_model_config_id = false;
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
    auto keys = BinaryColumn::create();
    keys->append("temperature");
    auto values = DoubleColumn::create();
    values->append(0.25);
    auto offsets = UInt32Column::create();
    offsets->append(0);
    offsets->append(1);
    auto map = MapColumn::create(ColumnHelper::cast_to_nullable_column(std::move(keys)),
                                 ColumnHelper::cast_to_nullable_column(std::move(values)), std::move(offsets));
    if (!top_level_null) {
        return ConstColumn::create(std::move(map), rows);
    }

    auto nulls = NullColumn::create();
    nulls->append(1);
    auto nullable = NullableColumn::create(std::move(map), std::move(nulls));
    return ConstColumn::create(std::move(nullable), rows);
}

ColumnPtr make_non_const_options() {
    auto keys = BinaryColumn::create();
    keys->append("temperature");
    auto values = DoubleColumn::create();
    values->append(0.25);
    auto offsets = UInt32Column::create();
    offsets->append(0);
    offsets->append(1);
    return MapColumn::create(ColumnHelper::cast_to_nullable_column(std::move(keys)),
                             ColumnHelper::cast_to_nullable_column(std::move(values)), std::move(offsets));
}

ColumnPtr make_const_wire_normalized_untyped_options(size_t rows, bool empty) {
    auto keys = BooleanColumn::create();
    auto values = BooleanColumn::create();
    if (!empty) {
        keys->append(true);
        values->append(false);
    }
    auto offsets = UInt32Column::create();
    offsets->append(0);
    offsets->append(empty ? 0 : 1);
    auto map = MapColumn::create(ColumnHelper::cast_to_nullable_column(std::move(keys)),
                                 ColumnHelper::cast_to_nullable_column(std::move(values)), std::move(offsets));
    return ConstColumn::create(std::move(map), rows);
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
        for (const std::string& function_name : {"ai_complete", "AI_COMPLETE", "Ai_Complete"}) {
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
            EXPECT_TRUE(ai->requires_default_model());
            EXPECT_EQ(kSystemConfigId, ai->model_config_id());
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
            {"missing source", [](TExprNode& node) { node.fn.__isset.ai_model_source = false; }},
            {"invalid source",
             [](TExprNode& node) { node.fn.ai_model_source = static_cast<TAIModelSource::type>(127); }},
            {"varargs", [](TExprNode& node) { node.fn.has_var_args = true; }},
            {"missing config", [](TExprNode& node) { node.__isset.ai_model_config_id = false; }},
            {"wrong config", [](TExprNode& node) { node.ai_model_config_id = "sensitive-ai-config"; }},
            {"missing nullable", [](TExprNode& node) { node.__isset.is_nullable = false; }},
            {"non-nullable", [](TExprNode& node) { node.__set_is_nullable(false); }},
            {"wrong node return", [](TExprNode& node) { node.type = TypeDescriptor(TYPE_INT).to_thrift(); }},
            {"wrong function return", [](TExprNode& node) { node.fn.ret_type = TypeDescriptor(TYPE_INT).to_thrift(); }},
            {"wrong arity", [](TExprNode& node) { node.num_children = 2; }},
            {"wrong argument count",
             [](TExprNode& node) { node.fn.arg_types.emplace_back(varchar_type().to_thrift()); }},
            {"wrong argument type",
             [](TExprNode& node) { node.fn.arg_types[0] = TypeDescriptor(TYPE_INT).to_thrift(); }},
            {"mismatched fid", [](TExprNode& node) { node.fn.fid = kModelPromptFid; }},
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
    ai_call.ai_model_config_id = "invalid-config";
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

TEST_F(AIFunctionCallExprTest, FactoryUsesExplicitAIMarkersToRedactMalformedTrees) {
    struct TestCase {
        const char* name;
        std::function<void(TExprNode&)> set_marker;
    };
    const std::vector<TestCase> test_cases = {
            {"binary type", [](TExprNode& node) { node.fn.binary_type = TFunctionBinaryType::AI; }},
            {"config id", [](TExprNode& node) { node.__set_ai_model_config_id(std::string(kSystemConfigId)); }},
            {"model source", [](TExprNode& node) { node.fn.__set_ai_model_source(TAIModelSource::SYSTEM); }},
    };

    for (const auto& test_case : test_cases) {
        SCOPED_TRACE(test_case.name);
        const std::string sentinel = std::string("marker-sensitive-") + test_case.name;
        TExpr expression = make_ai_expression(kPromptFid);
        TExprNode& node = expression.nodes.front();
        node.fn.binary_type = TFunctionBinaryType::BUILTIN;
        node.fn.name.function_name = "ordinary_function";
        node.fn.__isset.ai_model_source = false;
        node.fn.__isset.fid = false;
        node.__isset.ai_model_config_id = false;
        test_case.set_marker(node);
        expression.nodes[1].__isset.slot_ref = false;
        expression.nodes[1].__set_node_type(TExprNodeType::STRING_LITERAL);
        TStringLiteral prompt;
        prompt.__set_value(sentinel);
        expression.nodes[1].__set_string_literal(prompt);
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

TEST_F(AIFunctionCallExprTest, FactoryRejectsAIMarkersOnRootAndNestedNonFunctionNodes) {
    for (bool nested : {false, true}) {
        SCOPED_TRACE(nested ? "nested" : "root");
        const std::string sentinel = nested ? "nested-marker-sensitive-prompt" : "root-marker-sensitive-prompt";
        TExpr expression = make_string_expression(sentinel);
        if (nested) {
            expression = wrap_in_builtin_call(std::move(expression));
        }
        expression.nodes[nested ? 1 : 0].__set_ai_model_config_id(std::string(kSystemConfigId));

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

TEST_F(AIFunctionCallExprTest, FactoryRejectsNonVarcharOptionKeys) {
    TExpr expression = make_ai_expression(kPromptOptionsFid);
    expression.nodes.front().fn.arg_types[1] =
            TypeDescriptor::create_map_type(TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_DOUBLE)).to_thrift();

    ObjectPool pool;
    Expr* root = nullptr;
    Status status = ExprFactory::create_expr_tree(&pool, expression, &root, nullptr);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(nullptr, root);
    EXPECT_NE(std::string::npos, std::string(status.message()).find("Invalid AI function expression"));
}

TEST_F(AIFunctionCallExprTest, FactoryAcceptsOnlyTheWireNormalizedUntypedOptionsShape) {
    TExpr expression = make_ai_expression(kPromptOptionsFid);
    const auto normalized_type = wire_normalized_untyped_options_type().to_thrift();
    expression.nodes.front().fn.arg_types[1] = normalized_type;
    expression.nodes[2].type = normalized_type;

    ObjectPool pool;
    Expr* root = nullptr;
    Status status = ExprFactory::create_expr_tree(&pool, expression, &root, nullptr);

    ASSERT_TRUE(status.ok()) << status.message();
    ASSERT_NE(nullptr, dynamic_cast<AIFunctionCallExpr*>(root));
}

TEST_F(AIFunctionCallExprTest, FactoryRejectsArgumentTypesThatDoNotMatchEachFid) {
    struct TestCase {
        int64_t fid;
        size_t argument_index;
    };
    const std::vector<TestCase> test_cases = {
            {kPromptFid, 0},
            {kPromptOptionsFid, 1},
            {kModelPromptFid, 0},
            {kModelPromptOptionsFid, 2},
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
            {kPromptOptionsFid, 1},
            {kModelPromptFid, 1},
            {kModelPromptOptionsFid, 2},
    };

    for (const auto& test_case : test_cases) {
        SCOPED_TRACE(test_case.fid);
        TExpr expression = make_ai_expression(test_case.fid);
        expression.nodes[test_case.argument_index + 1].type = TypeDescriptor(TYPE_INT).to_thrift();

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
    TExpr expression = make_ai_expression(kPromptOptionsFid);
    expression.nodes[2].type = TypeDescriptor::create_map_type(varchar_type(), TypeDescriptor(TYPE_INT)).to_thrift();

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

TEST_F(AIFunctionCallExprTest, OptionCompatibilityIgnoresSafeStringLengthsButNeverWidensTheKeyType) {
    {
        TExpr expression = make_ai_expression(kPromptOptionsFid);
        expression.nodes.front().fn.arg_types[1] =
                TypeDescriptor::create_map_type(varchar_type(), varchar_type()).to_thrift();
        expression.nodes[2].type = TypeDescriptor::create_map_type(TypeDescriptor::create_varchar_type(12),
                                                                   TypeDescriptor::create_char_type(24))
                                           .to_thrift();

        ObjectPool pool;
        Expr* root = nullptr;
        ASSERT_TRUE(ExprFactory::create_expr_tree(&pool, expression, &root, nullptr).ok());
        RuntimeState state;
        ExprContext context(root);
        Status status = context.prepare(&state);
        EXPECT_TRUE(status.ok()) << status.message();
    }

    {
        TExpr expression = make_ai_expression(kPromptOptionsFid);
        expression.nodes[2].type =
                TypeDescriptor::create_map_type(TypeDescriptor::create_char_type(12), TypeDescriptor(TYPE_DOUBLE))
                        .to_thrift();

        ObjectPool pool;
        Expr* root = nullptr;
        ASSERT_TRUE(ExprFactory::create_expr_tree(&pool, expression, &root, nullptr).ok());
        RuntimeState state;
        ExprContext context(root);
        Status status = context.prepare(&state);
        EXPECT_FALSE(status.ok());
        EXPECT_EQ("Invalid AI function expression", status.message());
    }
}

TEST_F(AIFunctionCallExprTest, PrepareRejectsUnsafeKeysAtEveryNestedMapDepth) {
    struct TestCase {
        std::string name;
        TypeDescriptor nested_type;
    };
    const std::vector<TestCase> unsafe_nested_types = {
            {"char key in an empty nested map",
             TypeDescriptor::create_map_type(TypeDescriptor::create_char_type(12), TypeDescriptor(TYPE_INT))},
            {"boolean key hidden by a nested SQL NULL",
             TypeDescriptor::create_map_type(TypeDescriptor(TYPE_BOOLEAN), TypeDescriptor(TYPE_INT))},
    };

    for (const auto& test_case : unsafe_nested_types) {
        SCOPED_TRACE(test_case.name);
        const auto options_type = TypeDescriptor::create_map_type(varchar_type(), test_case.nested_type);
        TExpr expression = make_ai_expression(kPromptOptionsFid);
        expression.nodes.front().fn.arg_types[1] = options_type.to_thrift();
        expression.nodes[2].type = options_type.to_thrift();

        ObjectPool pool;
        Expr* root = nullptr;
        ASSERT_TRUE(ExprFactory::create_expr_tree(&pool, expression, &root, nullptr).ok());
        RuntimeState state;
        ExprContext context(root);
        Status status = context.prepare(&state);
        EXPECT_FALSE(status.ok());
        EXPECT_EQ("Invalid AI function expression", status.message());
    }
}

TEST_F(AIFunctionCallExprTest, PrepareAcceptsExactNestedWireNormalizedEmptyMapType) {
    const auto normalized_empty =
            TypeDescriptor::create_map_type(TypeDescriptor(TYPE_BOOLEAN), TypeDescriptor(TYPE_BOOLEAN));
    const auto options_type = TypeDescriptor::create_map_type(varchar_type(), normalized_empty);
    TExpr expression = make_ai_expression(kPromptOptionsFid);
    expression.nodes.front().fn.arg_types[1] = options_type.to_thrift();
    expression.nodes[2].type = options_type.to_thrift();

    ObjectPool pool;
    Expr* root = nullptr;
    ASSERT_TRUE(ExprFactory::create_expr_tree(&pool, expression, &root, nullptr).ok());
    RuntimeState state;
    ExprContext context(root);
    Status status = context.prepare(&state);
    EXPECT_TRUE(status.ok()) << status.message();
}

TEST_F(AIFunctionCallExprTest, CloneRetainsOnlyValidatedIdentityAndNeverBecomesConstant) {
    const std::vector<std::pair<int64_t, bool>> signatures = {
            {kPromptFid, true},
            {kPromptOptionsFid, true},
            {kModelPromptFid, false},
            {kModelPromptOptionsFid, false},
    };

    for (const auto& [fid, requires_default_model] : signatures) {
        SCOPED_TRACE(fid);
        ObjectPool pool;
        TExpr thrift_expression = make_ai_expression(fid);
        thrift_expression.nodes.front().fn.__set_comment("poison-comment");
        thrift_expression.nodes.front().fn.__set_signature("poison-signature");
        thrift_expression.nodes.front().fn.__set_checksum("poison-checksum");
        thrift_expression.nodes.front().fn.__set_input_type("poison-input-type");
        Expr* root = nullptr;
        ASSERT_TRUE(ExprFactory::create_expr_tree(&pool, thrift_expression, &root, nullptr).ok());
        auto* expression = dynamic_cast<AIFunctionCallExpr*>(root);
        ASSERT_NE(nullptr, expression);

        EXPECT_EQ(requires_default_model, expression->requires_default_model());
        EXPECT_EQ(kSystemConfigId, expression->model_config_id());
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
        EXPECT_EQ(requires_default_model, clone->requires_default_model());
        EXPECT_EQ(kSystemConfigId, clone->model_config_id());
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
    auto children = replace_children(&pool, expression,
                                     {{varchar_type(), make_varchar_column({std::string("first"), std::nullopt, ""})}});

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
    EXPECT_EQ(nullptr, batch_result->options);
    EXPECT_EQ(1, children[0]->evaluation_count());
}

TEST_F(AIFunctionCallExprTest, ExplicitModelAndPromptRowsRemainPositionallyStableAndOwning) {
    AIFunctionInputBatch owned_batch;
    {
        ObjectPool pool;
        auto expression_result = create_ai_expression(&pool, kModelPromptFid);
        ASSERT_TRUE(expression_result.ok()) << expression_result.status().message();
        auto* expression = expression_result.value();
        auto children = replace_children(
                &pool, expression,
                {{varchar_type(),
                  make_varchar_column({std::string("m0"), std::nullopt, std::string(" \t"), std::string("m3"),
                                       std::string(" \t"), std::string("  preserved-model  "), std::string("")})},
                 {varchar_type(),
                  make_varchar_column({std::string("p0"), std::string("p1"), std::string("p2"), std::nullopt,
                                       std::nullopt, std::string(""), std::string("exact-empty-model")})}});

        RuntimeState state;
        ExprContext context(expression);
        ASSERT_TRUE(context.prepare(&state).ok());
        ASSERT_TRUE(context.open(&state).ok());
        auto chunk = make_chunk(7);
        auto batch_result = expression->build_input_batch(&context, chunk.get(), "unused-default");

        ASSERT_TRUE(batch_result.ok()) << batch_result.status().message();
        ASSERT_EQ(7, batch_result->rows.size());
        EXPECT_EQ(AIFunctionRowAction::DISPATCH, batch_result->rows[0].action);
        EXPECT_EQ("m0", batch_result->rows[0].model);
        EXPECT_EQ("p0", batch_result->rows[0].prompt);
        EXPECT_EQ(AIFunctionRowAction::SQL_NULL, batch_result->rows[1].action);
        EXPECT_EQ(AIFunctionRowAction::TERMINAL_ROW_FAILURE, batch_result->rows[2].action);
        EXPECT_EQ(AIFunctionRowAction::SQL_NULL, batch_result->rows[3].action);
        EXPECT_EQ(AIFunctionRowAction::SQL_NULL, batch_result->rows[4].action);
        EXPECT_EQ(AIFunctionRowAction::DISPATCH, batch_result->rows[5].action);
        EXPECT_EQ("  preserved-model  ", batch_result->rows[5].model);
        EXPECT_TRUE(batch_result->rows[5].prompt.empty());
        EXPECT_EQ(AIFunctionRowAction::TERMINAL_ROW_FAILURE, batch_result->rows[6].action);
        EXPECT_EQ(1, children[0]->evaluation_count());
        EXPECT_EQ(1, children[1]->evaluation_count());
        owned_batch = std::move(batch_result).value();
    }

    ASSERT_EQ(7, owned_batch.rows.size());
    EXPECT_EQ("m0", owned_batch.rows[0].model);
    EXPECT_EQ("p0", owned_batch.rows[0].prompt);
    EXPECT_EQ("  preserved-model  ", owned_batch.rows[5].model);
}

TEST_F(AIFunctionCallExprTest, PromptOptionsAreConstantPreparedOnceAndSharedByTheBatch) {
    AIFunctionInputBatch owned_batch;
    {
        ObjectPool pool;
        auto expression_result = create_ai_expression(&pool, kPromptOptionsFid);
        ASSERT_TRUE(expression_result.ok()) << expression_result.status().message();
        auto* expression = expression_result.value();
        auto children =
                replace_children(&pool, expression,
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

TEST_F(AIFunctionCallExprTest, SemanticConstantMapExprIsPreparedOnceEvenWhenItsColumnIsNotConst) {
    ObjectPool pool;
    TExpr thrift_expression = make_ai_expression_with_literal_options();
    Expr* root = nullptr;
    ASSERT_TRUE(ExprFactory::create_expr_tree(&pool, thrift_expression, &root, nullptr).ok());
    auto* expression = dynamic_cast<AIFunctionCallExpr*>(root);
    ASSERT_NE(nullptr, expression);
    ASSERT_EQ(2, expression->get_num_children());
    Expr* options_expression = expression->get_child(1);
    ASSERT_TRUE(options_expression->is_constant());

    RuntimeState state;
    ExprContext context(expression);
    ASSERT_TRUE(context.prepare(&state).ok());
    ASSERT_TRUE(context.open(&state).ok());
    auto chunk = make_chunk(3);
    auto evaluated_options = context.evaluate(options_expression, chunk.get());
    ASSERT_TRUE(evaluated_options.ok()) << evaluated_options.status().message();
    EXPECT_FALSE(evaluated_options.value()->is_constant());
    EXPECT_EQ(chunk->num_rows(), evaluated_options.value()->size());

    auto batch_result = expression->build_input_batch(&context, chunk.get(), "default-model");

    ASSERT_TRUE(batch_result.ok()) << batch_result.status().message();
    ASSERT_NE(nullptr, batch_result->options);
    ASSERT_EQ(3, batch_result->rows.size());
    for (const auto& row : batch_result->rows) {
        EXPECT_EQ(AIFunctionRowAction::DISPATCH, row.action);
        EXPECT_NE(std::string::npos, request_body(*batch_result, row).find(R"("temperature":0.5)"));
    }
}

TEST_F(AIFunctionCallExprTest, NullTopLevelOptionsMeanAnEmptySharedOptionSet) {
    ObjectPool pool;
    auto expression_result = create_ai_expression(&pool, kPromptOptionsFid);
    ASSERT_TRUE(expression_result.ok()) << expression_result.status().message();
    auto* expression = expression_result.value();
    replace_children(&pool, expression,
                     {{varchar_type(), make_varchar_column({std::string("prompt")})},
                      {options_type(), make_const_options(1, true)}});

    RuntimeState state;
    ExprContext context(expression);
    ASSERT_TRUE(context.prepare(&state).ok());
    ASSERT_TRUE(context.open(&state).ok());
    auto chunk = make_chunk(1);
    auto batch_result = expression->build_input_batch(&context, chunk.get(), "default-model");

    ASSERT_TRUE(batch_result.ok()) << batch_result.status().message();
    ASSERT_NE(nullptr, batch_result->options);
    ASSERT_EQ(1, batch_result->rows.size());
    EXPECT_EQ(AIFunctionRowAction::DISPATCH, batch_result->rows[0].action);
    EXPECT_EQ(std::string::npos, request_body(*batch_result, batch_result->rows[0]).find("temperature"));
}

TEST_F(AIFunctionCallExprTest, WireNormalizedUntypedOptionsRequireAnActuallyEmptyMap) {
    for (bool empty : {true, false}) {
        SCOPED_TRACE(empty ? "empty" : "non-empty");
        ObjectPool pool;
        TExpr thrift_expression = make_ai_expression(kPromptOptionsFid);
        const auto normalized_type = wire_normalized_untyped_options_type().to_thrift();
        thrift_expression.nodes.front().fn.arg_types[1] = normalized_type;
        thrift_expression.nodes[2].type = normalized_type;

        Expr* root = nullptr;
        ASSERT_TRUE(ExprFactory::create_expr_tree(&pool, thrift_expression, &root, nullptr).ok());
        auto* expression = dynamic_cast<AIFunctionCallExpr*>(root);
        ASSERT_NE(nullptr, expression);
        replace_children(
                &pool, expression,
                {{varchar_type(), make_varchar_column({std::string("prompt")})},
                 {wire_normalized_untyped_options_type(), make_const_wire_normalized_untyped_options(1, empty)}});

        RuntimeState state;
        ExprContext context(expression);
        ASSERT_TRUE(context.prepare(&state).ok());
        ASSERT_TRUE(context.open(&state).ok());
        auto chunk = make_chunk(1);
        auto batch_result = expression->build_input_batch(&context, chunk.get(), "default-model");

        if (empty) {
            ASSERT_TRUE(batch_result.ok()) << batch_result.status().message();
            ASSERT_NE(nullptr, batch_result->options);
            EXPECT_EQ(std::string::npos, request_body(*batch_result, batch_result->rows[0]).find("temperature"));
        } else {
            ASSERT_FALSE(batch_result.ok());
            EXPECT_EQ("AI provider options are invalid", batch_result.status().message());
        }
    }
}

TEST_F(AIFunctionCallExprTest, NonConstantOptionsAreRejectedAfterEachChildIsEvaluatedOnce) {
    ObjectPool pool;
    auto expression_result = create_ai_expression(&pool, kPromptOptionsFid);
    ASSERT_TRUE(expression_result.ok()) << expression_result.status().message();
    auto* expression = expression_result.value();
    auto children = replace_children(&pool, expression,
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
        auto children = replace_children(
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

TEST_F(AIFunctionCallExprTest, ExplicitModelPromptOptionsUseFidDefinedChildPositions) {
    ObjectPool pool;
    auto expression_result = create_ai_expression(&pool, kModelPromptOptionsFid);
    ASSERT_TRUE(expression_result.ok()) << expression_result.status().message();
    auto* expression = expression_result.value();
    auto children =
            replace_children(&pool, expression,
                             {{varchar_type(), make_varchar_column({std::string("model-a"), std::string("model-b")})},
                              {varchar_type(), make_varchar_column({std::string("prompt-a"), std::nullopt})},
                              {options_type(), make_const_options(2)}});

    RuntimeState state;
    ExprContext context(expression);
    ASSERT_TRUE(context.prepare(&state).ok());
    ASSERT_TRUE(context.open(&state).ok());
    auto chunk = make_chunk(2);
    auto batch_result = expression->build_input_batch(&context, chunk.get(), "unused-default");

    ASSERT_TRUE(batch_result.ok()) << batch_result.status().message();
    ASSERT_EQ(2, batch_result->rows.size());
    EXPECT_EQ(AIFunctionRowAction::DISPATCH, batch_result->rows[0].action);
    EXPECT_EQ("model-a", batch_result->rows[0].model);
    EXPECT_EQ("prompt-a", batch_result->rows[0].prompt);
    EXPECT_EQ(AIFunctionRowAction::SQL_NULL, batch_result->rows[1].action);
    ASSERT_NE(nullptr, batch_result->options);
    EXPECT_EQ(1, children[0]->evaluation_count());
    EXPECT_EQ(1, children[1]->evaluation_count());
    EXPECT_EQ(1, children[2]->evaluation_count());
}

TExpr make_extended_ai_expression(int64_t fid, const std::string& name, const TypeDescriptor& result_type,
                                  const std::vector<TypeDescriptor>& arguments, bool ai_model = false) {
    TExpr expression = make_ai_expression(kPromptFid);
    auto& call = expression.nodes.front();
    call.fn.__set_fid(fid);
    call.fn.name.__set_function_name(name);
    call.fn.__set_ret_type(result_type.to_thrift());
    call.__set_type(result_type.to_thrift());
    call.fn.arg_types.clear();
    for (const auto& argument : arguments) {
        call.fn.arg_types.push_back(argument.to_thrift());
    }
    call.__set_num_children(arguments.size());
    call.fn.__set_ai_model_source(ai_model ? TAIModelSource::AI_MODEL : TAIModelSource::SYSTEM);
    call.__set_ai_model_config_id(ai_model ? "model:102:1"
                                           : result_type.type == TYPE_ARRAY ? "__system_text_embedding__"
                                                                            : "__system_chat__");
    expression.nodes.resize(1);
    for (size_t i = 0; i < arguments.size(); ++i) {
        expression.nodes.push_back(make_slot_ref(arguments[i], i + 1));
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
    int ai_model_argument = -1;
};

std::vector<AIOverloadCase> ai_overload_cases() {
    const TypeDescriptor text = varchar_type();
    const TypeDescriptor strings = TypeDescriptor::create_array_type(text);
    const TypeDescriptor embedding = TypeDescriptor::create_array_type(TypeDescriptor(TYPE_FLOAT));
    const TypeDescriptor json(TYPE_JSON);
    const TypeDescriptor similarity(TYPE_FLOAT);
    const TypeDescriptor boolean(TYPE_BOOLEAN);
    const TypeDescriptor options = options_type();
    using Result = AIFunctionResultKind;
    return {
            {200100, "ai_complete", text, {text}, Result::STRING},
            {200101, "ai_complete", text, {text, options}, Result::STRING},
            {200102, "ai_complete", text, {text, text}, Result::STRING, 0},
            {200103, "ai_complete", text, {text, text, options}, Result::STRING, 0},
            {200110, "ai_sentiment", text, {text}, Result::SENTIMENT},
            {200111, "ai_sentiment", text, {text, text}, Result::SENTIMENT, 0},
            {200112, "ai_classify", json, {text, strings}, Result::JSON},
            {200113, "ai_classify", json, {text, text, strings}, Result::JSON, 0},
            {200114, "ai_extract", json, {text, strings}, Result::JSON},
            {200115, "ai_extract", json, {text, text, strings}, Result::JSON, 0},
            {200116, "ai_fix_grammar", text, {text}, Result::STRING},
            {200117, "ai_fix_grammar", text, {text, text}, Result::STRING, 0},
            {200118, "ai_redact", text, {text, strings}, Result::STRING},
            {200119, "ai_redact", text, {text, text, strings}, Result::STRING, 0},
            {200120, "ai_translate", text, {text, text, text}, Result::STRING},
            {200121, "ai_translate", text, {text, text, text, text}, Result::STRING, 0},
            {200122, "ai_similarity", similarity, {text, text}, Result::SIMILARITY},
            {200123, "ai_similarity", similarity, {text, text, text}, Result::SIMILARITY, 0},
            {200124, "ai_summarize", text, {text}, Result::STRING},
            {200125, "ai_summarize", text, {text, text}, Result::STRING, 0},
            {200126, "ai_filter", boolean, {text, text}, Result::BOOLEAN},
            {200127, "ai_filter", boolean, {text, text, text}, Result::BOOLEAN, 0},
            {200130, "ai_embed", embedding, {text}, Result::EMBEDDING},
            {200131, "ai_embed", embedding, {text, options}, Result::EMBEDDING},
            {200132, "ai_embed", embedding, {text, text}, Result::EMBEDDING, 0},
            {200133, "ai_embed", embedding, {text, text, options}, Result::EMBEDDING, 0},
            {200140, "ai_custom_query", text, {text, text}, Result::STRING, -1, 0},
            {200141, "ai_custom_query", text, {text, text, options}, Result::STRING, -1, 0},
            {200142, "ai_custom_embedding", embedding, {text, text}, Result::EMBEDDING, -1, 0},
            {200143, "ai_custom_embedding", embedding, {text, text, options}, Result::EMBEDDING, -1, 0},
    };
}

TEST_F(AIFunctionCallExprTest, EveryRegisteredOverloadRetainsValidatedSemanticsWhenCloned) {
    const auto cases = ai_overload_cases();
    ASSERT_EQ(30, cases.size());
    for (const auto& test : cases) {
        SCOPED_TRACE(test.fid);
        TExpr wire = make_extended_ai_expression(test.fid, test.name, test.result, test.arguments,
                                                 test.ai_model_argument >= 0);
        ObjectPool pool;
        auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
        ASSERT_TRUE(created.ok()) << created.status();
        auto* clone = down_cast<AIFunctionCallExpr*>(created.value()->clone(&pool));
        for (auto* expression : {created.value(), clone}) {
            EXPECT_EQ(test.model_argument < 0, expression->requires_default_model());
            EXPECT_EQ(test.result_kind, expression->result_kind());
            EXPECT_EQ(test.result_kind == AIFunctionResultKind::EMBEDDING ? AICapability::TEXT_EMBEDDING
                                                                          : AICapability::CHAT,
                      expression->capability());
            EXPECT_EQ(wire.nodes.front().ai_model_config_id, expression->model_config_id());
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
        invalid = wire.nodes.front();
        invalid.fn.__set_ai_model_source(test.ai_model_argument >= 0 ? TAIModelSource::SYSTEM
                                                                     : TAIModelSource::AI_MODEL);
        EXPECT_FALSE(AIFunctionCallExpr::create(&pool, invalid).ok());
    }
    for (int64_t fid : {200000, 200104, 200109, 200128, 200129, 200134, 200139, 200144}) {
        EXPECT_FALSE(AIFunctionCallExpr::is_ai_function_id(fid)) << fid;
    }
    EXPECT_FALSE(AIFunctionCallExpr::is_ai_function_name("ai_query"));
}

TEST_F(AIFunctionCallExprTest, FilterPromptRequestsUnquotedBooleanTokens) {
    for (const auto& [fid, explicit_model] : std::vector<std::pair<int64_t, bool>>{{200126, false}, {200127, true}}) {
        SCOPED_TRACE(fid);
        ObjectPool pool;
        std::vector<TypeDescriptor> arguments(explicit_model ? 3 : 2, varchar_type());
        auto wire = make_extended_ai_expression(fid, "ai_filter", TypeDescriptor(TYPE_BOOLEAN), arguments);
        auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
        ASSERT_TRUE(created.ok()) << created.status();
        auto* expression = created.value();
        if (explicit_model) {
            expression->add_child(pool.add(new CountingColumnExpr(varchar_type(), make_varchar_column({"model"}))));
        }
        expression->add_child(pool.add(new CountingColumnExpr(varchar_type(), make_varchar_column({"sunny"}))));
        expression->add_child(pool.add(new CountingColumnExpr(varchar_type(), make_varchar_column({"is weather"}))));
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

TEST_F(AIFunctionCallExprTest, TranslationAutoDetectsNullSourceAndPreservesPerRowModel) {
    ObjectPool pool;
    auto wire = make_extended_ai_expression(200121, "ai_translate", varchar_type(),
                                            {varchar_type(), varchar_type(), varchar_type(), varchar_type()});
    auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
    ASSERT_TRUE(created.ok()) << created.status();
    auto* expression = created.value();
    replace_children(&pool, expression,
                     {{varchar_type(), make_varchar_column({"m1", "m2", "m3"})},
                      {varchar_type(), make_varchar_column({"hello", "bonjour", "null target"})},
                      {varchar_type(), make_varchar_column({std::nullopt, "French", "English"})},
                      {varchar_type(), make_varchar_column({"Chinese", "English", std::nullopt})}});
    RuntimeState state;
    ExprContext context(expression);
    ASSERT_TRUE(context.prepare(&state).ok());
    ASSERT_TRUE(context.open(&state).ok());
    auto chunk = make_chunk(3);
    auto batch = expression->build_input_batch(&context, chunk.get(), "ignored");
    ASSERT_TRUE(batch.ok()) << batch.status();
    EXPECT_EQ("m1", batch->rows[0].model);
    EXPECT_NE(std::string::npos, batch->rows[0].prompt.find("Auto-detect"));
    EXPECT_NE(std::string::npos, batch->rows[1].prompt.find("from French into English"));
    EXPECT_EQ(AIFunctionRowAction::SQL_NULL, batch->rows[2].action);
}

TEST_F(AIFunctionCallExprTest, TranslationNullAndEmptyPoliciesApplyAtBothDeclaredInputOffsets) {
    for (const auto& [fid, explicit_model] : std::vector<std::pair<int64_t, bool>>{{200120, false}, {200121, true}}) {
        SCOPED_TRACE(fid);
        ObjectPool pool;
        std::vector<TypeDescriptor> arguments(explicit_model ? 4 : 3, varchar_type());
        auto wire = make_extended_ai_expression(fid, "ai_translate", varchar_type(), arguments);
        auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
        ASSERT_TRUE(created.ok()) << created.status();
        auto* expression = created.value();
        if (explicit_model) {
            expression->add_child(pool.add(new CountingColumnExpr(
                    varchar_type(), make_varchar_column({"model", "model", "model", "model", "", " \t"}))));
        }
        expression->add_child(pool.add(new CountingColumnExpr(
                varchar_type(), make_varchar_column({"hello", "hello", "hello", "hello", std::nullopt, "hello"}))));
        expression->add_child(pool.add(new CountingColumnExpr(
                varchar_type(), make_varchar_column({std::nullopt, "", "English", "English", "English", "English"}))));
        expression->add_child(pool.add(new CountingColumnExpr(
                varchar_type(), make_varchar_column({"Chinese", "Chinese", "", " ", "Chinese", "Chinese"}))));
        RuntimeState state;
        ExprContext context(expression);
        ASSERT_TRUE(context.prepare(&state).ok());
        ASSERT_TRUE(context.open(&state).ok());
        auto chunk = make_chunk(6);
        auto batch = expression->build_input_batch(&context, chunk.get(), "snapshot-model");
        ASSERT_TRUE(batch.ok()) << batch.status();
        ASSERT_EQ(6, batch->rows.size());
        EXPECT_EQ(AIFunctionRowAction::DISPATCH, batch->rows[0].action);
        EXPECT_EQ(AIFunctionRowAction::DISPATCH, batch->rows[1].action);
        EXPECT_NE(std::string::npos, batch->rows[0].prompt.find("Auto-detect"));
        EXPECT_EQ(batch->rows[0].prompt, batch->rows[1].prompt);
        EXPECT_EQ(AIFunctionRowAction::SQL_NULL, batch->rows[2].action);
        EXPECT_EQ(AIFunctionRowAction::DISPATCH, batch->rows[3].action);
        EXPECT_EQ(AIFunctionRowAction::SQL_NULL, batch->rows[4].action);
        EXPECT_EQ(explicit_model ? AIFunctionRowAction::TERMINAL_ROW_FAILURE : AIFunctionRowAction::DISPATCH,
                  batch->rows[5].action);
    }
}

TEST_F(AIFunctionCallExprTest, AIModelSelectorIsConstantBindingNotTheProviderModel) {
    ObjectPool pool;
    auto wire = make_extended_ai_expression(200140, "ai_custom_query", varchar_type(), {varchar_type(), varchar_type()},
                                            true);
    auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
    ASSERT_TRUE(created.ok()) << created.status();
    auto* expression = created.value();
    auto* selector = pool.add(new CountingColumnExpr(
            varchar_type(), ColumnHelper::create_const_column<TYPE_VARCHAR>("embedding_model", 2)));
    expression->add_child(selector);
    expression->add_child(
            pool.add(new CountingColumnExpr(varchar_type(), make_varchar_column({"hello", std::nullopt}))));
    RuntimeState state;
    ExprContext context(expression);
    ASSERT_TRUE(context.prepare(&state).ok());
    ASSERT_TRUE(context.open(&state).ok());
    auto chunk = make_chunk(2);
    auto batch = expression->build_input_batch(&context, chunk.get(), "snapshot-model");
    ASSERT_TRUE(batch.ok()) << batch.status();
    EXPECT_EQ("snapshot-model", batch->rows[0].model);
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
                                                 test.ai_model_argument >= 0);
        auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
        ASSERT_TRUE(created.ok()) << created.status();
        const bool has_options = test.arguments.back().type == TYPE_MAP;
        for (size_t index = 0; index < test.arguments.size(); ++index) {
            ColumnPtr column;
            if (static_cast<int>(index) == test.ai_model_argument) {
                column = ColumnHelper::create_const_column<TYPE_VARCHAR>("embedding_model", 2);
            } else if (static_cast<int>(index) == test.model_argument) {
                column = make_varchar_column({"explicit-model", "explicit-model"});
            } else if (test.arguments[index].type == TYPE_MAP) {
                column = make_const_options(2, true);
            } else if (test.arguments[index].type == TYPE_ARRAY) {
                column = make_string_array({"category"}, true, 2);
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
            for (size_t index = 0; index < test.arguments.size(); ++index) {
                if (static_cast<int>(index) != test.model_argument &&
                    static_cast<int>(index) != test.ai_model_argument && test.arguments[index].type == TYPE_VARCHAR) {
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
    };
    for (const auto& test : ai_overload_cases()) {
        if (test.arguments.back().type != TYPE_ARRAY) {
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
            for (size_t i = 0; i + 1 < test.arguments.size(); ++i) {
                expression->add_child(pool.add(new CountingColumnExpr(varchar_type(), make_varchar_column({"text"}))));
            }
            expression->add_child(pool.add(new CountingColumnExpr(array_type, categories)));

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
        auto wire = make_extended_ai_expression(200112, "ai_classify", TypeDescriptor(TYPE_JSON),
                                                {varchar_type(), array_type});
        auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
        ASSERT_TRUE(created.ok()) << created.status();
        auto* expression = created.value();
        replace_children(&pool, expression,
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
            make_extended_ai_expression(200112, "ai_classify", TypeDescriptor(TYPE_JSON), {varchar_type(), array_type});
    auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
    ASSERT_TRUE(created.ok());
    auto* prompt = pool.add(new CountingColumnExpr(varchar_type(), make_varchar_column({"hello"})));
    auto* categories = pool.add(new ConstantArrayExpr(make_string_array({"greeting", "other"}, false)));
    created.value()->add_child(prompt);
    created.value()->add_child(categories);
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
    auto wire = make_extended_ai_expression(200112, "ai_classify", TypeDescriptor(TYPE_JSON),
                                            {varchar_type(), TypeDescriptor::create_array_type(varchar_type())});
    auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
    ASSERT_TRUE(created.ok()) << created.status();
    created.value()->add_child(pool.add(new CountingColumnExpr(varchar_type(), make_varchar_column({"hello"}))));
    created.value()->add_child(pool.add(new FailingArrayExpr()));
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
    const std::vector<TestCase> test_cases = {
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
        auto wire = make_extended_ai_expression(200115, "ai_extract", TypeDescriptor(TYPE_JSON),
                                                {varchar_type(), varchar_type(), array_type});
        wire.nodes.back() = cast_node(test.from_json ? TypeDescriptor(TYPE_JSON) : varchar_type(), array_type);
        if (test.from_json) {
            wire.nodes.emplace_back(cast_node(varchar_type(), TypeDescriptor(TYPE_JSON)));
        }
        wire.nodes.emplace_back(make_string_literal_node(varchar_type(), test.input));
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
            chunk->append_column(make_varchar_column({"explicit-model"}), 1);
            chunk->append_column(make_varchar_column({"hello"}), 2);
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
    auto wire = make_extended_ai_expression(200131, "ai_embed",
                                            TypeDescriptor::create_array_type(TypeDescriptor(TYPE_FLOAT)),
                                            {varchar_type(), options_type()});
    auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
    ASSERT_TRUE(created.ok()) << created.status();
    auto* expression = created.value();
    auto keys = BinaryColumn::create();
    keys->append("input");
    auto values = DoubleColumn::create();
    values->append(123);
    auto offsets = UInt32Column::create();
    offsets->append(0);
    offsets->append(1);
    auto options = ConstColumn::create(
            MapColumn::create(ColumnHelper::cast_to_nullable_column(std::move(keys)),
                              ColumnHelper::cast_to_nullable_column(std::move(values)), std::move(offsets)),
            1);
    replace_children(&pool, expression, {{varchar_type(), make_varchar_column({"text"})}, {options_type(), options}});
    RuntimeState state;
    ExprContext context(expression);
    ASSERT_TRUE(context.prepare(&state).ok());
    ASSERT_TRUE(context.open(&state).ok());
    auto chunk = make_chunk(1);
    EXPECT_FALSE(expression->build_input_batch(&context, chunk.get(), "embedding-model").ok());
}

TEST_F(AIFunctionCallExprTest, CapabilityAndModelSourceCannotBeSpoofed) {
    ObjectPool pool;
    const auto embedding_type = TypeDescriptor::create_array_type(TypeDescriptor(TYPE_FLOAT));
    auto embedding = make_extended_ai_expression(200130, "ai_embed", embedding_type, {varchar_type()});
    auto created = AIFunctionCallExpr::create(&pool, embedding.nodes.front());
    ASSERT_TRUE(created.ok());
    EXPECT_EQ(AICapability::TEXT_EMBEDDING, created.value()->capability());
    EXPECT_EQ(AIFunctionResultKind::EMBEDDING, created.value()->result_kind());
    EXPECT_TRUE(created.value()->requires_default_model());
    auto* clone = down_cast<AIFunctionCallExpr*>(created.value()->clone(&pool));
    EXPECT_EQ(created.value()->capability(), clone->capability());
    EXPECT_EQ(created.value()->result_kind(), clone->result_kind());
    EXPECT_EQ(created.value()->model_config_id(), clone->model_config_id());
    embedding.nodes.front().__set_ai_model_config_id("__system_chat__");
    EXPECT_FALSE(AIFunctionCallExpr::create(&pool, embedding.nodes.front()).ok());

    auto ai_model = make_extended_ai_expression(200142, "ai_custom_embedding", embedding_type,
                                                {varchar_type(), varchar_type()}, true);
    ai_model.nodes.front().fn.__set_ai_model_source(TAIModelSource::SYSTEM);
    EXPECT_FALSE(AIFunctionCallExpr::create(&pool, ai_model.nodes.front()).ok());
    ai_model.nodes.front().fn.__set_ai_model_source(TAIModelSource::AI_MODEL);
    ai_model.nodes.front().__set_ai_model_config_id("__system_text_embedding__");
    EXPECT_FALSE(AIFunctionCallExpr::create(&pool, ai_model.nodes.front()).ok());

    auto translate = make_extended_ai_expression(200121, "ai_translate", varchar_type(),
                                                 {varchar_type(), varchar_type(), varchar_type(), varchar_type()});
    created = AIFunctionCallExpr::create(&pool, translate.nodes.front());
    ASSERT_TRUE(created.ok());
    EXPECT_FALSE(created.value()->requires_default_model());
    EXPECT_EQ(AICapability::CHAT, created.value()->capability());
}

TEST_F(AIFunctionCallExprTest, AIModelBindingRejectsNonconstantOrBlankSelector) {
    for (bool constant : {false, true}) {
        ObjectPool pool;
        auto wire = make_extended_ai_expression(200140, "ai_custom_query", varchar_type(),
                                                {varchar_type(), varchar_type()}, true);
        auto created = AIFunctionCallExpr::create(&pool, wire.nodes.front());
        ASSERT_TRUE(created.ok());
        ColumnPtr selector = constant ? ColumnHelper::create_const_column<TYPE_VARCHAR>(" \t", 1)
                                      : make_varchar_column({"embedding_model"});
        replace_children(&pool, created.value(),
                         {{varchar_type(), selector}, {varchar_type(), make_varchar_column({"hello"})}});
        RuntimeState state;
        ExprContext context(created.value());
        ASSERT_TRUE(context.prepare(&state).ok());
        ASSERT_TRUE(context.open(&state).ok());
        auto chunk = make_chunk(1);
        EXPECT_FALSE(created.value()->build_input_batch(&context, chunk.get(), "snapshot-model").ok());
    }
}

TEST_F(AIFunctionCallExprTest, AIModelSourceAndCorrelationKeyAreExplicit) {
    ObjectPool pool;
    auto wire = make_extended_ai_expression(200140, "ai_custom_query", varchar_type(), {varchar_type(), varchar_type()},
                                            true);
    auto& node = wire.nodes.front();
    node.__set_ai_model_config_id("opaque-correlation-key");
    auto created = AIFunctionCallExpr::create(&pool, node);
    ASSERT_TRUE(created.ok()) << created.status();
    EXPECT_EQ(TAIModelSource::AI_MODEL, created.value()->model_source());
    for (const auto source :
         {TAIModelSource::SYSTEM, TAIModelSource::RESOURCE, static_cast<TAIModelSource::type>(99)}) {
        node.fn.__set_ai_model_source(source);
        EXPECT_FALSE(AIFunctionCallExpr::create(&pool, node).ok());
    }
    node.fn.__set_ai_model_source(TAIModelSource::AI_MODEL);
    for (const auto key : {"", " \t", "key\n", "__system_chat__", "__system_text_embedding__"}) {
        node.__set_ai_model_config_id(key);
        EXPECT_FALSE(AIFunctionCallExpr::create(&pool, node).ok()) << key;
    }
}

} // namespace
} // namespace starrocks
