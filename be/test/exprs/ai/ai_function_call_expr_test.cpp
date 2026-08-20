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

struct SignatureSpec {
    AIFunctionSignature signature;
    std::vector<TypeDescriptor> argument_types;
};

SignatureSpec signature_spec(int64_t fid) {
    switch (fid) {
    case kPromptFid:
        return {AIFunctionSignature::PROMPT, {varchar_type()}};
    case kPromptOptionsFid:
        return {AIFunctionSignature::PROMPT_OPTIONS, {varchar_type(), options_type()}};
    case kModelPromptFid:
        return {AIFunctionSignature::MODEL_PROMPT, {varchar_type(), varchar_type()}};
    case kModelPromptOptionsFid:
        return {AIFunctionSignature::MODEL_PROMPT_OPTIONS, {varchar_type(), varchar_type(), options_type()}};
    default:
        ADD_FAILURE() << "unexpected test fid " << fid;
        return {AIFunctionSignature::PROMPT, {}};
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
    const auto spec = signature_spec(fid);

    TFunctionName name;
    name.__set_function_name("ai_complete");

    std::vector<TTypeDesc> argument_types;
    argument_types.reserve(spec.argument_types.size());
    for (const auto& type : spec.argument_types) {
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
    call.__set_num_children(static_cast<int32_t>(spec.argument_types.size()));
    call.__set_type(varchar_type().to_thrift());
    call.__set_is_nullable(true);
    call.__set_fn(function);
    call.__set_ai_model_config_id(std::string(kSystemConfigId));

    TExpr expression;
    expression.nodes.emplace_back(std::move(call));
    for (size_t i = 0; i < spec.argument_types.size(); ++i) {
        expression.nodes.emplace_back(make_slot_ref(spec.argument_types[i], static_cast<int>(i + 1)));
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
            EXPECT_EQ(AIFunctionSignature::PROMPT, ai->signature());
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
    const std::vector<std::pair<int64_t, AIFunctionSignature>> signatures = {
            {kPromptFid, AIFunctionSignature::PROMPT},
            {kPromptOptionsFid, AIFunctionSignature::PROMPT_OPTIONS},
            {kModelPromptFid, AIFunctionSignature::MODEL_PROMPT},
            {kModelPromptOptionsFid, AIFunctionSignature::MODEL_PROMPT_OPTIONS},
    };

    for (const auto& [fid, signature] : signatures) {
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

        EXPECT_EQ(signature, expression->signature());
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
        EXPECT_EQ(signature, clone->signature());
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

} // namespace
} // namespace starrocks
