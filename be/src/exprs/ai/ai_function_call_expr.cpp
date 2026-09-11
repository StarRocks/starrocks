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

#include <algorithm>
#include <array>
#include <cstddef>
#include <optional>
#include <utility>

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_viewer.h"
#include "column/const_column.h"
#include "common/object_pool.h"
#include "exprs/ai/ai_provider_options_builder.h"
#include "exprs/expr_context.h"
#include "gutil/strings/ascii_ctype.h"
#include "gutil/strings/substitute.h"
#include "types/json_value.h"
#include "types/logical_type.h"

namespace starrocks {

enum class AIArgumentType : uint8_t {
    VARCHAR,
    OPTIONS,
    STRING_ARRAY,
};

enum class AIPromptKind : uint8_t {
    PASSTHROUGH,
    SENTIMENT,
    CLASSIFY,
    EXTRACT,
    FIX_GRAMMAR,
    REDACT,
    TRANSLATE,
    SIMILARITY,
    SUMMARIZE,
    FILTER,
};

#include "gen_cpp/opcode/AIFunctionDescriptors.inc"

namespace {

constexpr std::string_view kSystemConfigId = "__system_chat__";
constexpr std::string_view kEmbeddingConfigId = "__system_text_embedding__";

Status invalid_ai_expression() {
    return Status::InvalidArgument("Invalid AI function expression");
}

const AIFunctionDescriptor* find_descriptor(int64_t fid) {
    for (const auto& descriptor : kAIFunctionDescriptors) {
        if (descriptor.fid == fid) {
            return &descriptor;
        }
    }
    return nullptr;
}

bool consume_valid_type(const std::vector<TTypeNode>& nodes, size_t* index) {
    if (*index >= nodes.size()) {
        return false;
    }
    const TTypeNode& node = nodes[(*index)++];
    switch (node.type) {
    case TTypeNodeType::SCALAR:
        return node.__isset.scalar_type;
    case TTypeNodeType::ARRAY:
        return !node.__isset.scalar_type && consume_valid_type(nodes, index);
    case TTypeNodeType::MAP:
        return !node.__isset.scalar_type && consume_valid_type(nodes, index) && consume_valid_type(nodes, index);
    case TTypeNodeType::STRUCT:
        if (node.__isset.scalar_type || !node.__isset.struct_fields) {
            return false;
        }
        for (size_t i = 0; i < node.struct_fields.size(); ++i) {
            if (!consume_valid_type(nodes, index)) {
                return false;
            }
        }
        return true;
    }
    return false;
}

bool is_valid_type(const TTypeDesc& type) {
    if (!type.__isset.types || type.types.empty()) {
        return false;
    }
    size_t index = 0;
    return consume_valid_type(type.types, &index) && index == type.types.size();
}

bool is_varchar_type(const TTypeDesc& type) {
    return is_valid_type(type) && type.types.size() == 1 && type.types[0].type == TTypeNodeType::SCALAR &&
           type.types[0].scalar_type.type == TPrimitiveType::VARCHAR;
}

bool is_options_type(const TTypeDesc& type) {
    if (!is_valid_type(type) || type.types.size() < 3 || type.types[0].type != TTypeNodeType::MAP ||
        type.types[1].type != TTypeNodeType::SCALAR) {
        return false;
    }
    if (type.types[1].scalar_type.type == TPrimitiveType::VARCHAR) {
        return true;
    }
    // FE serializes the unresolved MAP<NULL,NULL> produced by map{} as MAP<BOOLEAN,BOOLEAN>.
    return type.types.size() == 3 && type.types[1].scalar_type.type == TPrimitiveType::BOOLEAN &&
           type.types[2].type == TTypeNodeType::SCALAR && type.types[2].scalar_type.type == TPrimitiveType::BOOLEAN;
}

bool matches_argument_type(const TTypeDesc& type, AIArgumentType expected) {
    if (expected == AIArgumentType::STRING_ARRAY) {
        return is_valid_type(type) && type.types.size() == 2 && type.types[0].type == TTypeNodeType::ARRAY &&
               type.types[1].type == TTypeNodeType::SCALAR && type.types[1].scalar_type.type == TPrimitiveType::VARCHAR;
    }
    return expected == AIArgumentType::VARCHAR ? is_varchar_type(type) : is_options_type(type);
}

bool matches_result_type(const TTypeDesc& type, AIFunctionResultKind kind) {
    if (!is_valid_type(type)) {
        return false;
    }
    if (kind == AIFunctionResultKind::EMBEDDING) {
        return type.types.size() == 2 && type.types[0].type == TTypeNodeType::ARRAY &&
               type.types[1].type == TTypeNodeType::SCALAR && type.types[1].scalar_type.type == TPrimitiveType::FLOAT;
    }
    const auto expected = kind == AIFunctionResultKind::JSON
                                  ? TPrimitiveType::JSON
                                  : kind == AIFunctionResultKind::BOOLEAN
                                            ? TPrimitiveType::BOOLEAN
                                            : kind == AIFunctionResultKind::SIMILARITY ? TPrimitiveType::FLOAT
                                                                                       : TPrimitiveType::VARCHAR;
    return type.types.size() == 1 && type.types[0].type == TTypeNodeType::SCALAR &&
           type.types[0].scalar_type.type == expected;
}

bool is_fe_string_type(const TypeDescriptor& type) {
    return type.type == TYPE_CHAR || type.type == TYPE_VARCHAR;
}

bool matches_safe_map_type(const TypeDescriptor& declared, const TypeDescriptor& child);

bool matches_main_compatible_type(const TypeDescriptor& declared, const TypeDescriptor& child) {
    if (is_fe_string_type(declared) && is_fe_string_type(child)) {
        return true;
    }
    if (declared.type != child.type || declared.children.size() != child.children.size()) {
        return false;
    }
    if (declared.type == TYPE_MAP) {
        return matches_safe_map_type(declared, child);
    }
    if (!declared.children.empty()) {
        for (size_t i = 0; i < declared.children.size(); ++i) {
            if (!matches_main_compatible_type(declared.children[i], child.children[i])) {
                return false;
            }
        }
        return true;
    }
    return declared == child;
}

bool matches_safe_map_type(const TypeDescriptor& declared, const TypeDescriptor& child) {
    if (declared.type != TYPE_MAP || declared.children.size() != 2 || child.type != TYPE_MAP ||
        child.children.size() != 2) {
        return false;
    }

    const bool declared_normalized =
            declared.children[0].type == TYPE_BOOLEAN && declared.children[1].type == TYPE_BOOLEAN;
    const bool child_normalized = child.children[0].type == TYPE_BOOLEAN && child.children[1].type == TYPE_BOOLEAN;
    if (declared_normalized || child_normalized) {
        return declared_normalized && child_normalized;
    }

    // Every ordinary provider-option map depth requires VARCHAR keys. CHAR is only
    // compatible in values (including ARRAY/STRUCT leaves), never in map keys.
    return declared.children[0].type == TYPE_VARCHAR && child.children[0].type == TYPE_VARCHAR &&
           matches_main_compatible_type(declared.children[1], child.children[1]);
}

bool matches_runtime_argument_type(const TypeDescriptor& declared, const TypeDescriptor& child,
                                   AIArgumentType expected) {
    if (expected == AIArgumentType::VARCHAR) {
        return declared.type == TYPE_VARCHAR && is_fe_string_type(child);
    }
    if (expected == AIArgumentType::STRING_ARRAY) {
        return declared.type == TYPE_ARRAY && child.type == TYPE_ARRAY && declared.children.size() == 1 &&
               child.children.size() == 1 && declared.children[0].type == TYPE_VARCHAR &&
               is_fe_string_type(child.children[0]);
    }
    return matches_safe_map_type(declared, child);
}

bool contains_only_ascii_whitespace(const Slice& value) {
    for (size_t i = 0; i < value.size; ++i) {
        if (!ascii_isspace(static_cast<unsigned char>(value.data[i]))) {
            return false;
        }
    }
    return true;
}

bool equal_name(std::string_view name, std::string_view expected) {
    if (name.size() != expected.size()) {
        return false;
    }
    for (size_t i = 0; i < name.size(); ++i) {
        if (ascii_tolower(static_cast<unsigned char>(name[i])) != expected[i]) {
            return false;
        }
    }
    return true;
}

StatusOr<std::string> read_constant_string_array(const Column& column) {
    // Constness belongs to the Expr contract: a constant array-producing
    // expression can materialize an ordinary column, just like an option MAP.
    const Column* data = &column;
    if (const auto* constant = dynamic_cast<const ConstColumn*>(data)) {
        data = constant->data_column().get();
    }
    if (data->empty()) {
        return Status::InvalidArgument("AI function categories or keys must be a nonempty constant string array");
    }
    if (const auto* nullable = dynamic_cast<const NullableColumn*>(data)) {
        if (nullable->is_null(0)) {
            return invalid_ai_expression();
        }
        data = nullable->data_column().get();
    }
    const auto* array = dynamic_cast<const ArrayColumn*>(data);
    if (array == nullptr || array->offsets().size() < 2) {
        return invalid_ai_expression();
    }
    const auto [offset, count] = array->get_element_offset_size(0);
    if (count == 0 || offset > array->elements().size() || count > array->elements().size() - offset) {
        return invalid_ai_expression();
    }
    const Column* elements = &array->elements();
    if (const auto* nullable = dynamic_cast<const NullableColumn*>(elements)) {
        elements = nullable->data_column().get();
    }
    if (!elements->is_binary()) {
        return invalid_ai_expression();
    }
    std::string result = "[";
    for (size_t index = offset; index < offset + count; ++index) {
        if (array->elements().is_null(index)) {
            return invalid_ai_expression();
        }
        const Slice value = elements->get(index).get_slice();
        if (contains_only_ascii_whitespace(value)) {
            return invalid_ai_expression();
        }
        if (index != offset) {
            result += ", ";
        }
        ASSIGN_OR_RETURN(auto quoted, JsonValue::from_string(value).to_string());
        result += quoted;
    }
    result += "]";
    return result;
}

std::string build_prompt(AIPromptKind kind, std::string text, const std::string& second, const std::string& third) {
    switch (kind) {
    case AIPromptKind::PASSTHROUGH:
        return text;
    case AIPromptKind::SENTIMENT:
        return strings::Substitute(
                "Analyze the overall sentiment of the following text. "
                "Output exactly one lowercase word from this list: positive, negative, neutral, mixed, unknown. "
                "No punctuation, no explanation.\n\nText: $0",
                text);
    case AIPromptKind::CLASSIFY:
        return strings::Substitute(
                "Classify the following text into exactly one of these categories: $0.\n"
                "Return a JSON object in this exact format: {\"labels\": [\"<chosen_category>\"]}\n"
                "The array must contain exactly one string that matches one of the given categories.\n"
                "Output only valid JSON, no markdown, no explanation.\n\nText: $1",
                second, text);
    case AIPromptKind::EXTRACT:
        return strings::Substitute(
                "Extract a value for each of the following keys from the text below.\n"
                "Keys: $0\nFor each key, extract exactly one value. If a key's value is not found, use null.\n"
                "Return a JSON object in this exact format: {\"response\": {\"key1\": \"value1\", \"key2\": null}}\n"
                "Output only valid JSON, no markdown, no explanation.\n\nText: $1",
                second, text);
    case AIPromptKind::FIX_GRAMMAR:
        return strings::Substitute(
                "Fix the grammar and spelling of the following text. "
                "Preserve the original meaning and tone. Output only the corrected text, nothing else.\n\nText: $0",
                text);
    case AIPromptKind::REDACT:
        return strings::Substitute(
                "Redact personally identifiable information (PII) in the text below.\n"
                "Categories to redact: $0\n"
                "Replace each detected PII value with its uppercase category name in square brackets, "
                "e.g. [NAME], [ADDRESS], [EMAIL], [PHONE], [SSN].\n"
                "If no PII is found, return the original text unchanged. "
                "Output only the redacted text, nothing else.\n\nText: $1",
                second, text);
    case AIPromptKind::TRANSLATE:
        if (second.empty()) {
            return strings::Substitute(
                    "Translate the following text into $0. Auto-detect the source language. "
                    "Preserve the original meaning and tone. Output only the translated text, nothing else.\n\nText: "
                    "$1",
                    third, text);
        }
        return strings::Substitute(
                "Translate the following text from $0 into $1. "
                "Preserve the original meaning and tone. Output only the translated text, nothing else.\n\nText: $2",
                second, third, text);
    case AIPromptKind::SIMILARITY:
        return strings::Substitute(
                "Calculate the semantic similarity between the following two texts.\n"
                "Output only a single decimal number between 0.00 and 1.00 (0 = completely different, "
                "1 = identical meaning). No explanation, no extra text.\n\nText 1: $0\nText 2: $1",
                text, second);
    case AIPromptKind::SUMMARIZE:
        return strings::Substitute(
                "Summarize the following text concisely, capturing the key points. "
                "Output only the summary, nothing else.\n\nText: $0",
                text);
    case AIPromptKind::FILTER:
        return strings::Substitute(
                "Given the following text, determine if this condition is true. "
                "You MUST respond with exactly true or false and nothing else.\n"
                "Text: $0\nCondition: $1",
                text, second);
    }
    return {};
}

} // namespace

bool AIFunctionCallExpr::is_ai_function_name(std::string_view name) {
    for (const auto& descriptor : kAIFunctionDescriptors) {
        if (equal_name(name, descriptor.name)) {
            return true;
        }
    }
    return false;
}

bool AIFunctionCallExpr::is_ai_function_id(int64_t fid) {
    return find_descriptor(fid) != nullptr;
}

AIFunctionCallExpr::AIFunctionCallExpr(const TExprNode& node, const AIFunctionDescriptor* descriptor,
                                       std::vector<TypeDescriptor> argument_types)
        : Expr(node),
          _descriptor(descriptor),
          _model_config_id(node.ai_model_config_id),
          _argument_types(std::move(argument_types)) {
    _fn = TFunction();
}

AIFunctionCallExpr::AIFunctionCallExpr(const AIFunctionCallExpr& other)
        : Expr(other),
          _descriptor(other._descriptor),
          _model_config_id(other._model_config_id),
          _argument_types(other._argument_types) {}

StatusOr<AIFunctionCallExpr*> AIFunctionCallExpr::create(ObjectPool* pool, const TExprNode& node) {
    if (pool == nullptr ||
        (node.node_type != TExprNodeType::FUNCTION_CALL && node.node_type != TExprNodeType::COMPUTE_FUNCTION_CALL) ||
        !node.__isset.fn || node.fn.binary_type != TFunctionBinaryType::AI ||
        !is_ai_function_name(node.fn.name.function_name) || !node.fn.__isset.fid || !node.fn.__isset.ai_model_source ||
        !node.__isset.ai_model_config_id || node.fn.has_var_args || !node.__isset.is_nullable || !node.is_nullable ||
        node.fn.__isset.hdfs_location || node.fn.__isset.content || node.fn.__isset.cloud_configuration ||
        node.fn.__isset.service_url) {
        return invalid_ai_expression();
    }

    const auto* spec = find_descriptor(node.fn.fid);
    if (spec == nullptr || node.num_children != spec->argument_count ||
        node.fn.arg_types.size() != spec->argument_count || !equal_name(node.fn.name.function_name, spec->name) ||
        !matches_result_type(node.type, spec->result_kind) ||
        !matches_result_type(node.fn.ret_type, spec->result_kind)) {
        return invalid_ai_expression();
    }
    if (node.fn.ai_model_source != spec->model_source) {
        return invalid_ai_expression();
    }
    if (spec->model_source == TAIModelSource::AI_MODEL) {
        const std::string& key = node.ai_model_config_id;
        if (contains_only_ascii_whitespace(Slice(key)) || key == kSystemConfigId || key == kEmbeddingConfigId ||
            std::any_of(key.begin(), key.end(), [](unsigned char c) { return c <= 0x1f || c == 0x7f; })) {
            return invalid_ai_expression();
        }
    } else if (node.ai_model_config_id !=
               (spec->capability == AICapability::CHAT ? kSystemConfigId : kEmbeddingConfigId)) {
        return invalid_ai_expression();
    }
    for (size_t i = 0; i < spec->argument_count; ++i) {
        if (!matches_argument_type(node.fn.arg_types[i], spec->argument_types[i])) {
            return invalid_ai_expression();
        }
    }

    std::vector<TypeDescriptor> argument_types;
    argument_types.reserve(spec->argument_count);
    for (const TTypeDesc& type : node.fn.arg_types) {
        argument_types.emplace_back(TypeDescriptor::from_thrift(type));
    }
    return pool->add(new AIFunctionCallExpr(node, spec, std::move(argument_types)));
}

Expr* AIFunctionCallExpr::clone(ObjectPool* pool) const {
    return pool->add(new AIFunctionCallExpr(*this));
}

AICapability AIFunctionCallExpr::capability() const {
    return _descriptor->capability;
}

TAIModelSource::type AIFunctionCallExpr::model_source() const {
    return _descriptor->model_source;
}

AIFunctionResultKind AIFunctionCallExpr::result_kind() const {
    return _descriptor->result_kind;
}

bool AIFunctionCallExpr::requires_default_model() const {
    return _descriptor->model_argument < 0;
}

Status AIFunctionCallExpr::_validate_children() const {
    const auto* spec = _descriptor;
    if (_argument_types.size() != spec->argument_count || _children.size() != spec->argument_count) {
        return invalid_ai_expression();
    }
    for (size_t i = 0; i < spec->argument_count; ++i) {
        if (_children[i] == nullptr ||
            !matches_runtime_argument_type(_argument_types[i], _children[i]->type(), spec->argument_types[i])) {
            return invalid_ai_expression();
        }
    }
    return Status::OK();
}

Status AIFunctionCallExpr::prepare(RuntimeState* state, ExprContext* context) {
    RETURN_IF_ERROR(_validate_children());
    return Expr::prepare(state, context);
}

Status AIFunctionCallExpr::open(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) {
    RETURN_IF_ERROR(Expr::open(state, context, scope));
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    // Native constant evaluation preserves CAST semantics and validates arrays even when no input rows arrive.
    for (size_t index = 0; index < _descriptor->argument_count; ++index) {
        if (_descriptor->argument_types[index] != AIArgumentType::STRING_ARRAY) {
            continue;
        }
        if (!_children[index]->is_constant()) {
            return Status::InvalidArgument("AI function categories or keys must be constant");
        }
        ASSIGN_OR_RETURN(ColumnPtr column, _children[index]->evaluate_const(context));
        if (column == nullptr) {
            return invalid_ai_expression();
        }
        RETURN_IF_ERROR(read_constant_string_array(*column).status());
    }
    return Status::OK();
}

StatusOr<AIFunctionInputBatch> AIFunctionCallExpr::build_input_batch(ExprContext* context, Chunk* chunk,
                                                                     std::string_view default_model) const {
    RETURN_IF_ERROR(_validate_children());
    if (context == nullptr || chunk == nullptr || chunk->num_rows() == 0) {
        return invalid_ai_expression();
    }

    const auto& spec = *_descriptor;

    Columns columns;
    columns.reserve(_children.size());
    for (Expr* child : _children) {
        ASSIGN_OR_RETURN(ColumnPtr column, context->evaluate(child, chunk));
        if (column == nullptr || column->size() != chunk->num_rows()) {
            return invalid_ai_expression();
        }
        columns.emplace_back(std::move(column));
    }

    AIFunctionInputBatch batch;
    std::vector<std::optional<ColumnViewer<TYPE_VARCHAR>>> strings(columns.size());
    std::vector<std::string> constant_arrays(columns.size());
    for (size_t index = 0; index < columns.size(); ++index) {
        switch (spec.argument_types[index]) {
        case AIArgumentType::VARCHAR:
            strings[index].emplace(columns[index]);
            break;
        case AIArgumentType::OPTIONS: {
            if (!_children[index]->is_constant()) {
                return Status::InvalidArgument("AI function options must be constant");
            }
            ASSIGN_OR_RETURN(auto options,
                             build_ai_provider_options(*columns[index], _children[index]->type(), 0, spec.capability));
            batch.options = std::make_shared<const AIProviderOptions>(std::move(options));
            break;
        }
        case AIArgumentType::STRING_ARRAY:
            if (!_children[index]->is_constant()) {
                return Status::InvalidArgument("AI function categories or keys must be constant");
            }
            ASSIGN_OR_RETURN(constant_arrays[index], read_constant_string_array(*columns[index]));
            break;
        }
    }
    if (spec.ai_model_argument >= 0) {
        const size_t index = spec.ai_model_argument;
        if (!_children[index]->is_constant() || !columns[index]->is_constant() || strings[index]->is_null(0) ||
            contains_only_ascii_whitespace(strings[index]->value(0))) {
            return invalid_ai_expression();
        }
    }
    batch.rows.reserve(chunk->num_rows());
    for (size_t row = 0; row < chunk->num_rows(); ++row) {
        bool is_null = false;
        for (size_t index = 0; index < strings.size(); ++index) {
            if (!strings[index]) {
                continue;
            }
            if (strings[index]->is_null(row)) {
                is_null |= !spec.null_as_empty_arguments[index];
            } else {
                is_null |= spec.empty_as_null_arguments[index] && strings[index]->value(row).empty();
            }
        }
        if (is_null) {
            batch.rows.emplace_back();
            continue;
        }
        if (spec.model_argument >= 0 && contains_only_ascii_whitespace(strings[spec.model_argument]->value(row))) {
            batch.rows.emplace_back(AIFunctionRowInput{
                    .action = AIFunctionRowAction::TERMINAL_ROW_FAILURE,
            });
            continue;
        }
        auto input_value = [&](size_t input_index) -> std::string {
            if (input_index >= spec.input_count) {
                return {};
            }
            const size_t argument = spec.input_arguments[input_index];
            if (spec.argument_types[argument] == AIArgumentType::STRING_ARRAY) {
                return constant_arrays[argument];
            }
            return strings[argument]->is_null(row) ? std::string{} : strings[argument]->value(row).to_string();
        };
        batch.rows.emplace_back(AIFunctionRowInput{
                .action = AIFunctionRowAction::DISPATCH,
                .model = spec.model_argument >= 0 ? strings[spec.model_argument]->value(row).to_string()
                                                  : std::string(default_model),
                .prompt = build_prompt(spec.prompt_kind, input_value(0), input_value(1), input_value(2)),
        });
    }
    return batch;
}

StatusOr<ColumnPtr> AIFunctionCallExpr::evaluate_checked(ExprContext*, Chunk*) {
    return Status::NotSupported("AI expression requires AIProject asynchronous execution");
}

} // namespace starrocks
