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

#include <array>
#include <cstddef>
#include <optional>
#include <utility>

#include "column/chunk.h"
#include "column/column_viewer.h"
#include "common/object_pool.h"
#include "exprs/ai/ai_provider_options_builder.h"
#include "exprs/expr_context.h"
#include "gutil/strings/ascii_ctype.h"
#include "types/logical_type.h"

namespace starrocks {
namespace {

constexpr int64_t kPromptFid = 200100;
constexpr int64_t kPromptOptionsFid = 200101;
constexpr int64_t kModelPromptFid = 200102;
constexpr int64_t kModelPromptOptionsFid = 200103;
constexpr std::string_view kSystemConfigId = "__system_chat__";

enum class SemanticArgumentType : uint8_t {
    VARCHAR,
    OPTIONS,
};

struct SignatureSpec {
    AIFunctionSignature signature;
    size_t argument_count;
    std::array<SemanticArgumentType, 3> argument_types;
};

Status invalid_ai_expression() {
    return Status::InvalidArgument("Invalid AI function expression");
}

std::optional<SignatureSpec> signature_spec(int64_t fid) {
    switch (fid) {
    case kPromptFid:
        return SignatureSpec{
                .signature = AIFunctionSignature::PROMPT,
                .argument_count = 1,
                .argument_types = {SemanticArgumentType::VARCHAR},
        };
    case kPromptOptionsFid:
        return SignatureSpec{
                .signature = AIFunctionSignature::PROMPT_OPTIONS,
                .argument_count = 2,
                .argument_types = {SemanticArgumentType::VARCHAR, SemanticArgumentType::OPTIONS},
        };
    case kModelPromptFid:
        return SignatureSpec{
                .signature = AIFunctionSignature::MODEL_PROMPT,
                .argument_count = 2,
                .argument_types = {SemanticArgumentType::VARCHAR, SemanticArgumentType::VARCHAR},
        };
    case kModelPromptOptionsFid:
        return SignatureSpec{
                .signature = AIFunctionSignature::MODEL_PROMPT_OPTIONS,
                .argument_count = 3,
                .argument_types = {SemanticArgumentType::VARCHAR, SemanticArgumentType::VARCHAR,
                                   SemanticArgumentType::OPTIONS},
        };
    default:
        return std::nullopt;
    }
}

std::optional<SignatureSpec> signature_spec_for(AIFunctionSignature signature) {
    switch (signature) {
    case AIFunctionSignature::PROMPT:
        return signature_spec(kPromptFid);
    case AIFunctionSignature::PROMPT_OPTIONS:
        return signature_spec(kPromptOptionsFid);
    case AIFunctionSignature::MODEL_PROMPT:
        return signature_spec(kModelPromptFid);
    case AIFunctionSignature::MODEL_PROMPT_OPTIONS:
        return signature_spec(kModelPromptOptionsFid);
    }
    return std::nullopt;
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

bool matches_argument_type(const TTypeDesc& type, SemanticArgumentType expected) {
    return expected == SemanticArgumentType::VARCHAR ? is_varchar_type(type) : is_options_type(type);
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
                                   SemanticArgumentType expected) {
    if (expected == SemanticArgumentType::VARCHAR) {
        return declared.type == TYPE_VARCHAR && is_fe_string_type(child);
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

} // namespace

bool AIFunctionCallExpr::is_ai_function_name(std::string_view name) {
    constexpr std::string_view kName = "ai_complete";
    if (name.size() != kName.size()) {
        return false;
    }
    for (size_t i = 0; i < name.size(); ++i) {
        if (ascii_tolower(static_cast<unsigned char>(name[i])) != kName[i]) {
            return false;
        }
    }
    return true;
}

bool AIFunctionCallExpr::is_ai_function_id(int64_t fid) {
    return signature_spec(fid).has_value();
}

AIFunctionCallExpr::AIFunctionCallExpr(const TExprNode& node, AIFunctionSignature signature,
                                       std::vector<TypeDescriptor> argument_types)
        : Expr(node),
          _signature(signature),
          _model_config_id(kSystemConfigId),
          _argument_types(std::move(argument_types)) {
    _fn = TFunction();
}

AIFunctionCallExpr::AIFunctionCallExpr(const AIFunctionCallExpr& other)
        : Expr(other),
          _signature(other._signature),
          _model_config_id(other._model_config_id),
          _argument_types(other._argument_types) {}

StatusOr<AIFunctionCallExpr*> AIFunctionCallExpr::create(ObjectPool* pool, const TExprNode& node) {
    if (pool == nullptr ||
        (node.node_type != TExprNodeType::FUNCTION_CALL && node.node_type != TExprNodeType::COMPUTE_FUNCTION_CALL) ||
        !node.__isset.fn || node.fn.binary_type != TFunctionBinaryType::AI ||
        !is_ai_function_name(node.fn.name.function_name) || !node.fn.__isset.fid || !node.fn.__isset.ai_model_source ||
        node.fn.ai_model_source != TAIModelSource::SYSTEM || !node.__isset.ai_model_config_id ||
        node.ai_model_config_id != kSystemConfigId || node.fn.has_var_args || !is_varchar_type(node.type) ||
        !is_varchar_type(node.fn.ret_type) || !node.__isset.is_nullable || !node.is_nullable ||
        node.fn.__isset.hdfs_location || node.fn.__isset.content || node.fn.__isset.cloud_configuration ||
        node.fn.__isset.service_url) {
        return invalid_ai_expression();
    }

    const auto spec = signature_spec(node.fn.fid);
    if (!spec.has_value() || node.num_children != spec->argument_count ||
        node.fn.arg_types.size() != spec->argument_count) {
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
    return pool->add(new AIFunctionCallExpr(node, spec->signature, std::move(argument_types)));
}

Expr* AIFunctionCallExpr::clone(ObjectPool* pool) const {
    return pool->add(new AIFunctionCallExpr(*this));
}

Status AIFunctionCallExpr::_validate_children() const {
    const auto spec = signature_spec_for(_signature);
    if (!spec.has_value() || _argument_types.size() != spec->argument_count ||
        _children.size() != spec->argument_count) {
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

StatusOr<AIFunctionInputBatch> AIFunctionCallExpr::build_input_batch(ExprContext* context, Chunk* chunk,
                                                                     std::string_view default_model) const {
    RETURN_IF_ERROR(_validate_children());
    if (context == nullptr || chunk == nullptr || chunk->num_rows() == 0) {
        return invalid_ai_expression();
    }

    size_t prompt_index = 0;
    std::optional<size_t> model_index;
    std::optional<size_t> options_index;
    switch (_signature) {
    case AIFunctionSignature::PROMPT:
        break;
    case AIFunctionSignature::PROMPT_OPTIONS:
        options_index = 1;
        break;
    case AIFunctionSignature::MODEL_PROMPT:
        model_index = 0;
        prompt_index = 1;
        break;
    case AIFunctionSignature::MODEL_PROMPT_OPTIONS:
        model_index = 0;
        prompt_index = 1;
        options_index = 2;
        break;
    }

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
    if (options_index.has_value()) {
        const ColumnPtr& options_column = columns[*options_index];
        if (!_children[*options_index]->is_constant()) {
            return Status::InvalidArgument("AI function options must be constant");
        }
        ASSIGN_OR_RETURN(auto options,
                         build_ai_provider_options(*options_column, _children[*options_index]->type(), 0));
        batch.options = std::make_shared<const AIProviderOptions>(std::move(options));
    }

    ColumnViewer<TYPE_VARCHAR> prompt(columns[prompt_index]);
    batch.rows.reserve(chunk->num_rows());
    if (!model_index.has_value()) {
        for (size_t row = 0; row < chunk->num_rows(); ++row) {
            if (prompt.is_null(row)) {
                batch.rows.emplace_back();
                continue;
            }
            batch.rows.emplace_back(AIFunctionRowInput{
                    .action = AIFunctionRowAction::DISPATCH,
                    .model = std::string(default_model),
                    .prompt = prompt.value(row).to_string(),
            });
        }
        return batch;
    }

    ColumnViewer<TYPE_VARCHAR> model(columns[*model_index]);
    for (size_t row = 0; row < chunk->num_rows(); ++row) {
        if (model.is_null(row) || prompt.is_null(row)) {
            batch.rows.emplace_back();
            continue;
        }
        const Slice model_value = model.value(row);
        if (contains_only_ascii_whitespace(model_value)) {
            batch.rows.emplace_back(AIFunctionRowInput{
                    .action = AIFunctionRowAction::TERMINAL_ROW_FAILURE,
            });
            continue;
        }
        batch.rows.emplace_back(AIFunctionRowInput{
                .action = AIFunctionRowAction::DISPATCH,
                .model = model_value.to_string(),
                .prompt = prompt.value(row).to_string(),
        });
    }
    return batch;
}

StatusOr<ColumnPtr> AIFunctionCallExpr::evaluate_checked(ExprContext*, Chunk*) {
    return Status::NotSupported("AI expression requires AIProject asynchronous execution");
}

} // namespace starrocks
