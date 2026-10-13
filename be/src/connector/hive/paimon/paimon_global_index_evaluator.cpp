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

#include "connector/hive/paimon/paimon_global_index_evaluator.h"

#include <fmt/format.h>
#include <paimon/predicate/literal.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace starrocks {
namespace {

constexpr std::string_view kOperatorType = "o";
constexpr std::string_view kBinaryType = "b";
constexpr std::string_view kChildren = "c";
constexpr std::string_view kCompoundType = "ct";
constexpr std::string_view kFunctionName = "f";
constexpr std::string_view kArguments = "a";
constexpr std::string_view kName = "n";
constexpr std::string_view kNegated = "ng";
constexpr std::string_view kType = "t";
constexpr std::string_view kValue = "v";

constexpr std::string_view kBinary = "b";
constexpr std::string_view kCompound = "cp";
constexpr std::string_view kIn = "ip";
constexpr std::string_view kIsNull = "isn";
constexpr std::string_view kCall = "ca";
constexpr std::string_view kColumn = "cr";
constexpr std::string_view kConstant = "co";

bool equals_ignore_case(std::string_view left, std::string_view right) {
    return left.size() == right.size() && std::equal(left.begin(), left.end(), right.begin(), [](char lhs, char rhs) {
               return std::tolower(static_cast<unsigned char>(lhs)) == std::tolower(static_cast<unsigned char>(rhs));
           });
}

Status invalid_node(std::string_view message) {
    return Status::InvalidArgument(fmt::format("Invalid Paimon Global Index predicate: {}", message));
}

Status paimon_error(std::string_view operation, const paimon::Status& status) {
    return Status::InternalError(fmt::format("Paimon Global Index {} failed: {}", operation, status.ToString()));
}

StatusOr<paimon::Literal> literal_from_json(const rapidjson::Value& node) {
    if (!node.IsObject() || !node.HasMember(kOperatorType.data()) || !node[kOperatorType.data()].IsString() ||
        std::string_view(node[kOperatorType.data()].GetString(), node[kOperatorType.data()].GetStringLength()) !=
                kConstant ||
        !node.HasMember(kType.data()) || !node[kType.data()].IsString() || !node.HasMember(kValue.data())) {
        return invalid_node("expected a typed constant");
    }

    std::string type = node[kType.data()].GetString();
    std::transform(type.begin(), type.end(), type.begin(),
                   [](unsigned char value) { return static_cast<char>(std::tolower(value)); });
    const rapidjson::Value& value = node[kValue.data()];
    if (type == "boolean" && value.IsBool()) {
        return paimon::Literal(value.GetBool());
    }
    if (type == "tinyint" && value.IsInt64()) {
        return paimon::Literal(static_cast<int8_t>(value.GetInt64()));
    }
    if (type == "smallint" && value.IsInt64()) {
        return paimon::Literal(static_cast<int16_t>(value.GetInt64()));
    }
    if (type == "int" && value.IsInt64()) {
        return paimon::Literal(static_cast<int32_t>(value.GetInt64()));
    }
    if (type == "bigint" && value.IsInt64()) {
        return paimon::Literal(value.GetInt64());
    }
    if (type == "float" && value.IsNumber()) {
        return paimon::Literal(value.GetFloat());
    }
    if (type == "double" && value.IsNumber()) {
        return paimon::Literal(value.GetDouble());
    }
    if ((type == "string" || type.rfind("varchar", 0) == 0 || type.rfind("char", 0) == 0) && value.IsString()) {
        return paimon::Literal(paimon::FieldType::STRING, value.GetString(), value.GetStringLength());
    }
    return invalid_node(fmt::format("unsupported or malformed literal type '{}'", type));
}

StatusOr<std::string_view> column_name(const rapidjson::Value& node) {
    if (!node.IsObject() || !node.HasMember(kOperatorType.data()) || !node[kOperatorType.data()].IsString() ||
        std::string_view(node[kOperatorType.data()].GetString(), node[kOperatorType.data()].GetStringLength()) !=
                kColumn ||
        !node.HasMember(kName.data()) || !node[kName.data()].IsString()) {
        return invalid_node("expected a column reference");
    }
    return std::string_view(node[kName.data()].GetString(), node[kName.data()].GetStringLength());
}

StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> checked_result(
        std::string_view operation, paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>>&& result) {
    if (!result.ok()) {
        return paimon_error(operation, result.status());
    }
    if (result.value() == nullptr) {
        return Status::NotSupported(fmt::format("Paimon Global Index reader does not support {}", operation));
    }
    return std::move(result).value();
}

} // namespace

StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> PaimonGlobalIndexEvaluator::evaluate(
        const rapidjson::Value& node) const {
    if (!node.IsObject() || !node.HasMember(kOperatorType.data()) || !node[kOperatorType.data()].IsString()) {
        return invalid_node("missing operator type");
    }
    const std::string_view type(node[kOperatorType.data()].GetString(), node[kOperatorType.data()].GetStringLength());
    if (type == kBinary) {
        return _evaluate_binary(node);
    }
    if (type == kCompound) {
        return _evaluate_compound(node);
    }
    if (type == kIn) {
        return _evaluate_in(node);
    }
    if (type == kIsNull) {
        return _evaluate_is_null(node);
    }
    if (type == kCall) {
        return _evaluate_call(node);
    }
    return invalid_node(fmt::format("unsupported operator type '{}'", type));
}

StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> PaimonGlobalIndexEvaluator::_evaluate_binary(
        const rapidjson::Value& node) const {
    if (!node.HasMember(kBinaryType.data()) || !node[kBinaryType.data()].IsString() ||
        !node.HasMember(kChildren.data()) || !node[kChildren.data()].IsArray() || node[kChildren.data()].Size() != 2) {
        return invalid_node("malformed binary predicate");
    }
    const rapidjson::Value& children = node[kChildren.data()];
    ASSIGN_OR_RETURN(std::string_view column, column_name(children[0]));
    ASSIGN_OR_RETURN(paimon::Literal literal, literal_from_json(children[1]));
    ASSIGN_OR_RETURN(std::shared_ptr<paimon::GlobalIndexReader> reader, _reader_getter(column));

    const std::string_view type(node[kBinaryType.data()].GetString(), node[kBinaryType.data()].GetStringLength());
    if (type == "EQ") {
        return checked_result("equality evaluation", reader->VisitEqual(literal));
    }
    if (type == "NE") {
        return checked_result("inequality evaluation", reader->VisitNotEqual(literal));
    }
    if (type == "LT") {
        return checked_result("less-than evaluation", reader->VisitLessThan(literal));
    }
    if (type == "LE") {
        return checked_result("less-or-equal evaluation", reader->VisitLessOrEqual(literal));
    }
    if (type == "GT") {
        return checked_result("greater-than evaluation", reader->VisitGreaterThan(literal));
    }
    if (type == "GE") {
        return checked_result("greater-or-equal evaluation", reader->VisitGreaterOrEqual(literal));
    }
    return invalid_node(fmt::format("unsupported binary predicate '{}'", type));
}

StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> PaimonGlobalIndexEvaluator::_evaluate_compound(
        const rapidjson::Value& node) const {
    if (!node.HasMember(kCompoundType.data()) || !node[kCompoundType.data()].IsString() ||
        !node.HasMember(kChildren.data()) || !node[kChildren.data()].IsArray() || node[kChildren.data()].Size() != 2) {
        return invalid_node("malformed compound predicate");
    }
    const rapidjson::Value& children = node[kChildren.data()];
    ASSIGN_OR_RETURN(std::shared_ptr<paimon::GlobalIndexResult> left, evaluate(children[0]));
    ASSIGN_OR_RETURN(std::shared_ptr<paimon::GlobalIndexResult> right, evaluate(children[1]));
    const std::string_view type(node[kCompoundType.data()].GetString(), node[kCompoundType.data()].GetStringLength());
    if (type == "AND") {
        return checked_result("AND evaluation", left->And(right));
    }
    if (type == "OR") {
        return checked_result("OR evaluation", left->Or(right));
    }
    return invalid_node(fmt::format("unsupported compound predicate '{}'", type));
}

StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> PaimonGlobalIndexEvaluator::_evaluate_in(
        const rapidjson::Value& node) const {
    if (!node.HasMember(kNegated.data()) || !node[kNegated.data()].IsBool() || !node.HasMember(kChildren.data()) ||
        !node[kChildren.data()].IsArray() || node[kChildren.data()].Size() < 2) {
        return invalid_node("malformed IN predicate");
    }
    const rapidjson::Value& children = node[kChildren.data()];
    ASSIGN_OR_RETURN(std::string_view column, column_name(children[0]));
    ASSIGN_OR_RETURN(std::shared_ptr<paimon::GlobalIndexReader> reader, _reader_getter(column));
    std::vector<paimon::Literal> literals;
    literals.reserve(children.Size() - 1);
    for (rapidjson::SizeType i = 1; i < children.Size(); ++i) {
        ASSIGN_OR_RETURN(paimon::Literal literal, literal_from_json(children[i]));
        literals.emplace_back(std::move(literal));
    }
    const bool negated = node[kNegated.data()].GetBool();
    return checked_result(negated ? "NOT IN evaluation" : "IN evaluation",
                          negated ? reader->VisitNotIn(literals) : reader->VisitIn(literals));
}

StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> PaimonGlobalIndexEvaluator::_evaluate_is_null(
        const rapidjson::Value& node) const {
    if (!node.HasMember(kNegated.data()) || !node[kNegated.data()].IsBool() || !node.HasMember(kChildren.data()) ||
        !node[kChildren.data()].IsArray() || node[kChildren.data()].Size() != 1) {
        return invalid_node("malformed IS NULL predicate");
    }
    ASSIGN_OR_RETURN(std::string_view column, column_name(node[kChildren.data()][0]));
    ASSIGN_OR_RETURN(std::shared_ptr<paimon::GlobalIndexReader> reader, _reader_getter(column));
    const bool not_null = node[kNegated.data()].GetBool();
    return checked_result(not_null ? "IS NOT NULL evaluation" : "IS NULL evaluation",
                          not_null ? reader->VisitIsNotNull() : reader->VisitIsNull());
}

StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> PaimonGlobalIndexEvaluator::_evaluate_call(
        const rapidjson::Value& node) const {
    if (!node.HasMember(kFunctionName.data()) || !node[kFunctionName.data()].IsString() ||
        !node.HasMember(kArguments.data()) || !node[kArguments.data()].IsArray() ||
        node[kArguments.data()].Size() != 2) {
        return invalid_node("malformed index function");
    }
    const std::string_view function(node[kFunctionName.data()].GetString(),
                                    node[kFunctionName.data()].GetStringLength());
    if (!equals_ignore_case(function, "starts_with")) {
        return invalid_node(fmt::format("unsupported index function '{}'", function));
    }
    const rapidjson::Value& arguments = node[kArguments.data()];
    ASSIGN_OR_RETURN(std::string_view column, column_name(arguments[0]));
    ASSIGN_OR_RETURN(paimon::Literal prefix, literal_from_json(arguments[1]));
    ASSIGN_OR_RETURN(std::shared_ptr<paimon::GlobalIndexReader> reader, _reader_getter(column));
    return checked_result("starts_with evaluation", reader->VisitStartsWith(prefix));
}

} // namespace starrocks
