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

#pragma once

#include "column/column_builder.h"
#include "column/type_traits.h"
#include "common/statusor.h"
#include "formats/parquet/variant.h"
#include "types/logical_type.h"
#include "variant_util.h"

namespace starrocks {

StatusOr<RunTimeCppType<TYPE_BOOLEAN>> cast_variant_to_bool(const Variant& variant,
                                                            ColumnBuilder<TYPE_BOOLEAN>& result);

StatusOr<RunTimeCppType<TYPE_VARCHAR>> cast_variant_to_string(const Variant& variant, const VariantValue& value,
                                                              const cctz::time_zone& zone,
                                                              ColumnBuilder<TYPE_VARCHAR>& result);

template <LogicalType ResultType>
StatusOr<RunTimeColumnType<ResultType>> cast_variant_to_arithmetic(const Variant& variant,
                                                                   ColumnBuilder<ResultType>& result) {
    VariantType type = variant.type();

    switch (type) {
    case VariantType::NULL_TYPE: {
        result.append_null();
        return Status::OK();
    }
    case VariantType::BOOLEAN: {
        auto value = variant.get_bool();
        if (!value.ok()) {
            return value.status();
        }

        result.append(static_cast<RunTimeCppType<ResultType>>(value.value()));
        return Status::OK();
    }
    case VariantType::INT8: {
        auto value = variant.get_int8();
        if (!value.ok()) {
            return value.status();
        }

        result.append(static_cast<RunTimeCppType<ResultType>>(value.value()));
        return Status::OK();
    }
    case VariantType::INT16: {
        auto value = variant.get_int16();
        if (!value.ok()) {
            return value.status();
        }

        result.append(static_cast<RunTimeCppType<ResultType>>(value.value()));
        return Status::OK();
    }
    case VariantType::INT32: {
        auto value = variant.get_int32();
        if (!value.ok()) {
            return value.status();
        }

        result.append(static_cast<RunTimeCppType<ResultType>>(value.value()));
        return Status::OK();
    }
    case VariantType::INT64: {
        auto value = variant.get_int64();
        if (!value.ok()) {
            return value.status();
        }

        result.append(static_cast<RunTimeCppType<ResultType>>(value.value()));
        return Status::OK();
    }
    default:
        return Status::NotSupported(fmt::format("Cannot cast variant of type {} to {}",
                                                VariantUtil::type_to_string(type), logical_type_to_string(ResultType)));
    }
}

template <LogicalType ResultType, bool AllowThrowException>
static Status cast_variant_value_to(const Variant& variant, const cctz::time_zone& zone,
                                    ColumnBuilder<ResultType>& result) {
    if constexpr (!lt_is_arithmetic<ResultType> && !lt_is_string<ResultType> && ResultType != TYPE_VARIANT) {
        std::cerr << "Cannot cast variant of type " << VariantUtil::type_to_string(variant.type()) << " to "
                  << logical_type_to_string(ResultType) << std::endl;
        if constexpr (AllowThrowException) {
            return Status::NotSupported(
                    fmt::format("Cannot cast variant to type {}", logical_type_to_string(ResultType)));
        }

        result.append_null();
        return Status::OK();
    }

    if constexpr (ResultType == TYPE_VARIANT) {
        // For TYPE_VARIANT, directly convert back to VariantValue
        auto variant_value_result = variant.to_value();
        if (!variant_value_result.ok()) {
            std::cerr << "Failed to convert variant to VariantValue: " << variant_value_result.status().to_string()
                      << std::endl;
            if constexpr (AllowThrowException) {
                return variant_value_result.status();
            } else {
                result.append_null();
                return Status::OK();
            }
        }
        std::cout << "Converting variant to VariantValue: " << variant_value_result->to_string() << std::endl;
        result.append(std::move(variant_value_result.value()));
        return Status::OK();
    }

    // For non-variant types, we need the VariantValue for string conversion
    auto variant_value_result = variant.to_value();
    if (!variant_value_result.ok()) {
        if constexpr (AllowThrowException) {
            return variant_value_result.status();
        } else {
            result.append_null();
            return Status::OK();
        }
    }

    VariantValue variant_value = std::move(variant_value_result.value());
    std::cout << "Converting non-variant type from VariantValue: " << variant_value.to_string() << std::endl;

    Status status;
    if constexpr (ResultType == TYPE_BOOLEAN) {
        status = cast_variant_to_bool(variant, result);
    } else if constexpr (lt_is_arithmetic<ResultType>) {
        status = cast_variant_to_arithmetic<ResultType>(variant, result);
    } else if constexpr (lt_is_string<ResultType>) {
        status = cast_variant_to_string(variant, variant_value, zone, result);
    }

    if (!status.ok()) {
        if constexpr (AllowThrowException) {
            return Status::VariantError(fmt::format("Cannot cast variant to type {}: {}",
                                                    logical_type_to_string(ResultType), status.to_string()));
        } else {
            result.append_null();
        }
    }

    return Status::OK();
}

} // namespace starrocks