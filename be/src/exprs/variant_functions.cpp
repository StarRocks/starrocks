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

#include "exprs/variant_functions.h"

#include <memory>

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "runtime/runtime_state.h"
#include "util/variant_converter.h"
#include "variant_path_parser.h"

namespace starrocks {

StatusOr<ColumnPtr> VariantFunctions::variant_query(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    if (columns.size() != 2) {
        return Status::InvalidArgument("VariantFunctions::variant_query requires 2 arguments");
    }

    return _do_variant_query<TYPE_VARIANT>(context, columns);
}

Status VariantFunctions::variant_segments_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    // Don't parse if the path is not constant
    if (!context->is_notnull_constant_column(1)) {
        return Status::OK();
    }

    const auto path_col = context->get_constant_column(1);
    const Slice variant_path = ColumnHelper::get_const_value<TYPE_VARCHAR>(path_col);

    std::string path_string = variant_path.to_string();
    auto variant_path_status = VariantPathParser::parse(path_string);
    RETURN_IF(!variant_path_status.ok(), variant_path_status.status());
    auto* path_state = new NativeVariantPath();
    path_state->variant_path.reset(std::move(variant_path_status.value()));
    context->set_function_state(scope, path_state);
    VLOG(10) << "Preloaded variant path: " << path_string;

    return Status::OK();
}

static StatusOr<VariantPath*> get_or_parse_variant_segments(FunctionContext* context, const Slice path_slice,
                                                            VariantPath* variant_path) {
    auto* cached = reinterpret_cast<NativeVariantPath*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    if (cached != nullptr) {
        // If we already have parsed segments, return them
        return &cached->variant_path;
    }

    std::string path_string = path_slice.to_string();
    auto path_status = VariantPathParser::parse(path_string);
    RETURN_IF(!path_status.ok(), path_status.status());
    variant_path->reset(std::move(path_status.value()));

    return variant_path;
}

Status VariantFunctions::variant_segments_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* variant_path = reinterpret_cast<NativeVariantPath*>(context->get_function_state(scope));
        delete variant_path;
    }

    return Status::OK();
}

template <LogicalType ResultType>
StatusOr<ColumnPtr> VariantFunctions::_do_variant_query(FunctionContext* context, const Columns& columns) {
    size_t num_rows = columns[0]->size();

    auto variant_viewer = ColumnViewer<TYPE_VARIANT>(columns[0]);
    auto json_path_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);

    ColumnBuilder<ResultType> result(num_rows);

    VariantPath stored_path;
    for (size_t row = 0; row < num_rows; ++row) {
        if (variant_viewer.is_null(row) || json_path_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto path_slice = json_path_viewer.value(row);
        auto variant_segments_status = get_or_parse_variant_segments(context, path_slice, &stored_path);
        if (!variant_segments_status.ok()) {
            result.append_null();
            continue;
        }

        const VariantValue* variant_value = variant_viewer.value(row);
        if (!variant_value) {
            result.append_null();
            continue;
        }

        const std::string& value = variant_value->get_value();
        if (value.empty()) {
            result.append_null();
            continue;
        }

        try {
            Variant variant(variant_value->get_metadata(), value);
            StatusOr<Variant> variant_field = VariantPath::seek(&variant, variant_segments_status.value());
            if (!variant_field.ok()) {
                LOG(WARNING) << "Failed to seek variant path: " << path_slice.to_string()
                             << "in the variant value: " << variant_value->to_string();
                result.append_null();
                continue;
            }

            RuntimeState* state = context->state();
            cctz::time_zone zone;
            if (state == nullptr) {
                zone = cctz::local_time_zone();
            } else {
                zone = context->state()->timezone_obj();
            }

            if constexpr (ResultType == TYPE_VARIANT) {
                VariantValue sub_variant_value = VariantValue::of_variant(variant_field.value());
                result.append(std::move(sub_variant_value));
            } else {
                Status status = cast_variant_value_to<ResultType, true>(variant_field.value(), zone, result);
                if (!status.ok()) {
                    result.append_null();
                }
            }
        } catch (const std::exception& e) {
            LOG(WARNING) << "Error processing variant query: " << variant_value->to_string() << " with path "
                         << path_slice.to_string() << ": " << e.what();
            result.append_null();
        }
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

} // namespace starrocks

#include "gen_cpp/opcode/VariantFunctions.inc"