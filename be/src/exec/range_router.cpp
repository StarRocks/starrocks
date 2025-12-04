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

#include "exec/range_router.h"

#include <algorithm>

#include "column/column_helper.h"
#include "column/datum_convert.h"
#include "column/schema.h"
#include "common/statusor.h"
#include "fmt/format.h"
#include "runtime/descriptors.h"
#include "storage/types.h"

namespace starrocks {

RangeRouter::RangeRouter() : _single_inf_range(false) {}

Status RangeRouter::init(const std::vector<TTabletRange>& tablet_ranges, size_t num_columns) {
    if (!_lower_boundary.empty()) {
        DCHECK_EQ(_lower_boundary.size(), _upper_boundary.size());
        return Status::OK();
    }

    if (tablet_ranges.empty()) {
        return Status::InternalError("no tablet ranges for RangeRouter init");
    }

    const size_t num_ranges = tablet_ranges.size();

    // Reset state in case init() is called after a previous failed attempt.
    _lower_boundary.resize(num_columns);
    _upper_boundary.resize(num_columns);
    _lower_bound_inclusive.resize(num_ranges);
    _upper_bound_inclusive.resize(num_ranges);
    _single_inf_range = false;

    RETURN_IF_ERROR(_validate_range(tablet_ranges, num_columns));
    if (_single_inf_range) {
        return Status::OK();
    }

    // Infer TypeDescriptor for each range-distributed column from tablet ranges.
    // Caller should guarantee that for every column for range distribution, at least one
    // tablet range provides a concrete type information for lower bound or upper bound.
    std::vector<std::unique_ptr<TypeDescriptor>> type_descs(num_columns);
    for (size_t i = 0; i < num_columns; ++i) {
        for (const auto& tablet_range : tablet_ranges) {
            const TTuple* lower = tablet_range.__isset.lower_bound ? &tablet_range.lower_bound : nullptr;
            const TTuple* upper = tablet_range.__isset.upper_bound ? &tablet_range.upper_bound : nullptr;
            DCHECK(lower != nullptr || upper != nullptr);

            if (lower != nullptr) {
                DCHECK(i < lower->values.size());
                type_descs[i] = std::make_unique<TypeDescriptor>(TypeDescriptor::from_thrift(lower->values[i].type));
                break;
            }

            if (upper != nullptr) {
                DCHECK(i < upper->values.size());
                type_descs[i] = std::make_unique<TypeDescriptor>(TypeDescriptor::from_thrift(upper->values[i].type));
                break;
            }
        }
    }

    auto parse_bound = [&](const TTuple* bound, std::vector<ColumnPtr>& boundary_columns) {
        for (size_t i = 0; i < num_columns; ++i) {
            const auto& type_desc = type_descs[i];
            DCHECK(type_desc != nullptr);
            ColumnPtr& column = boundary_columns[i];
            if (!column) {
                column = ColumnHelper::create_column(*type_desc, false);
                column->reserve(num_ranges);
            }

            if (bound == nullptr) {
                // [-inf, upper_bound] or [lower_bound, inf] for the current bound
                // Append a placeholder default value for the current column which will not
                // participate in comparisons
                column->append_default(1);
                continue;
            }

            const auto& value = bound->values[i];
            Datum datum;
            std::string value_str;
            auto type_info = get_type_info(type_desc->type, type_desc->precision, type_desc->scale);
            if (value.__isset.string_value) {
                value_str = value.string_value;
            } else if (value.__isset.long_value) {
                value_str = std::to_string(value.long_value);
            } else {
                return Status::InternalError("Missing long_value or string_value in range bound");
            }
            RETURN_IF_ERROR(datum_from_string(type_info.get(), &datum, value_str, nullptr));

            column->append_datum(datum);
        }
        return Status::OK();
    };

    for (size_t range_idx = 0; range_idx < num_ranges; ++range_idx) {
        const auto& tablet_range = tablet_ranges[range_idx];
        const TTuple* lower = tablet_range.__isset.lower_bound ? &tablet_range.lower_bound : nullptr;
        const TTuple* upper = tablet_range.__isset.upper_bound ? &tablet_range.upper_bound : nullptr;

        RETURN_IF_ERROR(parse_bound(lower, _lower_boundary));
        RETURN_IF_ERROR(parse_bound(upper, _upper_boundary));

        if (lower != nullptr) {
            DCHECK(tablet_range.__isset.lower_bound_included);
            _lower_bound_inclusive[range_idx] = tablet_range.lower_bound_included;
        }
        if (upper != nullptr) {
            DCHECK(tablet_range.__isset.upper_bound_included);
            _upper_bound_inclusive[range_idx] = tablet_range.upper_bound_included;
        }
    }

    return Status::OK();
}

Status RangeRouter::route_chunk_rows(Chunk* chunk, const std::vector<SlotDescriptor*>& slot_descs,
                                              const std::vector<uint16_t>& row_indices,
                                              const std::vector<int64_t>& candidate_dest,
                                              std::vector<int64_t>* target_dest) {
    DCHECK(target_dest != nullptr);
    if (_lower_boundary.empty() || _upper_boundary.empty()) {
        return Status::InternalError("RangeRouter::init() must be called before route_chunk_rows()");
    }
    if (candidate_dest.empty()) {
        return Status::InternalError("no candidate destinations routing range");
    }

    if (target_dest->size() < chunk->num_rows()) {
        target_dest->resize(chunk->num_rows());
    }

    if (_single_inf_range) {
        const int64_t dest = candidate_dest[0];
        for (uint16_t row_idx : row_indices) {
            (*target_dest)[row_idx] = dest;
        }
        return Status::OK();
    }

    std::vector<ColumnPtr> columns;
    columns.reserve(slot_descs.size());
    for (const auto& slot_desc : slot_descs) {
        columns.push_back(chunk->get_column_by_slot_id(slot_desc->id()));
    }

    for (uint16_t row_idx : row_indices) {
        ASSIGN_OR_RETURN(size_t t_idx, _find_tablet_index_for_row(columns, row_idx));
        if (t_idx >= candidate_dest.size()) {
            return Status::InternalError(
                    fmt::format("tablet index {} out of range for candidate destinations size {}", t_idx,
                                candidate_dest.size()));
        }
        (*target_dest)[row_idx] = candidate_dest[t_idx];
    }
    return Status::OK();
}

Status RangeRouter::_validate_range(const std::vector<TTabletRange>& tablet_ranges, size_t num_columns) const {
    // 1. check inf
    int lower_inf_count = 0;
    int upper_inf_count = 0;
    for (const auto& range : tablet_ranges) {
        if (!range.__isset.lower_bound) {
            lower_inf_count++;
        } else {
            const auto lower_bound = range.lower_bound;
            RETURN_IF(!lower_bound.__isset.values, Status::InternalError("lower_bound value is required"));
            RETURN_IF(lower_bound.values.size() != num_columns,
                      Status::InternalError("lower_bound value size is not equal to column size"));
        }

        if (!range.__isset.upper_bound) {
            upper_inf_count++;
        } else {
            const auto upper_bound = range.upper_bound;
            RETURN_IF(!upper_bound.__isset.values, Status::InternalError("upper_bound value is required"));
            RETURN_IF(upper_bound.values.size() != num_columns,
                      Status::InternalError("upper_bound value size is not equal to column size"));
        }
    }
    RETURN_IF(lower_inf_count > 1 || upper_inf_count > 1,
              Status::InternalError("lower_inf_count and upper_inf_count must be less than or equal to 1"));
    RETURN_IF(lower_inf_count == 1 && !tablet_ranges.empty() && tablet_ranges[0].__isset.lower_bound,
              Status::InternalError("-inf range is not allowed to be set in the non-first tablet range"));
    RETURN_IF(upper_inf_count == 1 && !tablet_ranges.empty() && tablet_ranges[tablet_ranges.size() - 1].__isset.upper_bound,
              Status::InternalError("+inf range is not allowed to be set in the non-last tablet range"));
    // If both lower and upper bounds are inf, and there is only one tablet range,
    // Means the range is [-inf, +inf]
    if (lower_inf_count == 1 && upper_inf_count == 1 && tablet_ranges.size() == 1) {
        _single_inf_range = true;
    }

    // 2. check range order and overlapping
    auto compare_variant = [](const TVariant& a, const TVariant& b) -> std::optional<int> {
        if (a.__isset.long_value && b.__isset.long_value) {
            return (a.long_value < b.long_value) ? -1 : (a.long_value > b.long_value) ? 1 : 0;
        }
        if (a.__isset.string_value && b.__isset.string_value) {
            return a.string_value.compare(b.string_value);
        }
        return std::nullopt;  // Type mismatch
    };

    for (size_t i = 1; i < tablet_ranges.size(); ++i) {
        const auto& prev = tablet_ranges[i - 1];
        const auto& curr = tablet_ranges[i];
        
        // Compare bounds lexicographically
        for (size_t col = 0; col < num_columns; ++col) {
            auto cmp = compare_variant(prev.upper_bound.values[col], curr.lower_bound.values[col]);
            RETURN_IF(!cmp.has_value(),
                      Status::InternalError(fmt::format("Type mismatch at column {} between range[{}] and range[{}]", 
                                                       col, i - 1, i)));
            
            if (cmp.value() < 0) {
                break;  // Valid ordering
            }
            if (cmp.value() > 0) {
                return Status::InternalError(fmt::format("Range[{}] > range[{}] at column {}", i - 1, i, col));
            }

            // All bonnds are equal values - check inclusiveness
            if (col == num_columns - 1 && prev.upper_bound_included && curr.lower_bound_included) {
                return Status::InternalError(fmt::format("Range[{}] overlaps with range[{}]", i - 1, i));
            }
        }
    }
    return Status::OK();
}

bool RangeRouter::_check_row_in_bound(const std::vector<ColumnPtr>& columns, uint16_t row_idx,
                                      size_t range_idx, bool is_lower_bound) const {
    auto compare_row = [&](const std::vector<ColumnPtr>& columns, uint16_t row_idx,
                           const std::vector<ColumnPtr>& targets, size_t target_idx) -> int {
        DCHECK_EQ(targets.size(), columns.size());
        for (size_t i = 0; i < targets.size(); ++i) {
            const ColumnPtr& target_col = targets[i];
            DCHECK(target_col != nullptr);
            int cmp = columns[i]->compare_at(row_idx, target_idx, *target_col, 1);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    };

    const auto& bound_inclusive = is_lower_bound ? _lower_bound_inclusive[range_idx] : _upper_bound_inclusive[range_idx];
    const auto& boundary = is_lower_bound ? _lower_boundary : _upper_boundary;
    // Check if bound is -inf or not
    if (bound_inclusive.has_value()) {
        const bool inclusive = bound_inclusive.value();
        int cmp = compare_row(columns, row_idx, boundary, range_idx);
        if ((is_lower_bound && ((inclusive && cmp < 0) || (!inclusive && cmp <= 0))) ||
            (!is_lower_bound && ((inclusive && cmp > 0) || (!inclusive && cmp >= 0)))) {
            return false;
        }
    }

    return true;
}

StatusOr<size_t> RangeRouter::_find_tablet_index_for_row(const std::vector<ColumnPtr>& columns, uint16_t row_idx) const {
    DCHECK(!_lower_boundary.empty());
    DCHECK(_lower_boundary[0] != nullptr);
    size_t num_ranges = _lower_boundary[0]->size();
    if (num_ranges == 0) {
        return Status::InternalError("no tablet ranges in RangeRouter");
    }

    // Binary search on tablet ranges using lower bound when available.
    // 1. Search by lower bound first. Find the first range that DOES NOT fit the lower bound.
    size_t left = 0;
    size_t right = num_ranges;
    size_t mid;
    while (left < right) {
        mid = left + (right - left) / 2;
        if (_check_row_in_bound(columns, row_idx, mid, true)) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    // 2. Search by upper bound. (left - 1) means the last lower bound the fits the row.
    if (left == 0 || !_check_row_in_bound(columns, row_idx, left - 1, false)) {
        return Status::DataQualityError(
            fmt::format("Failed to match range: row {} does not match any tablet range", row_idx));
    }
    return left - 1;
}

} // namespace starrocks