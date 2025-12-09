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
#include <boost/iterator/counting_iterator.hpp>

#include "column/column_helper.h"
#include "column/datum_convert.h"
#include "column/schema.h"
#include "common/statusor.h"
#include "fmt/format.h"
#include "runtime/descriptors.h"
#include "storage/types.h"

namespace starrocks {

Status RangeRouter::init(const std::vector<TTabletRange>& tablet_ranges, size_t num_columns) {
    if (!_boundaries.empty()) {
        DCHECK((_boundaries[0] == nullptr) ||  (_boundaries[0]->size() + 1 == _lower_bound_inclusive.size()));
        return Status::OK();
    }

    if (tablet_ranges.empty()) {
        return Status::InternalError("no tablet ranges for RangeRouter init");
    }

    const size_t num_ranges = tablet_ranges.size();
    _boundaries.resize(num_columns);
    _lower_bound_inclusive.resize(num_ranges, false);

    RETURN_IF_ERROR(_validate_range(tablet_ranges, num_columns));
    // (-inf, +inf) range, fast return
    if (num_ranges == 1) {
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

    auto parse_bound = [&](const TTuple& bound, std::vector<ColumnPtr>& boundary_columns) {
        for (size_t i = 0; i < num_columns; ++i) {
            const auto& type_desc = type_descs[i];
            DCHECK(type_desc != nullptr);
            ColumnPtr& column = boundary_columns[i];
            if (!column) {
                column = ColumnHelper::create_column(*type_desc, false);
                column->reserve(num_ranges);
            }

            const auto& value = bound.values[i];
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
        if (tablet_range.__isset.lower_bound) {
            DCHECK(tablet_range.__isset.lower_bound_included);
            _lower_bound_inclusive[range_idx] = tablet_range.lower_bound_included;
        }

        if (range_idx == 0) {
            continue;
        } else {
            DCHECK(tablet_range.__isset.lower_bound);
            RETURN_IF_ERROR(parse_bound(tablet_range.lower_bound, _boundaries));
        }
    }

    return Status::OK();
}

Status RangeRouter::route_chunk_rows(Chunk* chunk, const std::vector<SlotDescriptor*>& slot_descs,
                                     const std::vector<uint16_t>& row_indices,
                                     const std::vector<int64_t>& candidate_dest, std::vector<int64_t>* target_dest) {
    if (row_indices.empty()) {
        return Status::OK();
    }
    DCHECK(target_dest != nullptr);
    if (_boundaries.empty()) {
        return Status::InternalError("RangeRouter::init() must be called before route_chunk_rows()");
    }
    if (candidate_dest.empty()) {
        return Status::InternalError("no candidate destinations routing range");
    }

    target_dest->clear();
    target_dest->reserve(row_indices.size());
    // (-inf, +inf) range, fast return
    if (_lower_bound_inclusive.size() == 1) {
        const int64_t dest = candidate_dest[0];
        target_dest->resize(row_indices.size(), dest);
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
            return Status::InternalError(fmt::format("tablet index {} out of range for candidate destinations size {}",
                                                     t_idx, candidate_dest.size()));
        }
        target_dest->push_back(candidate_dest[t_idx]);
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
    RETURN_IF(lower_inf_count != 1 || upper_inf_count != 1,
              Status::InternalError("lower_inf_count and upper_inf_count must be 1"));
    RETURN_IF(tablet_ranges[0].__isset.lower_bound || tablet_ranges[tablet_ranges.size() - 1].__isset.upper_bound,
              Status::InternalError("-inf/inf range must be set for the first and last tablet range"));

    // 2. check if full range is covered by the tablet ranges
    auto compare_variant = [](const TVariant& a, const TVariant& b) -> std::optional<int> {
        if (a.__isset.long_value && b.__isset.long_value) {
            return (a.long_value < b.long_value) ? -1 : (a.long_value > b.long_value) ? 1 : 0;
        }
        if (a.__isset.string_value && b.__isset.string_value) {
            return a.string_value.compare(b.string_value);
        }
        return std::nullopt; // Type mismatch
    };

    for (size_t i = 1; i < tablet_ranges.size(); ++i) {
        const auto& prev = tablet_ranges[i - 1];
        const auto& curr = tablet_ranges[i];
        DCHECK(tablet_ranges[i - 1].__isset.upper_bound);
        DCHECK(tablet_ranges[i].__isset.lower_bound);
        const bool& prev_upper_inclusive = tablet_ranges[i - 1].upper_bound_included;
        const bool& curr_lower_inclusive = tablet_ranges[i].lower_bound_included;

        RETURN_IF(prev_upper_inclusive && curr_lower_inclusive || !prev_upper_inclusive && !curr_lower_inclusive,
                  Status::InternalError("adjacent ranges are overlapping for the inclusive/exclusive bound"));

        // Compare whether adjacent upper and lower bounds are equal
        for (size_t col = 0; col < num_columns; ++col) {
            auto cmp = compare_variant(prev.upper_bound.values[col], curr.lower_bound.values[col]);
            RETURN_IF(!cmp.has_value(),
                      Status::InternalError(fmt::format("Type mismatch at column {} between range[{}] and range[{}]",
                                                        col, i - 1, i)));

            if (cmp.value() != 0) {
                return Status::InternalError(fmt::format("Range[{}] != range[{}] at column {}", i - 1, i, col));
            }
        }
    }
    return Status::OK();
}

StatusOr<size_t> RangeRouter::_find_tablet_index_for_row(const std::vector<ColumnPtr>& columns,
                                                         uint16_t row_idx) const {
    size_t num_ranges = _lower_bound_inclusive.size();
    DCHECK(!_boundaries.empty());
    DCHECK(num_ranges == _boundaries[0]->size() + 1);

    auto _compare_row_with_boundary = [&](const std::vector<ColumnPtr>& columns, uint16_t row_idx,
                                          size_t boundary_idx) -> int {
        for (size_t i = 0; i < _boundaries.size(); ++i) {
            const auto& boundary_col = _boundaries[i];
            int cmp = columns[i]->compare_at(row_idx, boundary_idx, *boundary_col, -1);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    };

    // Binary search on tablet ranges using lower bound when available.
    // 1. Search by lower bound first. Find the first range that DOES NOT fit the lower bound.
    auto it = std::lower_bound(
            boost::counting_iterator<size_t>(0), boost::counting_iterator<size_t>(num_ranges), row_idx,
            [&](size_t range_idx, size_t row_idx) -> bool {
                // always true for the first lower bound (-inf)
                if (range_idx == 0) {
                    return true;
                }

                // range_idx is the index of the range, which is the index of the boundary + 1
                // because boundary does not include the lower bound of the first range and the upper bound of the last range
                int cmp = _compare_row_with_boundary(columns, row_idx, range_idx - 1);
                if (cmp == 0) {
                    return static_cast<bool>(_lower_bound_inclusive[range_idx]);
                }
                return cmp > 0;
            });
    size_t left = it - boost::counting_iterator<size_t>(0);

    // 2. Search by upper bound. (left - 1) means the last lower bound the fits the row.
    if (left == num_ranges) {
        return num_ranges - 1;
    }
    if (left == 0) {
        return Status::DataQualityError(
                fmt::format("Failed to match range: row {} does not match any tablet range", row_idx));
    }

    int upper_cmp = _compare_row_with_boundary(columns, row_idx, left - 1);
    bool upper_inclusive = !_lower_bound_inclusive[left];
    if (upper_cmp > 0 || (upper_cmp == 0 && !upper_inclusive)) {
        return Status::DataQualityError(
                fmt::format("Failed to match range: row {} does not match any tablet range", row_idx));
    }

    // return index of range
    return left - 1;
}

} // namespace starrocks