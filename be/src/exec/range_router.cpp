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
    const size_t num_ranges = tablet_ranges.size();
    if (num_ranges == 0) {
        return Status::InternalError("no tablet ranges for RangeRouter init");
    }
    if (!_upper_boundaries.empty()) {
        DCHECK((_upper_boundaries[0] == nullptr) || (_upper_boundaries[0]->size() + 1 == num_ranges));
        return Status::OK();
    }

    RETURN_IF_ERROR(_validate_range(tablet_ranges, num_columns));

    _upper_boundaries.resize(num_columns);
    for (size_t i = 0; i < num_ranges - 1; ++i) {
        _upper_boundaries_slice.emplace_back(&_upper_boundaries, i);
    }

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

    auto parse_bound = [&](const TTuple& bound, MutableColumns& boundary_columns) {
        for (size_t i = 0; i < num_columns; ++i) {
            const auto& type_desc = type_descs[i];
            DCHECK(type_desc != nullptr);
            MutableColumnPtr& column = boundary_columns[i];
            if (!column) {
                column = ColumnHelper::create_column(*type_desc, false);
                column->reserve(num_ranges);
            }

            const auto& value = bound.values[i];
            Datum datum;
            std::string value_str;
            auto type_info = get_type_info(type_desc->type, type_desc->precision, type_desc->scale);
            if (value.__isset.value) {
                value_str = value.value;
            } else {
                return Status::InternalError("Missing value in range bound");
            }
            RETURN_IF_ERROR(datum_from_string(type_info.get(), &datum, value_str, nullptr));

            column->append_datum(datum);
        }
        return Status::OK();
    };

    for (size_t range_idx = 0; range_idx < num_ranges - 1; ++range_idx) {
        const auto& tablet_range = tablet_ranges[range_idx];
        RETURN_IF_ERROR(parse_bound(tablet_range.upper_bound, _upper_boundaries));
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
    if (_upper_boundaries.empty()) {
        return Status::InternalError("RangeRouter::init() must be called before route_chunk_rows()");
    }
    if (candidate_dest.empty()) {
        return Status::InternalError("no candidate destinations routing range");
    }

    target_dest->clear();
    target_dest->reserve(row_indices.size());
    // (-inf, +inf) range, fast return
    if (_upper_boundaries[0] == nullptr) {
        const int64_t dest = candidate_dest[0];
        target_dest->resize(row_indices.size(), dest);
        return Status::OK();
    }

    MutableColumns columns(slot_descs.size());
    for (size_t i = 0; i < slot_descs.size(); ++i) {
        columns[i] = chunk->get_column_by_slot_id(slot_descs[i]->id())->as_mutable_ptr();
    }

    ChunkRow cur_check_row(&columns, 0);
    for (uint16_t row_idx : row_indices) {
        cur_check_row.index = row_idx;
        size_t t_idx = _find_tablet_index_for_row(cur_check_row);
        if (t_idx >= candidate_dest.size()) {
            return Status::InternalError(fmt::format("tablet index {} out of range for candidate destinations size {}",
                                                     t_idx, candidate_dest.size()));
        }
        target_dest->push_back(candidate_dest[t_idx]);
    }
    return Status::OK();
}

Status RangeRouter::_validate_range(const std::vector<TTabletRange>& tablet_ranges, size_t num_columns) const {
    // 1. check inf or bounds are set
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

    // 2. check if full range is covered by the tablet ranges and that adjacent
    //    boundaries are type-consistent and value-equal.
    auto variant_equal = [](const TVariant& a, const TVariant& b) -> StatusOr<bool> {
        if (a.__isset.value && b.__isset.value) {
            return a.value == b.value;
        }
        return Status::InternalError("TVariant.value is required for range validation");
    };

    for (size_t i = 1; i < tablet_ranges.size(); ++i) {
        const auto& prev = tablet_ranges[i - 1];
        const auto& curr = tablet_ranges[i];
        DCHECK(prev.__isset.upper_bound);
        DCHECK(curr.__isset.lower_bound);
        const bool prev_upper_inclusive = prev.upper_bound_included;
        const bool curr_lower_inclusive = curr.lower_bound_included;

        // overlapping or has gap in the boundary
        if ((!prev_upper_inclusive && !curr_lower_inclusive) || (prev_upper_inclusive && curr_lower_inclusive)) {
            return Status::InternalError(
                    "adjacent ranges are overlapping / not complementary for the inclusive/exclusive bound");
        }

        if (prev_upper_inclusive && !curr_lower_inclusive) {
            return Status::InternalError(
                    "Invalid range: previous upper bound is inclusive but current lower bound is exclusive");
        }

        // Compare whether adjacent upper and lower bounds are type-compatible
        // and equal in value for every column.
        for (size_t col = 0; col < num_columns; ++col) {
            const auto& prev_var = prev.upper_bound.values[col];
            const auto& curr_var = curr.lower_bound.values[col];

            // Type information must be present and identical.
            if (!prev_var.__isset.type || !curr_var.__isset.type) {
                return Status::InternalError("TVariant.type is required for range validation");
            }
            if (!(prev_var.type == curr_var.type)) {
                return Status::InternalError(
                        fmt::format("Type mismatch at column {} between range[{}] and range[{}]", col, i - 1, i));
            }

            // Then compare the encoded values.
            ASSIGN_OR_RETURN(auto eq, variant_equal(prev_var, curr_var));
            if (!eq) {
                return Status::InternalError(fmt::format("Range[{}] != range[{}] at column {}", i - 1, i, col));
            }
        }
    }
    return Status::OK();
}

size_t RangeRouter::_find_tablet_index_for_row(const ChunkRow& check_row) const {
    DCHECK(!_upper_boundaries.empty());
    DCHECK(_upper_boundaries[0] != nullptr);
    return std::upper_bound(_upper_boundaries_slice.begin(), _upper_boundaries_slice.end(), check_row,
                            PartionKeyComparator()) -
           _upper_boundaries_slice.begin();
}

} // namespace starrocks