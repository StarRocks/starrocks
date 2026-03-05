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

#include "formats/parquet/iceberg_row_id_reader.h"

#include <limits>

#include "formats/parquet/predicate_filter_evaluator.h"
#include "storage/range.h"
#include "types/datum.h"

namespace starrocks::parquet {

Status IcebergRowIdReader::read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) {
    Column* dst_col = dst->as_mutable_raw_ptr();
    if (filter == nullptr) {
        for (uint64_t i = range.begin(); i < range.end(); ++i) {
            int64_t row_id = _first_row_id + i;
            dst_col->append_datum(Datum(row_id));
        }
    } else {
        DCHECK_EQ(filter->size(), range.span_size()) << "Filter size must match range size";
        for (uint64_t i = range.begin(); i < range.end(); ++i) {
            size_t filter_index = i - range.begin();
            if ((*filter)[filter_index]) {
                int64_t row_id = _first_row_id + i;
                dst_col->append_datum(Datum(row_id));
            }
        }
    }
    return Status::OK();
}

Status IcebergRowIdReader::fill_dst_column(ColumnPtr& dst, ColumnPtr& src) {
    dst->as_mutable_raw_ptr()->swap_column(*(src->as_mutable_raw_ptr()));
    return Status::OK();
}

StatusOr<bool> IcebergRowIdReader::row_group_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                             CompoundNodeType pred_relation,
                                                             const uint64_t rg_first_row,
                                                             const uint64_t rg_num_rows) const {
    ZoneMapDetail zone_map{Datum(_first_row_id + rg_first_row), Datum(_first_row_id + rg_first_row + rg_num_rows - 1),
                           false};
    bool ret = !PredicateFilterEvaluatorUtils::zonemap_satisfy(predicates, zone_map, pred_relation);
    return ret;
}

StatusOr<bool> IcebergRowIdReader::page_index_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                              SparseRange<uint64_t>* row_ranges,
                                                              CompoundNodeType pred_relation,
                                                              const uint64_t rg_first_row, const uint64_t rg_num_rows) {
    SparseRange<int64_t> row_id_range(_first_row_id + rg_first_row, _first_row_id + rg_first_row + rg_num_rows);

    if (pred_relation == CompoundNodeType::AND) {
        // AND: intersect all predicate ranges sequentially
        for (const auto& pred : predicates) {
            SparseRange<int64_t> pred_range;
            StatusOr<bool> result = _apply_single_predicate(pred, pred_range);
            if (!result.ok()) {
                return result.status();
            }
            if (!result.value()) {
                return false;
            }
            row_id_range &= pred_range;
            if (row_id_range.empty()) {
                break;
            }
        }
    } else if (pred_relation == CompoundNodeType::OR) {
        // OR: union all predicate ranges, then intersect with row group range
        SparseRange<int64_t> union_range;
        bool has_valid_predicate = false;

        for (const auto& pred : predicates) {
            SparseRange<int64_t> pred_range;
            StatusOr<bool> result = _apply_single_predicate(pred, pred_range);
            if (!result.ok()) {
                return result.status();
            }
            if (!result.value()) {
                return false;
            }
            has_valid_predicate = true;
            union_range |= pred_range;
        }

        if (!has_valid_predicate) {
            return false;
        }

        row_id_range &= union_range;
    } else {
        return false;
    }

    if (row_id_range.span_size() == rg_num_rows) {
        return false;
    }

    // Convert row_id ranges back to row-group-relative row ranges
    for (size_t i = 0; i < row_id_range.size(); i++) {
        Range<int64_t> range = row_id_range[i];
        row_ranges->add(Range<uint64_t>(range.begin() - _first_row_id, range.end() - _first_row_id));
    }

    return true;
}

// Convert a single predicate on _row_id into a SparseRange of matching row_id values.
// Returns true if the predicate was converted, false if unsupported.
StatusOr<bool> IcebergRowIdReader::_apply_single_predicate(const ColumnPredicate* pred,
                                                           SparseRange<int64_t>& result_range) {
    switch (pred->type()) {
    case PredicateType::kEQ: {
        int64_t value = pred->value().get_int64();
        result_range = SparseRange<int64_t>(value, value + 1);
        return true;
    }
    case PredicateType::kNE: {
        int64_t value = pred->value().get_int64();
        if (value > 0) {
            result_range.add(Range<int64_t>(0, value));
        }
        if (value < std::numeric_limits<int64_t>::max()) {
            result_range.add(Range<int64_t>(value + 1, std::numeric_limits<int64_t>::max()));
        }
        return true;
    }
    case PredicateType::kGT: {
        int64_t value = pred->value().get_int64();
        result_range = SparseRange<int64_t>(value + 1, std::numeric_limits<int64_t>::max());
        return true;
    }
    case PredicateType::kGE: {
        int64_t value = pred->value().get_int64();
        result_range = SparseRange<int64_t>(value, std::numeric_limits<int64_t>::max());
        return true;
    }
    case PredicateType::kLT: {
        int64_t value = pred->value().get_int64();
        result_range = SparseRange<int64_t>(0, value);
        return true;
    }
    case PredicateType::kLE: {
        int64_t value = pred->value().get_int64();
        result_range = SparseRange<int64_t>(0, value + 1);
        return true;
    }
    case PredicateType::kInList: {
        const auto& values = pred->values();
        if (values.empty()) {
            result_range = SparseRange<int64_t>();
            return true;
        }
        for (const auto& value : values) {
            int64_t int_value = value.get_int64();
            result_range.add(Range<int64_t>(int_value, int_value + 1));
        }
        return true;
    }
    case PredicateType::kNotInList: {
        // Start with full range, then subtract each excluded value
        const auto& values = pred->values();
        if (values.empty()) {
            result_range = SparseRange<int64_t>(0, std::numeric_limits<int64_t>::max());
            return true;
        }
        result_range.add(Range<int64_t>(0, std::numeric_limits<int64_t>::max()));
        for (const auto& value : values) {
            int64_t int_value = value.get_int64();
            SparseRange<int64_t> temp_range;
            for (size_t i = 0; i < result_range.size(); i++) {
                const auto& range = result_range[i];
                if (range.begin() < int_value && range.end() > int_value + 1) {
                    temp_range.add(Range<int64_t>(range.begin(), int_value));
                    temp_range.add(Range<int64_t>(int_value + 1, range.end()));
                } else if (range.begin() < int_value && range.end() > int_value) {
                    temp_range.add(Range<int64_t>(range.begin(), int_value));
                } else if (range.begin() < int_value + 1 && range.end() > int_value + 1) {
                    temp_range.add(Range<int64_t>(int_value + 1, range.end()));
                } else if (range.begin() >= int_value && range.end() <= int_value + 1) {
                    continue;
                } else {
                    temp_range.add(range);
                }
            }
            result_range = temp_range;
        }
        return true;
    }
    case PredicateType::kIsNull: {
        result_range = SparseRange<int64_t>();
        return true;
    }
    case PredicateType::kNotNull: {
        result_range = SparseRange<int64_t>(0, std::numeric_limits<int64_t>::max());
        return true;
    }
    default: {
        DLOG(WARNING) << "Unsupported predicate type for row_id filtering: " << pred->type();
        return false;
    }
    }
}

} // namespace starrocks::parquet
