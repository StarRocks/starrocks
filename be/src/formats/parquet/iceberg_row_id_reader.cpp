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

#include "column/datum.h"
#include "formats/parquet/predicate_filter_evaluator.h"
#include "storage/range.h"

namespace starrocks::parquet {

Status IcebergRowIdReader::read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) {
    // LOG(INFO) << "IcebergRowIdReader::read_range, range: " << range.to_string()
    //           << ", dst: " << dst->get_name() << ", " << (void*)this;

    for (uint64_t i = range.begin(); i < range.end(); ++i) {
        // Generate row id based on the first row id and the current row index.
        int64_t row_id = _first_row_id + i;
        // Fill the dst column with the generated row id.
        // @TODO handle filter
        dst->append_datum(Datum(row_id));
    }
    return Status::OK();
}

Status IcebergRowIdReader::fill_dst_column(ColumnPtr& dst, ColumnPtr& src) {
    // LOG(INFO) << "IcebergRowIdReader::fill_dst_column, dst: " << dst->debug_string()
    //           << ", src: " << src->debug_string();
    dst->swap_column(*src);

    return Status::OK();
}

void IcebergRowIdReader::collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges,
                                                 int64_t* end_offset, ColumnIOTypeFlags types, bool active) {
    // No IO ranges to collect for row id reader.
}

void IcebergRowIdReader::select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) {
    // No offset index selection needed for row id reader.
}

StatusOr<bool> IcebergRowIdReader::row_group_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                             CompoundNodeType pred_relation,
                                                             const uint64_t rg_first_row,
                                                             const uint64_t rg_num_rows) const {
    DLOG(INFO) << "IcebergRowIdReader::row_group_zone_map_filter, predicates size: " << predicates.size()
               << ", rg_first_row: " << rg_first_row << ", rg_num_rows: " << rg_num_rows
               << ", rg_first_row_id: " << _first_row_id;

    ZoneMapDetail zone_map{Datum(_first_row_id + rg_first_row), Datum(_first_row_id + rg_first_row + rg_num_rows - 1),
                           false};
    bool ret = !PredicateFilterEvaluatorUtils::zonemap_satisfy(predicates, zone_map, pred_relation);
    if (ret == false) {
        DLOG(INFO) << "IcebergRowIdReader: row group zone map filter passed, no filtering applied."
                   << " row_id range(" << (_first_row_id + rg_first_row) << ", "
                   << (_first_row_id + rg_first_row + rg_num_rows - 1) << ")";
    } else {
        DLOG(INFO) << "IcebergRowIdReader: row group zone map filter applied, "
                   << "filtering happened, row_id range(" << (_first_row_id + rg_first_row) << ", "
                   << (_first_row_id + rg_first_row + rg_num_rows - 1) << ")";
    }
    return ret;
    // return Status::NotSupported("IcebergRowIdReader does not support row group zone map filter");
}

StatusOr<bool> IcebergRowIdReader::page_index_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                              SparseRange<uint64_t>* row_ranges,
                                                              CompoundNodeType pred_relation,
                                                              const uint64_t rg_first_row, const uint64_t rg_num_rows) {
    [[maybe_unused]] std::ostringstream oss;
    oss << "IcebergRowIdReader::page_index_zone_map_filter, rg_first_row: " << rg_first_row
        << ", rg_num_rows: " << rg_num_rows << ", predicates size: " << predicates.size();
    oss << " filters: [";
    for (const auto& pred : predicates) {
        oss << pred->debug_string() << ", ";
    }
    oss << "]";
    // @TODO we need arange
    DLOG(INFO) << oss.str();

    SparseRange<int64_t> row_id_range(_first_row_id + rg_first_row, _first_row_id + rg_first_row + rg_num_rows);
    DLOG(INFO) << "original range: " << row_id_range.to_string();
    for (const auto& pred : predicates) {
        if (pred->type() == PredicateType::kGE) {
            row_id_range &= SparseRange<int64_t>(pred->value().get_int64(), std::numeric_limits<int64_t>::max());
        } else if (pred->type() == PredicateType::kLT) {
            row_id_range &= SparseRange<int64_t>(0, pred->value().get_int64());
        }
        DLOG(INFO) << "after apply " << pred->debug_string() << ", ret: " << row_id_range.to_string();
    }
    if (row_id_range.span_size() == rg_num_rows) {
        DLOG(INFO) << "no filtering applied";
        return false;
    }
    // we should convert row_idrange to row ranges
    for (size_t i = 0; i < row_id_range.size(); i++) {
        Range<int64_t> range = row_id_range[i];
        DLOG(INFO) << "add range: " << range.to_string();
        row_ranges->add(Range<uint64_t>(range.begin() - _first_row_id, range.end() - _first_row_id));
    }

    return true;
}

} // namespace starrocks::parquet