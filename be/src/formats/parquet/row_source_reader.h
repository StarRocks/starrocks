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
#include "formats/parquet/column_reader.h"

namespace starrocks::parquet {

class RowSourceReader final : public ColumnReader {
public:
    explicit RowSourceReader(int32_t node_id) : ColumnReader(nullptr), _node_id(node_id) {}
    ~RowSourceReader() override = default;

    Status prepare() override { return Status::OK(); }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) override;
    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {}
    void set_need_parse_levels(bool need_parse_levels) override {}

    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOTypeFlags types, bool active) override {}
                                 
    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override {}

    StatusOr<bool> row_group_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                              CompoundNodeType pred_relation,
                                              const uint64_t rg_first_row, const uint64_t rg_num_rows) const override {
        return Status::NotSupported("RowSourceReader::row_group_zone_map_filter");
    }

    StatusOr<bool> page_index_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                              SparseRange<uint64_t>* row_ranges, CompoundNodeType pred_relation,
                                              const uint64_t rg_first_row, const uint64_t rg_num_rows) override {
        return Status::NotSupported("RowSourceReader::page_index_zone_map_filter");
    }


private:
    int32_t _node_id = 0;
};
}