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

#include "storage/rowset/column_writer.h"

namespace starrocks {

StatusOr<std::unique_ptr<ColumnWriter>> create_json_column_writer(const ColumnWriterOptions& opts,
                                                                  TypeInfoPtr type_info, WritableFile* wfile,
                                                                  std::unique_ptr<ScalarColumnWriter> json_writer);

class FlatJsonColumnWriter : public ColumnWriter {
public:
    FlatJsonColumnWriter(const ColumnWriterOptions& opts, TypeInfoPtr type_info, WritableFile* wfile,
                         std::unique_ptr<ScalarColumnWriter> json_writer);

    ~FlatJsonColumnWriter() override = default;

    Status init() override;

    Status append(const Column& column) override;

    Status finish_current_page() override;

    uint64_t estimate_buffer_size() override;

    Status finish() override;

    Status write_data() override;
    Status write_ordinal_index() override;
    Status write_zone_map() override;
    Status write_bitmap_index() override;
    Status write_bloom_filter_index() override;
    ordinal_t get_next_rowid() const override;

    uint64_t total_mem_footprint() const override;

protected:
    Status _init_flat_writers();
    Status _write_flat_column();

private:
    Status _flat_column(std::vector<ColumnPtr>& json_datas);

protected:
    ColumnMetaPB* _json_meta;
    WritableFile* _wfile;
    std::unique_ptr<ScalarColumnWriter> _json_writer;

    std::vector<std::unique_ptr<ColumnWriter>> _flat_writers;
    std::vector<std::string> _flat_paths;
    std::vector<LogicalType> _flat_types;
    std::vector<ColumnPtr> _flat_columns;

    std::vector<ColumnPtr> _json_datas;
    size_t _estimate_size = 0;

    bool _has_remain;
    bool _is_flat = false;
};
} // namespace starrocks
