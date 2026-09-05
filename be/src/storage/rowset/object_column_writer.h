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
class ObjectColumnWriter final : public ColumnWriter {
public:
    ObjectColumnWriter(const ColumnWriterOptions& opts, TypeInfoPtr type_info,
                       std::unique_ptr<ScalarColumnWriter> scalar_column_writer)
            : ColumnWriter(std::move(type_info), opts.meta->length(), opts.meta->is_nullable()),
              _scalar_column_writer(std::move(scalar_column_writer)) {}
    ~ObjectColumnWriter() override = default;
    Status init() override { return _scalar_column_writer->init(); }

    Status append(const Column& column) override;

    Status finish_current_page() override { return _scalar_column_writer->finish_current_page(); }
    uint64_t estimate_buffer_size() override { return _scalar_column_writer->estimate_buffer_size(); }
    Status finish() override { return _scalar_column_writer->finish(); }
    Status write_data() override { return _scalar_column_writer->write_data(); }
    Status write_ordinal_index() override { return _scalar_column_writer->write_ordinal_index(); }
    Status write_zone_map() override { return _scalar_column_writer->write_zone_map(); }
    Status write_bitmap_index() override { return _scalar_column_writer->write_bitmap_index(); }
    Status write_bloom_filter_index() override { return _scalar_column_writer->write_bloom_filter_index(); }
    Status write_inverted_index() override { return _scalar_column_writer->write_inverted_index(); }
    ordinal_t get_next_rowid() const override { return _scalar_column_writer->get_next_rowid(); }
    bool is_global_dict_valid() override { return _scalar_column_writer->is_global_dict_valid(); }
    uint64_t total_mem_footprint() const override { return _scalar_column_writer->total_mem_footprint(); }

private:
    std::unique_ptr<ScalarColumnWriter> _scalar_column_writer;
    Buffer<uint8_t> _serialize_buf; // reusable serialization buffer
    Buffer<Slice> _slices;          // reusable slice array
};
} // namespace starrocks
