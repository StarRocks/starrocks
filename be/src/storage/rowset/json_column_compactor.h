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
#include "storage/rowset/json_column_writer.h"

namespace starrocks {
class FlatJsonColumnCompactor final : public FlatJsonColumnWriter {
public:
    FlatJsonColumnCompactor(const ColumnWriterOptions& opts, TypeInfoPtr type_info, WritableFile* wfile,
                            std::unique_ptr<ScalarColumnWriter> json_writer)
            : FlatJsonColumnWriter(opts, std::move(type_info), wfile, std::move(json_writer)) {}

    Status append(const Column& column) override;

    Status finish() override;

private:
    Status _compact_columns(Columns& json_datas);

    Status _merge_columns(Columns& json_datas);

    Status _flatten_columns(Columns& json_datas);
};

class JsonColumnCompactor final : public ColumnWriter {
public:
    JsonColumnCompactor(const ColumnWriterOptions& opts, TypeInfoPtr type_info, WritableFile* wfile,
                        std::unique_ptr<ScalarColumnWriter> json_writer)
            : ColumnWriter(std::move(type_info), opts.meta->length(), opts.meta->is_nullable()),
              _json_meta(opts.meta),
              _json_writer(std::move(json_writer)) {}

    ~JsonColumnCompactor() override = default;

    Status init() override { return _json_writer->init(); }

    Status append(const Column& column) override;

    Status finish_current_page() override { return _json_writer->finish_current_page(); }

    uint64_t estimate_buffer_size() override { return _json_writer->estimate_buffer_size(); }

    Status finish() override;

    Status write_data() override { return _json_writer->write_data(); }
    Status write_ordinal_index() override { return _json_writer->write_ordinal_index(); }
    Status write_zone_map() override { return _json_writer->write_zone_map(); }
    Status write_bitmap_index() override { return _json_writer->write_bitmap_index(); }
    Status write_bloom_filter_index() override { return _json_writer->write_bloom_filter_index(); }
    ordinal_t get_next_rowid() const override { return _json_writer->get_next_rowid(); }
    uint64_t total_mem_footprint() const override { return _json_writer->total_mem_footprint(); }

private:
    void _flat_column(Columns& json_datas);

private:
    ColumnMetaPB* _json_meta;
    std::unique_ptr<ScalarColumnWriter> _json_writer;
};
} // namespace starrocks
