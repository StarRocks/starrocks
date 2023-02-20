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

#include "storage/rowset/struct_column_writer.h"

#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "common/status.h"
#include "gutil/casts.h"

namespace starrocks {

class StructColumnWriter final : public ColumnWriter {
public:
    explicit StructColumnWriter(const ColumnWriterOptions& opts, TypeInfoPtr type_info,
                                std::unique_ptr<ScalarColumnWriter> null_writer,
                                std::vector<std::unique_ptr<ColumnWriter>> field_writers);

    ~StructColumnWriter() override = default;

    Status init() override;

    Status append(const Column& column) override;

    uint64_t estimate_buffer_size() override;

    Status finish() override;
    Status write_data() override;
    Status write_ordinal_index() override;

    Status finish_current_page() override;

    Status write_zone_map() override { return Status::OK(); }

    Status write_bitmap_index() override { return Status::OK(); }

    Status write_bloom_filter_index() override { return Status::OK(); }

    ordinal_t get_next_rowid() const override { return _field_writers[0]->get_next_rowid(); }

    uint64_t total_mem_footprint() const override;

private:
    ColumnWriterOptions _opts;

    std::unique_ptr<ScalarColumnWriter> _null_writer;
    std::vector<std::unique_ptr<ColumnWriter>> _field_writers;
};

StatusOr<std::unique_ptr<ColumnWriter>> create_struct_column_writer(const ColumnWriterOptions& opts,
                                                                    TypeInfoPtr type_info, const TabletColumn* column,
                                                                    WritableFile* wfile) {
    DCHECK(column->subcolumn_count() > 0);
    auto num_fields = column->subcolumn_count();
    std::vector<std::unique_ptr<ColumnWriter>> field_writers;
    for (int i = 0; i < num_fields; ++i) {
        const TabletColumn& field_column = column->subcolumn(i);
        ColumnWriterOptions value_options;
        value_options.meta = opts.meta->mutable_children_columns(i);
        value_options.need_zone_map = false;
        value_options.need_bloom_filter = field_column.is_bf_column();
        value_options.need_bitmap_index = field_column.has_bitmap_index();
        ASSIGN_OR_RETURN(auto field_writer, ColumnWriter::create(value_options, &field_column, wfile));
        field_writers.emplace_back(std::move(field_writer));
    }

    std::unique_ptr<ScalarColumnWriter> null_writer;
    if (opts.meta->is_nullable()) {
        ColumnWriterOptions null_options;
        null_options.meta = opts.meta->add_children_columns();
        null_options.meta->set_column_id(opts.meta->column_id());
        null_options.meta->set_unique_id(opts.meta->unique_id());
        null_options.meta->set_type(TYPE_BOOLEAN);
        null_options.meta->set_length(1);
        null_options.meta->set_encoding(DEFAULT_ENCODING);
        null_options.meta->set_compression(opts.meta->compression());
        null_options.meta->set_is_nullable(false);

        TypeInfoPtr bool_type_info = get_type_info(TYPE_BOOLEAN);
        null_writer = std::make_unique<ScalarColumnWriter>(null_options, std::move(bool_type_info), wfile);
    }
    return std::make_unique<StructColumnWriter>(opts, std::move(type_info), std::move(null_writer),
                                                std::move(field_writers));
}

StructColumnWriter::StructColumnWriter(const ColumnWriterOptions& opts, TypeInfoPtr type_info,
                                       std::unique_ptr<ScalarColumnWriter> null_writer,
                                       std::vector<std::unique_ptr<ColumnWriter>> field_writers)
        : ColumnWriter(std::move(type_info), opts.meta->length(), opts.meta->is_nullable()),
          _opts(opts),
          _null_writer(std::move(null_writer)),
          _field_writers(std::move(field_writers)) {}

Status StructColumnWriter::init() {
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->init());
    }
    for (auto& writer : _field_writers) {
        RETURN_IF_ERROR(writer->init());
    }
    return Status::OK();
}

Status StructColumnWriter::append(const Column& column) {
    const StructColumn* struct_column = nullptr;
    NullColumn* null_column = nullptr;
    if (is_nullable()) {
        const auto& nullable_column = down_cast<const NullableColumn&>(column);
        struct_column = down_cast<StructColumn*>(nullable_column.data_column().get());
        null_column = down_cast<NullColumn*>(nullable_column.null_column().get());
    } else {
        struct_column = down_cast<const StructColumn*>(&column);
    }
    // write null column when necessary
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->append(*null_column));
    }
    // write all fields
    // TODO(Alvin): fields in column may have different order from _field_writers
    // FIXME later
    for (int i = 0; i < _field_writers.size(); ++i) {
        RETURN_IF_ERROR(_field_writers[i]->append(*struct_column->fields()[i]));
    }
    return Status::OK();
}

uint64_t StructColumnWriter::estimate_buffer_size() {
    size_t estimate_size = 0;
    if (is_nullable()) {
        estimate_size += _null_writer->estimate_buffer_size();
    }
    for (auto& writer : _field_writers) {
        estimate_size += writer->estimate_buffer_size();
    }
    return estimate_size;
}

Status StructColumnWriter::finish() {
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->finish());
    }
    for (auto& writer : _field_writers) {
        RETURN_IF_ERROR(writer->finish());
    }
    _opts.meta->set_num_rows(get_next_rowid());
    _opts.meta->set_total_mem_footprint(total_mem_footprint());
    return Status::OK();
}

uint64_t StructColumnWriter::total_mem_footprint() const {
    uint64_t total_mem_footprint = 0;
    if (is_nullable()) {
        total_mem_footprint += _null_writer->total_mem_footprint();
    }
    for (auto& writer : _field_writers) {
        total_mem_footprint += writer->total_mem_footprint();
    }
    return total_mem_footprint;
}

Status StructColumnWriter::write_data() {
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->write_data());
    }
    for (auto& writer : _field_writers) {
        RETURN_IF_ERROR(writer->write_data());
    }
    return Status::OK();
}

Status StructColumnWriter::write_ordinal_index() {
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->write_ordinal_index());
    }
    for (auto& writer : _field_writers) {
        RETURN_IF_ERROR(writer->write_ordinal_index());
    }
    return Status::OK();
}

Status StructColumnWriter::finish_current_page() {
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->finish_current_page());
    }
    for (auto& writer : _field_writers) {
        RETURN_IF_ERROR(writer->finish_current_page());
    }
    return Status::OK();
}

} // namespace starrocks
