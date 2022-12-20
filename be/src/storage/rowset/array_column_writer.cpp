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

#include "column/array_column.h"
#include "column/nullable_column.h"
#include "common/status.h"
#include "gutil/casts.h"
#include "storage/rowset/column_writer.h"

namespace starrocks {

class ArrayColumnWriter final : public ColumnWriter {
public:
    explicit ArrayColumnWriter(const ColumnWriterOptions& opts, TypeInfoPtr type_info,
                               std::unique_ptr<ScalarColumnWriter> null_writer,
                               std::unique_ptr<ScalarColumnWriter> offset_writer,
                               std::unique_ptr<ColumnWriter> element_writer);
    ~ArrayColumnWriter() override = default;

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

    ordinal_t get_next_rowid() const override { return _array_size_writer->get_next_rowid(); }

    uint64_t total_mem_footprint() const override;

private:
    ColumnWriterOptions _opts;

    std::unique_ptr<ScalarColumnWriter> _null_writer;
    std::unique_ptr<ScalarColumnWriter> _array_size_writer;
    std::unique_ptr<ColumnWriter> _element_writer;
};

StatusOr<std::unique_ptr<ColumnWriter>> create_array_column_writer(const ColumnWriterOptions& opts,
                                                                   TypeInfoPtr type_info, const TabletColumn* column,
                                                                   WritableFile* wfile) {
    DCHECK(column->subcolumn_count() == 1);
    const TabletColumn& element_column = column->subcolumn(0);
    ColumnWriterOptions element_options;
    element_options.meta = opts.meta->mutable_children_columns(0);
    element_options.need_zone_map = false;
    element_options.need_bloom_filter = element_column.is_bf_column();
    element_options.need_bitmap_index = element_column.has_bitmap_index();
    if (element_column.type() == LogicalType::TYPE_ARRAY) {
        if (element_options.need_bloom_filter) {
            return Status::NotSupported("Do not support bloom filter for array type");
        }
        if (element_options.need_bitmap_index) {
            return Status::NotSupported("Do not support bitmap index for array type");
        }
    }

    ASSIGN_OR_RETURN(auto element_writer, ColumnWriter::create(element_options, &element_column, wfile));

    std::unique_ptr<ScalarColumnWriter> null_writer = nullptr;
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

    ColumnWriterOptions array_size_options;
    array_size_options.meta = opts.meta->add_children_columns();
    array_size_options.meta->set_column_id(opts.meta->column_id());
    array_size_options.meta->set_unique_id(opts.meta->unique_id());
    array_size_options.meta->set_type(TYPE_INT);
    array_size_options.meta->set_length(4);
    array_size_options.meta->set_encoding(DEFAULT_ENCODING);
    array_size_options.meta->set_compression(opts.meta->compression());
    array_size_options.meta->set_is_nullable(false);
    array_size_options.need_zone_map = false;
    array_size_options.need_bloom_filter = false;
    array_size_options.need_bitmap_index = false;
    TypeInfoPtr int_type_info = get_type_info(TYPE_INT);
    std::unique_ptr<ScalarColumnWriter> offset_writer =
            std::make_unique<ScalarColumnWriter>(array_size_options, std::move(int_type_info), wfile);
    return std::make_unique<ArrayColumnWriter>(opts, std::move(type_info), std::move(null_writer),
                                               std::move(offset_writer), std::move(element_writer));
}

ArrayColumnWriter::ArrayColumnWriter(const ColumnWriterOptions& opts, TypeInfoPtr type_info,
                                     std::unique_ptr<ScalarColumnWriter> null_writer,
                                     std::unique_ptr<ScalarColumnWriter> offset_writer,
                                     std::unique_ptr<ColumnWriter> element_writer)
        : ColumnWriter(std::move(type_info), opts.meta->length(), opts.meta->is_nullable()),
          _opts(opts),
          _null_writer(std::move(null_writer)),
          _array_size_writer(std::move(offset_writer)),
          _element_writer(std::move(element_writer)) {}

Status ArrayColumnWriter::init() {
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->init());
    }
    RETURN_IF_ERROR(_array_size_writer->init());
    RETURN_IF_ERROR(_element_writer->init());

    return Status::OK();
}

Status ArrayColumnWriter::append(const Column& column) {
    const ArrayColumn* array_column = nullptr;
    NullColumn* null_column = nullptr;
    if (is_nullable()) {
        const auto& nullable_column = down_cast<const NullableColumn&>(column);
        array_column = down_cast<ArrayColumn*>(nullable_column.data_column().get());
        null_column = down_cast<NullColumn*>(nullable_column.null_column().get());
    } else {
        array_column = down_cast<const ArrayColumn*>(&column);
    }

    // 1. Write null column when necessary
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->append(*null_column));
    }

    // 2. Write offset column
    RETURN_IF_ERROR(_array_size_writer->append_array_offsets(array_column->offsets()));

    // 3. writer elements column recursively
    RETURN_IF_ERROR(_element_writer->append(array_column->elements()));

    return Status::OK();
}

uint64_t ArrayColumnWriter::estimate_buffer_size() {
    size_t estimate_size = _array_size_writer->estimate_buffer_size() + _element_writer->estimate_buffer_size();
    if (is_nullable()) {
        estimate_size += _null_writer->estimate_buffer_size();
    }
    return estimate_size;
}

Status ArrayColumnWriter::finish() {
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->finish());
    }
    RETURN_IF_ERROR(_array_size_writer->finish());
    RETURN_IF_ERROR(_element_writer->finish());

    _opts.meta->set_num_rows(get_next_rowid());
    _opts.meta->set_total_mem_footprint(total_mem_footprint());
    return Status::OK();
}

uint64_t ArrayColumnWriter::total_mem_footprint() const {
    uint64_t total_mem_footprint = 0;
    if (is_nullable()) {
        total_mem_footprint += _null_writer->total_mem_footprint();
    }
    total_mem_footprint += _array_size_writer->total_mem_footprint();
    total_mem_footprint += _element_writer->total_mem_footprint();
    return total_mem_footprint;
}

Status ArrayColumnWriter::write_data() {
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->write_data());
    }
    RETURN_IF_ERROR(_array_size_writer->write_data());
    RETURN_IF_ERROR(_element_writer->write_data());
    return Status::OK();
}

Status ArrayColumnWriter::write_ordinal_index() {
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->write_ordinal_index());
    }
    RETURN_IF_ERROR(_array_size_writer->write_ordinal_index());
    RETURN_IF_ERROR(_element_writer->write_ordinal_index());
    return Status::OK();
}

Status ArrayColumnWriter::finish_current_page() {
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->finish_current_page());
    }
    RETURN_IF_ERROR(_array_size_writer->finish_current_page());
    RETURN_IF_ERROR(_element_writer->finish_current_page());
    return Status::OK();
}

} // namespace starrocks
