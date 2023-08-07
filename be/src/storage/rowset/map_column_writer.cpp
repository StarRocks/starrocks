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

#include "column/map_column.h"
#include "column/nullable_column.h"
#include "common/status.h"
#include "gutil/casts.h"
#include "storage/rowset/column_writer.h"

namespace starrocks {

class MapColumnWriter final : public ColumnWriter {
public:
    explicit MapColumnWriter(const ColumnWriterOptions& opts, TypeInfoPtr type_info,
                             std::unique_ptr<ScalarColumnWriter> nulls_writer,
                             std::unique_ptr<ScalarColumnWriter> offsets_writer,
                             std::unique_ptr<ColumnWriter> keys_writer, std::unique_ptr<ColumnWriter> values_writer);

    ~MapColumnWriter() override = default;

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

    ordinal_t get_next_rowid() const override { return _offsets_writer->get_next_rowid(); }

    uint64_t total_mem_footprint() const override;

private:
    ColumnWriterOptions _opts;

    std::unique_ptr<ScalarColumnWriter> _nulls_writer;
    std::unique_ptr<ScalarColumnWriter> _offsets_writer;
    std::unique_ptr<ColumnWriter> _keys_writer;
    std::unique_ptr<ColumnWriter> _values_writer;
};

StatusOr<std::unique_ptr<ColumnWriter>> create_map_column_writer(const ColumnWriterOptions& opts, TypeInfoPtr type_info,
                                                                 const TabletColumn* column, WritableFile* wfile) {
    DCHECK(column->subcolumn_count() == 2);

    // create key columns writer
    std::unique_ptr<ColumnWriter> keys_writer;
    {
        const TabletColumn& key_column = column->subcolumn(0);
        ColumnWriterOptions key_options;
        key_options.meta = opts.meta->mutable_children_columns(0);
        key_options.need_zone_map = false;
        key_options.need_bloom_filter = key_column.is_bf_column();
        key_options.need_bitmap_index = key_column.has_bitmap_index();
        if (key_column.type() == LogicalType::TYPE_ARRAY) {
            if (key_options.need_bloom_filter) {
                return Status::NotSupported("Do not support bloom filter for array type");
            }
            if (key_options.need_bitmap_index) {
                return Status::NotSupported("Do not support bitmap index for array type");
            }
        }
        ASSIGN_OR_RETURN(keys_writer, ColumnWriter::create(key_options, &key_column, wfile));
    }

    // create value columns writer
    std::unique_ptr<ColumnWriter> values_writer;
    {
        const TabletColumn& value_column = column->subcolumn(1);
        ColumnWriterOptions value_options;
        value_options.meta = opts.meta->mutable_children_columns(1);
        value_options.need_zone_map = false;
        value_options.need_bloom_filter = value_column.is_bf_column();
        value_options.need_bitmap_index = value_column.has_bitmap_index();
        if (value_column.type() == LogicalType::TYPE_ARRAY) {
            if (value_options.need_bloom_filter) {
                return Status::NotSupported("Do not support bloom filter for array type");
            }
            if (value_options.need_bitmap_index) {
                return Status::NotSupported("Do not support bitmap index for array type");
            }
        }
        ASSIGN_OR_RETURN(values_writer, ColumnWriter::create(value_options, &value_column, wfile));
    }
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

    std::unique_ptr<ScalarColumnWriter> offsets_writer;
    {
        ColumnWriterOptions offsets_options;
        offsets_options.meta = opts.meta->add_children_columns();
        offsets_options.meta->set_column_id(opts.meta->column_id());
        offsets_options.meta->set_unique_id(opts.meta->unique_id());
        offsets_options.meta->set_type(TYPE_INT);
        offsets_options.meta->set_length(4);
        offsets_options.meta->set_encoding(DEFAULT_ENCODING);
        offsets_options.meta->set_compression(opts.meta->compression());
        offsets_options.meta->set_is_nullable(false);
        offsets_options.need_zone_map = false;
        offsets_options.need_bloom_filter = false;
        offsets_options.need_bitmap_index = false;
        TypeInfoPtr int_type_info = get_type_info(TYPE_INT);
        offsets_writer = std::make_unique<ScalarColumnWriter>(offsets_options, std::move(int_type_info), wfile);
    }
    return std::make_unique<MapColumnWriter>(opts, std::move(type_info), std::move(null_writer),
                                             std::move(offsets_writer), std::move(keys_writer),
                                             std::move(values_writer));
}

MapColumnWriter::MapColumnWriter(const ColumnWriterOptions& opts, TypeInfoPtr type_info,
                                 std::unique_ptr<ScalarColumnWriter> null_writer,
                                 std::unique_ptr<ScalarColumnWriter> offsets_writer,
                                 std::unique_ptr<ColumnWriter> keys_writer, std::unique_ptr<ColumnWriter> values_writer)
        : ColumnWriter(std::move(type_info), opts.meta->length(), opts.meta->is_nullable()),
          _opts(opts),
          _nulls_writer(std::move(null_writer)),
          _offsets_writer(std::move(offsets_writer)),
          _keys_writer(std::move(keys_writer)),
          _values_writer(std::move(values_writer)) {}

Status MapColumnWriter::init() {
    if (is_nullable()) {
        RETURN_IF_ERROR(_nulls_writer->init());
    }
    RETURN_IF_ERROR(_offsets_writer->init());
    RETURN_IF_ERROR(_keys_writer->init());
    RETURN_IF_ERROR(_values_writer->init());

    return Status::OK();
}

Status MapColumnWriter::append(const Column& column) {
    const MapColumn* map_column = nullptr;
    NullColumn* null_column = nullptr;
    if (is_nullable()) {
        const auto& nullable_column = down_cast<const NullableColumn&>(column);
        map_column = down_cast<MapColumn*>(nullable_column.data_column().get());
        null_column = down_cast<NullColumn*>(nullable_column.null_column().get());
    } else {
        map_column = down_cast<const MapColumn*>(&column);
    }

    // 1. write null column when necessary
    if (is_nullable()) {
        RETURN_IF_ERROR(_nulls_writer->append(*null_column));
    }

    // 2. write offsets column
    RETURN_IF_ERROR(_offsets_writer->append_array_offsets(map_column->offsets()));

    // 3. write keys and values
    RETURN_IF_ERROR(_keys_writer->append(map_column->keys()));
    RETURN_IF_ERROR(_values_writer->append(map_column->values()));

    return Status::OK();
}

uint64_t MapColumnWriter::estimate_buffer_size() {
    size_t estimate_size = _offsets_writer->estimate_buffer_size() + _keys_writer->estimate_buffer_size() +
                           _values_writer->estimate_buffer_size();
    if (is_nullable()) {
        estimate_size += _nulls_writer->estimate_buffer_size();
    }
    return estimate_size;
}

Status MapColumnWriter::finish() {
    if (is_nullable()) {
        RETURN_IF_ERROR(_nulls_writer->finish());
    }
    RETURN_IF_ERROR(_offsets_writer->finish());
    RETURN_IF_ERROR(_keys_writer->finish());
    RETURN_IF_ERROR(_values_writer->finish());

    _opts.meta->set_num_rows(get_next_rowid());
    _opts.meta->set_total_mem_footprint(total_mem_footprint());
    return Status::OK();
}

uint64_t MapColumnWriter::total_mem_footprint() const {
    uint64_t total_mem_footprint = 0;
    if (is_nullable()) {
        total_mem_footprint += _nulls_writer->total_mem_footprint();
    }
    total_mem_footprint += _offsets_writer->total_mem_footprint();
    total_mem_footprint += _keys_writer->total_mem_footprint();
    total_mem_footprint += _values_writer->total_mem_footprint();
    return total_mem_footprint;
}

Status MapColumnWriter::write_data() {
    if (is_nullable()) {
        RETURN_IF_ERROR(_nulls_writer->write_data());
    }
    RETURN_IF_ERROR(_offsets_writer->write_data());
    RETURN_IF_ERROR(_keys_writer->write_data());
    RETURN_IF_ERROR(_values_writer->write_data());
    return Status::OK();
}

Status MapColumnWriter::write_ordinal_index() {
    if (is_nullable()) {
        RETURN_IF_ERROR(_nulls_writer->write_ordinal_index());
    }
    RETURN_IF_ERROR(_offsets_writer->write_ordinal_index());
    RETURN_IF_ERROR(_keys_writer->write_ordinal_index());
    RETURN_IF_ERROR(_values_writer->write_ordinal_index());
    return Status::OK();
}

Status MapColumnWriter::finish_current_page() {
    if (is_nullable()) {
        RETURN_IF_ERROR(_nulls_writer->finish_current_page());
    }
    RETURN_IF_ERROR(_offsets_writer->finish_current_page());
    RETURN_IF_ERROR(_keys_writer->finish_current_page());
    RETURN_IF_ERROR(_values_writer->finish_current_page());
    return Status::OK();
}

} // namespace starrocks
