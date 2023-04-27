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
#include "column/struct_column.h"
#include "common/status.h"
#include "gutil/casts.h"
#include "storage/rowset/column_writer.h"

namespace starrocks {

class MapColumnWriter final : public ColumnWriter {
public:
    explicit MapColumnWriter(const ColumnWriterOptions& opts, TypeInfoPtr type_info,
                             std::unique_ptr<ScalarColumnWriter> nulls_writer,
                             std::unique_ptr<ScalarColumnWriter> offsets_writer,
                             std::unique_ptr<ColumnWriter> keys_writer, std::unique_ptr<ColumnWriter> values_writer,
                             const TabletColumn* column = nullptr, WritableFile* wfile = nullptr);

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
    const TabletColumn* _tablet_column;
    WritableFile* _wfile;
    std::unique_ptr<ColumnWriter> _flat_writer;
    size_t _total_rows;
    std::map<std::string, ColumnPtr> name_2_column;
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
                                             std::move(values_writer), column, wfile);
}

MapColumnWriter::MapColumnWriter(const ColumnWriterOptions& opts, TypeInfoPtr type_info,
                                 std::unique_ptr<ScalarColumnWriter> null_writer,
                                 std::unique_ptr<ScalarColumnWriter> offsets_writer,
                                 std::unique_ptr<ColumnWriter> keys_writer, std::unique_ptr<ColumnWriter> values_writer,
                                 const TabletColumn* column, WritableFile* wfile)
        : ColumnWriter(std::move(type_info), opts.meta->length(), opts.meta->is_nullable()),
          _opts(opts),
          _nulls_writer(std::move(null_writer)),
          _offsets_writer(std::move(offsets_writer)),
          _keys_writer(std::move(keys_writer)),
          _values_writer(std::move(values_writer)),
          _tablet_column(column),
          _wfile(wfile) {}

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

    /// recode key->name, values
    auto col_size = column.size();
    auto& offset = map_column->offsets().get_data();
    auto& values = map_column->values();
    for (auto i = 0; i < col_size; ++i) {
        if (null_column == nullptr || !null_column->get_data()[i]) {
            for (auto idx = offset[i]; idx < offset[i + 1]; ++idx) {
                auto name = map_column->keys().debug_item(idx);
                auto it = name_2_column.find(name);
                if (name_2_column.end() == it) { // new nullable column and append current value
                    ColumnPtr field_col = map_column->values().clone_empty();
                    NullableColumn* nullable_field = nullptr;
                    if (!field_col->is_nullable()) {
                        field_col = NullableColumn::create(std::move(field_col), std::move(NullColumn::create()));
                    }
                    nullable_field = down_cast<NullableColumn*>(field_col.get());
                    if (i + _total_rows > 0) {
                        nullable_field->append_default(i + _total_rows);
                    }
                    nullable_field->append(values, idx, 1);
                    name_2_column[name] = field_col;
                } else { // just append current value
                    auto nullable_field = down_cast<NullableColumn*>(it->second.get());
                    if (i + _total_rows - nullable_field->size() > 0) {
                        nullable_field->append_default(i + _total_rows - nullable_field->size());
                    }
                    nullable_field->append(values, idx, 1);
                }
            }
        }
    }
    _total_rows += col_size;
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
    /// construct struct column
    Columns field_columns;
    std::vector<std::string> field_names;
    for (auto it = name_2_column.begin(); it != name_2_column.end(); it++) {
        // append tails
        auto field = it->second.get();
        auto count = _total_rows - field->size();
        if (count > 0) {
            field->append_default(count);
        }
        // get field names and columns
        field_names.push_back(it->first);
        field_columns.push_back(it->second);
    }
    /// construct struct writer

    ColumnWriterOptions flat_options;
    flat_options.meta = _opts.meta->add_children_columns();
    flat_options.meta->set_column_id(_opts.meta->column_id());
    flat_options.meta->set_unique_id(_opts.meta->unique_id());
    flat_options.meta->set_type(TYPE_STRUCT);
    flat_options.meta->set_length(0);
    flat_options.meta->set_encoding(DEFAULT_ENCODING);
    flat_options.meta->set_compression(starrocks::LZ4_FRAME);
    flat_options.meta->set_is_nullable(false);
    flat_options.need_zone_map = false;
    flat_options.need_bloom_filter = false;
    flat_options.need_bitmap_index = false;
    for (auto i = 0; i < field_columns.size(); ++i) {
        ColumnMetaPB* sub_meta = flat_options.meta->add_children_columns();
        *sub_meta = _opts.meta->children_columns(1); // copy map.value's meta
        sub_meta->set_name(field_names[i]);
    }

    DCHECK(_tablet_column != nullptr);
    TabletColumn struct_column;
    struct_column.set_unique_id(_opts.meta->unique_id());
    struct_column.set_name("flat_columns");
    struct_column.set_type(TYPE_STRUCT);
    struct_column.set_is_nullable(false);
    struct_column.set_length(16);
    for (auto i = 0; i < field_columns.size(); ++i) {
        const TabletColumn& value_column = _tablet_column->subcolumn(1);
        struct_column.add_sub_column(value_column);
    }
    CHECK(_wfile != nullptr);
    ASSIGN_OR_RETURN(_flat_writer, ColumnWriter::create(flat_options, &struct_column, _wfile));

    if (_flat_writer == nullptr) {
        return Status::InternalError("flat writer is null.");
    }
    auto struct_col = StructColumn::create(std::move(field_columns), std::move(field_names));

    RETURN_IF_ERROR(_flat_writer->init());
    RETURN_IF_ERROR(_flat_writer->append(*struct_col));

    if (is_nullable()) {
        RETURN_IF_ERROR(_nulls_writer->finish());
    }
    RETURN_IF_ERROR(_offsets_writer->finish());
    RETURN_IF_ERROR(_keys_writer->finish());
    RETURN_IF_ERROR(_values_writer->finish());
    RETURN_IF_ERROR(_flat_writer->finish());

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
    if (_flat_writer != nullptr) {
        total_mem_footprint += _flat_writer->total_mem_footprint();
    }
    return total_mem_footprint;
}

Status MapColumnWriter::write_data() {
    if (is_nullable()) {
        RETURN_IF_ERROR(_nulls_writer->write_data());
    }
    RETURN_IF_ERROR(_offsets_writer->write_data());
    RETURN_IF_ERROR(_keys_writer->write_data());
    RETURN_IF_ERROR(_values_writer->write_data());
    if (_flat_writer != nullptr) {
        RETURN_IF_ERROR(_flat_writer->write_data());
    }
    return Status::OK();
}

Status MapColumnWriter::write_ordinal_index() {
    if (is_nullable()) {
        RETURN_IF_ERROR(_nulls_writer->write_ordinal_index());
    }
    RETURN_IF_ERROR(_offsets_writer->write_ordinal_index());
    RETURN_IF_ERROR(_keys_writer->write_ordinal_index());
    RETURN_IF_ERROR(_values_writer->write_ordinal_index());
    if (_flat_writer != nullptr) {
        RETURN_IF_ERROR(_flat_writer->write_ordinal_index());
    }
    return Status::OK();
}

Status MapColumnWriter::finish_current_page() {
    if (is_nullable()) {
        RETURN_IF_ERROR(_nulls_writer->finish_current_page());
    }
    RETURN_IF_ERROR(_offsets_writer->finish_current_page());
    RETURN_IF_ERROR(_keys_writer->finish_current_page());
    RETURN_IF_ERROR(_values_writer->finish_current_page());
    if (_flat_writer != nullptr) {
        RETURN_IF_ERROR(_flat_writer->finish_current_page());
    }
    return Status::OK();
}

} // namespace starrocks
