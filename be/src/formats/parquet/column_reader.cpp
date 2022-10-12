// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "formats/parquet/column_reader.h"

#include "column/array_column.h"
#include "exec/vectorized/hdfs_scanner.h"
#include "formats/parquet/column_converter.h"
#include "formats/parquet/stored_column_reader.h"
#include "util/runtime_profile.h"

namespace starrocks {
class RandomAccessFile;
}

namespace starrocks::parquet {

template <typename TOffset, typename TIsNull>
static void def_rep_to_offset(const LevelInfo& level_info, const level_t* def_levels, const level_t* rep_levels,
                              size_t num_levels, TOffset* offsets, TIsNull* is_nulls, size_t* num_offsets,
                              bool* has_null) {
    size_t offset_pos = 0;
    for (int i = 0; i < num_levels; ++i) {
        // when def_level is less than immediate_repeated_ancestor_def_level, it means that level
        // will affect its ancestor.
        // when rep_level is greater than max_rep_level, this means that level affects its
        // descendants.
        // So we can skip this levels
        if (def_levels[i] < level_info.immediate_repeated_ancestor_def_level ||
            rep_levels[i] > level_info.max_rep_level) {
            continue;
        }
        if (rep_levels[i] == level_info.max_rep_level) {
            offsets[offset_pos]++;
            continue;
        }

        offset_pos++;
        offsets[offset_pos] = offsets[offset_pos - 1];
        if (def_levels[i] >= level_info.max_def_level) {
            offsets[offset_pos]++;
        }

        // when def_level equals with max_def_level, this is a non null element or a required element
        // when def_level equals with (max_def_level - 1), this indicates an empty array
        // when def_level less than (max_def_level - 1) it means this array is null
        if (def_levels[i] >= level_info.max_def_level - 1) {
            is_nulls[offset_pos - 1] = 0;
        } else {
            is_nulls[offset_pos - 1] = 1;
            *has_null = true;
        }
    }
    *num_offsets = offset_pos;
}

class ScalarColumnReader : public ColumnReader {
public:
    explicit ScalarColumnReader(const ColumnReaderOptions& opts) : _opts(opts) {}
    ~ScalarColumnReader() override = default;

    Status init(const ParquetField* field, const TypeDescriptor& col_type,
                const tparquet::ColumnChunk* chunk_metadata) {
        RETURN_IF_ERROR(ColumnConverterFactory::create_converter(*field, col_type, _opts.timezone, &converter));
        return StoredColumnReader::create(_opts, field, chunk_metadata, &_reader);
    }

    Status prepare_batch(size_t* num_records, ColumnContentType content_type, vectorized::Column* dst) override {
        if (!converter->need_convert) {
            return _reader->read_records(num_records, content_type, dst);
        } else {
            SCOPED_RAW_TIMER(&_opts.stats->column_convert_ns);
            auto column = converter->create_src_column();

            Status status = _reader->read_records(num_records, content_type, column.get());
            if (!status.ok() && !status.is_end_of_file()) {
                return status;
            }

            RETURN_IF_ERROR(converter->convert(column, dst));

            return Status::OK();
        }
    }

    Status finish_batch() override { return Status::OK(); }

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        _reader->get_levels(def_levels, rep_levels, num_levels);
    }

    Status get_dict_values(vectorized::Column* column) override { return _reader->get_dict_values(column); }

    Status get_dict_values(const std::vector<int32_t>& dict_codes, vectorized::Column* column) override {
        return _reader->get_dict_values(dict_codes, column);
    }

    Status get_dict_codes(const std::vector<Slice>& dict_values, std::vector<int32_t>* dict_codes) override {
        return _reader->get_dict_codes(dict_values, dict_codes);
    }

private:
    const ColumnReaderOptions& _opts;

    std::unique_ptr<StoredColumnReader> _reader;
};

class ListColumnReader : public ColumnReader {
public:
    explicit ListColumnReader(const ColumnReaderOptions& opts) : _opts(opts) {}
    ~ListColumnReader() override = default;

    Status init(const ParquetField* field, std::unique_ptr<ColumnReader> element_reader) {
        _field = field;
        _element_reader = std::move(element_reader);
        return Status::OK();
    }

    Status prepare_batch(size_t* num_records, ColumnContentType content_type, vectorized::Column* dst) override {
        vectorized::NullableColumn* nullable_column = nullptr;
        vectorized::ArrayColumn* array_column = nullptr;
        if (dst->is_nullable()) {
            nullable_column = down_cast<vectorized::NullableColumn*>(dst);
            DCHECK(nullable_column->mutable_data_column()->is_array());
            array_column = down_cast<vectorized::ArrayColumn*>(nullable_column->mutable_data_column());
        } else {
            DCHECK(dst->is_array());
            array_column = down_cast<vectorized::ArrayColumn*>(dst);
        }
        auto* child_column = array_column->elements_column().get();
        auto st = _element_reader->prepare_batch(num_records, content_type, child_column);

        level_t* def_levels = nullptr;
        level_t* rep_levels = nullptr;
        size_t num_levels = 0;
        _element_reader->get_levels(&def_levels, &rep_levels, &num_levels);

        auto& offsets = array_column->offsets_column()->get_data();
        offsets.resize(num_levels + 1);
        vectorized::NullColumn null_column(num_levels);
        auto& is_nulls = null_column.get_data();
        size_t num_offsets = 0;
        bool has_null = false;
        def_rep_to_offset(_field->level_info, def_levels, rep_levels, num_levels, &offsets[0], &is_nulls[0],
                          &num_offsets, &has_null);
        offsets.resize(num_offsets + 1);
        is_nulls.resize(num_offsets);

        if (dst->is_nullable()) {
            DCHECK(nullable_column != nullptr);
            nullable_column->mutable_null_column()->swap_column(null_column);
            nullable_column->set_has_null(has_null);
        }

        return st;
    }

    Status finish_batch() override { return Status::OK(); }

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        _element_reader->get_levels(def_levels, rep_levels, num_levels);
    }

private:
    const ColumnReaderOptions& _opts;

    const ParquetField* _field = nullptr;
    std::unique_ptr<ColumnReader> _element_reader;
};

Status ColumnReader::create(const ColumnReaderOptions& opts, const ParquetField* field, const TypeDescriptor& col_type,
                            std::unique_ptr<ColumnReader>* output) {
    if (field->type.type == TYPE_MAP || field->type.type == TYPE_STRUCT) {
        return Status::InternalError("not supported type");
    }
    if (field->type.type == TYPE_ARRAY) {
        std::unique_ptr<ColumnReader> child_reader;
        RETURN_IF_ERROR(ColumnReader::create(opts, &field->children[0], col_type.children[0], &child_reader));
        std::unique_ptr<ListColumnReader> reader(new ListColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, std::move(child_reader)));
        *output = std::move(reader);
    } else {
        std::unique_ptr<ScalarColumnReader> reader(new ScalarColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, col_type, &opts.row_group_meta->columns[field->physical_column_index]));
        *output = std::move(reader);
    }
    return Status::OK();
}

} // namespace starrocks::parquet
