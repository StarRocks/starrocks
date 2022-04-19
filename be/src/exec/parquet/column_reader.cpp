// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/parquet/column_reader.h"

#include "column/array_column.h"
#include "exec/parquet/stored_column_reader.h"

namespace starrocks {
class RandomAccessFile;
}

namespace starrocks::parquet {

static void def_rep_to_offset(const LevelInfo& level_info, const level_t* def_levels, const level_t* rep_levels,
                              size_t num_levels, int32_t* offsets, int8_t* is_nulls, size_t* num_offsets) {
    size_t offset_pos = 0;
    for (int i = 0; i < num_levels; ++i) {
        // when dev_level is less than immediate_repeated_ancestor_def_level, it means that level
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

        // when del_level equals with max_def_level, this is a null element or a required element
        // when del_level equals with (max_def_level - 1), this indicates a empty array
        // when del_level less than (max_def_level - 1) it means this array is null
        if (def_levels[i] >= level_info.max_def_level - 1) {
            is_nulls[offset_pos - 1] = 0;
        } else {
            is_nulls[offset_pos - 1] = 1;
        }
    }
    *num_offsets = offset_pos;
}

class ScalarColumnReader : public ColumnReader {
public:
    ScalarColumnReader(ColumnReaderOptions opts) : _opts(std::move(opts)) {}
    ~ScalarColumnReader() override = default;

    Status init(int chunk_size, RandomAccessFile* file, const ParquetField* field,
                const tparquet::ColumnChunk* chunk_metadata, const TypeDescriptor& col_type) {
        StoredColumnReaderOptions opts;
        opts.stats = _opts.stats;

        RETURN_IF_ERROR(ColumnConverterFactory::create_converter(*field, col_type, _opts.timezone, &converter));

        return StoredColumnReader::create(file, field, chunk_metadata, opts, chunk_size, &_reader);
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
    ColumnReaderOptions _opts;

    std::unique_ptr<StoredColumnReader> _reader;
};

class ListColumnReader : public ColumnReader {
public:
    ListColumnReader(ColumnReaderOptions opts) : _opts(std::move(opts)) {}
    ~ListColumnReader() override = default;

    Status init(const ParquetField* field, std::unique_ptr<ColumnReader> element_reader) {
        _field = field;
        _element_reader = std::move(element_reader);
        return Status::OK();
    }

    Status prepare_batch(size_t* num_records, ColumnContentType content_type, vectorized::Column* dst) override {
        return _element_reader->prepare_batch(num_records, content_type, dst);
    }

    Status finish_batch() override {
        level_t* def_levels = nullptr;
        level_t* rep_levels = nullptr;
        size_t num_levels = 0;

        _element_reader->get_levels(&def_levels, &rep_levels, &num_levels);
        std::vector<int32_t> offsets(num_levels + 1);
        std::vector<int8_t> is_nulls(num_levels);
        size_t num_offsets = 0;

        offsets[0] = 0;
        def_rep_to_offset(_field->level_info, def_levels, rep_levels, num_levels, &offsets[0], &is_nulls[0],
                          &num_offsets);
        return Status::OK();
    }

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        _element_reader->get_levels(def_levels, rep_levels, num_levels);
    }

private:
    ColumnReaderOptions _opts;

    const ParquetField* _field = nullptr;
    std::unique_ptr<ColumnReader> _element_reader;
};

Status ColumnReader::create(RandomAccessFile* file, const ParquetField* field, const tparquet::RowGroup& row_group,
                            const TypeDescriptor& col_type, const ColumnReaderOptions& opts, int chunk_size,
                            std::unique_ptr<ColumnReader>* output) {
    if (field->type.type == TYPE_MAP || field->type.type == TYPE_STRUCT) {
        return Status::InternalError("not supported type");
    }
    if (field->type.type == TYPE_ARRAY) {
        std::unique_ptr<ColumnReader> child_reader;
        RETURN_IF_ERROR(
                ColumnReader::create(file, &field->children[0], row_group, col_type, opts, chunk_size, &child_reader));
        std::unique_ptr<ListColumnReader> reader(new ListColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, std::move(child_reader)));
        *output = std::move(reader);
    } else {
        std::unique_ptr<ScalarColumnReader> reader(new ScalarColumnReader(opts));
        RETURN_IF_ERROR(
                reader->init(chunk_size, file, field, &row_group.columns[field->physical_column_index], col_type));
        *output = std::move(reader);
    }
    return Status::OK();
}

} // namespace starrocks::parquet
