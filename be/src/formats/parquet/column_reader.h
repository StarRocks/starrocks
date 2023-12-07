// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once
#include "formats/parquet/column_converter.h"
#include "io/shared_buffered_input_stream.h"

namespace starrocks {
class RandomAccessFile;
class SharedBufferedInputStream;
namespace vectorized {
struct HdfsScanStats;
}
} // namespace starrocks

namespace starrocks::parquet {
struct ColumnReaderContext {
    vectorized::Buffer<uint8_t>* filter = nullptr;
    size_t next_row = 0;
    size_t rows_to_skip = 0;

    void advance(size_t num_rows) { next_row += num_rows; }
};

struct ColumnReaderOptions {
    std::string timezone;
    bool case_sensitive = false;
    int chunk_size = 0;
    vectorized::HdfsScanStats* stats = nullptr;
    RandomAccessFile* file = nullptr;
    tparquet::RowGroup* row_group_meta = nullptr;
    ColumnReaderContext* context = nullptr;
};

class ColumnReader {
public:
    // TODO(zc): review this,
    // create a column reader
    static Status create(const ColumnReaderOptions& opts, const ParquetField* field, const TypeDescriptor& col_type,
                         std::unique_ptr<ColumnReader>* reader);

    // for struct type without schema change
    static void get_subfield_pos_with_pruned_type(const ParquetField& field, const TypeDescriptor& col_type,
                                                  bool case_sensitive, std::vector<int32_t>& pos);

    virtual ~ColumnReader() = default;

    virtual Status prepare_batch(size_t* num_records, ColumnContentType content_type, vectorized::Column* column) = 0;
    virtual Status finish_batch() = 0;

    Status next_batch(size_t* num_records, ColumnContentType content_type, vectorized::Column* column) {
        RETURN_IF_ERROR(prepare_batch(num_records, content_type, column));
        return finish_batch();
    }

    virtual void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) = 0;

    virtual void set_need_parse_levels(bool need_parse_levels) = 0;

    virtual Status get_dict_values(vectorized::Column* column) {
        return Status::NotSupported("get_dict_values is not supported");
    }

    virtual Status get_dict_values(const std::vector<int32_t>& dict_codes, const vectorized::NullableColumn& nulls,
                                   vectorized::Column* column) {
        return Status::NotSupported("get_dict_values is not supported");
    }

    virtual Status get_dict_codes(const std::vector<Slice>& dict_values, const vectorized::NullableColumn& nulls,
                                  std::vector<int32_t>* dict_codes) {
        return Status::NotSupported("get_dict_codes is not supported");
    }

    std::unique_ptr<ColumnConverter> converter;

private:
    static Status do_create(const ColumnReaderOptions& opts, const ParquetField* field, const TypeDescriptor& col_type,
                            std::unique_ptr<ColumnReader>* reader);
};

} // namespace starrocks::parquet
