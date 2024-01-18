// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include "column_reader.h"
#include "common/status.h"
#include "formats/parquet/column_chunk_reader.h"
#include "formats/parquet/schema.h"
#include "formats/parquet/types.h"
#include "formats/parquet/utils.h"
#include "gen_cpp/parquet_types.h"

namespace starrocks {
namespace vectorized {
class Column;
class NullableColumn;
} // namespace vectorized
} // namespace starrocks

namespace starrocks::parquet {

class ColumnChunkReader;
class StoredColumnReader {
public:
    static Status create(const ColumnReaderOptions& opts, const ParquetField* field,
                         const tparquet::ColumnChunk* _chunk_metadata, std::unique_ptr<StoredColumnReader>* out);
    StoredColumnReader(const ColumnReaderOptions& opts) : _opts(opts) {}

    virtual ~StoredColumnReader() = default;

    // Reset internal state and ready for next read_values
    virtual void reset() = 0;

    // If need_levels is set, client will get all levels through get_levels function.
    // If need_levels is not set, read_records may not records levels information, this will
    // improve performance. So set this flag when you only needs it.
    // TODO(zc): to recosiderate to move this flag to StoredColumnReaderOptions
    // StoredColumnReaderOptions is shared by all StoredColumnReader, but we only want set StoredColumnReader specifically,
    // so currently we can't put need_parse_levels into StoredColumnReaderOptions.
    virtual void set_need_parse_levels(bool need_parse_levels) {}

    // Try to read values that can assemble up to num_rows rows. For example if we want to read
    // an array type, and stored value is [1, 2, 3], [4], [5, 6], when the input num_rows is 3,
    // this function will fill (1, 2, 3, 4, 5, 6) into 'dst'.
    Status read_records(size_t* num_rows, ColumnContentType content_type, vectorized::Column* dst) {
        reset();
        return do_read_records(num_rows, content_type, dst);
    }

    // This function can only be called after calling read_values. This function returns the
    // levels for last read_values.
    virtual void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) = 0;

    virtual Status get_dict_values(vectorized::Column* column) { return _reader->get_dict_values(column); }

    virtual Status get_dict_values(const std::vector<int32_t>& dict_codes, const vectorized::NullableColumn& nulls,
                                   vectorized::Column* column) {
        return _reader->get_dict_values(dict_codes, nulls, column);
    }

    virtual Status get_dict_codes(const std::vector<Slice>& dict_values, const vectorized::NullableColumn& nulls,
                                  std::vector<int32_t>* dict_codes) {
        return _reader->get_dict_codes(dict_values, nulls, dict_codes);
    }

protected:
    virtual bool page_selected(size_t num_values);

    virtual Status do_read_records(size_t* num_rows, ColumnContentType content_type, vectorized::Column* dst) = 0;

    Status next_page(size_t records_to_read, ColumnContentType content_type, size_t* records_read,
                     vectorized::Column* dst);

    void update_read_context(size_t records_read);

    // for RequiredColumn, there is no need to get levels.
    // for RepeatedColumn, there is no possible to get default levels.
    // for OptionalColumn, we will override it.
    virtual void append_default_levels(size_t row_nums) {}

    std::unique_ptr<ColumnChunkReader> _reader;
    size_t _num_values_left_in_cur_page = 0;
    size_t _num_values_skip_in_cur_page = 0;
    const ColumnReaderOptions& _opts;

private:
    Status _next_selected_page(size_t records_to_read, ColumnContentType content_type, size_t* records_to_skip,
                               vectorized::Column* dst);

    Status _lazy_load_page_rows(size_t batch_size, ColumnContentType content_type, vectorized::Column* dst);
};

} // namespace starrocks::parquet
