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

#include "formats/parquet/column_converter.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/page_index_reader.h"
#include "formats/parquet/stored_column_reader.h"
#include "util/thrift_util.h"

namespace starrocks::parquet {

class ScalarColumnReader : public ColumnReader {
public:
    explicit ScalarColumnReader(const ColumnReaderOptions& opts) : _opts(opts) {}
    ~ScalarColumnReader() override = default;

    Status init(const ParquetField* field, const TypeDescriptor& col_type,
                const tparquet::ColumnChunk* chunk_metadata) {
        _field = field;
        _col_type = &col_type;
        _chunk_metadata = chunk_metadata;
        RETURN_IF_ERROR(ColumnConverterFactory::create_converter(*field, col_type, _opts.timezone, &_converter));
        return StoredColumnReader::create(_opts, field, chunk_metadata, &_reader);
    }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) override;

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        _reader->get_levels(def_levels, rep_levels, num_levels);
    }

    void set_need_parse_levels(bool need_parse_levels) override { _reader->set_need_parse_levels(need_parse_levels); }

    bool try_to_use_dict_filter(ExprContext* ctx, bool is_decode_needed, const SlotId slotId,
                                const std::vector<std::string>& sub_field_path, const size_t& layer) override;

    Status rewrite_conjunct_ctxs_to_predicate(bool* is_group_filtered, const std::vector<std::string>& sub_field_path,
                                              const size_t& layer) override {
        DCHECK_EQ(sub_field_path.size(), layer);
        return _dict_filter_ctx->rewrite_conjunct_ctxs_to_predicate(_reader.get(), is_group_filtered);
    }

    void set_can_lazy_decode(bool can_lazy_decode) override {
        _can_lazy_decode = can_lazy_decode && _col_type->is_string_type() && _column_all_pages_dict_encoded();
    }

    Status filter_dict_column(const ColumnPtr& column, Filter* filter, const std::vector<std::string>& sub_field_path,
                              const size_t& layer) override {
        DCHECK_EQ(sub_field_path.size(), layer);
        return _dict_filter_ctx->predicate->evaluate_and(column.get(), filter->data());
    }

    Status fill_dst_column(ColumnPtr& dst, const ColumnPtr& src) override;

    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOType type, bool active) override;

    const tparquet::ColumnChunk* get_chunk_metadata() override { return _chunk_metadata; }

    const ParquetField* get_column_parquet_field() override { return _field; }

    StatusOr<tparquet::OffsetIndex*> get_offset_index(const uint64_t rg_first_row) override {
        if (_offset_index_ctx == nullptr) {
            _offset_index_ctx = std::make_unique<ColumnOffsetIndexCtx>();
            _offset_index_ctx->rg_first_row = rg_first_row;
            int64_t offset_index_offset = _chunk_metadata->offset_index_offset;
            uint32_t offset_index_length = _chunk_metadata->offset_index_length;
            std::vector<uint8_t> offset_index_data;
            offset_index_data.reserve(offset_index_length);
            RETURN_IF_ERROR(
                    _opts.file->read_at_fully(offset_index_offset, offset_index_data.data(), offset_index_length));

            RETURN_IF_ERROR(deserialize_thrift_msg(offset_index_data.data(), &offset_index_length,
                                                   TProtocolType::COMPACT, &_offset_index_ctx->offset_index));
        }
        return &_offset_index_ctx->offset_index;
    }

    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override;

private:
    // Returns true if all of the data pages in the column chunk are dict encoded
    bool _column_all_pages_dict_encoded();

    const ColumnReaderOptions& _opts;

    std::unique_ptr<StoredColumnReader> _reader;
    const ParquetField* _field = nullptr;

    std::unique_ptr<ColumnConverter> _converter;

    std::unique_ptr<ColumnDictFilterContext> _dict_filter_ctx;
    const TypeDescriptor* _col_type = nullptr;
    const tparquet::ColumnChunk* _chunk_metadata = nullptr;
    std::unique_ptr<ColumnOffsetIndexCtx> _offset_index_ctx;

    // _can_lazy_decode means string type and all page dict code
    bool _can_lazy_decode = false;
    // we use lazy decode adaptively because of RLE && decoder may be better than filter && decoder
    static constexpr double FILTER_RATIO = 0.2;
    bool _need_lazy_decode = false;
    // dict code
    ColumnPtr _dict_code = nullptr;
};

} // namespace starrocks::parquet