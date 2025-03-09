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
#include "formats/parquet/stored_column_reader.h"
#include "formats/parquet/utils.h"
#include "util/thrift_util.h"

namespace starrocks::parquet {

class FixedValueColumnReader final : public ColumnReader {
public:
    explicit FixedValueColumnReader(Datum fixed_value) : ColumnReader(nullptr), _fixed_value(std::move(fixed_value)) {}

    ~FixedValueColumnReader() override = default;

    Status prepare() override { return Status::OK(); }

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {}

    void set_need_parse_levels(bool need_parse_levels) override {}

    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOType type, bool active) override {}

    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override {}

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) override {
        return Status::NotSupported("Not implemented");
    }

    StatusOr<bool> row_group_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                             CompoundNodeType pred_relation, const uint64_t rg_first_row,
                                             const uint64_t rg_num_rows) const override;

    StatusOr<bool> page_index_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                              SparseRange<uint64_t>* row_ranges, CompoundNodeType pred_relation,
                                              const uint64_t rg_first_row, const uint64_t rg_num_rows) override;

private:
    const Datum _fixed_value;
};

class RawColumnReader : public ColumnReader {
public:
    explicit RawColumnReader(const ParquetField* parquet_field, const tparquet::ColumnChunk* column_chunk_metadata,
                             const ColumnReaderOptions& opts)
            : ColumnReader(parquet_field), _opts(opts), _chunk_metadata(column_chunk_metadata) {}
    explicit RawColumnReader(const RawColumnReader& reader)
            : ColumnReader(reader.get_column_parquet_field()),
              _opts(reader._opts),
              _chunk_metadata(reader._chunk_metadata) {}
    ~RawColumnReader() override = default;

    Status prepare() override {
        return StoredColumnReader::create(_opts, get_column_parquet_field(), get_chunk_metadata(), &_reader);
    }

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        _reader->get_levels(def_levels, rep_levels, num_levels);
    }

    void set_need_parse_levels(bool need_parse_levels) override { _reader->set_need_parse_levels(need_parse_levels); }

    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOType type, bool active) override;

    const tparquet::ColumnChunk* get_chunk_metadata() const override { return _chunk_metadata; }

    StatusOr<tparquet::OffsetIndex*> get_offset_index(const uint64_t rg_first_row) override {
        if (_offset_index_ctx == nullptr) {
            _offset_index_ctx = std::make_unique<ColumnOffsetIndexCtx>();
            _offset_index_ctx->rg_first_row = rg_first_row;
            int64_t offset_index_offset = get_chunk_metadata()->offset_index_offset;
            uint32_t offset_index_length = get_chunk_metadata()->offset_index_length;
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

    // Returns true if all of the data pages in the column chunk are dict encoded
    bool column_all_pages_dict_encoded() const;

protected:
    StatusOr<bool> _row_group_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                              CompoundNodeType pred_relation, const TypeDescriptor& col_type,
                                              uint64_t rg_first_row, uint64_t rg_num_rows) const;

    StatusOr<bool> _page_index_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                               SparseRange<uint64_t>* row_ranges, CompoundNodeType pred_relation,
                                               const TypeDescriptor& col_type, const uint64_t rg_first_row,
                                               const uint64_t rg_num_rows);

    const ColumnReaderOptions& _opts;

    std::unique_ptr<StoredColumnReader> _reader;

    const tparquet::ColumnChunk* _chunk_metadata = nullptr;
    std::unique_ptr<ColumnOffsetIndexCtx> _offset_index_ctx;
};

class ScalarColumnReader final : public RawColumnReader {
public:
    explicit ScalarColumnReader(const ParquetField* parquet_field, const tparquet::ColumnChunk* column_chunk_metadata,
                                const TypeDescriptor* col_type, const ColumnReaderOptions& opts)
            : RawColumnReader(parquet_field, column_chunk_metadata, opts), _col_type(col_type) {}
    ~ScalarColumnReader() override = default;

    Status prepare() override {
        RETURN_IF_ERROR(ColumnConverterFactory::create_converter(*get_column_parquet_field(), *_col_type,
                                                                 _opts.timezone, &_converter));
        return RawColumnReader::prepare();
    }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) override;

    bool try_to_use_dict_filter(ExprContext* ctx, bool is_decode_needed, const SlotId slotId,
                                const std::vector<std::string>& sub_field_path, const size_t& layer) override;

    Status rewrite_conjunct_ctxs_to_predicate(bool* is_group_filtered, const std::vector<std::string>& sub_field_path,
                                              const size_t& layer) override {
        DCHECK_EQ(sub_field_path.size(), layer);
        return _dict_filter_ctx->rewrite_conjunct_ctxs_to_predicate(_reader.get(), is_group_filtered);
    }

    void set_can_lazy_decode(bool can_lazy_decode) override {
        _can_lazy_convert = can_lazy_decode;
        _can_lazy_dict_decode = can_lazy_decode && _col_type->is_string_type() && column_all_pages_dict_encoded();
    }

    Status filter_dict_column(ColumnPtr& column, Filter* filter, const std::vector<std::string>& sub_field_path,
                              const size_t& layer) override {
        DCHECK_EQ(sub_field_path.size(), layer);
        return _dict_filter_ctx->predicate->evaluate_and(column.get(), filter->data());
    }

    Status fill_dst_column(ColumnPtr& dst, ColumnPtr& src) override;

    StatusOr<bool> row_group_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                             CompoundNodeType pred_relation, const uint64_t rg_first_row,
                                             const uint64_t rg_num_rows) const override {
        return _row_group_zone_map_filter(predicates, pred_relation, *_col_type, rg_first_row, rg_num_rows);
    }

    StatusOr<bool> page_index_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                              SparseRange<uint64_t>* row_ranges, CompoundNodeType pred_relation,
                                              const uint64_t rg_first_row, const uint64_t rg_num_rows) override {
        return _page_index_zone_map_filter(predicates, row_ranges, pred_relation, *_col_type, rg_first_row,
                                           rg_num_rows);
    }

private:
    template <bool LAZY_DICT_DECODE, bool LAZY_CONVERT>
    Status _read_range_impl(const Range<uint64_t>& range, const Filter* filter, ColumnContentType content_type,
                            ColumnPtr& dst);

    template <bool LAZY_DICT_DECODE, bool LAZY_CONVERT>
    Status _fill_dst_column_impl(ColumnPtr& dst, ColumnPtr& src);

    Status _dict_decode(ColumnPtr& dst, ColumnPtr& src);

    std::unique_ptr<ColumnConverter> _converter;

    std::unique_ptr<ColumnDictFilterContext> _dict_filter_ctx;
    const TypeDescriptor* _col_type = nullptr;

    // _can_lazy_dict_decode means string type and all page dict code
    bool _can_lazy_dict_decode = false;
    bool _can_lazy_convert = false;
    // we use lazy decode adaptively because of RLE && decoder may be better than filter && decoder
    static constexpr double FILTER_RATIO = 0.2;
    bool _need_lazy_decode = false;
    // dict code
    ColumnPtr _tmp_code_column = nullptr;
    ColumnPtr _tmp_intermediate_column = nullptr;
    ColumnPtr _ori_column = nullptr;
};

class LowCardColumnReader final : public RawColumnReader {
public:
    explicit LowCardColumnReader(const RawColumnReader& reader, const GlobalDictMap* dict, SlotId slot_id)
            : RawColumnReader(reader), _dict(dict), _slot_id(slot_id) {}

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) override;

    bool try_to_use_dict_filter(ExprContext* ctx, bool is_decode_needed, const SlotId slotId,
                                const std::vector<std::string>& sub_field_path, const size_t& layer) override;

    Status rewrite_conjunct_ctxs_to_predicate(bool* is_group_filtered, const std::vector<std::string>& sub_field_path,
                                              const size_t& layer) override {
        DCHECK_EQ(sub_field_path.size(), layer);
        return _dict_filter_ctx->rewrite_conjunct_ctxs_to_predicate(_reader.get(), is_group_filtered);
    }

    Status filter_dict_column(ColumnPtr& column, Filter* filter, const std::vector<std::string>& sub_field_path,
                              const size_t& layer) override {
        DCHECK_EQ(sub_field_path.size(), layer);
        return _dict_filter_ctx->predicate->evaluate_and(column.get(), filter->data());
    }

    Status fill_dst_column(ColumnPtr& dst, ColumnPtr& src) override;

    StatusOr<bool> row_group_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                             CompoundNodeType pred_relation, const uint64_t rg_first_row,
                                             const uint64_t rg_num_rows) const override {
        return _row_group_zone_map_filter(predicates, pred_relation, TypeDescriptor(LogicalType::TYPE_VARCHAR),
                                          rg_first_row, rg_num_rows);
    }

    StatusOr<bool> page_index_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                              SparseRange<uint64_t>* row_ranges, CompoundNodeType pred_relation,
                                              const uint64_t rg_first_row, const uint64_t rg_num_rows) override {
        return _page_index_zone_map_filter(predicates, row_ranges, pred_relation,
                                           TypeDescriptor(LogicalType::TYPE_VARCHAR), rg_first_row, rg_num_rows);
    }

private:
    Status _check_current_dict();

    std::unique_ptr<ColumnDictFilterContext> _dict_filter_ctx;

    const GlobalDictMap* _dict = nullptr;
    const SlotId _slot_id;

    std::optional<std::vector<int16_t>> _code_convert_map;

    ColumnPtr _dict_code = nullptr;
    ColumnPtr _ori_column = nullptr;
};

class LowRowsColumnReader final : public RawColumnReader {
public:
    explicit LowRowsColumnReader(const RawColumnReader& reader, const GlobalDictMap* dict, SlotId slot_id)
            : RawColumnReader(reader), _dict(dict), _slot_id(slot_id) {}

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) override;

    Status fill_dst_column(ColumnPtr& dst, ColumnPtr& src) override;

    StatusOr<bool> row_group_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                             CompoundNodeType pred_relation, const uint64_t rg_first_row,
                                             const uint64_t rg_num_rows) const override {
        return _row_group_zone_map_filter(predicates, pred_relation, TypeDescriptor(LogicalType::TYPE_VARCHAR),
                                          rg_first_row, rg_num_rows);
    }

    StatusOr<bool> page_index_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                              SparseRange<uint64_t>* row_ranges, CompoundNodeType pred_relation,
                                              const uint64_t rg_first_row, const uint64_t rg_num_rows) override {
        return _page_index_zone_map_filter(predicates, row_ranges, pred_relation,
                                           TypeDescriptor(LogicalType::TYPE_VARCHAR), rg_first_row, rg_num_rows);
    }

private:
    const GlobalDictMap* _dict = nullptr;
    const SlotId _slot_id;

    ColumnPtr _tmp_column = nullptr;
    ColumnPtr _ori_column = nullptr;
};

} // namespace starrocks::parquet