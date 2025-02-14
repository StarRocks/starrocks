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

#include "formats/parquet/scalar_column_reader.h"

#include "formats/parquet/column_reader.h"
#include "formats/parquet/stored_column_reader_with_index.h"
#include "formats/parquet/zone_map_filter_evaluator.h"
#include "gutil/casts.h"
#include "io/shared_buffered_input_stream.h"
#include "runtime/global_dict/dict_column.h"
#include "runtime/types.h"
#include "simd/gather.h"
#include "simd/simd.h"
#include "statistics_helper.h"

namespace starrocks::parquet {

// FixedValueColumnReader

StatusOr<bool> FixedValueColumnReader::row_group_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                                 CompoundNodeType pred_relation,
                                                                 const uint64_t rg_first_row,
                                                                 const uint64_t rg_num_rows) const {
    ZoneMapDetail zone_map{_fixed_value, _fixed_value, _fixed_value.is_null()};
    return ZoneMapEvaluatorUtils::is_satisfy(predicates, zone_map, pred_relation);
}

StatusOr<bool> FixedValueColumnReader::page_index_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                                  SparseRange<uint64_t>* row_ranges,
                                                                  CompoundNodeType pred_relation,
                                                                  const uint64_t rg_first_row,
                                                                  const uint64_t rg_num_rows) {
    DCHECK(row_ranges->empty());
    ZoneMapDetail zone_map{_fixed_value, _fixed_value, _fixed_value.is_null()};

    // is_satisfy = true means no filter happened, return false
    // is_satisfy = false means entire row group can be filtered, filter happened, return true
    return !ZoneMapEvaluatorUtils::is_satisfy(predicates, zone_map, pred_relation);
}

// RawColumnReader

void RawColumnReader::collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges,
                                              int64_t* end_offset, ColumnIOType type, bool active) {
    const auto& column = *get_chunk_metadata();
    if (type == ColumnIOType::PAGES) {
        const tparquet::ColumnMetaData& column_metadata = column.meta_data;
        if (_offset_index_ctx != nullptr && !_offset_index_ctx->page_selected.empty()) {
            // add dict page
            if (column_metadata.__isset.dictionary_page_offset) {
                auto r = io::SharedBufferedInputStream::IORange(
                        column_metadata.dictionary_page_offset,
                        column_metadata.data_page_offset - column_metadata.dictionary_page_offset, active);
                ranges->emplace_back(r);
                *end_offset = std::max(*end_offset, r.offset + r.size);
            }
            _offset_index_ctx->collect_io_range(ranges, end_offset, active);
        } else {
            int64_t offset = 0;
            if (column_metadata.__isset.dictionary_page_offset) {
                offset = column_metadata.dictionary_page_offset;
            } else {
                offset = column_metadata.data_page_offset;
            }
            int64_t size = column_metadata.total_compressed_size;
            auto r = io::SharedBufferedInputStream::IORange(offset, size, active);
            ranges->emplace_back(r);
            *end_offset = std::max(*end_offset, offset + size);
        }
    } else if (type == ColumnIOType::PAGE_INDEX) {
        // only active column need column index
        if (column.__isset.column_index_offset && active) {
            auto r = io::SharedBufferedInputStream::IORange(column.column_index_offset, column.column_index_length);
            ranges->emplace_back(r);
        }
        // all column need offset index
        if (column.__isset.offset_index_offset) {
            auto r = io::SharedBufferedInputStream::IORange(column.offset_index_offset, column.offset_index_length);
            ranges->emplace_back(r);
        }
    }
}

void RawColumnReader::select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) {
    if (_offset_index_ctx == nullptr) {
        if (!get_chunk_metadata()->__isset.offset_index_offset) {
            return;
        }
        auto st = get_offset_index(rg_first_row);
        if (!st.ok()) {
            return;
        }
    }
    size_t page_num = _offset_index_ctx->offset_index.page_locations.size();
    size_t range_size = range.size();

    size_t range_idx = 0;
    Range<uint64_t> r = range[range_idx++];

    for (size_t i = 0; i < page_num; i++) {
        int64_t first_row = _offset_index_ctx->offset_index.page_locations[i].first_row_index + rg_first_row;
        int64_t end_row = first_row;
        if (i != page_num - 1) {
            end_row = _offset_index_ctx->offset_index.page_locations[i + 1].first_row_index + rg_first_row;
        } else {
            // a little trick, we don't care about the real rows of the last page.
            if (range.end() < first_row) {
                _offset_index_ctx->page_selected.emplace_back(false);
                continue;
            } else {
                end_row = range.end();
            }
        }
        if (end_row <= r.begin()) {
            _offset_index_ctx->page_selected.emplace_back(false);
            continue;
        }
        while (first_row >= r.end() && range_idx < range_size) {
            r = range[range_idx++];
        }
        _offset_index_ctx->page_selected.emplace_back(first_row < r.end() && end_row > r.begin());
    }
    const tparquet::ColumnMetaData& column_metadata = get_chunk_metadata()->meta_data;
    bool has_dict_page = column_metadata.__isset.dictionary_page_offset;
    // be compatible with PARQUET-1850
    has_dict_page |= _offset_index_ctx->check_dictionary_page(column_metadata.data_page_offset);
    _reader = std::make_unique<StoredColumnReaderWithIndex>(std::move(_reader), _offset_index_ctx.get(), has_dict_page);
}

bool RawColumnReader::column_all_pages_dict_encoded() const {
    // The Parquet spec allows for column chunks to have mixed encodings
    // where some data pages are dictionary-encoded and others are plain
    // encoded. For example, a Parquet file writer might start writing
    // a column chunk as dictionary encoded, but it will switch to plain
    // encoding if the dictionary grows too large.
    //
    // In order for dictionary filters to skip the entire row group,
    // the conjuncts must be evaluated on column chunks that are entirely
    // encoded with the dictionary encoding. There are two checks
    // available to verify this:
    // 1. The encoding_stats field on the column chunk metadata provides
    //    information about the number of data pages written in each
    //    format. This allows for a specific check of whether all the
    //    data pages are dictionary encoded.
    // 2. The encodings field on the column chunk metadata lists the
    //    encodings used. If this list contains the dictionary encoding
    //    and does not include unexpected encodings (i.e. encodings not
    //    associated with definition/repetition levels), then it is entirely
    //    dictionary encoded.
    const tparquet::ColumnMetaData& column_metadata = get_chunk_metadata()->meta_data;
    if (column_metadata.__isset.encoding_stats) {
        // Condition #1 above
        for (const tparquet::PageEncodingStats& enc_stat : column_metadata.encoding_stats) {
            if (enc_stat.page_type == tparquet::PageType::DATA_PAGE &&
                (enc_stat.encoding != tparquet::Encoding::PLAIN_DICTIONARY &&
                 enc_stat.encoding != tparquet::Encoding::RLE_DICTIONARY) &&
                enc_stat.count > 0) {
                return false;
            }
        }
    } else {
        // Condition #2 above
        bool has_dict_encoding = false;
        bool has_nondict_encoding = false;
        for (const tparquet::Encoding::type& encoding : column_metadata.encodings) {
            if (encoding == tparquet::Encoding::PLAIN_DICTIONARY || encoding == tparquet::Encoding::RLE_DICTIONARY) {
                has_dict_encoding = true;
            }

            // RLE and BIT_PACKED are used for repetition/definition levels
            if (encoding != tparquet::Encoding::PLAIN_DICTIONARY && encoding != tparquet::Encoding::RLE_DICTIONARY &&
                encoding != tparquet::Encoding::RLE && encoding != tparquet::Encoding::BIT_PACKED) {
                has_nondict_encoding = true;
                break;
            }
        }
        // Not entirely dictionary encoded if:
        // 1. No dictionary encoding listed
        // OR
        // 2. Some non-dictionary encoding is listed
        if (!has_dict_encoding || has_nondict_encoding) {
            return false;
        }
    }

    return true;
}

StatusOr<bool> RawColumnReader::_row_group_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                           CompoundNodeType pred_relation,
                                                           const TypeDescriptor& col_type, const uint64_t rg_first_row,
                                                           const uint64_t rg_num_rows) const {
    if (!get_chunk_metadata()->meta_data.__isset.statistics || get_column_parquet_field() == nullptr) {
        // statistics is not existed, select all
        return true;
    }

    bool has_null = true;
    bool is_all_null = false;

    if (get_chunk_metadata()->meta_data.statistics.__isset.null_count) {
        has_null = get_chunk_metadata()->meta_data.statistics.null_count > 0;
        is_all_null = get_chunk_metadata()->meta_data.statistics.null_count == rg_num_rows;
    } else {
        return true;
    }

    std::optional<ZoneMapDetail> zone_map_detail = std::nullopt;

    // used to hold min/max slice values
    const ColumnPtr min_column = ColumnHelper::create_column(col_type, true);
    const ColumnPtr max_column = ColumnHelper::create_column(col_type, true);
    if (is_all_null) {
        // if the entire column's value is null, the min/max value not existed
        zone_map_detail = ZoneMapDetail{Datum{}, Datum{}, true};
        zone_map_detail->set_num_rows(rg_num_rows);
    } else {
        std::vector<string> min_values;
        std::vector<string> max_values;
        std::vector<bool> null_pages{false};
        Status st =
                StatisticsHelper::get_min_max_value(_opts.file_meta_data, col_type, &get_chunk_metadata()->meta_data,
                                                    get_column_parquet_field(), min_values, max_values);
        if (st.ok()) {
            RETURN_IF_ERROR(StatisticsHelper::decode_value_into_column(min_column, min_values, null_pages, col_type,
                                                                       get_column_parquet_field(), _opts.timezone));
            RETURN_IF_ERROR(StatisticsHelper::decode_value_into_column(max_column, max_values, null_pages, col_type,
                                                                       get_column_parquet_field(), _opts.timezone));

            zone_map_detail = ZoneMapDetail{min_column->get(0), max_column->get(0), has_null};
            zone_map_detail->set_num_rows(rg_num_rows);
        }
    }

    if (!zone_map_detail.has_value()) {
        // ZoneMapDetail not set, means select all
        return true;
    }

    return ZoneMapEvaluatorUtils::is_satisfy(predicates, zone_map_detail.value(), pred_relation);
}

StatusOr<bool> RawColumnReader::_page_index_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                            SparseRange<uint64_t>* row_ranges,
                                                            CompoundNodeType pred_relation,
                                                            const TypeDescriptor& col_type, const uint64_t rg_first_row,
                                                            const uint64_t rg_num_rows) {
    DCHECK(row_ranges->empty());
    const tparquet::ColumnChunk* chunk_meta = get_chunk_metadata();
    if (!chunk_meta->__isset.column_index_offset || !chunk_meta->__isset.offset_index_offset ||
        !chunk_meta->__isset.meta_data) {
        // no page index, dont filter
        return false;
    }

    // get column index
    int64_t column_index_offset = chunk_meta->column_index_offset;
    uint32_t column_index_length = chunk_meta->column_index_length;

    std::vector<uint8_t> page_index_data;
    page_index_data.reserve(column_index_length);
    RETURN_IF_ERROR(_opts.file->read_at_fully(column_index_offset, page_index_data.data(), column_index_length));

    tparquet::ColumnIndex column_index;
    RETURN_IF_ERROR(deserialize_thrift_msg(page_index_data.data(), &column_index_length, TProtocolType::COMPACT,
                                           &column_index));

    ASSIGN_OR_RETURN(const tparquet::OffsetIndex* offset_index, get_offset_index(rg_first_row));

    const size_t page_num = column_index.min_values.size();
    const std::vector<bool> null_pages = column_index.null_pages;

    ColumnPtr min_column = ColumnHelper::create_column(col_type, true);
    ColumnPtr max_column = ColumnHelper::create_column(col_type, true);
    // deal with min_values
    auto st = StatisticsHelper::decode_value_into_column(min_column, column_index.min_values, null_pages, col_type,
                                                         get_column_parquet_field(), _opts.timezone);
    if (!st.ok()) {
        // swallow error status
        LOG(INFO) << "Error when decode min/max statistics, type " << col_type.debug_string();
        return false;
    }
    // deal with max_values
    st = StatisticsHelper::decode_value_into_column(max_column, column_index.max_values, null_pages, col_type,
                                                    get_column_parquet_field(), _opts.timezone);
    if (!st.ok()) {
        // swallow error status
        LOG(INFO) << "Error when decode min/max statistics, type " << col_type.debug_string();
        return false;
    }

    DCHECK_EQ(page_num, min_column->size());
    DCHECK_EQ(page_num, max_column->size());

    // fill ZoneMapDetail
    std::vector<ZoneMapDetail> zone_map_details{};
    for (size_t i = 0; i < page_num; i++) {
        if (null_pages[i]) {
            // all null
            zone_map_details.emplace_back(Datum{}, Datum{}, true);
        } else {
            bool has_null = column_index.null_counts[i] > 0;
            zone_map_details.emplace_back(min_column->get(i), max_column->get(i), has_null);
        }
    }

    // select all pages by default
    Filter page_filter(page_num, 1);
    for (size_t i = 0; i < page_num; i++) {
        page_filter[i] = ZoneMapEvaluatorUtils::is_satisfy(predicates, zone_map_details[i], pred_relation);
    }

    if (!SIMD::contain_zero(page_filter)) {
        // no page has been filtered
        return false;
    }

    for (int i = 0; i < page_num; i++) {
        if (page_filter[i]) {
            int64_t first_row = offset_index->page_locations[i].first_row_index + rg_first_row;
            int64_t end_row = first_row;
            if (i != page_num - 1) {
                end_row = offset_index->page_locations[i + 1].first_row_index + rg_first_row;
            } else {
                end_row = rg_first_row + rg_num_rows;
            }
            row_ranges->add(Range<uint64_t>(first_row, end_row));
        }
    }
    return true;
}

// ScalarColumnReader

Status ScalarColumnReader::read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) {
    DCHECK(get_column_parquet_field()->is_nullable ? dst->is_nullable() : true);
    _need_lazy_decode =
            _dict_filter_ctx != nullptr || (_can_lazy_dict_decode && filter != nullptr &&
                                            SIMD::count_nonzero(*filter) * 1.0 / filter->size() < FILTER_RATIO);
    ColumnContentType content_type = !_need_lazy_decode ? ColumnContentType::VALUE : ColumnContentType::DICT_CODE;
    auto need_lazy_covert = _can_lazy_convert && _converter->need_convert;
    if (_need_lazy_decode) {
        return _read_range_impl<true, false>(range, filter, content_type, dst);
    } else if (need_lazy_covert) {
        return _read_range_impl<false, true>(range, filter, content_type, dst);
    } else {
        return _read_range_impl<false, false>(range, filter, content_type, dst);
    }
}

bool ScalarColumnReader::try_to_use_dict_filter(ExprContext* ctx, bool is_decode_needed, const SlotId slotId,
                                                const std::vector<std::string>& sub_field_path, const size_t& layer) {
    if (sub_field_path.size() != layer) {
        return false;
    }

    if (!_col_type->is_string_type()) {
        return false;
    }

    if (column_all_pages_dict_encoded()) {
        if (_dict_filter_ctx == nullptr) {
            _dict_filter_ctx = std::make_unique<ColumnDictFilterContext>();
            _dict_filter_ctx->is_decode_needed = is_decode_needed;
            _dict_filter_ctx->sub_field_path = sub_field_path;
            _dict_filter_ctx->slot_id = slotId;
        }
        _dict_filter_ctx->conjunct_ctxs.push_back(ctx);
        return true;
    } else {
        return false;
    }
}

Status ScalarColumnReader::fill_dst_column(ColumnPtr& dst, ColumnPtr& src) {
    auto need_lazy_covert = _can_lazy_convert && _converter->need_convert;
    if (_need_lazy_decode) {
        return _fill_dst_column_impl<true, false>(dst, src);
    } else if (need_lazy_covert) {
        return _fill_dst_column_impl<false, true>(dst, src);
    } else {
        return _fill_dst_column_impl<false, false>(dst, src);
    }
}

template <bool LAZY_DICT_DECODE, bool LAZY_CONVERT>
Status ScalarColumnReader::_read_range_impl(const Range<uint64_t>& range, const Filter* filter,
                                            ColumnContentType content_type, ColumnPtr& dst) {
    if constexpr (LAZY_DICT_DECODE) {
        if (_tmp_code_column == nullptr) {
            _tmp_code_column = ColumnHelper::create_column(
                    TypeDescriptor::from_logical_type(ColumnDictFilterContext::kDictCodePrimitiveType), true);
        }
        _ori_column = dst;
        dst = _tmp_code_column;
        dst->reserve(range.span_size());
        SCOPED_RAW_TIMER(&_opts.stats->column_read_ns);
        return _reader->read_range(range, filter, content_type, dst.get());
    } else {
        if (!_converter->need_convert) {
            SCOPED_RAW_TIMER(&_opts.stats->column_read_ns);
            return _reader->read_range(range, filter, content_type, dst.get());
        } else {
            if (_tmp_intermediate_column == nullptr) {
                _tmp_intermediate_column = _converter->create_src_column();
            }
            _tmp_intermediate_column->reserve(range.span_size());
            {
                SCOPED_RAW_TIMER(&_opts.stats->column_read_ns);
                RETURN_IF_ERROR(_reader->read_range(range, filter, content_type, _tmp_intermediate_column.get()));
            }
            if constexpr (!LAZY_CONVERT) {
                {
                    SCOPED_RAW_TIMER(&_opts.stats->column_convert_ns);
                    RETURN_IF_ERROR(_converter->convert(_tmp_intermediate_column, dst.get()));
                }
                _tmp_intermediate_column->reset_column();
                return Status::OK();
            } else {
                _ori_column = dst;
                dst = _tmp_intermediate_column;
                return Status::OK();
            }
        }
    }
}

Status ScalarColumnReader::_dict_decode(ColumnPtr& dst, ColumnPtr& src) {
    Column* dict_values = ColumnHelper::get_data_column(dst.get());
    dict_values->reserve(src->size());

    // decode dict code to dict values.
    // note that in dict code, there could be null value.
    const ColumnPtr& dict_codes = src;
    auto* codes_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(dict_codes);
    auto* codes_column = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(codes_nullable_column->data_column());
    RETURN_IF_ERROR(_reader->get_dict_values(codes_column->get_data(), *codes_nullable_column, dict_values));
    DCHECK_EQ(dict_codes->size(), dict_values->size());
    if (dst->is_nullable()) {
        auto* nullable_values = down_cast<NullableColumn*>(dst.get());
        nullable_values->swap_null_column(*codes_nullable_column);
    }
    src->reset_column();
    return Status::OK();
}

template <bool LAZY_DICT_DECODE, bool LAZY_CONVERT>
Status ScalarColumnReader::_fill_dst_column_impl(ColumnPtr& dst, ColumnPtr& src) {
    if constexpr (LAZY_DICT_DECODE) {
        if (_dict_filter_ctx != nullptr && !_dict_filter_ctx->is_decode_needed) {
            dst->append_default(src->size());
            src->reset_column();
        } else {
            static_assert(!LAZY_CONVERT, "LAZY_DICT_DECODE && LAZY_CONVERT == true is not supported by now");
            RETURN_IF_ERROR(_dict_decode(dst, src));
        }
        src = _ori_column;
    } else {
        if constexpr (LAZY_CONVERT) {
            {
                SCOPED_RAW_TIMER(&_opts.stats->column_convert_ns);
                RETURN_IF_ERROR(_converter->convert(src, dst.get()));
            }
            src->reset_column();
            src = _ori_column;
        } else {
            dst->swap_column(*src);
        }
    }
    return Status::OK();
}

// LowCardColumnReader

Status LowCardColumnReader::read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) {
    DCHECK(get_column_parquet_field()->is_nullable ? dst->is_nullable() : true);
    ColumnContentType content_type = ColumnContentType::DICT_CODE;

    if (_dict_code == nullptr) {
        _dict_code = ColumnHelper::create_column(
                TypeDescriptor::from_logical_type(ColumnDictFilterContext::kDictCodePrimitiveType), true);
    }
    _ori_column = dst;
    dst = _dict_code;
    dst->reserve(range.span_size());

    {
        SCOPED_RAW_TIMER(&_opts.stats->column_read_ns);
        return _reader->read_range(range, filter, content_type, dst.get());
    }
}

bool LowCardColumnReader::try_to_use_dict_filter(ExprContext* ctx, bool is_decode_needed, const SlotId slotId,
                                                 const std::vector<std::string>& sub_field_path, const size_t& layer) {
    if (sub_field_path.size() != layer) {
        return false;
    }

    if (column_all_pages_dict_encoded()) {
        if (_dict_filter_ctx == nullptr) {
            _dict_filter_ctx = std::make_unique<ColumnDictFilterContext>();
            _dict_filter_ctx->is_decode_needed = is_decode_needed;
            _dict_filter_ctx->sub_field_path = sub_field_path;
            _dict_filter_ctx->slot_id = slotId;
        }
        _dict_filter_ctx->conjunct_ctxs.push_back(ctx);
        return true;
    } else {
        return false;
    }
}

Status LowCardColumnReader::fill_dst_column(ColumnPtr& dst, ColumnPtr& src) {
    if (!_code_convert_map.has_value()) {
        RETURN_IF_ERROR(_check_current_dict());
    }

    dst->resize(src->size());

    const ColumnPtr& dict_codes = src;
    auto* codes_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(dict_codes);
    auto* codes_column = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(codes_nullable_column->data_column());
    const NullData& null_data_ptr = codes_nullable_column->immutable_null_column_data();

    auto& codes = codes_column->get_data();
    if (codes_nullable_column->has_null()) {
        for (size_t i = 0; i < src->size(); i++) {
            // if null, we assign dict code 0
            // null = 0, mask = 0xffffffff
            // null = 1, mask = 0x00000000
            uint32_t mask = ~(static_cast<uint32_t>(-null_data_ptr[i]));
            codes[i] = mask & codes[i];
        }
    }

    auto* dst_data_column = down_cast<LowCardDictColumn*>(ColumnHelper::get_data_column(dst.get()));
    SIMDGather::gather(dst_data_column->get_data().data(), _code_convert_map->data(), codes.data(),
                       DICT_DECODE_MAX_SIZE, src->size());

    if (dst->is_nullable()) {
        auto* nullable_dst = down_cast<NullableColumn*>(dst.get());
        nullable_dst->swap_null_column(*codes_nullable_column);
    }

    src->reset_column();
    src = _ori_column;

    return Status::OK();
}

Status LowCardColumnReader::_check_current_dict() {
    std::vector<int16_t> code_convert_map;

    // create dict value chunk for evaluation.
    ColumnPtr dict_value_column = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true);
    RETURN_IF_ERROR(_reader->get_dict_values(dict_value_column.get()));

    size_t dict_size = dict_value_column->size();

    code_convert_map.resize(dict_size + 2);
    std::fill(code_convert_map.begin(), code_convert_map.end(), 0);
    auto* local_to_global = code_convert_map.data();

    auto viewer = ColumnViewer<TYPE_VARCHAR>(dict_value_column);

    for (int i = 0; i < dict_size; ++i) {
        auto slice = viewer.value(i);
        auto res = _dict->find(slice);
        if (res == _dict->end()) {
            // error message format used to extract info, carefully
            return Status::GlobalDictNotMatch(fmt::format("SlotId: {}, FileName: {} , file doesn't match global dict. ",
                                                          _slot_id, _opts.file->filename()));
        } else {
            local_to_global[i] = res->second;
        }
    }

#ifdef DEBUG
    std::stringstream ss;
    ss << "dict mapping: ";
    for (int i = 0; i < dict_size; ++i) {
        ss << code_convert_map[i] << " ";
    }
    LOG(INFO) << ss.str();
#endif

    _code_convert_map = std::move(code_convert_map);

    return Status::OK();
}

// LowRowsColumnReader

Status LowRowsColumnReader::read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) {
    DCHECK(get_column_parquet_field()->is_nullable ? dst->is_nullable() : true);
    ColumnContentType content_type = ColumnContentType::VALUE;

    if (_tmp_column == nullptr) {
        _tmp_column = ColumnHelper::create_column(TYPE_VARCHAR_DESC, true);
    }
    _ori_column = dst;
    dst = _tmp_column;
    dst->reserve(range.span_size());

    {
        SCOPED_RAW_TIMER(&_opts.stats->column_read_ns);
        return _reader->read_range(range, filter, content_type, dst.get());
    }
}

Status LowRowsColumnReader::fill_dst_column(ColumnPtr& dst, ColumnPtr& src) {
    dst->resize(src->size());

    const ColumnPtr& readed_column = src;
    auto* nullable_string_column = ColumnHelper::as_raw_column<NullableColumn>(readed_column);
    auto* binary_column = ColumnHelper::as_raw_column<BinaryColumn>(nullable_string_column->data_column());
    auto* dst_data_column = down_cast<LowCardDictColumn*>(ColumnHelper::get_data_column(dst.get()));
    for (size_t i = 0; i < src->size(); i++) {
        if (src->is_null(i)) {
            dst_data_column->get_data()[i] = 1;
            continue;
        }
        const auto& slice = binary_column->get_slice(i);
        auto res = _dict->find(slice);
        if (res == _dict->end()) {
            // error message format used to extract info, carefully
            return Status::GlobalDictNotMatch(fmt::format("SlotId: {}, FileName: {} , file doesn't match global dict. ",
                                                          _slot_id, _opts.file->filename()));
        } else {
            dst_data_column->get_data()[i] = res->second;
        }
    }

    if (dst->is_nullable()) {
        auto* nullable_dst = down_cast<NullableColumn*>(dst.get());
        nullable_dst->swap_null_column(*nullable_string_column);
    }

    src->reset_column();
    src = _ori_column;

    return Status::OK();
}
} // namespace starrocks::parquet