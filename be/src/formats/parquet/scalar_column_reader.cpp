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

#include "formats/parquet/stored_column_reader_with_index.h"
#include "gutil/casts.h"
#include "io/shared_buffered_input_stream.h"
#include "simd/simd.h"
#include "utils.h"

namespace starrocks::parquet {

Status ScalarColumnReader::read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) {
    DCHECK(_field->is_nullable ? dst->is_nullable() : true);
    _need_lazy_decode =
            _dict_filter_ctx != nullptr || (_can_lazy_decode && filter != nullptr &&
                                            SIMD::count_nonzero(*filter) * 1.0 / filter->size() < FILTER_RATIO);
    ColumnContentType content_type = !_need_lazy_decode ? ColumnContentType::VALUE : ColumnContentType::DICT_CODE;
    if (_need_lazy_decode) {
        if (_dict_code == nullptr) {
            _dict_code = ColumnHelper::create_column(
                    TypeDescriptor::from_logical_type(ColumnDictFilterContext::kDictCodePrimitiveType), true);
        }
        dst = _dict_code;
        dst->reserve(range.span_size());
    }
    if (!_converter->need_convert) {
        SCOPED_RAW_TIMER(&_opts.stats->column_read_ns);
        return _reader->read_range(range, filter, content_type, dst.get());
    } else {
        auto column = _converter->create_src_column();
        {
            SCOPED_RAW_TIMER(&_opts.stats->column_read_ns);
            RETURN_IF_ERROR(_reader->read_range(range, filter, content_type, column.get()));
        }
        SCOPED_RAW_TIMER(&_opts.stats->column_convert_ns);
        return _converter->convert(column, dst.get());
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

    if (_column_all_pages_dict_encoded()) {
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

Status ScalarColumnReader::fill_dst_column(ColumnPtr& dst, const ColumnPtr& src) {
    if (!_need_lazy_decode) {
        dst->swap_column(*src);
    } else {
        if (_dict_filter_ctx == nullptr || _dict_filter_ctx->is_decode_needed) {
            ColumnPtr& dict_values = dst;
            dict_values->reserve(src->size());

            // decode dict code to dict values.
            // note that in dict code, there could be null value.
            const ColumnPtr& dict_codes = src;
            auto* codes_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(dict_codes);
            auto* codes_column =
                    ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(codes_nullable_column->data_column());
            RETURN_IF_ERROR(
                    _reader->get_dict_values(codes_column->get_data(), *codes_nullable_column, dict_values.get()));
            DCHECK_EQ(dict_codes->size(), dict_values->size());
            if (dict_values->is_nullable()) {
                auto* nullable_codes = down_cast<NullableColumn*>(dict_codes.get());
                auto* nullable_values = down_cast<NullableColumn*>(dict_values.get());
                nullable_values->null_column_data().swap(nullable_codes->null_column_data());
                nullable_values->set_has_null(nullable_codes->has_null());
            }
        } else {
            dst->append_default(src->size());
        }

        src->reset_column();
    }
    return Status::OK();
}

bool ScalarColumnReader::_column_all_pages_dict_encoded() {
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
    const tparquet::ColumnMetaData& column_metadata =
            _opts.row_group_meta->columns[_field->physical_column_index].meta_data;
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

void ScalarColumnReader::collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges,
                                                 int64_t* end_offset, ColumnIOType type, bool active) {
    const auto& column = _opts.row_group_meta->columns[_field->physical_column_index];
    if (type == ColumnIOType::PAGES) {
        const tparquet::ColumnMetaData& column_metadata = column.meta_data;
        if (_offset_index_ctx != nullptr) {
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

void ScalarColumnReader::select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) {
    if (_offset_index_ctx == nullptr) {
        if (!_chunk_metadata->__isset.offset_index_offset) {
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
    const tparquet::ColumnMetaData& column_metadata =
            _opts.row_group_meta->columns[_field->physical_column_index].meta_data;
    bool has_dict_page = column_metadata.__isset.dictionary_page_offset;
    // be compatible with PARQUET-1850
    has_dict_page |= _offset_index_ctx->check_dictionary_page(column_metadata.data_page_offset);
    _reader = std::make_unique<StoredColumnReaderWithIndex>(std::move(_reader), _offset_index_ctx.get(), has_dict_page);
}

} // namespace starrocks::parquet