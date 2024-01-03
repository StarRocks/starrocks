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

#include "formats/parquet/page_index_reader.h"

#include "column/column_helper.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "formats/parquet/encoding_plain.h"
#include "fs/fs.h"
#include "gen_cpp/parquet_types.h"
#include "simd/simd.h"
#include "util/thrift_util.h"

namespace starrocks::parquet {

void ColumnOffsetIndexCtx::collect_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges,
                                            int64_t* end_offset, bool active) {
    for (size_t i = 0; i < page_selected.size(); i++) {
        if (page_selected[i]) {
            auto r = io::SharedBufferedInputStream::IORange(
                    offset_index.page_locations[i].offset, offset_index.page_locations[i].compressed_page_size, active);
            ranges->emplace_back(r);
            *end_offset = std::max(*end_offset, r.offset + r.size);
        }
    }
}

StatusOr<bool> PageIndexReader::generate_read_range(SparseRange<uint64_t>& sparse_range) {
    // _min_max_conjunct_ctxs to map<slotId, std::vector<ExprContext*>>
    bool page_filtered_flag = false;
    std::unordered_map<SlotId, std::vector<ExprContext*>> slot_id_to_ctx_map;
    for (auto* ctx : _min_max_conjunct_ctxs) {
        std::vector<SlotId> slot_ids;
        ctx->root()->get_slot_ids(&slot_ids);
        if (slot_ids.size() != 1) {
            continue;
        }
        if (_column_readers.find(slot_ids[0]) == _column_readers.end()) {
            continue;
        }

        if (slot_id_to_ctx_map.find(slot_ids[0]) == slot_id_to_ctx_map.end()) {
            slot_id_to_ctx_map.insert({slot_ids[0], std::vector<ExprContext*>({ctx})});
        } else {
            slot_id_to_ctx_map[slot_ids[0]].emplace_back(ctx);
        }
    }

    for (int idx : _group_reader->_active_column_indices) {
        const auto& column = _group_reader->_param.read_cols[idx];
        // complex type will be supported later
        if (column.col_type_in_chunk.is_complex_type()) {
            continue;
        }
        SlotId slotId = column.slot_id;
        // no min_max conjunct
        if (slot_id_to_ctx_map.find(slotId) == slot_id_to_ctx_map.end()) {
            continue;
        }

        // no page index
        const tparquet::ColumnChunk* chunk_meta = _column_readers.at(slotId)->get_chunk_metadata();
        if (!chunk_meta->__isset.column_index_offset || !chunk_meta->__isset.offset_index_offset ||
            !chunk_meta->__isset.meta_data) {
            continue;
        }

        // get column index
        int64_t column_index_offset = chunk_meta->column_index_offset;
        uint32_t column_index_length = chunk_meta->column_index_length;

        std::vector<uint8_t> page_index_data;
        page_index_data.reserve(column_index_length);
        RETURN_IF_ERROR(_file->read_at_fully(column_index_offset, page_index_data.data(), column_index_length));

        tparquet::ColumnIndex column_index;
        RETURN_IF_ERROR(deserialize_thrift_msg(page_index_data.data(), &column_index_length, TProtocolType::COMPACT,
                                               &column_index));
        auto min_chunk = std::make_unique<Chunk>();
        ColumnPtr min_column = ColumnHelper::create_column(column.col_type_in_chunk, true);
        min_chunk->append_column(min_column, slotId);
        auto max_chunk = std::make_unique<Chunk>();
        ColumnPtr max_column = ColumnHelper::create_column(column.col_type_in_chunk, true);
        max_chunk->append_column(max_column, slotId);
        // deal with min_values
        Status st;
        st = _decode_value_into_column(min_column, column_index.min_values, column.col_type_in_chunk,
                                       _column_readers.at(slotId)->get_column_parquet_field(),
                                       _group_reader->_param.timezone);
        if (!st.ok()) {
            continue;
        }
        // deal with max_values
        st = _decode_value_into_column(max_column, column_index.max_values, column.col_type_in_chunk,
                                       _column_readers.at(slotId)->get_column_parquet_field(),
                                       _group_reader->_param.timezone);
        if (!st.ok()) {
            continue;
        }
        size_t page_num = column_index.min_values.size();
        Filter page_filter(page_num, 1);

        // both min and max value are filtered, the page is filtered.
        // for example pages {100, 200}, {200, 400}, {400, 600}, {500, 800}, {800, 1000}
        // conjuncts is >= 300, <= 700
        // for >= 300, min_selected is {0, 0, 1, 1, 1}, max_selected is {0, 1, 1, 1, 1}
        // min_selected or max_selected is {0, 1, 1, 1, 1}
        // so the page_filter will be {0, 1, 1, 1, 1}
        // for <= 700, min_selected is {1, 1, 1, 1, 0}, max_selected is {1, 1, 1, 0, 0}
        // min_selected or max_selected is {1, 1, 1, 1, 0}
        // so the page_filter will be {0, 1, 1, 1, 0}
        for (auto* ctx : slot_id_to_ctx_map.at(slotId)) {
            ASSIGN_OR_RETURN(ColumnPtr min_selected, ctx->evaluate(min_chunk.get()));
            ASSIGN_OR_RETURN(ColumnPtr max_selected, ctx->evaluate(max_chunk.get()));
            auto unpack_min_selected = ColumnHelper::unpack_and_duplicate_const_column(page_num, min_selected);
            auto unpack_max_selected = ColumnHelper::unpack_and_duplicate_const_column(page_num, max_selected);
            Filter min_filter = ColumnHelper::merge_nullable_filter(unpack_min_selected.get());
            Filter max_filter = ColumnHelper::merge_nullable_filter(unpack_max_selected.get());
            ColumnHelper::or_two_filters(&min_filter, max_filter.data());
            ColumnHelper::merge_two_filters(&page_filter, min_filter.data());
        }

        if (!SIMD::contain_zero(page_filter)) {
            continue;
        }

        page_filtered_flag = true;

        ASSIGN_OR_RETURN(const tparquet::OffsetIndex* offset_index,
                         _column_readers.at(slotId)->get_offset_index(_group_reader->_row_group_first_row));

        SparseRange<uint64_t> column_sparse_range;
        for (int i = 0; i < page_num; i++) {
            if (page_filter[i]) {
                int64_t first_row =
                        offset_index->page_locations[i].first_row_index + _group_reader->_row_group_first_row;
                int64_t end_row = first_row;
                if (i != page_num - 1) {
                    end_row = offset_index->page_locations[i + 1].first_row_index + _group_reader->_row_group_first_row;
                } else {
                    end_row = _group_reader->_row_group_first_row + _row_group_metadata->num_rows;
                }
                column_sparse_range.add(Range<uint64_t>(first_row, end_row));
            }
        }
        sparse_range = sparse_range.intersection(column_sparse_range);
    }

    if (sparse_range.empty()) {
        _group_reader->_is_group_filtered = true;
    }

    return page_filtered_flag;
}

Status PageIndexReader::_decode_value_into_column(ColumnPtr column, const std::vector<string>& values,
                                                  const TypeDescriptor& type, const ParquetField* field,
                                                  const std::string& timezone) {
    std::unique_ptr<ColumnConverter> converter;
    RETURN_IF_ERROR(ColumnConverterFactory::create_converter(*field, type, timezone, &converter));
    bool ret = true;
    switch (field->physical_type) {
    case tparquet::Type::type::INT32: {
        int32_t decode_value = 0;
        if (!converter->need_convert) {
            for (size_t i = 0; i < values.size(); i++) {
                RETURN_IF_ERROR(PlainDecoder<int32_t>::decode(values[i], &decode_value));
                ret &= (column->append_numbers(&decode_value, sizeof(int32_t)) > 0);
            }
        } else {
            ColumnPtr src_column = converter->create_src_column();
            for (size_t i = 0; i < values.size(); i++) {
                RETURN_IF_ERROR(PlainDecoder<int32_t>::decode(values[i], &decode_value));
                ret &= (src_column->append_numbers(&decode_value, sizeof(int32_t)) > 0);
            }
            RETURN_IF_ERROR(converter->convert(src_column, column.get()));
        }
        break;
    }
    case tparquet::Type::type::INT64: {
        int64_t decode_value = 0;
        if (!converter->need_convert) {
            for (size_t i = 0; i < values.size(); i++) {
                RETURN_IF_ERROR(PlainDecoder<int64_t>::decode(values[i], &decode_value));
                ret &= (column->append_numbers(&decode_value, sizeof(int64_t)) > 0);
            }
        } else {
            ColumnPtr src_column = converter->create_src_column();
            for (size_t i = 0; i < values.size(); i++) {
                RETURN_IF_ERROR(PlainDecoder<int64_t>::decode(values[i], &decode_value));
                ret &= (src_column->append_numbers(&decode_value, sizeof(int64_t)) > 0);
            }
            RETURN_IF_ERROR(converter->convert(src_column, column.get()));
        }
        break;
    }
    case tparquet::Type::type::BYTE_ARRAY:
    // todo: FLBA need more test
    case tparquet::Type::type::FIXED_LEN_BYTE_ARRAY: {
        Slice decode_value;
        if (!converter->need_convert) {
            for (size_t i = 0; i < values.size(); i++) {
                RETURN_IF_ERROR(PlainDecoder<Slice>::decode(values[i], &decode_value));
                ret &= column->append_strings(std::vector<Slice>{decode_value});
            }
        } else {
            ColumnPtr src_column = converter->create_src_column();
            for (size_t i = 0; i < values.size(); i++) {
                RETURN_IF_ERROR(PlainDecoder<Slice>::decode(values[i], &decode_value));
                ret &= src_column->append_strings(std::vector<Slice>{decode_value});
            }
            RETURN_IF_ERROR(converter->convert(src_column, column.get()));
        }
        break;
    }
    default:
        return Status::InternalError("Not Supported min/max value type");
    }

    if (UNLIKELY(!ret)) {
        return Status::InternalError("Decode min-max column failed");
    }
    return Status::OK();
}

void PageIndexReader::select_column_offset_index() {
    for (const auto& pair : _column_readers) {
        pair.second->select_offset_index(_group_reader->_range, _group_reader->_row_group_first_row);
    }
}
} // namespace starrocks::parquet