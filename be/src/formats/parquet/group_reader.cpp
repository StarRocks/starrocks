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

#include "formats/parquet/group_reader.h"

#include "column/column_helper.h"
#include "common/status.h"
#include "exec/exec_node.h"
#include "exec/hdfs_scanner.h"
#include "exprs/expr.h"
#include "gutil/strings/substitute.h"
#include "runtime/types.h"
#include "simd/simd.h"
#include "storage/chunk_helper.h"
#include "utils.h"

namespace starrocks::parquet {

constexpr static const LogicalType kDictCodePrimitiveType = TYPE_INT;
constexpr static const LogicalType kDictCodeFieldType = TYPE_INT;

GroupReader::GroupReader(GroupReaderParam& param, int row_group_number) : _param(param) {
    _row_group_metadata =
            std::make_shared<tparquet::RowGroup>(param.file_metadata->t_metadata().row_groups[row_group_number]);
}

Status GroupReader::init() {
    // the calling order matters, do not change unless you know why.
    RETURN_IF_ERROR(_init_column_readers());
    _dict_filter_ctx.init(_param.read_cols.size());
    _process_columns_and_conjunct_ctxs();
    RETURN_IF_ERROR(_dict_filter_ctx.rewrite_conjunct_ctxs_to_predicates(_param, _column_readers, &_obj_pool,
                                                                         &_is_group_filtered));
    _init_read_chunk();
    return Status::OK();
}

Status GroupReader::get_next(ChunkPtr* chunk, size_t* row_count) {
    if (_is_group_filtered) {
        *row_count = 0;
        return Status::EndOfFile("");
    }

    _read_chunk->reset();
    size_t rows_to_read = *row_count;
    size_t rows_read = 0;
    bool has_more_filter = !_left_conjunct_ctxs.empty();

    ChunkPtr active_chunk = _create_read_chunk(_active_column_indices);
    {
        SCOPED_RAW_TIMER(&_param.stats->group_chunk_read_ns);
        // read data into active_chunk
        ASSIGN_OR_RETURN(rows_read, _read(_active_column_indices, rows_to_read, &active_chunk));
        _param.stats->raw_rows_read += rows_read;
    }

    bool has_filter = false;
    int chunk_size = -1;
    Filter chunk_filter(rows_read, 1);
    if (active_chunk->num_rows() != rows_read) {
        std::cout << active_chunk->num_rows() << "!=" << rows_read;
    }
    DCHECK_EQ(active_chunk->num_rows(), rows_read);

    // dict filter chunk
    {
        SCOPED_RAW_TIMER(&_param.stats->expr_filter_ns);
        SCOPED_RAW_TIMER(&_param.stats->group_dict_filter_ns);
        has_filter = _dict_filter_ctx.filter_chunk(&active_chunk, &chunk_filter);
    }

    // other filter that not dict
    if (has_more_filter) {
        SCOPED_RAW_TIMER(&_param.stats->expr_filter_ns);
        ASSIGN_OR_RETURN(chunk_size,
                         ExecNode::eval_conjuncts_into_filter(_left_conjunct_ctxs, active_chunk.get(), &chunk_filter));
        has_filter = true;
    }

    if (has_filter) {
        size_t hit_count = chunk_size >= 0 ? chunk_size : SIMD::count_nonzero(chunk_filter.data(), rows_read);
        if (hit_count == 0) {
            active_chunk->set_num_rows(0);
        } else if (hit_count != rows_read) {
            active_chunk->filter_range(chunk_filter, 0, rows_read);
        }
        active_chunk->check_or_die();
    }

    size_t eager_rows = active_chunk->num_rows();
    if (!_lazy_column_indices.empty()) {
        ChunkPtr lazy_chunk = _create_read_chunk(_lazy_column_indices);
        if (eager_rows == 0) {
            std::cout << "skip all active chunk: " << rows_read << std::endl;
            _param.stats->skip_read_rows += rows_read;
            _previous_rows_to_skip += rows_read;
        } else {
            RETURN_IF_ERROR(_skip(_lazy_column_indices, _previous_rows_to_skip));
            _previous_rows_to_skip = 0;
            for (size_t index = 0; index < chunk_filter.size();) {
                bool is_selected = chunk_filter[index++];
                size_t run = 1;
                while (index < chunk_filter.size() && chunk_filter[index] == is_selected) {
                    index++;
                    run++;
                }
                if (is_selected) {
                    std::cout << "read: " << run << std::endl;
                    ASSIGN_OR_RETURN(size_t temp_rows_read, _read(_lazy_column_indices, run, &lazy_chunk));
                    DCHECK(temp_rows_read == run);
                } else {
                    std::cout << "skip: " << run << std::endl;
                    _param.stats->skip_read_rows += run;
                    RETURN_IF_ERROR(_skip(_lazy_column_indices, run));
                }
            }
            if (lazy_chunk->num_rows() != eager_rows) {
                return Status::InternalError(strings::Substitute("Unmatched row count, active_rows=$0, lazy_rows=$1",
                                                                 eager_rows, lazy_chunk->num_rows()));
            }
        }
        active_chunk->merge(std::move(*lazy_chunk));
    }

    _read_chunk->swap_chunk(*active_chunk);
    Status st = Status::OK();
    if (rows_read < rows_to_read) {
        st = Status::EndOfFile("");
    }
    *row_count = rows_read;

    SCOPED_RAW_TIMER(&_param.stats->group_dict_decode_ns);
    // convert from _read_chunk to chunk.
    RETURN_IF_ERROR(_dict_filter_ctx.decode_chunk(_param, _column_readers, _read_chunk, chunk));
    return st;
}

void GroupReader::close() {
    if (_param.sb_stream) {
        _param.sb_stream->release_to_offset(_end_offset);
    }
    _column_readers.clear();
}

Status GroupReader::_init_column_readers() {
    // ColumnReaderOptions is used by all column readers in one row group
    ColumnReaderOptions& opts = _column_reader_opts;
    opts.timezone = _param.timezone;
    opts.case_sensitive = _param.case_sensitive;
    opts.chunk_size = _param.chunk_size;
    opts.stats = _param.stats;
    opts.file = _param.file;
    opts.row_group_meta = _row_group_metadata.get();
    opts.sb_stream = _param.sb_stream;
    for (const auto& column : _param.read_cols) {
        RETURN_IF_ERROR(_create_column_reader(column));
    }
    return Status::OK();
}

Status GroupReader::_create_column_reader(const GroupReaderParam::Column& column) {
    std::unique_ptr<ColumnReader> column_reader = nullptr;
    const auto* schema_node = _param.file_metadata->schema().get_stored_column_by_idx(column.col_idx_in_parquet);
    {
        SCOPED_RAW_TIMER(&_param.stats->column_reader_init_ns);
        if (column.t_iceberg_schema_field == nullptr) {
            RETURN_IF_ERROR(
                    ColumnReader::create(_column_reader_opts, schema_node, column.col_type_in_chunk, &column_reader));
        } else {
            RETURN_IF_ERROR(ColumnReader::create(_column_reader_opts, schema_node, column.col_type_in_chunk,
                                                 column.t_iceberg_schema_field, &column_reader));
        }

        if (column.col_type_in_chunk.is_complex_type()) {
            // For complex type columns, we need parse def & rep levels.
            // For OptionalColumnReader, by default, we will not parse it's def level for performance. But if
            // column is complex type, we have to parse def level to calculate nullbility.
            column_reader->set_need_parse_levels(true);
        }
    }
    _column_readers[column.slot_id] = std::move(column_reader);
    return Status::OK();
}

void GroupReader::_process_columns_and_conjunct_ctxs() {
    const auto& conjunct_ctxs_by_slot = _param.conjunct_ctxs_by_slot;
    const auto& slots = _param.tuple_desc->slots();
    int read_col_idx = 0;

    for (auto& column : _param.read_cols) {
        int chunk_index = column.col_idx_in_chunk;
        SlotId slot_id = column.slot_id;
        const tparquet::ColumnMetaData& column_metadata =
                _row_group_metadata->columns[column.col_idx_in_parquet].meta_data;
        if (_can_use_as_dict_filter_column(slots[chunk_index], conjunct_ctxs_by_slot, column_metadata)) {
            _dict_filter_ctx.use_as_dict_filter_column(read_col_idx, slot_id, conjunct_ctxs_by_slot.at(slot_id));
            _active_column_indices.emplace_back(read_col_idx);
        } else {
            bool has_conjunct = false;
            if (conjunct_ctxs_by_slot.find(slot_id) != conjunct_ctxs_by_slot.end()) {
                for (ExprContext* ctx : conjunct_ctxs_by_slot.at(slot_id)) {
                    _left_conjunct_ctxs.emplace_back(ctx);
                }
                has_conjunct = true;
            }
            if (config::parquet_late_materialization_enable && !has_conjunct) {
                _lazy_column_indices.emplace_back(read_col_idx);
            } else {
                _active_column_indices.emplace_back(read_col_idx);
            }
        }
        ++read_col_idx;
    }
    if (_active_column_indices.empty()) {
        _active_column_indices.swap(_lazy_column_indices);
    }
}

ChunkPtr GroupReader::_create_read_chunk(const std::vector<int>& column_indices) {
    auto chunk = std::make_shared<Chunk>();
    chunk->columns().reserve(column_indices.size());
    for (auto col_idx : column_indices) {
        SlotId slot_id = _param.read_cols[col_idx].slot_id;
        ColumnPtr& column = _read_chunk->get_column_by_slot_id(slot_id);
        chunk->append_column(column, slot_id);
    }
    return chunk;
}

void GroupReader::collect_io_ranges(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset) {
    int64_t end = 0;
    for (const auto& column : _param.read_cols) {
        auto schema_node = _param.file_metadata->schema().get_stored_column_by_idx(column.col_idx_in_parquet);
        _collect_field_io_range(*schema_node, ranges, &end);
    }
    *end_offset = end;
}

void GroupReader::_collect_field_io_range(const ParquetField& field,
                                          std::vector<io::SharedBufferedInputStream::IORange>* ranges,
                                          int64_t* end_offset) {
    // 1. We collect column io ranges for each row group to make up the shared buffer, so we get column
    // metadata (such as page offset and compressed_size) from _row_group_meta directly rather than file_metadata.
    // 2. For map or struct columns, the physical_column_index indicates the real column index in rows group meta,
    // and it may not be equal to col_idx_in_chunk.
    // 3. For array type, the physical_column_index is 0, we need to iterate the children and
    // collect their io ranges.
    if (field.type.type == LogicalType::TYPE_ARRAY || field.type.type == LogicalType::TYPE_STRUCT) {
        for (auto& child : field.children) {
            _collect_field_io_range(child, ranges, end_offset);
        }
    } else if (field.type.type == LogicalType::TYPE_MAP) {
        // ParquetFiled Map -> Map<Struct<key,value>>
        DCHECK(field.children[0].type.type == TYPE_STRUCT);
        for (auto& child : field.children[0].children) {
            _collect_field_io_range(child, ranges, end_offset);
        }
    } else {
        auto& column = _row_group_metadata->columns[field.physical_column_index].meta_data;
        int64_t offset = 0;
        if (column.__isset.dictionary_page_offset) {
            offset = column.dictionary_page_offset;
        } else {
            offset = column.data_page_offset;
        }
        int64_t size = column.total_compressed_size;
        auto r = io::SharedBufferedInputStream::IORange{.offset = offset, .size = size};
        ranges->emplace_back(r);
        *end_offset = std::max(*end_offset, offset + size);
    }
}

bool GroupReader::_can_use_as_dict_filter_column(const SlotDescriptor* slot,
                                                 const SlotIdExprContextsMap& conjunct_ctxs_by_slot,
                                                 const tparquet::ColumnMetaData& column_metadata) {
    // only varchar and char type support dict filter
    if (!slot->type().is_string_type()) {
        return false;
    }

    // check slot has conjuncts
    SlotId slot_id = slot->id();
    if (conjunct_ctxs_by_slot.find(slot_id) == conjunct_ctxs_by_slot.end()) {
        return false;
    }

    // check is null or is not null
    // is null or is not null conjunct should not eval dict value, this will always return empty set
    for (ExprContext* ctx : conjunct_ctxs_by_slot.at(slot_id)) {
        const Expr* root_expr = ctx->root();
        if (root_expr->node_type() == TExprNodeType::FUNCTION_CALL) {
            std::string is_null_str;
            if (root_expr->is_null_scalar_function(is_null_str)) {
                return false;
            }
        }
    }

    // check all data pages dict encoded
    if (!_column_all_pages_dict_encoded(column_metadata)) {
        return false;
    }

    return true;
}

bool GroupReader::_column_all_pages_dict_encoded(const tparquet::ColumnMetaData& column_metadata) {
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

void GroupReader::_init_read_chunk() {
    const auto &slots = _param.tuple_desc->slots();
    std::vector<SlotDescriptor *> read_slots;
    for (const auto &column: _param.read_cols) {
        int chunk_index = column.col_idx_in_chunk;
        read_slots.emplace_back(slots[chunk_index]);
    }
    size_t chunk_size = _param.chunk_size;
    _read_chunk = ChunkHelper::new_chunk(read_slots, chunk_size);
    _dict_filter_ctx.init_chunk(_param, &_read_chunk);
}

Status GroupReader::_skip(const std::vector<int> &read_columns, size_t skip_num_rows) {
    for (int col_idx: read_columns) {
        auto &column = _param.read_cols[col_idx];
        SlotId slot_id = column.slot_id;
        RETURN_IF_ERROR(_column_readers[slot_id]->skip(skip_num_rows));
    }
    return Status::OK();
}

StatusOr<size_t> GroupReader::_read(const std::vector<int> &read_columns, size_t rows_to_read, ChunkPtr *chunk) {
    if (read_columns.empty()) {
        DCHECK(false);
    }

    size_t rows_read = 0;
    for (int col_idx: read_columns) {
        auto &column = _param.read_cols[col_idx];
        ColumnContentType content_type = _dict_filter_ctx.column_content_type(col_idx);
        SlotId slot_id = column.slot_id;
        ASSIGN_OR_RETURN(size_t temp_rows_read, _column_readers[slot_id]->next_batch(rows_to_read, content_type,
                                                                                         (*chunk)->get_column_by_slot_id(
                                                                                                 slot_id).get()));
        if (rows_read == 0) {
            rows_read = temp_rows_read;
        }
        DCHECK(rows_read == temp_rows_read);
    }
    return rows_read;
}
//
//Status GroupReader::_lazy_skip_rows(const std::vector<int>& read_columns, const ChunkPtr& chunk, size_t chunk_size) {
//    auto& ctx = _column_reader_opts.context;
//    if (ctx->rows_to_skip == 0) {
//        return Status::OK();
//    }
//
//    size_t rows_to_skip = ctx->rows_to_skip;
//    Filter empty_filter(1, 0);
//    ctx->filter = &empty_filter;
//    for (int col_idx : read_columns) {
//        auto& column = _param.read_cols[col_idx];
//        ColumnContentType content_type = _dict_filter_ctx.column_content_type(col_idx);
//        SlotId slot_id = column.slot_id;
//        _column_reader_opts.context->next_row = 0;
//
//        ctx->rows_to_skip = rows_to_skip;
//        while (ctx->rows_to_skip > 0) {
//            size_t to_read = std::min(ctx->rows_to_skip, chunk_size);
//            auto temp_column = chunk->get_column_by_slot_id(slot_id)->clone_empty();
//            Status status = _column_readers[slot_id]->next_batch(&to_read, content_type, temp_column.get());
//            if (!status.ok()) {
//                return status;
//            }
//        }
//    }
//
//    ctx->rows_to_skip = 0;
//    return Status::OK();
//}

void GroupReader::DictFilterContext::init(size_t column_number) {
    _is_dict_filter_column.assign(column_number, false);
}

void GroupReader::DictFilterContext::use_as_dict_filter_column(int col_idx, SlotId slot_id,
                                                               const std::vector<ExprContext*>& conjunct_ctxs) {
    _is_dict_filter_column[col_idx] = true;
    _dict_column_indices.emplace_back(col_idx);
    _conjunct_ctxs_by_slot[slot_id] = conjunct_ctxs;
}

Status GroupReader::DictFilterContext::rewrite_conjunct_ctxs_to_predicates(
        const GroupReaderParam& param, std::unordered_map<SlotId, std::unique_ptr<ColumnReader>>& column_readers,
        ObjectPool* obj_pool, bool* is_group_filtered) {
    for (int col_idx : _dict_column_indices) {
        const auto& column = param.read_cols[col_idx];
        SlotId slot_id = column.slot_id;
        ChunkPtr dict_value_chunk = std::make_shared<Chunk>();
        std::shared_ptr<BinaryColumn> dict_value_column = BinaryColumn::create();
        dict_value_chunk->append_column(dict_value_column, slot_id);

        RETURN_IF_ERROR(column_readers[slot_id]->get_dict_values(dict_value_column.get()));
        RETURN_IF_ERROR(ExecNode::eval_conjuncts(_conjunct_ctxs_by_slot[slot_id], dict_value_chunk.get()));
        dict_value_chunk->check_or_die();

        // dict column is empty after conjunct eval, file group can be skipped
        if (dict_value_chunk->num_rows() == 0) {
            *is_group_filtered = true;
            return Status::OK();
        }

        // get dict codes
        std::vector<int32_t> dict_codes;
        RETURN_IF_ERROR(column_readers[slot_id]->get_dict_codes(dict_value_column->get_data(), &dict_codes));

        // eq predicate is faster than in predicate
        // TODO: improve not eq and not in
        if (dict_codes.size() == 1) {
            _predicates[slot_id] =
                    new_column_eq_predicate(get_type_info(kDictCodeFieldType), slot_id, std::to_string(dict_codes[0]));
        } else {
            std::vector<std::string> str_codes;
            str_codes.reserve(dict_codes.size());
            for (int code : dict_codes) {
                str_codes.emplace_back(std::to_string(code));
            }
            _predicates[slot_id] = new_column_in_predicate(get_type_info(kDictCodeFieldType), slot_id, str_codes);
        }
        obj_pool->add(_predicates[slot_id]);
    }

    return Status::OK();
}

void GroupReader::DictFilterContext::init_chunk(const GroupReaderParam& param, ChunkPtr* chunk) {
    // replace dict filter column
    size_t chunk_size = param.chunk_size;
    for (int col_idx : _dict_column_indices) {
        const auto& column = param.read_cols[col_idx];
        SlotId slot_id = column.slot_id;
        auto dict_code_column =
                ColumnHelper::create_column(TypeDescriptor::from_logical_type(kDictCodePrimitiveType), true);
        dict_code_column->reserve(chunk_size);
        (*chunk)->update_column(dict_code_column, slot_id);
    }
}

bool GroupReader::DictFilterContext::filter_chunk(ChunkPtr* chunk, Filter* filter) {
    if (_predicates.empty()) return false;

    auto iter = _predicates.begin();
    SlotId slot_id = iter->first;
    auto pred = iter->second;
    pred->evaluate((*chunk)->get_column_by_slot_id(slot_id).get(), filter->data());
    while (++iter != _predicates.end()) {
        slot_id = iter->first;
        pred = iter->second;
        pred->evaluate_and((*chunk)->get_column_by_slot_id(slot_id).get(), filter->data());
    }
    return true;
}

Status GroupReader::DictFilterContext::decode_chunk(
        const GroupReaderParam& param, std::unordered_map<SlotId, std::unique_ptr<ColumnReader>>& column_readers,
        const ChunkPtr& read_chunk, ChunkPtr* chunk) {
    const auto& slots = param.tuple_desc->slots();

    for (int col_idx = 0; col_idx < _is_dict_filter_column.size(); col_idx++) {
        const auto& column = param.read_cols[col_idx];
        SlotId slot_id = column.slot_id;

        if (_is_dict_filter_column[col_idx]) {
            int chunk_index = column.col_idx_in_chunk;

            ColumnPtr& dict_codes = read_chunk->get_column_by_slot_id(slot_id);
            ColumnPtr& dict_values = (*chunk)->get_column_by_slot_id(slot_id);
            dict_values->resize(0);

            auto* codes_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(dict_codes);
            auto* codes_column =
                    ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(codes_nullable_column->data_column());
            RETURN_IF_ERROR(column_readers[slot_id]->get_dict_values(codes_column->get_data(), dict_values.get()));

            DCHECK_EQ(dict_codes->size(), dict_values->size());
            if (slots[chunk_index]->is_nullable()) {
                auto* nullable_codes = down_cast<NullableColumn*>(dict_codes.get());
                auto* nullable_values = down_cast<NullableColumn*>(dict_values.get());
                nullable_values->null_column_data().swap(nullable_codes->null_column_data());
                nullable_values->set_has_null(nullable_codes->has_null());
            }
        } else {
            (*chunk)->get_column_by_slot_id(slot_id)->swap_column(*(read_chunk->get_column_by_slot_id(slot_id)));
        }
    }
    return Status::OK();
}

} // namespace starrocks::parquet
