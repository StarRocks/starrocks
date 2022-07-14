// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
//

#include "formats/parquet/group_reader.h"

#include "column/column_helper.h"
#include "exec/exec_node.h"
#include "exec/vectorized/hdfs_scanner.h"
#include "exprs/expr.h"
#include "runtime/types.h"
#include "simd/simd.h"
#include "storage/chunk_helper.h"

namespace starrocks::parquet {

constexpr static const PrimitiveType kDictCodePrimitiveType = TYPE_INT;
constexpr static const FieldType kDictCodeFieldType = OLAP_FIELD_TYPE_INT;

GroupReader::GroupReader(const GroupReaderParam& param, int row_group_number) : _param(param) {
    _row_group_metadata =
            std::make_shared<tparquet::RowGroup>(param.file_metadata->t_metadata().row_groups[row_group_number]);
}

Status GroupReader::init() {
    // the calling order matters, do not change unless you know why.
    RETURN_IF_ERROR(_init_column_readers());
    _process_columns_and_conjunct_ctxs();
    RETURN_IF_ERROR(_rewrite_dict_column_predicates());
    _init_read_chunk();
    return Status::OK();
}

Status GroupReader::get_next(vectorized::ChunkPtr* chunk, size_t* row_count) {
    if (_is_group_filtered) {
        *row_count = 0;
        return Status::EndOfFile("");
    }

    _read_chunk->reset();
    size_t count = *row_count;
    bool has_dict_filter = !_dict_filter_preds.empty();
    bool has_more_filter = !_left_conjunct_ctxs.empty();
    Status status;

    {
        SCOPED_RAW_TIMER(&_param.stats->group_chunk_read_ns);
        // read data into _read_chunk
        status = _read(&count);
        _param.stats->raw_rows_read += count;
        if (!status.ok() && !status.is_end_of_file()) {
            return status;
        }
    }

    // dict filter
    if (has_dict_filter) {
        SCOPED_RAW_TIMER(&_param.stats->expr_filter_ns);
        SCOPED_RAW_TIMER(&_param.stats->group_dict_filter_ns);
        _dict_filter();
        _read_chunk->check_or_die();
    }

    // other filter that not dict
    if (has_more_filter) {
        SCOPED_RAW_TIMER(&_param.stats->expr_filter_ns);
        RETURN_IF_ERROR(ExecNode::eval_conjuncts(_left_conjunct_ctxs, _read_chunk.get()));
        _read_chunk->check_or_die();
    }

    *row_count = _read_chunk->num_rows();

    SCOPED_RAW_TIMER(&_param.stats->group_dict_decode_ns);
    // convert from _read_chunk
    RETURN_IF_ERROR(_dict_decode(chunk));

    return status;
}

void GroupReader::close() {
    if (_param.shared_buffered_stream) {
        _param.shared_buffered_stream->release_to_offset(_end_offset);
    }
    _column_readers.clear();
}

Status GroupReader::_init_column_readers() {
    ColumnReaderOptions& opts = _column_reader_opts;
    opts.timezone = _param.timezone;
    opts.chunk_size = _param.chunk_size;
    opts.stats = _param.stats;
    opts.sb_stream = _param.shared_buffered_stream;
    opts.file = _param.file;
    opts.row_group_meta = _row_group_metadata.get();

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
        RETURN_IF_ERROR(
                ColumnReader::create(_column_reader_opts, schema_node, column.col_type_in_chunk, &column_reader));
    }
    _column_readers[column.slot_id] = std::move(column_reader);
    return Status::OK();
}

void GroupReader::_process_columns_and_conjunct_ctxs() {
    const auto& conjunct_ctxs_by_slot = _param.conjunct_ctxs_by_slot;
    const auto& slots = _param.tuple_desc->slots();
    for (const auto& column : _param.read_cols) {
        int chunk_index = column.col_idx_in_chunk;
        SlotId slot_id = column.slot_id;
        const tparquet::ColumnMetaData& column_metadata =
                _row_group_metadata->columns[column.col_idx_in_parquet].meta_data;
        if (_can_using_dict_filter(slots[chunk_index], conjunct_ctxs_by_slot, column_metadata)) {
            _dict_filter_columns.emplace_back(column);
            _dict_filter_conjunct_ctxs[slot_id] = conjunct_ctxs_by_slot.at(slot_id);
        } else {
            _direct_read_columns.emplace_back(column);
            if (conjunct_ctxs_by_slot.find(slot_id) != conjunct_ctxs_by_slot.end()) {
                for (ExprContext* ctx : conjunct_ctxs_by_slot.at(slot_id)) {
                    _left_conjunct_ctxs.emplace_back(ctx);
                }
            }
        }
    }
}

void GroupReader::collect_io_ranges(std::vector<SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset) {
    int64_t end = 0;
    for (const auto& column : _param.read_cols) {
        auto& rg = _row_group_metadata->columns[column.col_idx_in_parquet].meta_data;
        int64_t offset = 0;
        if (rg.__isset.dictionary_page_offset) {
            offset = rg.dictionary_page_offset;
        } else {
            offset = rg.data_page_offset;
        }
        int64_t size = rg.total_compressed_size;
        auto r = SharedBufferedInputStream::IORange{.offset = offset, .size = size};
        ranges->emplace_back(r);
        end = std::max(end, offset + size);
    }
    *end_offset = end;
}

bool GroupReader::_can_using_dict_filter(const SlotDescriptor* slot, const SlotIdExprContextsMap& conjunct_ctxs_by_slot,
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

Status GroupReader::_rewrite_dict_column_predicates() {
    for (const auto& column : _dict_filter_columns) {
        SlotId slot_id = column.slot_id;
        vectorized::ChunkPtr dict_value_chunk = std::make_shared<vectorized::Chunk>();
        std::shared_ptr<vectorized::BinaryColumn> dict_value_column = vectorized::BinaryColumn::create();
        dict_value_chunk->append_column(dict_value_column, slot_id);

        RETURN_IF_ERROR(_column_readers[slot_id]->get_dict_values(dict_value_column.get()));
        RETURN_IF_ERROR(ExecNode::eval_conjuncts(_dict_filter_conjunct_ctxs[slot_id], dict_value_chunk.get()));
        dict_value_chunk->check_or_die();

        // dict column is empty after conjunct eval, file group can be skipped
        if (dict_value_chunk->num_rows() == 0) {
            _is_group_filtered = true;
            return Status::OK();
        }

        // get dict codes
        std::vector<int32_t> dict_codes;
        RETURN_IF_ERROR(_column_readers[slot_id]->get_dict_codes(dict_value_column->get_data(), &dict_codes));

        // eq predicate is faster than in predicate
        // TODO: improve not eq and not in
        if (dict_codes.size() == 1) {
            _dict_filter_preds[slot_id] = vectorized::new_column_eq_predicate(get_type_info(kDictCodeFieldType),
                                                                              slot_id, std::to_string(dict_codes[0]));
        } else {
            std::vector<std::string> str_codes;
            str_codes.reserve(dict_codes.size());
            for (int code : dict_codes) {
                str_codes.emplace_back(std::to_string(code));
            }
            _dict_filter_preds[slot_id] =
                    vectorized::new_column_in_predicate(get_type_info(kDictCodeFieldType), slot_id, str_codes);
        }
        _obj_pool.add(_dict_filter_preds[slot_id]);
    }

    return Status::OK();
}

void GroupReader::_init_read_chunk() {
    const auto& slots = _param.tuple_desc->slots();
    std::vector<SlotDescriptor*> read_slots;
    for (const auto& column : _param.read_cols) {
        int chunk_index = column.col_idx_in_chunk;
        read_slots.emplace_back(slots[chunk_index]);
    }

    size_t chunk_size = _param.chunk_size;
    _read_chunk = vectorized::ChunkHelper::new_chunk(read_slots, chunk_size);
    raw::stl_vector_resize_uninitialized(&_selection, chunk_size);

    // replace dict filter column
    for (const auto& column : _dict_filter_columns) {
        SlotId slot_id = column.slot_id;
        auto dict_code_column = vectorized::ColumnHelper::create_column(
                TypeDescriptor::from_primtive_type(kDictCodePrimitiveType), true);
        dict_code_column->reserve(chunk_size);
        _read_chunk->update_column(dict_code_column, slot_id);
    }
}

Status GroupReader::_read(size_t* row_count) {
    size_t count = *row_count;

    for (const auto& column : _dict_filter_columns) {
        SlotId slot_id = column.slot_id;
        count = *row_count;
        Status status = _column_readers[slot_id]->next_batch(&count, ColumnContentType::DICT_CODE,
                                                             _read_chunk->get_column_by_slot_id(slot_id).get());
        if (!status.ok() && !status.is_end_of_file()) {
            return status;
        }
    }

    for (const auto& column : _direct_read_columns) {
        SlotId slot_id = column.slot_id;
        count = *row_count;
        Status status = _column_readers[slot_id]->next_batch(&count, ColumnContentType::VALUE,
                                                             _read_chunk->get_column_by_slot_id(slot_id).get());
        if (!status.ok() && !status.is_end_of_file()) {
            return status;
        }
    }

    if (count != *row_count) {
        *row_count = count;
        return Status::EndOfFile("");
    }

    *row_count = count;
    return Status::OK();
}

void GroupReader::_dict_filter() {
    DCHECK(!_dict_filter_preds.empty());

    size_t count = _read_chunk->num_rows();
    auto iter = _dict_filter_preds.begin();
    SlotId slot_id = iter->first;
    auto pred = iter->second;
    pred->evaluate(_read_chunk->get_column_by_slot_id(slot_id).get(), _selection.data());
    while (++iter != _dict_filter_preds.end()) {
        slot_id = iter->first;
        pred = iter->second;
        pred->evaluate_and(_read_chunk->get_column_by_slot_id(slot_id).get(), _selection.data());
    }

    auto hit_count = SIMD::count_nonzero(_selection.data(), count);
    if (hit_count == 0) {
        _read_chunk->set_num_rows(0);
    } else if (hit_count != count) {
        _read_chunk->filter_range(_selection, 0, count);
    }
}

Status GroupReader::_dict_decode(vectorized::ChunkPtr* chunk) {
    const auto& slots = _param.tuple_desc->slots();

    for (const auto& column : _dict_filter_columns) {
        int chunk_index = column.col_idx_in_chunk;
        SlotId slot_id = column.slot_id;

        vectorized::ColumnPtr& dict_codes = _read_chunk->get_column_by_slot_id(slot_id);
        vectorized::ColumnPtr& dict_values = (*chunk)->get_column_by_slot_id(slot_id);
        dict_values->resize(0);

        auto* codes_nullable_column = vectorized::ColumnHelper::as_raw_column<vectorized::NullableColumn>(dict_codes);
        auto* codes_column = vectorized::ColumnHelper::as_raw_column<vectorized::FixedLengthColumn<int32_t>>(
                codes_nullable_column->data_column());
        RETURN_IF_ERROR(_column_readers[slot_id]->get_dict_values(codes_column->get_data(), dict_values.get()));

        DCHECK_EQ(dict_codes->size(), dict_values->size());
        if (slots[chunk_index]->is_nullable()) {
            auto* nullable_codes = down_cast<vectorized::NullableColumn*>(dict_codes.get());
            auto* nullable_values = down_cast<vectorized::NullableColumn*>(dict_values.get());
            nullable_values->null_column_data().swap(nullable_codes->null_column_data());
            nullable_values->set_has_null(nullable_codes->has_null());
        }
    }

    for (const auto& column : _direct_read_columns) {
        SlotId slot_id = column.slot_id;
        (*chunk)->get_column_by_slot_id(slot_id)->swap_column(*(_read_chunk->get_column_by_slot_id(slot_id)));
    }
    return Status::OK();
}
} // namespace starrocks::parquet
