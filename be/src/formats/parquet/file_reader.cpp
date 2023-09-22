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

#include "formats/parquet/file_reader.h"

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "exec/exec_node.h"
#include "exec/hdfs_scanner.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/runtime_filter_bank.h"
#include "formats/parquet/encoding_plain.h"
#include "formats/parquet/metadata.h"
#include "fs/fs.h"
#include "gutil/strings/substitute.h"
#include "storage/chunk_helper.h"
#include "util/coding.h"
#include "util/defer_op.h"
#include "util/memcmp.h"
#include "util/thrift_util.h"

namespace starrocks::parquet {

FileReader::FileReader(int chunk_size, RandomAccessFile* file, size_t file_size,
                       io::SharedBufferedInputStream* sb_stream, const std::set<int64_t>* _need_skip_rowids)
        : _chunk_size(chunk_size),
          _file(file),
          _file_size(file_size),
          _sb_stream(sb_stream),
          _need_skip_rowids(_need_skip_rowids) {}

FileReader::~FileReader() = default;

Status FileReader::init(HdfsScannerContext* ctx) {
    _scanner_ctx = ctx;
    RETURN_IF_ERROR(_parse_footer());

    if (_scanner_ctx->iceberg_schema != nullptr && _file_metadata->schema().exist_filed_id()) {
        // If we want read this parquet file with iceberg schema,
        // we also need to make sure it contains parquet field id.
        _meta_helper =
                std::make_shared<IcebergMetaHelper>(_file_metadata, ctx->case_sensitive, _scanner_ctx->iceberg_schema);
    } else {
        _meta_helper = std::make_shared<ParquetMetaHelper>(_file_metadata, ctx->case_sensitive);
    }

    // set existed SlotDescriptor in this parquet file
    std::unordered_set<std::string> names;
    _meta_helper->set_existed_column_names(&names);
    _scanner_ctx->update_materialized_columns(names);

    ASSIGN_OR_RETURN(_is_file_filtered, _scanner_ctx->should_skip_by_evaluating_not_existed_slots());
    if (_is_file_filtered) {
        return Status::OK();
    }
    _prepare_read_columns();
    RETURN_IF_ERROR(_init_group_readers());
    return Status::OK();
}

Status FileReader::_parse_footer() {
    std::vector<char> footer_buffer;
    ASSIGN_OR_RETURN(uint32_t footer_read_size, _get_footer_read_size());
    footer_buffer.resize(footer_read_size);

    {
        SCOPED_RAW_TIMER(&_scanner_ctx->stats->footer_read_ns);
        RETURN_IF_ERROR(_file->read_at_fully(_file_size - footer_read_size, footer_buffer.data(), footer_read_size));
        _scanner_ctx->stats->request_bytes_read += footer_read_size;
        _scanner_ctx->stats->request_bytes_read_uncompressed += footer_read_size;
    }

    ASSIGN_OR_RETURN(uint32_t metadata_length, _parse_metadata_length(footer_buffer));

    if (footer_read_size < (metadata_length + PARQUET_FOOTER_SIZE)) {
        // footer_buffer's size is not enough to read the whole metadata, we need to re-read for larger size
        size_t re_read_size = metadata_length + PARQUET_FOOTER_SIZE;
        footer_buffer.resize(re_read_size);
        {
            SCOPED_RAW_TIMER(&_scanner_ctx->stats->footer_read_ns);
            RETURN_IF_ERROR(_file->read_at_fully(_file_size - re_read_size, footer_buffer.data(), re_read_size));
            _scanner_ctx->stats->request_bytes_read += re_read_size;
            _scanner_ctx->stats->request_bytes_read_uncompressed += re_read_size;
        }
    }

    tparquet::FileMetaData t_metadata;
    // deserialize footer
    RETURN_IF_ERROR(deserialize_thrift_msg(reinterpret_cast<const uint8*>(footer_buffer.data()) + footer_buffer.size() -
                                                   PARQUET_FOOTER_SIZE - metadata_length,
                                           &metadata_length, TProtocolType::COMPACT, &t_metadata));
    _file_metadata.reset(new FileMetaData());
    RETURN_IF_ERROR(_file_metadata->init(t_metadata, _scanner_ctx->case_sensitive));
    return Status::OK();
}

StatusOr<uint32_t> FileReader::_get_footer_read_size() const {
    if (_file_size == 0) {
        return Status::Corruption("Parquet file size is 0 bytes");
    } else if (_file_size < PARQUET_FOOTER_SIZE) {
        return Status::Corruption(strings::Substitute(
                "Parquet file size is $0 bytes, smaller than the minimum parquet file footer ($1 bytes)", _file_size,
                PARQUET_FOOTER_SIZE));
    }
    return std::min(_file_size, DEFAULT_FOOTER_BUFFER_SIZE);
}

StatusOr<uint32_t> FileReader::_parse_metadata_length(const std::vector<char>& footer_buff) const {
    size_t size = footer_buff.size();
    if (memequal(footer_buff.data() + size - 4, 4, PARQUET_EMAIC_NUMBER, 4)) {
        return Status::NotSupported("StarRocks parquet reader not support encrypted parquet file yet");
    }

    if (!memequal(footer_buff.data() + size - 4, 4, PARQUET_MAGIC_NUMBER, 4)) {
        return Status::Corruption("Parquet file magic not matched");
    }

    uint32_t metadata_length = decode_fixed32_le(reinterpret_cast<const uint8_t*>(footer_buff.data()) + size - 8);
    if (metadata_length > _file_size - PARQUET_FOOTER_SIZE) {
        return Status::Corruption(strings::Substitute(
                "Parquet file size is $0 bytes, smaller than the size reported by footer's ($1 bytes)", _file_size,
                metadata_length));
    }
    return metadata_length;
}

int64_t FileReader::_get_row_group_start_offset(const tparquet::RowGroup& row_group) {
    if (row_group.__isset.file_offset) {
        return row_group.file_offset;
    }
    const tparquet::ColumnMetaData& first_column = row_group.columns[0].meta_data;

    return first_column.data_page_offset;
}

StatusOr<bool> FileReader::_filter_group(const tparquet::RowGroup& row_group) {
    // filter by min/max conjunct ctxs.
    if (!_scanner_ctx->min_max_conjunct_ctxs.empty()) {
        const TupleDescriptor& tuple_desc = *(_scanner_ctx->min_max_tuple_desc);
        ChunkPtr min_chunk = ChunkHelper::new_chunk(tuple_desc, 0);
        ChunkPtr max_chunk = ChunkHelper::new_chunk(tuple_desc, 0);

        bool exist = false;
        RETURN_IF_ERROR(_read_min_max_chunk(row_group, tuple_desc.slots(), &min_chunk, &max_chunk, &exist));
        if (!exist) {
            return false;
        }

        for (auto& min_max_conjunct_ctx : _scanner_ctx->min_max_conjunct_ctxs) {
            ASSIGN_OR_RETURN(auto min_column, min_max_conjunct_ctx->evaluate(min_chunk.get()));
            ASSIGN_OR_RETURN(auto max_column, min_max_conjunct_ctx->evaluate(max_chunk.get()));
            auto f = [&](Column* c) {
                // is_null(0) only when something unexpected happens
                if (c->is_null(0)) return (int8_t)0;
                return c->get(0).get_int8();
            };
            auto min = f(min_column.get());
            auto max = f(max_column.get());
            if (min == 0 && max == 0) {
                return true;
            }
        }
    }

    // filter by min/max in runtime filter.
    if (_scanner_ctx->runtime_filter_collector) {
        std::vector<SlotDescriptor*> min_max_slots(1);

        const TupleDescriptor& tuple_desc = *(_scanner_ctx->tuple_desc);
        const std::vector<SlotDescriptor*>& slots = tuple_desc.slots();

        for (auto& it : _scanner_ctx->runtime_filter_collector->descriptors()) {
            RuntimeFilterProbeDescriptor* rf_desc = it.second;
            const JoinRuntimeFilter* filter = rf_desc->runtime_filter();
            SlotId probe_slot_id;
            if (filter == nullptr || filter->has_null() || !rf_desc->is_probe_slot_ref(&probe_slot_id)) continue;
            // !!linear search slot by slot_id.
            SlotDescriptor* slot = nullptr;
            for (SlotDescriptor* s : slots) {
                if (s->id() == probe_slot_id) {
                    slot = s;
                    break;
                }
            }
            if (!slot) continue;
            min_max_slots[0] = slot;
            ChunkPtr min_chunk = ChunkHelper::new_chunk(min_max_slots, 0);
            ChunkPtr max_chunk = ChunkHelper::new_chunk(min_max_slots, 0);
            bool exist = false;
            RETURN_IF_ERROR(_read_min_max_chunk(row_group, min_max_slots, &min_chunk, &max_chunk, &exist));
            if (!exist) continue;
            bool discard = RuntimeFilterHelper::filter_zonemap_with_min_max(
                    slot->type().type, filter, min_chunk->columns()[0].get(), max_chunk->columns()[0].get());
            if (discard) {
                return true;
            }

            // skip topn runtime filter, because it has taken effect on min/max filtering
            // if row-group contains exactly one value(i.e. min_value = max_value), use bloom filter to test
            if (!filter->always_true() && min_chunk->columns()[0]->equals(0, *max_chunk->columns()[0], 0)) {
                ColumnPtr& chunk_part_column = min_chunk->columns()[0];
                JoinRuntimeFilter::RunningContext ctx;
                ctx.use_merged_selection = false;
                auto& selection = ctx.selection;
                selection.assign(chunk_part_column->size(), 1);
                filter->compute_partition_index(rf_desc->layout(), {chunk_part_column.get()}, &ctx);
                filter->evaluate(chunk_part_column.get(), &ctx);
                if (selection[0] == 0) {
                    return true;
                }
            }
        }
    }

    return false;
}

Status FileReader::_read_min_max_chunk(const tparquet::RowGroup& row_group, const std::vector<SlotDescriptor*>& slots,
                                       ChunkPtr* min_chunk, ChunkPtr* max_chunk, bool* exist) const {
    const HdfsScannerContext& ctx = *_scanner_ctx;

    // Key is column name, format with case sensitive, comes from SlotDescription.
    // Value is the position of the filed in parquet schema.
    std::unordered_map<std::string, size_t> column_name_2_pos_in_meta{};
    _meta_helper->build_column_name_2_pos_in_meta(column_name_2_pos_in_meta, row_group, slots);

    for (size_t i = 0; i < slots.size(); i++) {
        const SlotDescriptor* slot = slots[i];
        const tparquet::ColumnMetaData* column_meta =
                _meta_helper->get_column_meta(column_name_2_pos_in_meta, row_group, slot->col_name());
        if (column_meta == nullptr) {
            int col_idx = _get_partition_column_idx(slot->col_name());
            if (col_idx < 0) {
                // column not exist in parquet file
                (*min_chunk)->columns()[i]->append_nulls(1);
                (*max_chunk)->columns()[i]->append_nulls(1);
            } else {
                // is partition column
                auto* const_column = ColumnHelper::as_raw_column<ConstColumn>(ctx.partition_values[col_idx]);
                (*min_chunk)->columns()[i]->append(*const_column->data_column(), 0, 1);
                (*max_chunk)->columns()[i]->append(*const_column->data_column(), 0, 1);
            }
        } else if (!column_meta->__isset.statistics) {
            // statistics not exist in parquet file
            *exist = false;
            return Status::OK();
        } else {
            const ParquetField* field = _meta_helper->get_parquet_field(slot->col_name());
            if (field == nullptr) {
                LOG(WARNING) << "Can't get " + slot->col_name() + "'s ParquetField in _read_min_max_chunk.";
                *exist = false;
                return Status::OK();
            }
            const tparquet::ColumnOrder* column_order = nullptr;
            if (_file_metadata->t_metadata().__isset.column_orders) {
                const auto& column_orders = _file_metadata->t_metadata().column_orders;
                int column_idx = field->physical_column_index;
                column_order = column_idx < column_orders.size() ? &column_orders[column_idx] : nullptr;
            }

            bool decode_ok = false;
            RETURN_IF_ERROR(_decode_min_max_column(*field, ctx.timezone, slot->type(), *column_meta, column_order,
                                                   &(*min_chunk)->columns()[i], &(*max_chunk)->columns()[i],
                                                   &decode_ok));
            if (!decode_ok) {
                *exist = false;
                return Status::OK();
            }
        }
    }

    *exist = true;
    return Status::OK();
}

int32_t FileReader::_get_partition_column_idx(const std::string& col_name) const {
    for (int32_t i = 0; i < _scanner_ctx->partition_columns.size(); i++) {
        if (_scanner_ctx->partition_columns[i].col_name == col_name) {
            return i;
        }
    }
    return -1;
}

Status FileReader::_decode_min_max_column(const ParquetField& field, const std::string& timezone,
                                          const TypeDescriptor& type, const tparquet::ColumnMetaData& column_meta,
                                          const tparquet::ColumnOrder* column_order, ColumnPtr* min_column,
                                          ColumnPtr* max_column, bool* decode_ok) const {
    DCHECK_EQ(field.physical_type, column_meta.type);
    *decode_ok = true;

    // We need to make sure min_max column append value succeed
    bool ret = true;
    auto sort_order = sort_order_of_logical_type(type.type);
    if (!_has_correct_min_max_stats(column_meta, sort_order)) {
        *decode_ok = false;
        return Status::OK();
    }

    switch (column_meta.type) {
    case tparquet::Type::type::INT32: {
        int32_t min_value = 0;
        int32_t max_value = 0;
        if (column_meta.statistics.__isset.min_value) {
            RETURN_IF_ERROR(PlainDecoder<int32_t>::decode(column_meta.statistics.min_value, &min_value));
            RETURN_IF_ERROR(PlainDecoder<int32_t>::decode(column_meta.statistics.max_value, &max_value));
        } else {
            RETURN_IF_ERROR(PlainDecoder<int32_t>::decode(column_meta.statistics.min, &min_value));
            RETURN_IF_ERROR(PlainDecoder<int32_t>::decode(column_meta.statistics.max, &max_value));
        }
        std::unique_ptr<ColumnConverter> converter;
        RETURN_IF_ERROR(ColumnConverterFactory::create_converter(field, type, timezone, &converter));

        if (!converter->need_convert) {
            ret &= ((*min_column)->append_numbers(&min_value, sizeof(int32_t)) > 0);
            ret &= ((*max_column)->append_numbers(&max_value, sizeof(int32_t)) > 0);
        } else {
            ColumnPtr min_scr_column = converter->create_src_column();
            ret &= (min_scr_column->append_numbers(&min_value, sizeof(int32_t)) > 0);
            converter->convert(min_scr_column, min_column->get());

            ColumnPtr max_scr_column = converter->create_src_column();
            ret &= (max_scr_column->append_numbers(&max_value, sizeof(int32_t)) > 0);
            converter->convert(max_scr_column, max_column->get());
        }
        break;
    }
    case tparquet::Type::type::INT64: {
        int64_t min_value = 0;
        int64_t max_value = 0;
        if (column_meta.statistics.__isset.min_value) {
            RETURN_IF_ERROR(PlainDecoder<int64_t>::decode(column_meta.statistics.min_value, &min_value));
            RETURN_IF_ERROR(PlainDecoder<int64_t>::decode(column_meta.statistics.max_value, &max_value));
        } else {
            RETURN_IF_ERROR(PlainDecoder<int64_t>::decode(column_meta.statistics.max, &max_value));
            RETURN_IF_ERROR(PlainDecoder<int64_t>::decode(column_meta.statistics.min, &min_value));
        }
        std::unique_ptr<ColumnConverter> converter;
        RETURN_IF_ERROR(ColumnConverterFactory::create_converter(field, type, timezone, &converter));

        if (!converter->need_convert) {
            ret &= ((*min_column)->append_numbers(&min_value, sizeof(int64_t)) > 0);
            ret &= ((*max_column)->append_numbers(&max_value, sizeof(int64_t)) > 0);
        } else {
            ColumnPtr min_scr_column = converter->create_src_column();
            ret &= (min_scr_column->append_numbers(&min_value, sizeof(int64_t)) > 0);
            converter->convert(min_scr_column, min_column->get());

            ColumnPtr max_scr_column = converter->create_src_column();
            ret &= (max_scr_column->append_numbers(&max_value, sizeof(int64_t)) > 0);
            converter->convert(max_scr_column, max_column->get());
        }
        break;
    }
    case tparquet::Type::type::BYTE_ARRAY: {
        Slice min_slice;
        Slice max_slice;

        if (column_meta.statistics.__isset.min_value) {
            RETURN_IF_ERROR(PlainDecoder<Slice>::decode(column_meta.statistics.min_value, &min_slice));
            RETURN_IF_ERROR(PlainDecoder<Slice>::decode(column_meta.statistics.max_value, &max_slice));
        } else {
            RETURN_IF_ERROR(PlainDecoder<Slice>::decode(column_meta.statistics.min, &min_slice));
            RETURN_IF_ERROR(PlainDecoder<Slice>::decode(column_meta.statistics.max, &max_slice));
        }
        std::unique_ptr<ColumnConverter> converter;
        RETURN_IF_ERROR(ColumnConverterFactory::create_converter(field, type, timezone, &converter));

        if (!converter->need_convert) {
            ret &= (*min_column)->append_strings(std::vector<Slice>{min_slice});
            ret &= (*max_column)->append_strings(std::vector<Slice>{max_slice});
        } else {
            ColumnPtr min_scr_column = converter->create_src_column();
            ret &= min_scr_column->append_strings(std::vector<Slice>{min_slice});
            converter->convert(min_scr_column, min_column->get());

            ColumnPtr max_scr_column = converter->create_src_column();
            ret &= max_scr_column->append_strings(std::vector<Slice>{max_slice});
            converter->convert(max_scr_column, max_column->get());
        }
        break;
    }
    default:
        *decode_ok = false;
    }

    if (UNLIKELY(!ret)) {
        return Status::InternalError("Decode min-max column failed");
    }

    return Status::OK();
}

bool FileReader::_has_correct_min_max_stats(const tparquet::ColumnMetaData& column_meta,
                                            const SortOrder& sort_order) const {
    return _file_metadata->writer_version().HasCorrectStatistics(column_meta, sort_order);
}

void FileReader::_prepare_read_columns() {
    _meta_helper->prepare_read_columns(_scanner_ctx->materialized_columns, _group_reader_param.read_cols);
    _no_materialized_column_scan = (_group_reader_param.read_cols.size() == 0);
}

bool FileReader::_select_row_group(const tparquet::RowGroup& row_group) {
    size_t row_group_start = _get_row_group_start_offset(row_group);

    for (auto* scan_range : _scanner_ctx->scan_ranges) {
        size_t scan_start = scan_range->offset;
        size_t scan_end = scan_range->length + scan_start;

        if (row_group_start >= scan_start && row_group_start < scan_end) {
            return true;
        }
    }

    return false;
}

Status FileReader::_init_group_readers() {
    const HdfsScannerContext& fd_scanner_ctx = *_scanner_ctx;

    // _group_reader_param is used by all group readers
    _group_reader_param.tuple_desc = fd_scanner_ctx.tuple_desc;
    _group_reader_param.conjunct_ctxs_by_slot = fd_scanner_ctx.conjunct_ctxs_by_slot;
    _group_reader_param.timezone = fd_scanner_ctx.timezone;
    _group_reader_param.stats = fd_scanner_ctx.stats;
    _group_reader_param.sb_stream = nullptr;
    _group_reader_param.chunk_size = _chunk_size;
    _group_reader_param.file = _file;
    _group_reader_param.file_metadata = _file_metadata.get();
    _group_reader_param.case_sensitive = fd_scanner_ctx.case_sensitive;

    int64_t row_group_first_row = 0;
    // select and create row group readers.
    for (size_t i = 0; i < _file_metadata->t_metadata().row_groups.size(); i++) {
        bool selected = _select_row_group(_file_metadata->t_metadata().row_groups[i]);

        if (i > 0) {
            row_group_first_row += _file_metadata->t_metadata().row_groups[i - 1].num_rows;
        }
        if (selected) {
            StatusOr<bool> st = _filter_group(_file_metadata->t_metadata().row_groups[i]);
            if (!st.ok()) return st.status();
            if (st.value()) {
                LOG(INFO) << "row group " << i << " of file has been filtered by min/max conjunct";
                continue;
            }

            auto row_group_reader =
                    std::make_shared<GroupReader>(_group_reader_param, i, _need_skip_rowids, row_group_first_row);
            _row_group_readers.emplace_back(row_group_reader);
            int64_t num_rows = _file_metadata->t_metadata().row_groups[i].num_rows;
            // for iceberg v2 pos delete
            if (_need_skip_rowids != nullptr && !_need_skip_rowids->empty()) {
                auto start_iter = _need_skip_rowids->lower_bound(row_group_first_row);
                auto end_iter = _need_skip_rowids->upper_bound(row_group_first_row + num_rows - 1);
                num_rows -= std::distance(start_iter, end_iter);
            }
            _total_row_count += num_rows;
        } else {
            continue;
        }
    }
    _row_group_size = _row_group_readers.size();

    // if coalesce read enabled, we have to
    // 1. allocate shared buffered input stream and
    // 2. collect io ranges of every row group reader.
    // 3. set io ranges to the stream.
    if (config::parquet_coalesce_read_enable && _sb_stream != nullptr) {
        std::vector<io::SharedBufferedInputStream::IORange> ranges;
        for (auto& r : _row_group_readers) {
            int64_t end_offset = 0;
            r->collect_io_ranges(&ranges, &end_offset);
            r->set_end_offset(end_offset);
        }
        _sb_stream->set_io_ranges(ranges);
        _group_reader_param.sb_stream = _sb_stream;
    }

    // initialize row group readers.
    for (auto& r : _row_group_readers) {
        RETURN_IF_ERROR(r->init());
    }
    return Status::OK();
}

Status FileReader::get_next(ChunkPtr* chunk) {
    if (_is_file_filtered) {
        return Status::EndOfFile("");
    }
    if (_no_materialized_column_scan) {
        RETURN_IF_ERROR(_exec_no_materialized_column_scan(chunk));
        return Status::OK();
    }

    if (_cur_row_group_idx < _row_group_size) {
        size_t row_count = _chunk_size;
        Status status = _row_group_readers[_cur_row_group_idx]->get_next(chunk, &row_count);
        if (status.ok() || status.is_end_of_file()) {
            if (row_count > 0) {
                _scanner_ctx->update_not_existed_columns_of_chunk(chunk, row_count);
                _scanner_ctx->update_partition_column_of_chunk(chunk, row_count);
                _scan_row_count += (*chunk)->num_rows();
            }
            if (status.is_end_of_file()) {
                _row_group_readers[_cur_row_group_idx]->close();
                _cur_row_group_idx++;
                return Status::OK();
            }
        } else {
            auto s = strings::Substitute("FileReader::get_next failed. reason = $0, file = $1", status.to_string(),
                                         _file->filename());
            LOG(WARNING) << s;
            return Status::InternalError(s);
        }

        return status;
    }

    return Status::EndOfFile("");
}

Status FileReader::_exec_no_materialized_column_scan(ChunkPtr* chunk) {
    if (_scan_row_count < _total_row_count) {
        size_t read_size = std::min(static_cast<size_t>(_chunk_size), _total_row_count - _scan_row_count);
        _scanner_ctx->update_not_existed_columns_of_chunk(chunk, read_size);
        _scanner_ctx->update_partition_column_of_chunk(chunk, read_size);
        _scan_row_count += read_size;
        return Status::OK();
    }

    return Status::EndOfFile("");
}

} // namespace starrocks::parquet
