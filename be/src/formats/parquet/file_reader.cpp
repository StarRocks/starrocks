// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "formats/parquet/file_reader.h"

#include "column/column_helper.h"
#include "exec/exec_node.h"
#include "exec/vectorized/hdfs_scanner.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "formats/parquet/encoding_plain.h"
#include "formats/parquet/metadata.h"
#include "fs/fs.h"
#include "gen_cpp/parquet_types.h"
#include "gutil/strings/substitute.h"
#include "storage/chunk_helper.h"
#include "util/coding.h"
#include "util/defer_op.h"
#include "util/memcmp.h"
#include "util/thrift_util.h"

namespace starrocks::parquet {

static constexpr uint32_t kFooterSize = 8;

FileReader::FileReader(int chunk_size, RandomAccessFile* file, uint64_t file_size)
        : _chunk_size(chunk_size), _file(file), _file_size(file_size) {}

FileReader::~FileReader() = default;

Status FileReader::init(vectorized::HdfsScannerContext* ctx) {
    _scanner_ctx = ctx;
    RETURN_IF_ERROR(_parse_footer());

    std::unordered_set<std::string> names;
    _file_metadata->schema().get_field_names(&names);
    _scanner_ctx->set_columns_from_file(names);
    ASSIGN_OR_RETURN(_is_file_filtered, _scanner_ctx->should_skip_by_evaluating_not_existed_slots());
    if (_is_file_filtered) {
        return Status::OK();
    }
    _prepare_read_columns();
    RETURN_IF_ERROR(_init_group_readers());
    return Status::OK();
}

Status FileReader::_parse_footer() {
    // try with buffer on stack
    constexpr uint64_t footer_buf_size = 16 * 1024;
    uint8_t local_buf[footer_buf_size];
    uint8_t* footer_buf = local_buf;
    // we may allocate on heap if local_buf is not large enough.
    DeferOp deferop([&] {
        if (footer_buf != local_buf) {
            delete[] footer_buf;
        }
    });

    uint64_t to_read = std::min(_file_size, footer_buf_size);
    {
        SCOPED_RAW_TIMER(&_scanner_ctx->stats->footer_read_ns);
        RETURN_IF_ERROR(_file->read_at_fully(_file_size - to_read, footer_buf, to_read));
    }
    // check magic
    RETURN_IF_ERROR(_check_magic(footer_buf + to_read - 4));
    // deserialize footer
    uint32_t footer_size = decode_fixed32_le(footer_buf + to_read - 8);

    // if local buf is not large enough, we have to allocate on heap and re-read.
    // 4 bytes magic number, 4 bytes for footer_size, so total size is footer_size + 8.
    if ((footer_size + 8) > to_read) {
        VLOG_FILE << "parquet file has large footer. name = " << _file->filename() << ", footer_size = " << footer_size;
        to_read = footer_size + 8;
        if (_file_size < to_read) {
            return Status::Corruption(strings::Substitute("Invalid parquet file: name=$0, file_size=$1, footer_size=$2",
                                                          _file->filename(), _file_size, to_read));
        }
        footer_buf = new uint8[to_read];
        {
            SCOPED_RAW_TIMER(&_scanner_ctx->stats->footer_read_ns);
            RETURN_IF_ERROR(_file->read_at_fully(_file_size - to_read, footer_buf, to_read));
        }
    }

    tparquet::FileMetaData t_metadata;
    // deserialize footer
    RETURN_IF_ERROR(deserialize_thrift_msg(footer_buf + to_read - 8 - footer_size, &footer_size, TProtocolType::COMPACT,
                                           &t_metadata));
    _file_metadata.reset(new FileMetaData());
    RETURN_IF_ERROR(_file_metadata->init(t_metadata));

    return Status::OK();
}

Status FileReader::_check_magic(const uint8_t* file_magic) {
    static const char* s_magic = "PAR1";
    if (!memequal(reinterpret_cast<const char*>(file_magic), 4, s_magic, 4)) {
        return Status::Corruption("Parquet file magic not match");
    }
    return Status::OK();
}

int64_t FileReader::_get_row_group_start_offset(const tparquet::RowGroup& row_group) {
    if (row_group.__isset.file_offset) {
        return row_group.file_offset;
    }
    const tparquet::ColumnMetaData& first_column = row_group.columns[0].meta_data;

    return first_column.data_page_offset;
}

StatusOr<bool> FileReader::_filter_group(const tparquet::RowGroup& row_group) {
    if (!_scanner_ctx->min_max_conjunct_ctxs.empty()) {
        auto min_chunk = ChunkHelper::new_chunk(*_scanner_ctx->min_max_tuple_desc, 0);
        auto max_chunk = ChunkHelper::new_chunk(*_scanner_ctx->min_max_tuple_desc, 0);

        bool exist = false;
        RETURN_IF_ERROR(_read_min_max_chunk(row_group, &min_chunk, &max_chunk, &exist));
        if (!exist) {
            return false;
        }

        for (auto& min_max_conjunct_ctx : _scanner_ctx->min_max_conjunct_ctxs) {
            ASSIGN_OR_RETURN(auto min_column, min_max_conjunct_ctx->evaluate(min_chunk.get()));
            ASSIGN_OR_RETURN(auto max_column, min_max_conjunct_ctx->evaluate(max_chunk.get()));

            auto min = min_column->get(0).get_int8();
            auto max = max_column->get(0).get_int8();

            if (min == 0 && max == 0) {
                return true;
            }
        }
    }

    return false;
}

Status FileReader::_read_min_max_chunk(const tparquet::RowGroup& row_group, vectorized::ChunkPtr* min_chunk,
                                       vectorized::ChunkPtr* max_chunk, bool* exist) const {
    const vectorized::HdfsScannerContext& ctx = *_scanner_ctx;
    for (size_t i = 0; i < ctx.min_max_tuple_desc->slots().size(); i++) {
        const auto* slot = ctx.min_max_tuple_desc->slots()[i];
        const auto* column_meta = _get_column_meta(row_group, slot->col_name());
        if (column_meta == nullptr) {
            int col_idx = _get_partition_column_idx(slot->col_name());
            if (col_idx < 0) {
                // column not exist in parquet file
                (*min_chunk)->columns()[i]->append_nulls(1);
                (*max_chunk)->columns()[i]->append_nulls(1);
            } else {
                // is partition column
                auto* const_column =
                        vectorized::ColumnHelper::as_raw_column<vectorized::ConstColumn>(ctx.partition_values[col_idx]);

                (*min_chunk)->columns()[i]->append(*const_column->data_column(), 0, 1);
                (*max_chunk)->columns()[i]->append(*const_column->data_column(), 0, 1);
            }
        } else if (!column_meta->__isset.statistics) {
            // statistics not exist in parquet file
            *exist = false;
            return Status::OK();
        } else {
            const ParquetField* field = _file_metadata->schema().resolve_by_name(slot->col_name());
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

int FileReader::_get_partition_column_idx(const std::string& col_name) const {
    for (size_t i = 0; i < _scanner_ctx->partition_columns.size(); i++) {
        if (_scanner_ctx->partition_columns[i].col_name == col_name) {
            return i;
        }
    }
    return -1;
}

Status FileReader::_decode_min_max_column(const ParquetField& field, const std::string& timezone,
                                          const TypeDescriptor& type, const tparquet::ColumnMetaData& column_meta,
                                          const tparquet::ColumnOrder* column_order, vectorized::ColumnPtr* min_column,
                                          vectorized::ColumnPtr* max_column, bool* decode_ok) {
    *decode_ok = true;

    if (!_can_use_min_max_stats(column_meta, column_order)) {
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
            (*min_column)->append_numbers(&min_value, sizeof(int32_t));
            (*max_column)->append_numbers(&max_value, sizeof(int32_t));
        } else {
            vectorized::ColumnPtr min_scr_column = converter->create_src_column();
            min_scr_column->append_numbers(&min_value, sizeof(int32_t));
            converter->convert(min_scr_column, min_column->get());

            vectorized::ColumnPtr max_scr_column = converter->create_src_column();
            max_scr_column->append_numbers(&max_value, sizeof(int32_t));
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
            (*min_column)->append_numbers(&min_value, sizeof(int64_t));
            (*max_column)->append_numbers(&max_value, sizeof(int64_t));
        } else {
            vectorized::ColumnPtr min_scr_column = converter->create_src_column();
            min_scr_column->append_numbers(&min_value, sizeof(int64_t));
            converter->convert(min_scr_column, min_column->get());

            vectorized::ColumnPtr max_scr_column = converter->create_src_column();
            max_scr_column->append_numbers(&max_value, sizeof(int64_t));
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
            (*min_column)->append_strings(std::vector<Slice>{min_slice});
            (*max_column)->append_strings(std::vector<Slice>{max_slice});
        } else {
            vectorized::ColumnPtr min_scr_column = converter->create_src_column();
            min_scr_column->append_strings(std::vector<Slice>{min_slice});
            converter->convert(min_scr_column, min_column->get());

            vectorized::ColumnPtr max_scr_column = converter->create_src_column();
            max_scr_column->append_strings(std::vector<Slice>{max_slice});
            converter->convert(max_scr_column, max_column->get());
        }
        break;
    }
    default:
        *decode_ok = false;
    }
    return Status::OK();
}

bool FileReader::_can_use_min_max_stats(const tparquet::ColumnMetaData& column_meta,
                                        const tparquet::ColumnOrder* column_order) {
    if (column_meta.statistics.__isset.min_value && _can_use_stats(column_meta.type, column_order)) {
        return true;
    }
    if (column_meta.statistics.__isset.min && _can_use_deprecated_stats(column_meta.type, column_order)) {
        return true;
    }
    return false;
}

bool FileReader::_can_use_stats(const tparquet::Type::type& type, const tparquet::ColumnOrder* column_order) {
    // If column order is not set, only statistics for numeric types can be trusted.
    if (column_order == nullptr) {
        // is boolean | is interger | is floating
        return type == tparquet::Type::type::BOOLEAN || _is_integer_type(type) || type == tparquet::Type::type::DOUBLE;
    }
    // Stats can be used if the column order is TypeDefinedOrder (see parquet.thrift).
    return column_order->__isset.TYPE_ORDER;
}

bool FileReader::_can_use_deprecated_stats(const tparquet::Type::type& type,
                                           const tparquet::ColumnOrder* column_order) {
    // If column order is set to something other than TypeDefinedOrder, we shall not use the
    // stats (see parquet.thrift).
    if (column_order != nullptr && !column_order->__isset.TYPE_ORDER) {
        return false;
    }
    return type == tparquet::Type::type::BOOLEAN || _is_integer_type(type) || type == tparquet::Type::type::DOUBLE;
}

bool FileReader::_is_integer_type(const tparquet::Type::type& type) {
    return type == tparquet::Type::type::INT32 || type == tparquet::Type::type::INT64 ||
           type == tparquet::Type::type::INT96;
}

void FileReader::_prepare_read_columns() {
    const vectorized::HdfsScannerContext& param = *_scanner_ctx;
    for (auto& materialized_column : param.materialized_columns) {
        int field_index = _file_metadata->schema().get_column_index(materialized_column.col_name, param.case_sensitive);
        if (field_index < 0) continue;

        auto parquet_type = _file_metadata->schema().get_stored_column_by_idx(field_index)->physical_type;
        GroupReaderParam::Column column{};
        column.col_idx_in_parquet = field_index;
        column.col_type_in_parquet = parquet_type;
        column.col_idx_in_chunk = materialized_column.col_idx;
        column.col_type_in_chunk = materialized_column.col_type;
        column.slot_id = materialized_column.slot_id;
        _read_cols.emplace_back(column);
    }
    _is_only_partition_scan = _read_cols.empty();
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
    const vectorized::HdfsScannerContext& fd_scanner_ctx = *_scanner_ctx;
    GroupReaderParam& param = _group_reader_param;
    param.tuple_desc = fd_scanner_ctx.tuple_desc;
    param.conjunct_ctxs_by_slot = fd_scanner_ctx.conjunct_ctxs_by_slot;
    param.read_cols = _read_cols;
    param.timezone = fd_scanner_ctx.timezone;
    param.stats = fd_scanner_ctx.stats;
    param.shared_buffered_stream = nullptr;
    param.chunk_size = _chunk_size;
    param.file = _file;
    param.file_metadata = _file_metadata.get();
    param.case_sensitive = fd_scanner_ctx.case_sensitive;

    // select and create row group readers.
    for (size_t i = 0; i < _file_metadata->t_metadata().row_groups.size(); i++) {
        bool selected = _select_row_group(_file_metadata->t_metadata().row_groups[i]);

        if (selected) {
            StatusOr<bool> st = _filter_group(_file_metadata->t_metadata().row_groups[i]);
            if (!st.ok()) return st.status();
            if (st.value()) {
                LOG(INFO) << "row group " << i << " of file has been filtered by min/max conjunct";
                continue;
            }

            auto row_group_reader = std::make_shared<GroupReader>(_group_reader_param, i);
            _row_group_readers.emplace_back(row_group_reader);
            _total_row_count += _file_metadata->t_metadata().row_groups[i].num_rows;
        } else {
            continue;
        }
    }
    _row_group_size = _row_group_readers.size();

    // if coalesce read enabled, we have to
    // 1. allocate shared buffered input stream and
    // 2. collect io ranges of every row group reader.
    // 3. set io ranges to the stream.
    if (config::parquet_coalesce_read_enable) {
        _sb_stream = std::make_shared<SharedBufferedInputStream>(_file);
        SharedBufferedInputStream::CoalesceOptions options = {
                .max_dist_size = config::io_coalesce_read_max_distance_size,
                .max_buffer_size = config::io_coalesce_read_max_buffer_size};
        _sb_stream->set_coalesce_options(options);

        std::vector<SharedBufferedInputStream::IORange> ranges;
        for (auto& r : _row_group_readers) {
            int64_t end_offset = 0;
            r->collect_io_ranges(&ranges, &end_offset);
            r->set_end_offset(end_offset);
        }
        _sb_stream->set_io_ranges(ranges);
        param.shared_buffered_stream = _sb_stream.get();
    }

    // initialize row group readers.
    for (auto& r : _row_group_readers) {
        RETURN_IF_ERROR(r->init());
    }
    return Status::OK();
}

Status FileReader::get_next(vectorized::ChunkPtr* chunk) {
    if (_is_file_filtered) {
        return Status::EndOfFile("");
    }
    if (_is_only_partition_scan) {
        RETURN_IF_ERROR(_exec_only_partition_scan(chunk));
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
        }

        return status;
    }

    return Status::EndOfFile("");
}

Status FileReader::_exec_only_partition_scan(vectorized::ChunkPtr* chunk) {
    if (_scan_row_count < _total_row_count) {
        size_t read_size = std::min(static_cast<size_t>(_chunk_size), _total_row_count - _scan_row_count);
        _scanner_ctx->update_not_existed_columns_of_chunk(chunk, read_size);
        _scanner_ctx->update_partition_column_of_chunk(chunk, read_size);
        _scan_row_count += read_size;
        return Status::OK();
    }

    return Status::EndOfFile("");
}

const tparquet::ColumnMetaData* FileReader::_get_column_meta(const tparquet::RowGroup& row_group,
                                                             const std::string& col_name) {
    for (const auto& column : row_group.columns) {
        // TODO: support not scalar type
        if (column.meta_data.path_in_schema[0] == col_name) {
            return &column.meta_data;
        }
    }
    return nullptr;
}

} // namespace starrocks::parquet
