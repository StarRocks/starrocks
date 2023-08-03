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
                       int64_t file_mtime, io::SharedBufferedInputStream* sb_stream)
    : _chunk_size(chunk_size), _file(file), _file_size(file_size), _file_mtime(file_mtime), _sb_stream(sb_stream) {}

FileReader::~FileReader() {
    if (!_is_metadata_cached && _file_metadata) {
        delete _file_metadata;
    }
}

void FileReader::_build_meta_cache_key() {
    auto& filename = _file->filename();
    //_meta_cache_key = filename + "_" + std::to_string(file_size);
    _meta_cache_key.resize(14);
    char* data = _meta_cache_key.data();
    const std::string footer_suffix = "ft";
    uint64_t hash_value = HashUtil::hash64(filename.data(), filename.size(), 0);
    memcpy(data, &hash_value, sizeof(hash_value));
    memcpy(data + 8, footer_suffix.data(), footer_suffix.length());
    // The modification time is more appropriate to indicate the different file versions.
    // While some data source, such as Hudi, have no modification time because their files
    // cannot be overwritten. So, if the modification time is unsupported, we use file size instead.
    // Also, to reduce memory usage, we only use the high four bytes to represent the second timestamp.
    if (_file_mtime > 0) {
        int32_t mtime_s = (_file_mtime >> 32) & 0x00000000FFFFFFFF;
        memcpy(data + 10, &mtime_s, sizeof(mtime_s));
    } else {
        int32_t size = _file_size;
        memcpy(data + 10, &size, sizeof(size));
    }
}

Status FileReader::init(HdfsScannerContext* ctx) {
    _scanner_ctx = ctx;
    if (ctx->use_filemeta_cache && config::block_cache_enable) {
        _build_meta_cache_key();
        _cache = BlockCache::instance();
    }
    RETURN_IF_ERROR(_get_footer());

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
    _scanner_ctx->set_columns_from_file(names);

    ASSIGN_OR_RETURN(_is_file_filtered, _scanner_ctx->should_skip_by_evaluating_not_existed_slots());
    if (_is_file_filtered) {
        return Status::OK();
    }
    _prepare_read_columns();
    RETURN_IF_ERROR(_init_group_readers());
    return Status::OK();
}

Status FileReader::_get_footer() {
    _is_metadata_cached = false;
    if (!_cache) {
        size_t metadata_size = 0;
        return _parse_footer(&_file_metadata, &metadata_size);
    }

    {
        SCOPED_RAW_TIMER(&_scanner_ctx->stats->footer_cache_read_ns);
        Status st = _cache->read_object(_meta_cache_key, &_cache_handle);
        if (st.ok()) {
            _file_metadata = (FileMetaData*)_cache_handle.ptr();
            _scanner_ctx->stats->footer_cache_read_count += 1;
            _is_metadata_cached = true;
            //LOG(INFO) << "get file meta cache key, filename: " << _file->filename() << ", file_metadata: " << _file_metadata;
            return st;
        }
    }

    size_t metadata_size = 0;
    FileMetaData* file_metadata = nullptr;
    RETURN_IF_ERROR(_parse_footer(&file_metadata, &metadata_size));
    size_t metadata_space = metadata_size + 8 + sizeof(file_metadata->estimate_memory());

    auto deleter = [file_metadata]() { delete file_metadata; };
    Status st = _cache->write_object(_meta_cache_key, file_metadata, metadata_space, deleter, &_cache_handle);
    if (st.ok()) {
        _scanner_ctx->stats->footer_cache_write_bytes += metadata_space;
        _scanner_ctx->stats->footer_cache_write_count += 1;
        _is_metadata_cached = true;
        //LOG(INFO) << "set file meta cache key, filename: " << _file->filename() << ", file_metadata: "
        //          << file_metadata << ", st: " << st.message();
    }
    _file_metadata = file_metadata;
    return Status::OK();
}

Status FileReader::_parse_footer(FileMetaData** file_metadata, size_t* metadata_size) {
    // try with buffer on stack
    uint8_t local_buf[FOOTER_BUFFER_SIZE];
    uint8_t* footer_buf = local_buf;
    // we may allocate on heap if local_buf is not large enough.
    DeferOp deferop([&] {
        if (footer_buf != local_buf) {
            delete[] footer_buf;
        }
    });

    uint64_t to_read = std::min(_file_size, FOOTER_BUFFER_SIZE);
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
        // VLOG_FILE << "parquet file has large footer. name = " << _file->filename() << ", footer_size = " << footer_size;
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

    _scanner_ctx->stats->request_bytes_read += footer_size + 8;

    tparquet::FileMetaData t_metadata;
    // deserialize footer
    RETURN_IF_ERROR(deserialize_thrift_msg(footer_buf + to_read - 8 - footer_size, &footer_size, TProtocolType::COMPACT,
                                           &t_metadata));
    *file_metadata = new FileMetaData();
    RETURN_IF_ERROR((*file_metadata)->init(t_metadata, _scanner_ctx->case_sensitive));
    *metadata_size = footer_size;

    return Status::OK();
}

Status FileReader::_check_magic(const uint8_t* file_magic) {
    if (!memequal(reinterpret_cast<const char*>(file_magic), 4, PARQUET_MAGIC_NUMBER, 4)) {
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
                                          ColumnPtr* max_column, bool* decode_ok) {
    DCHECK_EQ(field.physical_type, column_meta.type);
    *decode_ok = true;
    // We need to make sure min_max column append value succeed
    bool ret = true;
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
    _meta_helper->prepare_read_columns(_scanner_ctx->materialized_columns, _group_reader_param.read_cols,
                                       _is_only_partition_scan);
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
    _group_reader_param.file_metadata = _file_metadata;
    _group_reader_param.case_sensitive = fd_scanner_ctx.case_sensitive;

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

Status FileReader::_exec_only_partition_scan(ChunkPtr* chunk) {
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
