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

#include "exec/iceberg/iceberg_delete_builder.h"

#include <storage/chunk_helper.h>

#include "column/vectorized_fwd.h"
#include "exec/iceberg/iceberg_delete_file_iterator.h"
#include "formats/orc/orc_chunk_reader.h"
#include "formats/orc/orc_input_stream.h"
#include "formats/parquet/file_reader.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"

namespace starrocks {

struct IcebergColumnMeta {
    int64_t id;
    std::string col_name;
    TPrimitiveType::type type;
};

static const IcebergColumnMeta k_delete_file_path{
        .id = INT32_MAX - 101, .col_name = "file_path", .type = TPrimitiveType::VARCHAR};

static const IcebergColumnMeta k_delete_file_pos{
        .id = INT32_MAX - 102, .col_name = "pos", .type = TPrimitiveType::BIGINT};

StatusOr<std::unique_ptr<RandomAccessFile>> IcebergDeleteBuilder::open_random_access_file(
        const TIcebergDeleteFile& delete_file, HdfsScanStats& fs_scan_stats, HdfsScanStats& app_scan_stats,
        std::shared_ptr<io::SharedBufferedInputStream>& shared_buffered_input_stream,
        std::shared_ptr<io::CacheInputStream>& cache_input_stream) const {
    const OpenFileOptions options{.fs = _params.fs,
                                  .path = delete_file.full_path,
                                  .file_size = delete_file.length,
                                  .fs_stats = &fs_scan_stats,
                                  .app_stats = &app_scan_stats,
                                  .datacache_options = _params.datacache_options};
    ASSIGN_OR_RETURN(auto file,
                     HdfsScanner::create_random_access_file(shared_buffered_input_stream, cache_input_stream, options));
    std::vector<io::SharedBufferedInputStream::IORange> io_ranges{};
    int64_t offset = 0;
    while (offset < delete_file.length) {
        const int64_t remain_length =
                std::min(static_cast<int64_t>(config::io_coalesce_read_max_buffer_size), delete_file.length - offset);
        io_ranges.emplace_back(offset, remain_length);
        offset += remain_length;
    }

    RETURN_IF_ERROR(shared_buffered_input_stream->set_io_ranges(io_ranges));
    return file;
}

Status IcebergDeleteBuilder::fill_skip_rowids(const ChunkPtr& chunk) const {
    const ColumnPtr& file_path = chunk->get_column_by_slot_id(k_delete_file_path.id);
    const ColumnPtr& pos = chunk->get_column_by_slot_id(k_delete_file_pos.id);
    for (int i = 0; i < chunk->num_rows(); i++) {
        if (file_path->get(i).get_slice() == _params.path) {
            _deletion_bitmap->add_value(pos->get(i).get_int64());
        }
    }
    return Status::OK();
}

Status IcebergDeleteBuilder::build_parquet(const TIcebergDeleteFile& delete_file) const {
    HdfsScanStats app_scan_stats;
    HdfsScanStats fs_scan_stats;
    std::shared_ptr<io::SharedBufferedInputStream> shared_buffered_input_stream = nullptr;
    std::shared_ptr<io::CacheInputStream> cache_input_stream = nullptr;

    ASSIGN_OR_RETURN(auto file, open_random_access_file(delete_file, fs_scan_stats, app_scan_stats,
                                                        shared_buffered_input_stream, cache_input_stream));

    std::unique_ptr<parquet::FileReader> reader;
    try {
        reader = std::make_unique<parquet::FileReader>(_runtime_state->chunk_size(), file.get(),
                                                       file->get_size().value());
    } catch (std::exception& e) {
        const auto s = strings::Substitute(
                "IcebergDeleteBuilder::build_parquet create parquet::FileReader failed. reason = $0", e.what());
        LOG(WARNING) << s;
        return Status::InternalError(s);
    }

    auto scanner_ctx = std::make_unique<HdfsScannerContext>();
    std::vector<HdfsScannerContext::ColumnInfo> columns;
    THdfsScanRange scan_range;
    scan_range.offset = 0;
    scan_range.length = delete_file.length;
    std::vector slot_descriptors{&(IcebergDeleteFileMeta::get_delete_file_path_slot()),
                                 &(IcebergDeleteFileMeta::get_delete_file_pos_slot())};
    for (size_t i = 0; i < slot_descriptors.size(); i++) {
        auto* slot = slot_descriptors[i];
        HdfsScannerContext::ColumnInfo column;
        column.slot_desc = slot;
        column.idx_in_chunk = i;
        column.decode_needed = true;
        columns.emplace_back(column);
    }

    std::vector<TIcebergSchemaField> schema_fields;

    // build file path field
    TIcebergSchemaField file_path_field;
    file_path_field.__set_field_id(k_delete_file_path.id);
    file_path_field.__set_name(k_delete_file_path.col_name);
    schema_fields.push_back(file_path_field);

    // build position field
    TIcebergSchemaField pos_field;
    pos_field.__set_field_id(k_delete_file_pos.id);
    pos_field.__set_name(k_delete_file_pos.col_name);
    schema_fields.push_back(pos_field);

    TIcebergSchema iceberg_schema = TIcebergSchema();
    iceberg_schema.__set_fields(schema_fields);

    std::atomic<int32_t> lazy_column_coalesce_counter = 0;
    scanner_ctx->timezone = timezone;
    scanner_ctx->slot_descs = slot_descriptors;
    scanner_ctx->lake_schema = &iceberg_schema;
    scanner_ctx->materialized_columns = std::move(columns);
    scanner_ctx->scan_range = &scan_range;
    scanner_ctx->lazy_column_coalesce_counter = &lazy_column_coalesce_counter;
    scanner_ctx->stats = &app_scan_stats;
    RETURN_IF_ERROR(reader->init(scanner_ctx.get()));

    while (true) {
        ChunkPtr chunk = ChunkHelper::new_chunk(slot_descriptors, _runtime_state->chunk_size());
        Status status = reader->get_next(&chunk);
        if (status.is_end_of_file()) {
            break;
        }

        RETURN_IF_ERROR(status);
        RETURN_IF_ERROR(fill_skip_rowids(chunk));
    }
    _skip_rows_ctx->deletion_bitmap = _deletion_bitmap;
    update_delete_file_io_counter(_params.profile->runtime_profile, app_scan_stats, fs_scan_stats, cache_input_stream,
                                  shared_buffered_input_stream);
    return Status::OK();
}

Status IcebergDeleteBuilder::build_orc(const TIcebergDeleteFile& delete_file) const {
    std::vector slot_descriptors{&(IcebergDeleteFileMeta::get_delete_file_path_slot()),
                                 &(IcebergDeleteFileMeta::get_delete_file_pos_slot())};

    HdfsScanStats app_scan_stats;
    HdfsScanStats fs_scan_stats;
    std::shared_ptr<io::SharedBufferedInputStream> shared_buffered_input_stream;
    std::shared_ptr<io::CacheInputStream> cache_input_stream;

    ASSIGN_OR_RETURN(auto file, open_random_access_file(delete_file, fs_scan_stats, app_scan_stats,
                                                        shared_buffered_input_stream, cache_input_stream));

    auto input_stream = std::make_unique<ORCHdfsFileStream>(file.get(), delete_file.length, nullptr);
    std::unique_ptr<orc::Reader> reader;
    try {
        orc::ReaderOptions options;
        reader = createReader(std::move(input_stream), options);
    } catch (std::exception& e) {
        auto s =
                strings::Substitute("ORCPositionDeleteBuilder::build create orc::Reader failed. reason = $0", e.what());
        LOG(WARNING) << s;
        return Status::InternalError(s);
    }

    auto orc_reader = std::make_unique<OrcChunkReader>(_runtime_state->chunk_size(), slot_descriptors);
    orc_reader->disable_broker_load_mode();
    orc_reader->set_current_file_name(delete_file.full_path);
    RETURN_IF_ERROR(orc_reader->set_timezone(_runtime_state->timezone()));
    RETURN_IF_ERROR(orc_reader->init(std::move(reader)));

    orc::RowReader::ReadPosition position;
    Status s;

    while (true) {
        s = orc_reader->read_next(&position);
        if (s.is_end_of_file()) {
            break;
        }

        RETURN_IF_ERROR(s);

        auto ret = orc_reader->get_chunk();
        if (!ret.ok()) {
            return ret.status();
        }
        RETURN_IF_ERROR(fill_skip_rowids(ret.value()));
    }
    _skip_rows_ctx->deletion_bitmap = _deletion_bitmap;
    update_delete_file_io_counter(_params.profile->runtime_profile, app_scan_stats, fs_scan_stats, cache_input_stream,
                                  shared_buffered_input_stream);
    return Status::OK();
}

SlotDescriptor IcebergDeleteFileMeta::gen_slot_helper(const IcebergColumnMeta& meta) {
    TSlotDescriptor desc;
    desc.__set_id(meta.id);
    desc.__set_parent(-1);
    TTypeNode type_node;
    type_node.__set_type(TTypeNodeType::SCALAR);
    type_node.__set_scalar_type({});
    type_node.scalar_type.__set_type(meta.type);
    type_node.scalar_type.__set_len(-1);
    desc.__set_slotType({});
    desc.slotType.__set_types({type_node});
    desc.__set_colName(meta.col_name);
    desc.__set_slotIdx(meta.id);
    desc.__set_isMaterialized(true);
    desc.__set_nullIndicatorByte(0);
    desc.__set_nullIndicatorBit(-1);

    return {desc};
}

void IcebergDeleteBuilder::update_delete_file_io_counter(
        RuntimeProfile* parent_profile, const HdfsScanStats& app_stats, const HdfsScanStats& fs_stats,
        const std::shared_ptr<io::CacheInputStream>& cache_input_stream,
        const std::shared_ptr<io::SharedBufferedInputStream>& shared_buffered_input_stream) {
    const std::string ICEBERG_TIMER = "ICEBERG_V2_MOR";
    ADD_COUNTER(parent_profile, ICEBERG_TIMER, TUnit::NONE);
    {
        static const char* prefix = "MOR_InputStream";
        ADD_CHILD_COUNTER(parent_profile, prefix, TUnit::NONE, ICEBERG_TIMER);

        RuntimeProfile::Counter* app_io_bytes_read_counter =
                ADD_CHILD_COUNTER(parent_profile, "MOR_AppIOBytesRead", TUnit::BYTES, prefix);
        RuntimeProfile::Counter* app_io_timer = ADD_CHILD_TIMER(parent_profile, "MOR_AppIOTime", prefix);
        RuntimeProfile::Counter* app_io_counter =
                ADD_CHILD_COUNTER(parent_profile, "MOR_AppIOCounter", TUnit::UNIT, prefix);

        RuntimeProfile::Counter* fs_bytes_read_counter =
                ADD_CHILD_COUNTER(parent_profile, "MOR_FSIOBytesRead", TUnit::BYTES, prefix);
        RuntimeProfile::Counter* fs_io_timer = ADD_CHILD_TIMER(parent_profile, "MOR_FSIOTime", prefix);
        RuntimeProfile::Counter* fs_io_counter =
                ADD_CHILD_COUNTER(parent_profile, "MOR_FSIOCounter", TUnit::UNIT, prefix);

        COUNTER_UPDATE(app_io_timer, app_stats.io_ns);
        COUNTER_UPDATE(app_io_counter, app_stats.io_count);
        COUNTER_UPDATE(app_io_bytes_read_counter, app_stats.bytes_read);
        COUNTER_UPDATE(fs_bytes_read_counter, fs_stats.bytes_read);
        COUNTER_UPDATE(fs_io_timer, fs_stats.io_ns);
        COUNTER_UPDATE(fs_io_counter, fs_stats.io_count);
    }

    {
        static const char* prefix = "MOR_SharedBuffered";
        ADD_CHILD_COUNTER(parent_profile, prefix, TUnit::NONE, ICEBERG_TIMER);
        RuntimeProfile::Counter* shared_buffered_shared_io_bytes =
                ADD_CHILD_COUNTER(parent_profile, "MOR_SharedIOBytes", TUnit::BYTES, prefix);
        RuntimeProfile::Counter* shared_buffered_shared_align_io_bytes =
                ADD_CHILD_COUNTER(parent_profile, "MOR_SharedAlignIOBytes", TUnit::BYTES, prefix);
        RuntimeProfile::Counter* shared_buffered_shared_io_count =
                ADD_CHILD_COUNTER(parent_profile, "MOR_SharedIOCount", TUnit::UNIT, prefix);
        RuntimeProfile::Counter* shared_buffered_shared_io_timer =
                ADD_CHILD_TIMER(parent_profile, "SharedIOTime", prefix);
        RuntimeProfile::Counter* shared_buffered_direct_io_bytes =
                ADD_CHILD_COUNTER(parent_profile, "MOR_DirectIOBytes", TUnit::BYTES, prefix);
        RuntimeProfile::Counter* shared_buffered_direct_io_count =
                ADD_CHILD_COUNTER(parent_profile, "MOR_DirectIOCount", TUnit::UNIT, prefix);
        RuntimeProfile::Counter* shared_buffered_direct_io_timer =
                ADD_CHILD_TIMER(parent_profile, "MOR_DirectIOTime", prefix);

        COUNTER_UPDATE(shared_buffered_shared_io_count, shared_buffered_input_stream->shared_io_count());
        COUNTER_UPDATE(shared_buffered_shared_io_bytes, shared_buffered_input_stream->shared_io_bytes());
        COUNTER_UPDATE(shared_buffered_shared_align_io_bytes, shared_buffered_input_stream->shared_align_io_bytes());
        COUNTER_UPDATE(shared_buffered_shared_io_timer, shared_buffered_input_stream->shared_io_timer());
        COUNTER_UPDATE(shared_buffered_direct_io_count, shared_buffered_input_stream->direct_io_count());
        COUNTER_UPDATE(shared_buffered_direct_io_bytes, shared_buffered_input_stream->direct_io_bytes());
        COUNTER_UPDATE(shared_buffered_direct_io_timer, shared_buffered_input_stream->direct_io_timer());
    }

    if (cache_input_stream) {
        static const char* prefix = "MOR_DataCache";
        ADD_CHILD_COUNTER(parent_profile, prefix, TUnit::NONE, ICEBERG_TIMER);
        RuntimeProfile::Counter* datacache_read_counter =
                ADD_CHILD_COUNTER(parent_profile, "MOR_DataCacheReadCounter", TUnit::UNIT, prefix);
        RuntimeProfile::Counter* datacache_read_bytes =
                ADD_CHILD_COUNTER(parent_profile, "MOR_DataCacheReadBytes", TUnit::BYTES, prefix);
        RuntimeProfile::Counter* datacache_read_mem_bytes =
                ADD_CHILD_COUNTER(parent_profile, "MOR_DataCacheReadMemBytes", TUnit::BYTES, "MOR_DataCacheReadBytes");
        RuntimeProfile::Counter* datacache_read_disk_bytes =
                ADD_CHILD_COUNTER(parent_profile, "MOR_DataCacheReadDiskBytes", TUnit::BYTES, "MOR_DataCacheReadBytes");
        RuntimeProfile::Counter* datacache_skip_read_counter =
                ADD_CHILD_COUNTER(parent_profile, "MOR_DataCacheSkipReadCounter", TUnit::UNIT, prefix);
        RuntimeProfile::Counter* datacache_skip_read_bytes =
                ADD_CHILD_COUNTER(parent_profile, "MOR_DataCacheSkipReadBytes", TUnit::BYTES, prefix);
        RuntimeProfile::Counter* datacache_read_timer =
                ADD_CHILD_TIMER(parent_profile, "MOR_DataCacheReadTimer", prefix);
        RuntimeProfile::Counter* datacache_write_counter =
                ADD_CHILD_COUNTER(parent_profile, "MOR_DataCacheWriteCounter", TUnit::UNIT, prefix);
        RuntimeProfile::Counter* datacache_write_bytes =
                ADD_CHILD_COUNTER(parent_profile, "MOR_DataCacheWriteBytes", TUnit::BYTES, prefix);
        RuntimeProfile::Counter* datacache_write_timer =
                ADD_CHILD_TIMER(parent_profile, "MOR_DataCacheWriteTimer", prefix);
        RuntimeProfile::Counter* datacache_write_fail_counter =
                ADD_CHILD_COUNTER(parent_profile, "MOR_DataCacheWriteFailCounter", TUnit::UNIT, prefix);
        RuntimeProfile::Counter* datacache_write_fail_bytes =
                ADD_CHILD_COUNTER(parent_profile, "MOR_DataCacheWriteFailBytes", TUnit::BYTES, prefix);
        RuntimeProfile::Counter* datacache_read_block_buffer_counter =
                ADD_CHILD_COUNTER(parent_profile, "MOR_DataCacheReadBlockBufferCounter", TUnit::UNIT, prefix);
        RuntimeProfile::Counter* datacache_read_block_buffer_bytes =
                ADD_CHILD_COUNTER(parent_profile, "MOR_DataCacheReadBlockBufferBytes", TUnit::BYTES, prefix);

        const io::CacheInputStream::Stats& stats = cache_input_stream->stats();
        COUNTER_UPDATE(datacache_read_counter, stats.read_cache_count);
        COUNTER_UPDATE(datacache_read_bytes, stats.read_cache_bytes);
        COUNTER_UPDATE(datacache_read_mem_bytes, stats.read_mem_cache_bytes);
        COUNTER_UPDATE(datacache_read_disk_bytes, stats.read_disk_cache_bytes);
        COUNTER_UPDATE(datacache_read_timer, stats.read_cache_ns);
        COUNTER_UPDATE(datacache_skip_read_counter, stats.skip_read_cache_count);
        COUNTER_UPDATE(datacache_skip_read_bytes, stats.skip_read_cache_bytes);
        COUNTER_UPDATE(datacache_write_counter, stats.write_cache_count);
        COUNTER_UPDATE(datacache_write_bytes, stats.write_cache_bytes);
        COUNTER_UPDATE(datacache_write_timer, stats.write_cache_ns);
        COUNTER_UPDATE(datacache_write_fail_counter, stats.write_cache_fail_count);
        COUNTER_UPDATE(datacache_write_fail_bytes, stats.write_cache_fail_bytes);
        COUNTER_UPDATE(datacache_read_block_buffer_counter, stats.read_block_buffer_count);
        COUNTER_UPDATE(datacache_read_block_buffer_bytes, stats.read_block_buffer_bytes);
    }
}

SlotDescriptor& IcebergDeleteFileMeta::get_delete_file_path_slot() {
    static SlotDescriptor k_delete_file_path_slot = gen_slot_helper(k_delete_file_path);

    return k_delete_file_path_slot;
}

SlotDescriptor& IcebergDeleteFileMeta::get_delete_file_pos_slot() {
    static SlotDescriptor k_delete_file_pos_slot = gen_slot_helper(k_delete_file_pos);

    return k_delete_file_pos_slot;
}
} // namespace starrocks
