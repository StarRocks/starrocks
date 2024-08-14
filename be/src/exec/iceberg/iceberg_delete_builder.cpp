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

#include "column/vectorized_fwd.h"
#include "exec/iceberg/iceberg_delete_file_iterator.h"
#include "formats/orc/orc_chunk_reader.h"
#include "formats/orc/orc_input_stream.h"
#include "formats/parquet/file_reader.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"
#include "storage/chunk_helper.h"

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

Status ParquetPositionDeleteBuilder::build(const std::string& timezone, const std::string& delete_file_path,
                                           int64_t file_length, std::set<int64_t>* need_skip_rowids,
                                           const HdfsScannerParams& scanner_params, RuntimeState* state) {
    std::unique_ptr<RandomAccessFile> raw_file;
    ASSIGN_OR_RETURN(raw_file, _fs->new_random_access_file(delete_file_path));
    std::vector slot_descriptors{&(IcebergDeleteFileMeta::get_delete_file_path_slot()),
                                 &(IcebergDeleteFileMeta::get_delete_file_pos_slot())};

    HdfsScanStats app_scan_stats;
    HdfsScanStats fs_scan_stats;

    const int64_t file_size = file_length;
    raw_file->set_size(file_size);
    const std::string& filename = raw_file->filename();

    std::shared_ptr<io::SeekableInputStream> input_stream = raw_file->stream();

    input_stream = std::make_shared<CountedSeekableInputStream>(input_stream, &fs_scan_stats);

    auto shared_buffered_input_stream =
            std::make_shared<io::SharedBufferedInputStream>(input_stream, filename, file_size);
    const io::SharedBufferedInputStream::CoalesceOptions options = {
            .max_dist_size = config::io_coalesce_read_max_distance_size,
            .max_buffer_size = config::io_coalesce_read_max_buffer_size};
    shared_buffered_input_stream->set_coalesce_options(options);
    if (file_size < config::io_coalesce_read_max_buffer_size) {
        std::vector<io::SharedBufferedInputStream::IORange> io_ranges{};
        io_ranges.emplace_back(0, file_size);
        RETURN_IF_ERROR(shared_buffered_input_stream->set_io_ranges(io_ranges));
    }

    input_stream = shared_buffered_input_stream;

    std::shared_ptr<io::CacheInputStream> cache_input_stream = nullptr;
    // input_stream = CacheInputStream(input_stream)
    if (scanner_params.use_datacache) {
        cache_input_stream = std::make_shared<io::CacheInputStream>(shared_buffered_input_stream, filename, file_size,
                                                                    scanner_params.modification_time);
        cache_input_stream->set_enable_populate_cache(scanner_params.enable_populate_datacache);
        cache_input_stream->set_enable_async_populate_mode(scanner_params.enable_datacache_async_populate_mode);
        cache_input_stream->set_enable_cache_io_adaptor(scanner_params.enable_datacache_io_adaptor);
        cache_input_stream->set_enable_block_buffer(config::datacache_block_buffer_enable);
        shared_buffered_input_stream->set_align_size(cache_input_stream->get_align_size());
        input_stream = cache_input_stream;
    }

    // input_stream = CountedInputStream(input_stream)
    // NOTE: make sure `CountedInputStream` is last applied, so io time can be accurately timed.
    input_stream = std::make_shared<CountedSeekableInputStream>(input_stream, &app_scan_stats);

    // so wrap function is f(x) = (CountedInputStream (CacheInputStream (DecompressInputStream (CountedInputStream x))))
    auto file = std::make_unique<RandomAccessFile>(input_stream, filename);
    file->set_size(file_size);

    std::unique_ptr<parquet::FileReader> reader;
    try {
        reader = std::make_unique<parquet::FileReader>(state->chunk_size(), file.get(), file->get_size().value(), 0);
    } catch (std::exception& e) {
        const auto s = strings::Substitute(
                "ParquetPositionDeleteBuilder::build create parquet::FileReader failed. reason = $0", e.what());
        LOG(WARNING) << s;
        return Status::InternalError(s);
    }

    auto scanner_ctx = std::make_unique<HdfsScannerContext>();
    std::vector<HdfsScannerContext::ColumnInfo> columns;
    THdfsScanRange scan_range;
    scan_range.offset = 0;
    scan_range.length = file_length;
    for (size_t i = 0; i < slot_descriptors.size(); i++) {
        auto* slot = slot_descriptors[i];
        HdfsScannerContext::ColumnInfo column;
        column.slot_desc = slot;
        column.idx_in_chunk = i;
        column.decode_needed = true;
        columns.emplace_back(column);
    }

    std::vector<TIcebergSchemaField> schema_fields;

    TIcebergSchemaField file_path_field = TIcebergSchemaField();
    file_path_field.__set_field_id(k_delete_file_path.id);
    file_path_field.__set_name(k_delete_file_path.col_name);

    TIcebergSchemaField pos_field = TIcebergSchemaField();
    pos_field.__set_field_id(k_delete_file_pos.id);
    pos_field.__set_name(k_delete_file_pos.col_name);

    schema_fields.push_back(file_path_field);
    schema_fields.push_back(pos_field);

    TIcebergSchema iceberg_schema = TIcebergSchema();
    iceberg_schema.__set_fields(schema_fields);

    scanner_ctx->timezone = timezone;
    scanner_ctx->slot_descs = slot_descriptors;
    scanner_ctx->iceberg_schema = &iceberg_schema;
    scanner_ctx->materialized_columns = std::move(columns);
    scanner_ctx->scan_range = &scan_range;
    scanner_ctx->lazy_column_coalesce_counter = &_lazy_column_coalesce_counter;
    scanner_ctx->stats = &app_scan_stats;
    RETURN_IF_ERROR(reader->init(scanner_ctx.get()));

    while (true) {
        ChunkPtr chunk = ChunkHelper::new_chunk(slot_descriptors, state->chunk_size());
        Status status = reader->get_next(&chunk);
        if (status.is_end_of_file()) {
            break;
        }

        ColumnPtr& file_path = chunk->get_column_by_slot_id(k_delete_file_path.id);
        ColumnPtr& pos = chunk->get_column_by_slot_id(k_delete_file_pos.id);
        for (int i = 0; i < chunk->num_rows(); i++) {
            if (file_path->get(i).get_slice() == _datafile_path) {
                need_skip_rowids->emplace(pos->get(i).get_int64());
            }
        }

        RETURN_IF_ERROR(status);
    }
    update_v2_io_counter(scanner_params.profile->runtime_profile, app_scan_stats, fs_scan_stats, cache_input_stream,
                         shared_buffered_input_stream);
    return Status::OK();
}

Status ORCPositionDeleteBuilder::build(const std::string& timezone, const std::string& delete_file_path,
                                       int64_t file_length, std::set<int64_t>* need_skip_rowids,
                                       const HdfsScannerParams& scanner_params, RuntimeState* state) {
    std::vector<SlotDescriptor*> slot_descriptors{&(IcebergDeleteFileMeta::get_delete_file_path_slot()),
                                                  &(IcebergDeleteFileMeta::get_delete_file_pos_slot())};

    std::unique_ptr<RandomAccessFile> file;
    ASSIGN_OR_RETURN(file, _fs->new_random_access_file(delete_file_path));

    auto input_stream = std::make_unique<ORCHdfsFileStream>(file.get(), file_length, nullptr);
    std::unique_ptr<orc::Reader> reader;
    try {
        orc::ReaderOptions options;
        reader = orc::createReader(std::move(input_stream), options);
    } catch (std::exception& e) {
        auto s =
                strings::Substitute("ORCPositionDeleteBuilder::build create orc::Reader failed. reason = $0", e.what());
        LOG(WARNING) << s;
        return Status::InternalError(s);
    }

    auto orc_reader = std::make_unique<OrcChunkReader>(4096, slot_descriptors);
    orc_reader->disable_broker_load_mode();
    orc_reader->set_current_file_name(delete_file_path);
    RETURN_IF_ERROR(orc_reader->set_timezone(timezone));
    RETURN_IF_ERROR(orc_reader->init(std::move(reader)));

    orc::RowReader::ReadPosition position;
    Status s;

    while (true) {
        s = orc_reader->read_next(&position);
        if (s.is_end_of_file()) {
            return Status::OK();
        }

        RETURN_IF_ERROR(s);

        auto ret = orc_reader->get_chunk();
        if (!ret.ok()) {
            return ret.status();
        }

        ChunkPtr chunk = ret.value();
        size_t chunk_size = chunk->num_rows();
        const auto& slot_id_to_idx = chunk->get_slot_id_to_index_map();
        if (!slot_id_to_idx.contains(k_delete_file_path.id) || !slot_id_to_idx.contains(k_delete_file_pos.id)) {
            auto str = strings::Substitute("delete file schema doesn't meet requirement, need: [file_path, pos]");
            LOG(WARNING) << str;
            return Status::InternalError(str);
        }

        auto* file_path_col = static_cast<BinaryColumn*>(chunk->get_column_by_slot_id(k_delete_file_path.id).get());
        auto* position_col = static_cast<Int64Column*>(chunk->get_column_by_slot_id(k_delete_file_pos.id).get());
        for (auto row = 0; row < chunk_size; row++) {
            if (file_path_col->get_slice(row) != _datafile_path) {
                continue;
            }
            need_skip_rowids->emplace(position_col->get_data()[row]);
        }
    }
}

Status ORCEqualityDeleteBuilder::build(const std::string& timezone, const std::string& delete_file_path,
                                       int64_t file_length, const std::shared_ptr<DefaultMORProcessor> mor_processor,
                                       std::vector<SlotDescriptor*> slot_descs,
                                       TupleDescriptor* delete_column_tuple_desc,
                                       const TIcebergSchema* iceberg_equal_delete_schema, RuntimeState* state,
                                       const HdfsScannerParams& scanner_params) {
    std::unique_ptr<RandomAccessFile> file;
    ASSIGN_OR_RETURN(file, _fs->new_random_access_file(delete_file_path));

    auto input_stream = std::make_unique<ORCHdfsFileStream>(file.get(), file_length, nullptr);
    std::unique_ptr<orc::Reader> reader;
    try {
        const orc::ReaderOptions options;
        reader = createReader(std::move(input_stream), options);
    } catch (std::exception& e) {
        const auto s =
                strings::Substitute("ORCEqualityDeleteBuilder::build create orc::Reader failed. reason = $0", e.what());
        LOG(WARNING) << s;
        return Status::InternalError(s);
    }

    const auto orc_reader = std::make_unique<OrcChunkReader>(4096, slot_descs);
    orc_reader->disable_broker_load_mode();
    orc_reader->set_current_file_name(delete_file_path);
    RETURN_IF_ERROR(orc_reader->set_timezone(timezone));
    RETURN_IF_ERROR(orc_reader->init(std::move(reader)));

    orc::RowReader::ReadPosition position;

    while (true) {
        Status status = orc_reader->read_next(&position);
        if (status.is_end_of_file()) {
            break;
        }

        RETURN_IF_ERROR(status);

        auto ret = orc_reader->get_chunk();
        if (!ret.ok()) {
            return ret.status();
        }

        ChunkPtr chunk = ret.value();
        RETURN_IF_ERROR(mor_processor->append_chunk_to_hashtable(chunk));
    }
    return Status::OK();
}

Status ParquetEqualityDeleteBuilder::build(const std::string& timezone, const std::string& file_path,
                                           int64_t file_length,
                                           const std::shared_ptr<DefaultMORProcessor> mor_processor,
                                           std::vector<SlotDescriptor*> slot_descs,
                                           TupleDescriptor* delete_column_tuple_desc,
                                           const TIcebergSchema* iceberg_equal_delete_schema, RuntimeState* state,
                                           const HdfsScannerParams& scanner_params) {
    std::unique_ptr<RandomAccessFile> raw_file;
    ASSIGN_OR_RETURN(raw_file, _fs->new_random_access_file(file_path));

    HdfsScanStats app_scan_stats;
    HdfsScanStats fs_scan_stats;

    const int64_t file_size = file_length;
    raw_file->set_size(file_size);
    const std::string& filename = raw_file->filename();

    std::shared_ptr<io::SeekableInputStream> input_stream = raw_file->stream();

    input_stream = std::make_shared<CountedSeekableInputStream>(input_stream, &fs_scan_stats);

    auto shared_buffered_input_stream =
            std::make_shared<io::SharedBufferedInputStream>(input_stream, filename, file_size);
    const io::SharedBufferedInputStream::CoalesceOptions options = {
            .max_dist_size = config::io_coalesce_read_max_distance_size,
            .max_buffer_size = config::io_coalesce_read_max_buffer_size};
    shared_buffered_input_stream->set_coalesce_options(options);
    if (file_size < config::io_coalesce_read_max_buffer_size) {
        std::vector<io::SharedBufferedInputStream::IORange> io_ranges{};
        io_ranges.emplace_back(0, file_size);
        RETURN_IF_ERROR(shared_buffered_input_stream->set_io_ranges(io_ranges));
    }

    input_stream = shared_buffered_input_stream;

    std::shared_ptr<io::CacheInputStream> cache_input_stream = nullptr;
    // input_stream = CacheInputStream(input_stream)
    if (scanner_params.use_datacache) {
        cache_input_stream = std::make_shared<io::CacheInputStream>(shared_buffered_input_stream, filename, file_size,
                                                                    scanner_params.modification_time);
        cache_input_stream->set_enable_populate_cache(scanner_params.enable_populate_datacache);
        cache_input_stream->set_enable_async_populate_mode(scanner_params.enable_datacache_async_populate_mode);
        cache_input_stream->set_enable_cache_io_adaptor(scanner_params.enable_datacache_io_adaptor);
        cache_input_stream->set_enable_block_buffer(config::datacache_block_buffer_enable);
        shared_buffered_input_stream->set_align_size(cache_input_stream->get_align_size());
        input_stream = cache_input_stream;
    }

    // input_stream = CountedInputStream(input_stream)
    // NOTE: make sure `CountedInputStream` is last applied, so io time can be accurately timed.
    input_stream = std::make_shared<CountedSeekableInputStream>(input_stream, &app_scan_stats);

    // so wrap function is f(x) = (CountedInputStream (CacheInputStream (DecompressInputStream (CountedInputStream x))))
    auto file = std::make_unique<RandomAccessFile>(input_stream, filename);
    file->set_size(file_size);

    std::unique_ptr<parquet::FileReader> reader;
    try {
        reader = std::make_unique<parquet::FileReader>(state->chunk_size(), file.get(), file->get_size().value(), 0);
    } catch (std::exception& e) {
        const auto s = strings::Substitute(
                "ParquetEqualityDeleteBuilder::build create parquet::FileReader failed. reason = $0", e.what());
        LOG(WARNING) << s;
        return Status::InternalError(s);
    }

    auto scanner_ctx = std::make_unique<HdfsScannerContext>();
    std::vector<HdfsScannerContext::ColumnInfo> columns;
    THdfsScanRange scan_range;
    scan_range.offset = 0;
    scan_range.length = file_length;
    for (size_t i = 0; i < slot_descs.size(); i++) {
        auto* slot = slot_descs[i];
        HdfsScannerContext::ColumnInfo column;
        column.slot_desc = slot;
        column.idx_in_chunk = i;
        column.decode_needed = true;
        columns.emplace_back(column);
    }
    scanner_ctx->timezone = timezone;
    scanner_ctx->slot_descs = delete_column_tuple_desc->slots();
    scanner_ctx->iceberg_schema = iceberg_equal_delete_schema;
    scanner_ctx->materialized_columns = std::move(columns);
    scanner_ctx->scan_range = &scan_range;
    scanner_ctx->lazy_column_coalesce_counter = &_lazy_column_coalesce_counter;
    scanner_ctx->stats = &app_scan_stats;
    RETURN_IF_ERROR(reader->init(scanner_ctx.get()));

    int64_t hashtable_rows = 0;
    while (true) {
        ChunkPtr chunk = ChunkHelper::new_chunk(*delete_column_tuple_desc, state->chunk_size());
        Status status = reader->get_next(&chunk);
        if (status.is_end_of_file()) {
            break;
        }

        RETURN_IF_ERROR(status);
        ChunkPtr& result = chunk;
        hashtable_rows += result->num_rows();
        RETURN_IF_ERROR(mor_processor->append_chunk_to_hashtable(result));
    }

    update_v2_io_counter(scanner_params.profile->runtime_profile, app_scan_stats, fs_scan_stats, cache_input_stream,
                         shared_buffered_input_stream, hashtable_rows);
    return Status::OK();
}

void ParquetPositionDeleteBuilder::update_v2_io_counter(
        RuntimeProfile* parent_profile, const HdfsScanStats& app_stats, const HdfsScanStats& fs_stats,
        std::shared_ptr<io::CacheInputStream> cache_input_stream,
        std::shared_ptr<io::SharedBufferedInputStream> shared_buffered_input_stream) {
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

void ParquetEqualityDeleteBuilder::update_v2_io_counter(
        RuntimeProfile* parent_profile, const HdfsScanStats& app_stats, const HdfsScanStats& fs_stats,
        std::shared_ptr<io::CacheInputStream> cache_input_stream,
        std::shared_ptr<io::SharedBufferedInputStream> shared_buffered_input_stream, int64_t hashtable_rows) {
    const std::string ICEBERG_TIMER = "ICEBERG_V2_MOR";
    ADD_COUNTER(parent_profile, ICEBERG_TIMER, TUnit::NONE);

    {
        RuntimeProfile::Counter* app_io_bytes_read_counter =
                ADD_CHILD_COUNTER(parent_profile, "MOR_HashTableRows", TUnit::UNIT, ICEBERG_TIMER);
        COUNTER_UPDATE(app_io_bytes_read_counter, hashtable_rows);
    }

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

SlotDescriptor& IcebergDeleteFileMeta::get_delete_file_path_slot() {
    static SlotDescriptor k_delete_file_path_slot = IcebergDeleteFileMeta::gen_slot_helper(k_delete_file_path);

    return k_delete_file_path_slot;
}

SlotDescriptor& IcebergDeleteFileMeta::get_delete_file_pos_slot() {
    static SlotDescriptor k_delete_file_pos_slot = IcebergDeleteFileMeta::gen_slot_helper(k_delete_file_pos);

    return k_delete_file_pos_slot;
}
} // namespace starrocks
