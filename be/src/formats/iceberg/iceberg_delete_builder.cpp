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

#include "formats/iceberg/iceberg_delete_builder.h"

#include "base/concurrency/stopwatch.hpp"
#include "base/utility/defer_op.h"
#include "cache/scan/cache_input_stream.h"
#include "cache/scan/shared_buffered_input_stream.h"
#include "column/vectorized_fwd.h"
#include "common/config_scan_io_fwd.h"
#include "common/runtime_profile.h"
#include "formats/file_input_stream.h"
#include "formats/orc/orc_chunk_reader.h"
#include "formats/orc/orc_input_stream.h"
#include "formats/parquet/file_reader.h"
#include "formats/scan_context.h"
#include "formats/utils.h"
#include "gen_cpp/Types_types.h"
#include "runtime/chunk_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage_primitive/predicate_tree/predicate_tree.h"

namespace starrocks::formats {

struct IcebergColumnMeta {
    int64_t id;
    std::string col_name;
    TPrimitiveType::type type;
};

static const IcebergColumnMeta k_delete_file_path{
        .id = INT32_MAX - 101, .col_name = "file_path", .type = TPrimitiveType::VARCHAR};

static const IcebergColumnMeta k_delete_file_pos{
        .id = INT32_MAX - 102, .col_name = "pos", .type = TPrimitiveType::BIGINT};

namespace {

Status visit_position_delete_rows(const ChunkPtr& chunk, const IcebergPositionDeleteReader::RowCallback& cb) {
    const ColumnPtr& file_path = chunk->get_column_by_slot_id(k_delete_file_path.id);
    const ColumnPtr& pos = chunk->get_column_by_slot_id(k_delete_file_pos.id);
    if (file_path == nullptr || pos == nullptr) {
        return Status::InternalError("position-delete chunk is missing file_path/pos columns");
    }
    for (int i = 0; i < chunk->num_rows(); i++) {
        cb(file_path->get(i).get_slice(), pos->get(i).get_int64());
    }
    return Status::OK();
}

Status read_parquet_rows(RandomAccessFile* file, int64_t length, int32_t chunk_size, const std::string& timezone,
                         const FormatScannerOptions& options, FormatScannerStats* stats,
                         const IcebergPositionDeleteReader::RowCallback& cb) {
    std::unique_ptr<parquet::FileReader> reader;
    try {
        reader = std::make_unique<parquet::FileReader>(chunk_size, file, length);
    } catch (std::exception& e) {
        const auto s = strings::Substitute(
                "IcebergPositionDeleteReader: create parquet::FileReader failed. reason = $0", e.what());
        LOG(WARNING) << s;
        return Status::InternalError(s);
    }

    std::vector slot_descriptors{&(IcebergDeleteFileMeta::get_delete_file_path_slot()),
                                 &(IcebergDeleteFileMeta::get_delete_file_pos_slot())};

    std::vector<FormatColumnInfo> columns;
    for (size_t i = 0; i < slot_descriptors.size(); i++) {
        FormatColumnInfo column;
        column.slot_desc = slot_descriptors[i];
        column.idx_in_chunk = i;
        column.decode_needed = true;
        columns.emplace_back(column);
    }

    std::vector<TIcebergSchemaField> schema_fields;
    for (const auto* slot : slot_descriptors) {
        TIcebergSchemaField field;
        field.__set_field_id(slot->id());
        field.__set_name(std::string(slot->col_name()));
        schema_fields.push_back(field);
    }
    TIcebergSchema iceberg_schema;
    iceberg_schema.__set_fields(schema_fields);

    std::atomic<int32_t> lazy_column_coalesce_counter = 0;
    // TODO: Remove this empty placeholder once FileReader supports a null predicate tree for predicate-free scans.
    PredicateTree predicate_tree;
    FormatScanContext format_scan_context;
    format_scan_context.timezone = timezone;
    format_scan_context.materialized_columns = std::move(columns);
    format_scan_context.stats = stats;
    format_scan_context.options = options;
    format_scan_context.options.enable_split_tasks = false;
    format_scan_context.lake_schema = &iceberg_schema;
    format_scan_context.scan_range_offset = 0;
    format_scan_context.scan_range_length = length;
    format_scan_context.lazy_column_coalesce_counter = &lazy_column_coalesce_counter;
    format_scan_context.predicate_tree = &predicate_tree;
    RETURN_IF_ERROR(reader->init(&format_scan_context));

    while (true) {
        ASSIGN_OR_RETURN(ChunkPtr chunk, RuntimeChunkHelper::new_chunk_checked(slot_descriptors, chunk_size));
        Status status = reader->get_next(&chunk);
        if (status.is_end_of_file()) {
            break;
        }
        RETURN_IF_ERROR(status);
        RETURN_IF_ERROR(visit_position_delete_rows(chunk, cb));
    }
    return Status::OK();
}

Status read_orc_rows(RandomAccessFile* file, const std::string& path, int64_t length, int32_t chunk_size,
                     const std::string& timezone, const IcebergPositionDeleteReader::RowCallback& cb) {
    std::vector slot_descriptors{&(IcebergDeleteFileMeta::get_delete_file_path_slot()),
                                 &(IcebergDeleteFileMeta::get_delete_file_pos_slot())};

    auto input_stream = std::make_unique<ORCHdfsFileStream>(file, length, nullptr);
    std::unique_ptr<orc::Reader> reader;
    try {
        orc::ReaderOptions options;
        reader = createReader(std::move(input_stream), options);
    } catch (std::exception& e) {
        auto s = strings::Substitute("IcebergPositionDeleteReader: create orc::Reader failed. reason = $0", e.what());
        LOG(WARNING) << s;
        return Status::InternalError(s);
    }

    auto orc_reader = std::make_unique<OrcChunkReader>(chunk_size, slot_descriptors);
    orc_reader->disable_broker_load_mode();
    orc_reader->set_current_file_name(path);
    RETURN_IF_ERROR(orc_reader->set_timezone(timezone));
    RETURN_IF_ERROR(orc_reader->init(std::move(reader)));

    orc::RowReader::ReadPosition position;
    while (true) {
        Status s = orc_reader->read_next(&position);
        if (s.is_end_of_file()) {
            break;
        }
        RETURN_IF_ERROR(s);
        ASSIGN_OR_RETURN(ChunkPtr chunk, orc_reader->get_chunk());
        RETURN_IF_ERROR(visit_position_delete_rows(chunk, cb));
    }
    return Status::OK();
}

} // namespace

Status IcebergPositionDeleteReader::read_rows(RandomAccessFile* file, const std::string& path, int64_t length,
                                              const std::string& format, int32_t chunk_size,
                                              const std::string& timezone, const FormatScannerOptions& options,
                                              FormatScannerStats* stats, const RowCallback& cb) {
    FormatScannerStats local_stats;
    if (stats == nullptr) {
        stats = &local_stats;
    }
    if (format == PARQUET) {
        return read_parquet_rows(file, length, chunk_size, timezone, options, stats, cb);
    }
    if (format == ORC) {
        return read_orc_rows(file, path, length, chunk_size, timezone, cb);
    }
    return Status::NotSupported(strings::Substitute("unsupported iceberg position-delete file format: $0", format));
}

StatusOr<std::unique_ptr<RandomAccessFile>> IcebergDeleteBuilder::open_cached_file(
        const TIcebergDeleteFile& delete_file, FormatScannerStats& fs_stats, FormatScannerStats& app_stats,
        std::shared_ptr<SharedBufferedInputStream>& shared_buffered_input_stream,
        std::shared_ptr<CacheInputStream>& cache_input_stream) const {
    const FileInputStreamOptions options{.fs = _ctx.fs,
                                         .file_path = delete_file.full_path,
                                         .file_size = delete_file.length,
                                         .fs_stats = &fs_stats,
                                         .app_stats = &app_stats,
                                         .datacache_options = _ctx.datacache_options};
    ASSIGN_OR_RETURN(auto file, create_random_access_file(shared_buffered_input_stream, cache_input_stream, options));
    if (cache_input_stream != nullptr) {
        // Lets a local miss fall back to the node that cache select warmed, as the main data
        // stream does.
        cache_input_stream->set_peer_cache_node(_ctx.candidate_node);
    }
    return file;
}

StatusOr<std::unique_ptr<RandomAccessFile>> IcebergDeleteBuilder::open_random_access_file(
        const TIcebergDeleteFile& delete_file, FormatScannerStats& fs_stats, FormatScannerStats& app_stats,
        std::shared_ptr<SharedBufferedInputStream>& shared_buffered_input_stream,
        std::shared_ptr<CacheInputStream>& cache_input_stream) const {
    ASSIGN_OR_RETURN(auto file, open_cached_file(delete_file, fs_stats, app_stats, shared_buffered_input_stream,
                                                 cache_input_stream));
    // A position-delete file is read end to end, so register the whole file as io ranges.
    std::vector<SharedBufferedInputStream::IORange> io_ranges{};
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

Status IcebergDeleteBuilder::build_deletion_vector(const TIcebergDeleteFile& delete_file) const {
    if (!delete_file.__isset.deletion_vector) {
        return Status::InternalError("Iceberg deletion vector blob descriptor is not set");
    }
    const auto& blob = delete_file.deletion_vector;
    if (!blob.__isset.content_offset || !blob.__isset.content_size_in_bytes) {
        return Status::InternalError(strings::Substitute(
                "Iceberg deletion vector is missing content_offset/content_size_in_bytes: $0", delete_file.full_path));
    }
    // length is what tells create_random_access_file how big the Puffin is; a 0 from an unset
    // optional would silently produce a zero-length view and turn the read into a bogus
    // "blob too small" corruption further down.
    if (!delete_file.__isset.length || delete_file.length <= 0) {
        return Status::InternalError(strings::Substitute(
                "Iceberg deletion vector is missing the puffin file length: $0", delete_file.full_path));
    }
    const int64_t offset = blob.content_offset;
    const int64_t size = blob.content_size_in_bytes;
    // Bound the range against the Puffin size before allocating: a corrupt manifest must fail as
    // Corruption, not as a multi-terabyte allocation.
    if (offset < 0 || size <= 0 || offset > delete_file.length - size) {
        return Status::Corruption(
                strings::Substitute("Iceberg deletion vector range $0+$1 is out of bounds for puffin $2 of $3 bytes",
                                    offset, size, delete_file.full_path, delete_file.length));
    }
    // The DV must belong to the data file this scanner is reading. Checked before any IO: a
    // mismatch means the scan range was assembled wrong, and an unset value is exactly the
    // assembly bug this guards against, so treat it as an error rather than skipping the check.
    if (!blob.__isset.referenced_data_file || blob.referenced_data_file != _ctx.data_file_path) {
        return Status::InternalError(strings::Substitute(
                "Iceberg deletion vector references data file $0 but the scanner is reading $1 [puffin=$2]",
                blob.__isset.referenced_data_file ? blob.referenced_data_file : "<unset>", _ctx.data_file_path,
                delete_file.full_path));
    }

    FormatScannerStats app_stats;
    FormatScannerStats fs_stats;
    std::shared_ptr<SharedBufferedInputStream> shared_buffered_input_stream;
    std::shared_ptr<CacheInputStream> cache_input_stream;
    IcebergDVBuildStats dv_stats;
    // Counterpart of the v2 path's DeleteFileBuildTime. Timed by hand rather than with
    // SCOPED_RAW_TIMER: that would only write build_ns when the function returns, i.e. after
    // update_dv_counter has already published the counters.
    MonotonicStopWatch build_watch;
    build_watch.start();

    std::vector<uint8_t> buffer(size);
    {
        SCOPED_RAW_TIMER(&dv_stats.read_ns);
        // A DV blob is one exact contiguous range, so no io_ranges are registered: the shared
        // buffer stays pass-through and only the DataCache layer wraps the read. Registering the
        // whole puffin here would pull up to io_coalesce_read_max_buffer_size per split.
        ASSIGN_OR_RETURN(auto file, open_cached_file(delete_file, fs_stats, app_stats, shared_buffered_input_stream,
                                                     cache_input_stream));
        RETURN_IF_ERROR(file->read_at_fully(offset, buffer.data(), size));
        dv_stats.read_bytes += size;
    }

    const int64_t record_count = blob.__isset.record_count ? blob.record_count : -1;
    auto res = parse_deletion_vector_blob(buffer.data(), size, record_count, &dv_stats);
    if (!res.ok()) {
        return Status::Corruption(strings::Substitute("$0 [puffin=$1 offset=$2 size=$3 referenced_data_file=$4]",
                                                      std::string(res.status().message()), delete_file.full_path,
                                                      offset, size, blob.referenced_data_file));
    }
    // parse_deletion_vector_blob hands over ownership; merge() only ORs it in, so free it here on
    // every path.
    roaring64_bitmap_t* parsed = res.value();
    DeferOp free_parsed([&parsed] { roaring::api::roaring64_bitmap_free(parsed); });

    _deletion_bitmap->merge(parsed);
    dv_stats.build_ns = static_cast<int64_t>(build_watch.elapsed_time());

    if (_ctx.runtime_profile != nullptr) {
        update_dv_counter(_ctx.runtime_profile, dv_stats, cache_input_stream);
    }
    return Status::OK();
}

void IcebergDeleteBuilder::update_dv_counter(RuntimeProfile* parent_profile, const IcebergDVBuildStats& stats,
                                             const std::shared_ptr<CacheInputStream>& cache_input_stream) {
    static const char* kSection = "IcebergDeletionVector";
    ADD_COUNTER(parent_profile, kSection, TUnit::NONE);
    RuntimeProfile::Counter* build_time = ADD_CHILD_TIMER(parent_profile, "IcebergDVBuildTime", kSection);
    RuntimeProfile::Counter* read_bytes =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDVReadBytes", TUnit::BYTES, kSection);
    RuntimeProfile::Counter* read_time = ADD_CHILD_TIMER(parent_profile, "IcebergDVReadTime", kSection);
    RuntimeProfile::Counter* deser_time = ADD_CHILD_TIMER(parent_profile, "IcebergDVDeserializeTime", kSection);
    RuntimeProfile::Counter* crc_time = ADD_CHILD_TIMER(parent_profile, "IcebergDVChecksumTime", kSection);
    RuntimeProfile::Counter* build_count =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDVBuildCount", TUnit::UNIT, kSection);
    RuntimeProfile::Counter* cardinality =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDVCardinality", TUnit::UNIT, kSection);
    COUNTER_UPDATE(build_time, stats.build_ns);
    COUNTER_UPDATE(read_bytes, stats.read_bytes);
    COUNTER_UPDATE(read_time, stats.read_ns);
    COUNTER_UPDATE(deser_time, stats.deserialize_ns);
    COUNTER_UPDATE(crc_time, stats.checksum_ns);
    COUNTER_UPDATE(build_count, stats.build_count);
    COUNTER_UPDATE(cardinality, stats.cardinality);

    if (cache_input_stream == nullptr) {
        return;
    }
    static const char* kCacheSection = "IcebergDV_DataCache";
    ADD_CHILD_COUNTER(parent_profile, kCacheSection, TUnit::NONE, kSection);
    RuntimeProfile::Counter* cache_read_counter =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDV_DataCacheReadCounter", TUnit::UNIT, kCacheSection);
    RuntimeProfile::Counter* cache_read_bytes =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDV_DataCacheReadBytes", TUnit::BYTES, kCacheSection);
    RuntimeProfile::Counter* cache_read_mem_bytes = ADD_CHILD_COUNTER(parent_profile, "IcebergDV_DataCacheReadMemBytes",
                                                                      TUnit::BYTES, "IcebergDV_DataCacheReadBytes");
    RuntimeProfile::Counter* cache_read_disk_bytes = ADD_CHILD_COUNTER(
            parent_profile, "IcebergDV_DataCacheReadDiskBytes", TUnit::BYTES, "IcebergDV_DataCacheReadBytes");
    RuntimeProfile::Counter* cache_read_timer =
            ADD_CHILD_TIMER(parent_profile, "IcebergDV_DataCacheReadTimer", kCacheSection);
    RuntimeProfile::Counter* cache_write_counter =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDV_DataCacheWriteCounter", TUnit::UNIT, kCacheSection);
    RuntimeProfile::Counter* cache_write_bytes =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDV_DataCacheWriteBytes", TUnit::BYTES, kCacheSection);
    RuntimeProfile::Counter* cache_skip_read_counter =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDV_DataCacheSkipReadCounter", TUnit::UNIT, kCacheSection);
    RuntimeProfile::Counter* cache_skip_read_bytes =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDV_DataCacheSkipReadBytes", TUnit::BYTES, kCacheSection);
    RuntimeProfile::Counter* cache_read_peer_counter =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDV_DataCacheReadPeerCounter", TUnit::UNIT, kCacheSection);
    RuntimeProfile::Counter* cache_read_peer_bytes =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDV_DataCacheReadPeerBytes", TUnit::BYTES, kCacheSection);
    RuntimeProfile::Counter* cache_read_peer_timer =
            ADD_CHILD_TIMER(parent_profile, "IcebergDV_DataCacheReadPeerTimer", kCacheSection);
    RuntimeProfile::Counter* cache_skip_read_peer_counter =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDV_DataCacheSkipReadPeerCounter", TUnit::UNIT, kCacheSection);
    RuntimeProfile::Counter* cache_skip_read_peer_bytes =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDV_DataCacheSkipReadPeerBytes", TUnit::BYTES, kCacheSection);

    const CacheInputStream::Stats& cache_stats = cache_input_stream->stats();
    COUNTER_UPDATE(cache_read_counter, cache_stats.read_block_cache_count);
    COUNTER_UPDATE(cache_read_bytes, cache_stats.read_block_cache_bytes);
    COUNTER_UPDATE(cache_read_mem_bytes, cache_stats.read_mem_cache_bytes);
    COUNTER_UPDATE(cache_read_disk_bytes, cache_stats.read_disk_cache_bytes);
    COUNTER_UPDATE(cache_read_timer, cache_stats.read_block_cache_ns);
    COUNTER_UPDATE(cache_write_counter, cache_stats.write_block_cache_count);
    COUNTER_UPDATE(cache_write_bytes, cache_stats.write_block_cache_bytes);
    COUNTER_UPDATE(cache_skip_read_counter, cache_stats.skip_read_cache_count);
    COUNTER_UPDATE(cache_skip_read_bytes, cache_stats.skip_read_cache_bytes);
    COUNTER_UPDATE(cache_read_peer_counter, cache_stats.read_peer_cache_count);
    COUNTER_UPDATE(cache_read_peer_bytes, cache_stats.read_peer_cache_bytes);
    COUNTER_UPDATE(cache_read_peer_timer, cache_stats.read_peer_cache_ns);
    COUNTER_UPDATE(cache_skip_read_peer_counter, cache_stats.skip_read_peer_cache_count);
    COUNTER_UPDATE(cache_skip_read_peer_bytes, cache_stats.skip_read_peer_cache_bytes);
}

Status IcebergDeleteBuilder::build(const TIcebergDeleteFile& delete_file, const std::string& format) const {
    FormatScannerStats app_stats;
    FormatScannerStats fs_stats;
    std::shared_ptr<SharedBufferedInputStream> shared_buffered_input_stream;
    std::shared_ptr<CacheInputStream> cache_input_stream;

    ASSIGN_OR_RETURN(auto file, open_random_access_file(delete_file, fs_stats, app_stats, shared_buffered_input_stream,
                                                        cache_input_stream));

    RETURN_IF_ERROR(IcebergPositionDeleteReader::read_rows(
            file.get(), delete_file.full_path, delete_file.length, format, _ctx.chunk_size, _ctx.scan_context->timezone,
            _ctx.scan_context->options, &app_stats, [this](const Slice& file_path, int64_t pos) {
                if (file_path == _ctx.data_file_path) {
                    _deletion_bitmap->add_value(pos);
                }
            }));
    update_delete_file_io_counter(_ctx.runtime_profile, app_stats, fs_stats, cache_input_stream,
                                  shared_buffered_input_stream);
    return Status::OK();
}

Status IcebergDeleteBuilder::build_parquet(const TIcebergDeleteFile& delete_file) const {
    return build(delete_file, PARQUET);
}

Status IcebergDeleteBuilder::build_orc(const TIcebergDeleteFile& delete_file) const {
    return build(delete_file, ORC);
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
    desc.__set_isNullable(true);

    return {desc};
}

void IcebergDeleteBuilder::update_delete_file_io_counter(
        RuntimeProfile* parent_profile, const FormatScannerStats& app_stats, const FormatScannerStats& fs_stats,
        const std::shared_ptr<CacheInputStream>& cache_input_stream,
        const std::shared_ptr<SharedBufferedInputStream>& shared_buffered_input_stream) {
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
        RuntimeProfile::Counter* datacache_read_peer_counter =
                ADD_CHILD_COUNTER(parent_profile, "MOR_DataCacheReadPeerCounter", TUnit::UNIT, prefix);
        RuntimeProfile::Counter* datacache_read_peer_bytes =
                ADD_CHILD_COUNTER(parent_profile, "MOR_DataCacheReadPeerBytes", TUnit::BYTES, prefix);
        RuntimeProfile::Counter* datacache_read_peer_timer =
                ADD_CHILD_TIMER(parent_profile, "MOR_DataCacheReadPeerTimer", prefix);
        RuntimeProfile::Counter* datacache_skip_read_peer_counter =
                ADD_CHILD_COUNTER(parent_profile, "MOR_DataCacheSkipReadPeerCounter", TUnit::UNIT, prefix);
        RuntimeProfile::Counter* datacache_skip_read_peer_bytes =
                ADD_CHILD_COUNTER(parent_profile, "MOR_DataCacheSkipReadPeerBytes", TUnit::BYTES, prefix);
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

        const CacheInputStream::Stats& stats = cache_input_stream->stats();
        COUNTER_UPDATE(datacache_read_counter, stats.read_block_cache_count);
        COUNTER_UPDATE(datacache_read_bytes, stats.read_block_cache_bytes);
        COUNTER_UPDATE(datacache_read_mem_bytes, stats.read_mem_cache_bytes);
        COUNTER_UPDATE(datacache_read_disk_bytes, stats.read_disk_cache_bytes);
        COUNTER_UPDATE(datacache_read_timer, stats.read_block_cache_ns);
        COUNTER_UPDATE(datacache_skip_read_counter, stats.skip_read_cache_count);
        COUNTER_UPDATE(datacache_skip_read_bytes, stats.skip_read_cache_bytes);
        COUNTER_UPDATE(datacache_read_peer_bytes, stats.read_peer_cache_bytes);
        COUNTER_UPDATE(datacache_read_peer_counter, stats.read_peer_cache_count);
        COUNTER_UPDATE(datacache_read_peer_timer, stats.read_peer_cache_ns);
        COUNTER_UPDATE(datacache_skip_read_peer_counter, stats.skip_read_peer_cache_count);
        COUNTER_UPDATE(datacache_skip_read_peer_bytes, stats.skip_read_peer_cache_bytes);
        COUNTER_UPDATE(datacache_write_counter, stats.write_block_cache_count);
        COUNTER_UPDATE(datacache_write_bytes, stats.write_block_cache_bytes);
        COUNTER_UPDATE(datacache_write_timer, stats.write_block_cache_ns);
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
} // namespace starrocks::formats
