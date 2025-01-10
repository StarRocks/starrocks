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

#include "deletion_vector.h"

#include <roaring/roaring64.h>

#include "deletion_bitmap.h"
#include "util/base85.h"
#include "util/uuid_generator.h"

namespace starrocks {

const std::string DeletionVector::DELETION_VECTOR = "DeletionVector";

Status DeletionVector::fill_row_indexes(const SkipRowsContextPtr& skip_rows_ctx) {
    if (_deletion_vector_descriptor->__isset.cardinality && _deletion_vector_descriptor->__isset.cardinality == 0) {
        return Status::OK();
    } else if (is_inline()) {
        return deserialized_inline_dv(_deletion_vector_descriptor->pathOrInlineDv, skip_rows_ctx);
    } else {
        std::shared_ptr<io::SharedBufferedInputStream> shared_buffered_input_stream = nullptr;
        std::shared_ptr<io::CacheInputStream> cache_input_stream = nullptr;
        HdfsScanStats app_scan_stats;
        HdfsScanStats fs_scan_stats;

        ASSIGN_OR_RETURN(auto path, get_absolute_path(_params.table_location));
        int64_t offset = _deletion_vector_descriptor->offset;
        int64_t length = _deletion_vector_descriptor->sizeInBytes;

        ASSIGN_OR_RETURN(auto dv_file, open_random_access_file(path, fs_scan_stats, app_scan_stats,
                                                               shared_buffered_input_stream, cache_input_stream));
        // Check the dv size
        uint32_t size_from_deletion_vector_file;
        RETURN_IF_ERROR(dv_file->read_at_fully(offset, &size_from_deletion_vector_file, DV_SIZE_LENGTH));
        offset += DV_SIZE_LENGTH;
        // the size_from_deletion_vector_file is big endian byte order
        if (LittleEndian::IsLittleEndian()) {
            size_from_deletion_vector_file = BigEndian::ToHost32(size_from_deletion_vector_file);
        }

        if (size_from_deletion_vector_file != length) {
            std::stringstream ss;
            ss << "DV size mismatch, expected : " << length << " , actual : " << size_from_deletion_vector_file;
            return Status::RuntimeError(ss.str());
        }

        // Check the correctness of magic number
        uint32_t magic_number_from_deletion_vector_file;
        RETURN_IF_ERROR(dv_file->read_at_fully(offset, &magic_number_from_deletion_vector_file, MAGIC_NUMBER_LENGTH));
        // magic_number_from_deletion_vector_file is little endian byte order
        if (!LittleEndian::IsLittleEndian()) {
            magic_number_from_deletion_vector_file = LittleEndian::ToHost32(magic_number_from_deletion_vector_file);
        }
        offset += MAGIC_NUMBER_LENGTH;

        int64_t serialized_bitmap_length = length - MAGIC_NUMBER_LENGTH;
        std::vector<char> deletion_vector(serialized_bitmap_length);
        RETURN_IF_ERROR(dv_file->read_at_fully(offset, deletion_vector.data(), serialized_bitmap_length));

        update_dv_file_io_counter(_params.profile->runtime_profile, app_scan_stats, fs_scan_stats, cache_input_stream,
                                  shared_buffered_input_stream);
        return deserialized_deletion_vector(magic_number_from_deletion_vector_file, deletion_vector,
                                            serialized_bitmap_length, skip_rows_ctx);
    }
}

StatusOr<std::unique_ptr<RandomAccessFile>> DeletionVector::open_random_access_file(
        const std::string& file_path, HdfsScanStats& fs_scan_stats, HdfsScanStats& app_scan_stats,
        std::shared_ptr<io::SharedBufferedInputStream>& shared_buffered_input_stream,
        std::shared_ptr<io::CacheInputStream>& cache_input_stream) const {
    const OpenFileOptions options{.fs = _params.fs,
                                  .path = file_path,
                                  .fs_stats = &fs_scan_stats,
                                  .app_stats = &app_scan_stats,
                                  .datacache_options = _params.datacache_options};
    ASSIGN_OR_RETURN(auto file,
                     HdfsScanner::create_random_access_file(shared_buffered_input_stream, cache_input_stream, options));
    std::vector<io::SharedBufferedInputStream::IORange> io_ranges{};
    int64_t offset = _deletion_vector_descriptor->offset;
    int64_t length = _deletion_vector_descriptor->sizeInBytes + DV_SIZE_LENGTH;
    while (offset < length) {
        const int64_t remain_length = std::min(static_cast<int64_t>(config::io_coalesce_read_max_buffer_size), length);
        io_ranges.emplace_back(offset, remain_length);
        offset += remain_length;
    }

    RETURN_IF_ERROR(shared_buffered_input_stream->set_io_ranges(io_ranges));
    return file;
}

Status DeletionVector::deserialized_inline_dv(std::string& encoded_bitmap_data,
                                              const SkipRowsContextPtr& skip_rows_ctx) {
    ASSIGN_OR_RETURN(auto decoded_bitmap_data, base85_decode(encoded_bitmap_data));
    uint32_t inline_magic_number;
    memcpy(&inline_magic_number, decoded_bitmap_data.data(), DeletionVector::MAGIC_NUMBER_LENGTH);

    int64_t serialized_bitmap_length = decoded_bitmap_data.size() - MAGIC_NUMBER_LENGTH;
    std::vector<char> deletion_vector(serialized_bitmap_length);
    memcpy(deletion_vector.data(), decoded_bitmap_data.data() + MAGIC_NUMBER_LENGTH, serialized_bitmap_length);

    return deserialized_deletion_vector(inline_magic_number, deletion_vector, serialized_bitmap_length, skip_rows_ctx);
}

Status DeletionVector::deserialized_deletion_vector(uint32_t magic_number, std::vector<char>& serialized_dv,
                                                    int64_t serialized_bitmap_length,
                                                    const SkipRowsContextPtr& skip_rows_ctx) {
    {
        SCOPED_RAW_TIMER(&_build_stats.bitmap_deserialize_ns);
        if (magic_number != MAGIC_NUMBER) {
            std::stringstream ss;
            ss << "Unexpected magic number : " << magic_number;
            return Status::RuntimeError(ss.str());
        }

        // Construct the roaring bitmap of corresponding deletion vector
        roaring64_bitmap_t* bitmap =
                roaring64_bitmap_portable_deserialize_safe(serialized_dv.data(), serialized_bitmap_length);
        if (bitmap == nullptr) {
            return Status::RuntimeError("deserialize roaring64 bitmap error");
        }
        skip_rows_ctx->deletion_bitmap = std::make_shared<DeletionBitmap>(bitmap);
    }
#ifndef BE_TEST
    update_dv_build_counter(_params.profile->runtime_profile, _build_stats);
#endif
    return Status::OK();
}

StatusOr<std::string> DeletionVector::get_absolute_path(const std::string& table_location) const {
    std::string& storage_type = _deletion_vector_descriptor->storageType;
    std::string& path = _deletion_vector_descriptor->pathOrInlineDv;
    if (storage_type == "u") {
        uint32_t random_prefix_len = path.length() - ENCODED_UUID_LENGTH;
        std::string random_prefix = path.substr(0, random_prefix_len);
        std::string encoded_uuid = path.substr(random_prefix_len);
        ASSIGN_OR_RETURN(auto decoded_uuid, base85_decode(encoded_uuid));
        if (decoded_uuid.length() < 16) {
            return Status::RuntimeError("decoded uuid length less than 16");
        }
        boost::uuids::uuid uuid{};
        memcpy(uuid.data, decoded_uuid.data(), 8);
        memcpy(uuid.data + 8, decoded_uuid.data() + 8, 8);
        return assemble_deletion_vector_path(table_location, boost::uuids::to_string(uuid), random_prefix);
    } else if (storage_type == "p") {
        return path;
    } else {
        return Status::RuntimeError(fmt::format("unsupported storage type {}", storage_type));
    }
}

std::string DeletionVector::assemble_deletion_vector_path(const string& table_location, string&& uuid,
                                                          string& prefix) const {
    std::string file_name = fmt::format("deletion_vector_{}.bin", uuid);
    if (prefix.empty()) {
        return fmt::format("{}/{}", table_location, file_name);
    } else {
        return fmt::format("{}/{}/{}", table_location, prefix, file_name);
    }
}

void DeletionVector::update_dv_file_io_counter(
        RuntimeProfile* parent_profile, const HdfsScanStats& app_stats, const HdfsScanStats& fs_stats,
        const std::shared_ptr<io::CacheInputStream>& cache_input_stream,
        const std::shared_ptr<io::SharedBufferedInputStream>& shared_buffered_input_stream) {
    const std::string DV_TIMER = DeletionVector::DELETION_VECTOR;
    ADD_COUNTER(parent_profile, DV_TIMER, TUnit::NONE);
    {
        static const char* prefix = "DV_InputStream";
        ADD_CHILD_COUNTER(parent_profile, prefix, TUnit::NONE, DV_TIMER);

        RuntimeProfile::Counter* app_io_bytes_read_counter =
                ADD_CHILD_COUNTER(parent_profile, "DV_AppIOBytesRead", TUnit::BYTES, prefix);
        RuntimeProfile::Counter* app_io_timer = ADD_CHILD_TIMER(parent_profile, "DV_AppIOTime", prefix);
        RuntimeProfile::Counter* app_io_counter =
                ADD_CHILD_COUNTER(parent_profile, "DV_AppIOCounter", TUnit::UNIT, prefix);

        RuntimeProfile::Counter* fs_bytes_read_counter =
                ADD_CHILD_COUNTER(parent_profile, "DV_FSIOBytesRead", TUnit::BYTES, prefix);
        RuntimeProfile::Counter* fs_io_timer = ADD_CHILD_TIMER(parent_profile, "DV_FSIOTime", prefix);
        RuntimeProfile::Counter* fs_io_counter =
                ADD_CHILD_COUNTER(parent_profile, "DV_FSIOCounter", TUnit::UNIT, prefix);

        COUNTER_UPDATE(app_io_timer, app_stats.io_ns);
        COUNTER_UPDATE(app_io_counter, app_stats.io_count);
        COUNTER_UPDATE(app_io_bytes_read_counter, app_stats.bytes_read);
        COUNTER_UPDATE(fs_bytes_read_counter, fs_stats.bytes_read);
        COUNTER_UPDATE(fs_io_timer, fs_stats.io_ns);
        COUNTER_UPDATE(fs_io_counter, fs_stats.io_count);
    }

    {
        static const char* prefix = "DV_SharedBuffered";
        ADD_CHILD_COUNTER(parent_profile, prefix, TUnit::NONE, DV_TIMER);
        RuntimeProfile::Counter* shared_buffered_shared_io_bytes =
                ADD_CHILD_COUNTER(parent_profile, "DV_SharedIOBytes", TUnit::BYTES, prefix);
        RuntimeProfile::Counter* shared_buffered_shared_align_io_bytes =
                ADD_CHILD_COUNTER(parent_profile, "DV_SharedAlignIOBytes", TUnit::BYTES, prefix);
        RuntimeProfile::Counter* shared_buffered_shared_io_count =
                ADD_CHILD_COUNTER(parent_profile, "DV_SharedIOCount", TUnit::UNIT, prefix);
        RuntimeProfile::Counter* shared_buffered_shared_io_timer =
                ADD_CHILD_TIMER(parent_profile, "DV_SharedIOTime", prefix);
        RuntimeProfile::Counter* shared_buffered_direct_io_bytes =
                ADD_CHILD_COUNTER(parent_profile, "DV_DirectIOBytes", TUnit::BYTES, prefix);
        RuntimeProfile::Counter* shared_buffered_direct_io_count =
                ADD_CHILD_COUNTER(parent_profile, "DV_DirectIOCount", TUnit::UNIT, prefix);
        RuntimeProfile::Counter* shared_buffered_direct_io_timer =
                ADD_CHILD_TIMER(parent_profile, "DV_DirectIOTime", prefix);

        COUNTER_UPDATE(shared_buffered_shared_io_count, shared_buffered_input_stream->shared_io_count());
        COUNTER_UPDATE(shared_buffered_shared_io_bytes, shared_buffered_input_stream->shared_io_bytes());
        COUNTER_UPDATE(shared_buffered_shared_align_io_bytes, shared_buffered_input_stream->shared_align_io_bytes());
        COUNTER_UPDATE(shared_buffered_shared_io_timer, shared_buffered_input_stream->shared_io_timer());
        COUNTER_UPDATE(shared_buffered_direct_io_count, shared_buffered_input_stream->direct_io_count());
        COUNTER_UPDATE(shared_buffered_direct_io_bytes, shared_buffered_input_stream->direct_io_bytes());
        COUNTER_UPDATE(shared_buffered_direct_io_timer, shared_buffered_input_stream->direct_io_timer());
    }

    if (cache_input_stream) {
        static const char* prefix = "DV_DataCache";
        ADD_CHILD_COUNTER(parent_profile, prefix, TUnit::NONE, DV_TIMER);
        RuntimeProfile::Counter* datacache_read_counter =
                ADD_CHILD_COUNTER(parent_profile, "DV_DataCacheReadCounter", TUnit::UNIT, prefix);
        RuntimeProfile::Counter* datacache_read_bytes =
                ADD_CHILD_COUNTER(parent_profile, "DV_DataCacheReadBytes", TUnit::BYTES, prefix);
        RuntimeProfile::Counter* datacache_read_mem_bytes =
                ADD_CHILD_COUNTER(parent_profile, "DV_DataCacheReadMemBytes", TUnit::BYTES, "DV_DataCacheReadBytes");
        RuntimeProfile::Counter* datacache_read_disk_bytes =
                ADD_CHILD_COUNTER(parent_profile, "DV_DataCacheReadDiskBytes", TUnit::BYTES, "DV_DataCacheReadBytes");
        RuntimeProfile::Counter* datacache_skip_read_counter =
                ADD_CHILD_COUNTER(parent_profile, "DV_DataCacheSkipReadCounter", TUnit::UNIT, prefix);
        RuntimeProfile::Counter* datacache_skip_read_bytes =
                ADD_CHILD_COUNTER(parent_profile, "DV_DataCacheSkipReadBytes", TUnit::BYTES, prefix);
        RuntimeProfile::Counter* datacache_read_timer =
                ADD_CHILD_TIMER(parent_profile, "DV_DataCacheReadTimer", prefix);
        RuntimeProfile::Counter* datacache_write_counter =
                ADD_CHILD_COUNTER(parent_profile, "DV_DataCacheWriteCounter", TUnit::UNIT, prefix);
        RuntimeProfile::Counter* datacache_write_bytes =
                ADD_CHILD_COUNTER(parent_profile, "DV_DataCacheWriteBytes", TUnit::BYTES, prefix);
        RuntimeProfile::Counter* datacache_write_timer =
                ADD_CHILD_TIMER(parent_profile, "DV_DataCacheWriteTimer", prefix);
        RuntimeProfile::Counter* datacache_write_fail_counter =
                ADD_CHILD_COUNTER(parent_profile, "DV_DataCacheWriteFailCounter", TUnit::UNIT, prefix);
        RuntimeProfile::Counter* datacache_write_fail_bytes =
                ADD_CHILD_COUNTER(parent_profile, "DV_DataCacheWriteFailBytes", TUnit::BYTES, prefix);
        RuntimeProfile::Counter* datacache_read_block_buffer_counter =
                ADD_CHILD_COUNTER(parent_profile, "DV_DataCacheReadBlockBufferCounter", TUnit::UNIT, prefix);
        RuntimeProfile::Counter* datacache_read_block_buffer_bytes =
                ADD_CHILD_COUNTER(parent_profile, "DV_DataCacheReadBlockBufferBytes", TUnit::BYTES, prefix);

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

void DeletionVector::update_dv_build_counter(RuntimeProfile* parent_profile,
                                             const DeletionVectorBuildStats& build_stats) {
    const std::string DV_TIMER = DeletionVector::DELETION_VECTOR;
    ADD_COUNTER(parent_profile, DV_TIMER, TUnit::NONE);
    {
        static const char* prefix = "DV_BuildTime";
        ADD_CHILD_COUNTER(parent_profile, prefix, TUnit::NONE, DV_TIMER);

        RuntimeProfile::Counter* bitmap_deserialize_timer =
                ADD_CHILD_COUNTER(parent_profile, "DV_BitmapDeserializeTime", TUnit::TIME_NS, prefix);

        COUNTER_UPDATE(bitmap_deserialize_timer, build_stats.bitmap_deserialize_ns);
    }
}

} // namespace starrocks
