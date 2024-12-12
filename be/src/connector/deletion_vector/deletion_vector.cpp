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

#include "util/base85.h"
#include "util/uuid_generator.h"

namespace starrocks {

Status starrocks::DeletionVector::fill_row_indexes(std::set<int64_t>* need_skip_rowids) {
    if (_deletion_vector_descriptor->__isset.cardinality && _deletion_vector_descriptor->__isset.cardinality == 0) {
        return Status::OK();
    } else if (is_inline()) {
        return deserialized_inline_dv(_deletion_vector_descriptor->pathOrInlineDv, need_skip_rowids);
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
        std::unique_ptr<char[]> deletion_vector(new char[serialized_bitmap_length]);
        RETURN_IF_ERROR(dv_file->read_at_fully(offset, deletion_vector.get(), serialized_bitmap_length));

        return deserialized_deletion_vector(magic_number_from_deletion_vector_file, std::move(deletion_vector),
                                            serialized_bitmap_length, need_skip_rowids);
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
                                              std::set<int64_t>* need_skip_rowids) const {
    ASSIGN_OR_RETURN(auto decoded_bitmap_data, base85_decode(encoded_bitmap_data));
    uint32_t inline_magic_number;
    memcpy(&inline_magic_number, decoded_bitmap_data.data(), DeletionVector::MAGIC_NUMBER_LENGTH);

    int64_t serialized_bitmap_length = decoded_bitmap_data.size() - MAGIC_NUMBER_LENGTH;
    std::unique_ptr<char[]> deletion_vector(new char[serialized_bitmap_length]);
    memcpy(deletion_vector.get(), decoded_bitmap_data.data() + MAGIC_NUMBER_LENGTH, serialized_bitmap_length);

    return deserialized_deletion_vector(inline_magic_number, std::move(deletion_vector), serialized_bitmap_length,
                                        need_skip_rowids);
}

Status DeletionVector::deserialized_deletion_vector(uint32_t magic_number, std::unique_ptr<char[]> serialized_dv,
                                                    int64_t serialized_bitmap_length,
                                                    std::set<int64_t>* need_skip_rowids) const {
    if (magic_number != MAGIC_NUMBER) {
        std::stringstream ss;
        ss << "Unexpected magic number : " << magic_number;
        return Status::RuntimeError(ss.str());
    }

    // Construct the roaring bitmap of corresponding deletion vector
    roaring64_bitmap_t* bitmap =
            roaring64_bitmap_portable_deserialize_safe(serialized_dv.get(), serialized_bitmap_length);
    if (bitmap == nullptr) {
        return Status::RuntimeError("deserialize roaring64 bitmap error");
    }

    // Construct _need_skip_rowids from bitmap
    uint64_t bitmap_cardinality = roaring64_bitmap_get_cardinality(bitmap);
    std::unique_ptr<uint64_t[]> bitmap_array(new uint64_t[bitmap_cardinality]);
    roaring64_bitmap_to_uint64_array(bitmap, bitmap_array.get());
    need_skip_rowids->insert(bitmap_array.get(), bitmap_array.get() + bitmap_cardinality);

    roaring64_bitmap_free(bitmap);
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

} // namespace starrocks
