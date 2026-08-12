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

#include "formats/paimon/paimon_delete_file_builder.h"

#include <fmt/format.h>
#include <roaring/roaring.h>
#include <roaring/roaring64.h>

#include <limits>

#include "formats/deletion_bitmap.h"
#include "gutil/endian.h"

namespace starrocks::formats {

StatusOr<DeletionBitmapPtr> PaimonDeleteFileBuilder::build(const TPaimonDeletionFile& paimon_deletion_file) {
    const auto& path = paimon_deletion_file.path;
    const auto& length = paimon_deletion_file.length;
    const auto& offset = paimon_deletion_file.offset;

    if (offset < 0 || offset > std::numeric_limits<int64_t>::max() - BITMAP_SIZE_LENGTH - MAGIC_NUMBER_LENGTH) {
        return Status::InvalidArgument(fmt::format(
                "invalid paimon deletion vector offset, path: {}, offset: {}, length: {}", path, offset, length));
    }

    std::shared_ptr<RandomAccessFile> raw_deletion_vector;
    ASSIGN_OR_RETURN(raw_deletion_vector, _fs->new_random_access_file(path));

    // Read the magic number to determine the version (v1 vs v2).
    // v1 (BitmapDeletionVector) stores magic in Big-Endian, v2 (Bitmap64DeletionVector) in Little-Endian.
    int32_t magic_number_raw;
    RETURN_IF_ERROR(raw_deletion_vector->read_at_fully(offset + BITMAP_SIZE_LENGTH, &magic_number_raw,
                                                       sizeof(magic_number_raw)));

    int32_t magic_number_big_endian = BigEndian::ToHost32(magic_number_raw);
    int32_t magic_number_little_endian = LittleEndian::ToHost32(magic_number_raw);

    bool is_roaring64 = false;
    if (magic_number_big_endian == MAGIC_NUMBER) {
        is_roaring64 = false;
    } else if (magic_number_little_endian == MAGIC_NUMBER_64) {
        is_roaring64 = true;
    } else {
        return Status::InternalError(
                fmt::format("invalid paimon deletion vector magic number, path: {}, offset: {}, length: {}, "
                            "file magic (big-endian): {}, file magic (little-endian): {}, "
                            "expected v1 magic: {}, expected v2 magic: {}",
                            path, offset, length, magic_number_big_endian, magic_number_little_endian, MAGIC_NUMBER,
                            MAGIC_NUMBER_64));
    }

    // Validate the size field stored in the deletion file.
    // v1: DeletionFile.length = magic + bitmap, size_in_file should equal length
    // v2: DeletionFile.length = length_field(4) + magic(4) + bitmap + CRC(4), size_in_file = magic + bitmap
    int32_t size_from_deletion_vector_file;
    RETURN_IF_ERROR(raw_deletion_vector->read_at_fully(offset, &size_from_deletion_vector_file,
                                                       sizeof(size_from_deletion_vector_file)));
    size_from_deletion_vector_file = BigEndian::ToHost32(size_from_deletion_vector_file);

    const int64_t framing_length =
            is_roaring64 ? BITMAP_SIZE_LENGTH + MAGIC_NUMBER_LENGTH + CRC_LENGTH : MAGIC_NUMBER_LENGTH;
    if (length < framing_length + MIN_SERIALIZED_BITMAP_LENGTH || length > std::numeric_limits<int32_t>::max()) {
        return Status::InvalidArgument(
                fmt::format("invalid paimon deletion vector length, path: {}, offset: {}, length: {}, is_v2: {}", path,
                            offset, length, is_roaring64));
    }

    int64_t expected_size_from_file;
    int64_t serialized_bitmap_length;

    if (is_roaring64) {
        expected_size_from_file = length - BITMAP_SIZE_LENGTH - CRC_LENGTH;
        serialized_bitmap_length = expected_size_from_file - MAGIC_NUMBER_LENGTH;
    } else {
        expected_size_from_file = length;
        serialized_bitmap_length = length - MAGIC_NUMBER_LENGTH;
    }

    const int64_t record_length = is_roaring64 ? length : BITMAP_SIZE_LENGTH + length;
    if (offset > std::numeric_limits<int64_t>::max() - record_length) {
        return Status::InvalidArgument(
                fmt::format("paimon deletion vector range overflows, path: {}, offset: {}, length: {}, is_v2: {}", path,
                            offset, length, is_roaring64));
    }

    if (size_from_deletion_vector_file != expected_size_from_file) {
        return Status::InternalError(
                fmt::format("paimon deletion vector length mismatch, path: {}, offset: {}, "
                            "file length: {}, expected length: {}, is_v2: {}",
                            path, offset, size_from_deletion_vector_file, expected_size_from_file, is_roaring64));
    }

    std::unique_ptr<char[]> deletion_vector(new char[static_cast<size_t>(serialized_bitmap_length)]);
    RETURN_IF_ERROR(raw_deletion_vector->read_at_fully(offset + BITMAP_SIZE_LENGTH + MAGIC_NUMBER_LENGTH,
                                                       deletion_vector.get(), serialized_bitmap_length));

    if (!is_roaring64) {
        roaring_bitmap_t* bitmap =
                roaring_bitmap_portable_deserialize_safe(deletion_vector.get(), serialized_bitmap_length);
        if (bitmap == nullptr) {
            return Status::RuntimeError("deserialize roaring bitmap error");
        }
        roaring64_bitmap_t* bitmap64 = roaring64_bitmap_move_from_roaring32(bitmap);
        auto deletion_bitmap = std::make_shared<DeletionBitmap>(bitmap64);
        roaring_bitmap_free(bitmap);
        return deletion_bitmap;
    }

    roaring64_bitmap_t* bitmap64 =
            roaring64_bitmap_portable_deserialize_safe(deletion_vector.get(), serialized_bitmap_length);
    if (bitmap64 == nullptr) {
        return Status::RuntimeError("deserialize roaring64 bitmap error");
    }
    return std::make_shared<DeletionBitmap>(bitmap64);
}

} // namespace starrocks::formats
