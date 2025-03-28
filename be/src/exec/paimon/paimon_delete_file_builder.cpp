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

#include "paimon_delete_file_builder.h"

#include <roaring/roaring.h>
#include <roaring/roaring64.h>

#include <bitset>

#include "connector/deletion_vector/deletion_bitmap.h"
#include "util/raw_container.h"

namespace starrocks {

Status PaimonDeleteFileBuilder::build(const TPaimonDeletionFile* paimon_deletion_file) {
    auto& path = paimon_deletion_file->path;
    auto& length = paimon_deletion_file->length;
    auto& offset = paimon_deletion_file->offset;
    auto serialized_bitmap_length = length - MAGIC_NUMBER_LENGTH;

    std::shared_ptr<RandomAccessFile> raw_deletion_vector;
    ASSIGN_OR_RETURN(raw_deletion_vector, _fs->new_random_access_file(path));

    // Check whether the bitmap size stored in deletion file is equals to the size from paimon api
    uint32_t size_from_deletion_vector_file;
    RETURN_IF_ERROR(raw_deletion_vector->read_at_fully(offset, &size_from_deletion_vector_file, BITMAP_SIZE_LENGTH));
#ifdef IS_LITTLE_ENDIAN
    size_from_deletion_vector_file = swap_endian32(size_from_deletion_vector_file);
#endif

    DCHECK(size_from_deletion_vector_file == length);

    // Check the correctness of magic number
    uint32_t magic_number_from_deletion_vector_file;
    RETURN_IF_ERROR(raw_deletion_vector->read_at_fully(offset + BITMAP_SIZE_LENGTH,
                                                       &magic_number_from_deletion_vector_file, MAGIC_NUMBER_LENGTH));
#ifdef IS_LITTLE_ENDIAN
    magic_number_from_deletion_vector_file = swap_endian32(magic_number_from_deletion_vector_file);
#endif
    DCHECK(magic_number_from_deletion_vector_file == MAGIC_NUMBER);

    std::unique_ptr<char[]> deletion_vector(new char[serialized_bitmap_length]);
    RETURN_IF_ERROR(raw_deletion_vector->read_at_fully(offset + BITMAP_SIZE_LENGTH + MAGIC_NUMBER_LENGTH,
                                                       deletion_vector.get(), serialized_bitmap_length));

    // Construct the roaring bitmap of corresponding deletion vector
    roaring_bitmap_t* bitmap =
            roaring_bitmap_portable_deserialize_safe(deletion_vector.get(), serialized_bitmap_length);
    roaring64_bitmap_t* bitmap64 = roaring64_bitmap_move_from_roaring32(bitmap);
    _skip_rows_ctx->deletion_bitmap = std::make_shared<DeletionBitmap>(bitmap64);

    roaring_bitmap_free(bitmap);
    return Status::OK();
}

} // namespace starrocks
