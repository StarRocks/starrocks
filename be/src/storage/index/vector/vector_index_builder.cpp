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

#include "storage/index/vector/vector_index_builder.h"

#include "fs/fs_util.h"
#include "storage/index/index_descriptor.h"

namespace starrocks {

Status VectorIndexBuilder::flush_empty(const std::string& segment_index_path) {
    ASSIGN_OR_RETURN(auto empty_file, fs::new_writable_file(segment_index_path));
    RETURN_IF_ERROR(empty_file->append(IndexDescriptor::mark_word));
    RETURN_IF_ERROR(empty_file->flush(WritableFile::FLUSH_SYNC));
    return empty_file->close();
}

} // namespace starrocks