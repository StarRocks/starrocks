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

#ifdef WITH_TENANN

#include "storage/index/vector/vector_index_file_reader.h"

#include <memory>
#include <utility>

#include "common/status.h"
#include "common/statusor.h"
#include "fs/fs.h"

namespace starrocks {

StatusOr<std::unique_ptr<VectorIndexFileReader>> VectorIndexFileReader::open(FileSystem* fs, const std::string& path) {
    ASSIGN_OR_RETURN(auto raf, fs->new_random_access_file(path));
    ASSIGN_OR_RETURN(uint64_t size, raf->get_size());
    return std::make_unique<VectorIndexFileReader>(std::move(raf), static_cast<int64_t>(size));
}

} // namespace starrocks

#endif
