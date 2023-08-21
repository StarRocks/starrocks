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

#include "storage/vector_index_reader_factory.h"
#include "storage/empty_index_reader.h"
#include "storage/ten_ann_reader.h"

namespace starrocks {
Status VectorIndexReaderFactory::create_from_file(const std::string& index_path,
                                                  const std::shared_ptr<tenann::IndexMeta>& index_meta,
                                                  std::shared_ptr<VectorIndexReader>* vector_index_reader) {
    if (!fs::path_exist(index_path)) {
        return Status::NotFound(fmt::format("index path {} not found", index_path));
    }
    ASSIGN_OR_RETURN(auto index_file, fs::new_random_access_file(index_path))
    ASSIGN_OR_RETURN(auto file_size, index_file->get_size())

    if (file_size == MARK_WORD_LEN) {
        std::unique_ptr<unsigned char[]> buf(new (std::nothrow) unsigned char[file_size]);
        index_file->read_fully(buf.get(), file_size);
        std::string buf_str(reinterpret_cast<char*>(buf.get()), file_size);
        if (buf_str == MARK_WORD) {
            (*vector_index_reader) = std::make_shared<EmptyIndexReader>();
            return Status::OK();
        }
    }
    (*vector_index_reader) = std::make_shared<TenANNReader>();
    return Status::OK();
}

} // namespace starrocks