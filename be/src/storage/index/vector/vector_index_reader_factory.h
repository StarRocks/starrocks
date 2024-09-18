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

#pragma once

#include "fmt/format.h"
#include "fs/fs_util.h"
#include "storage/index/index_descriptor.h"
#include "vector_index_reader.h"

namespace starrocks {

class VectorIndexReaderFactory {
public:
    static Status create_from_file(const std::string& index_path, const std::shared_ptr<tenann::IndexMeta>& index_meta,
                                   std::shared_ptr<VectorIndexReader>* vector_index_reader);
};

} // namespace starrocks