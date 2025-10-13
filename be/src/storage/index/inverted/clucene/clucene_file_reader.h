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

#include <memory>

#include "CLucene.h"
#include "common/config.h"
#include "common/statusor.h"

namespace starrocks {

class FileSystem;
class TabletIndex;
struct FileEntry;

class StarRocksIndexInput;
class StarRocksMergedDirectory;

class CLuceneFileReader {
public:
    CLuceneFileReader(std::shared_ptr<FileSystem> fs, std::string index_file_path);
    ~CLuceneFileReader();

    Status init();
    StatusOr<std::unique_ptr<StarRocksMergedDirectory>> open(const std::shared_ptr<TabletIndex>& index_meta) const;

private:
    std::map<int64_t, std::map<std::string, FileEntry>> _index_to_entries;

    std::shared_ptr<FileSystem> _fs;
    std::string _index_file_path;

    std::unique_ptr<StarRocksIndexInput> _stream = nullptr;

    constexpr static int64_t BUFFER_SIZE = 4 * 1024;
};

} // namespace starrocks
