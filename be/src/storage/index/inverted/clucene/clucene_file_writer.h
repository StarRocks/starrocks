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

#include <string>

#include "common/statusor.h"

namespace lucene::store {
class Directory;
class IndexInput;
class IndexOutput;
} // namespace lucene::store

namespace starrocks {

class TabletIndex;

struct FileEntry;

class StarRocksMergingDirectory;

class WritableFile;
struct WritableFileOptions;

class CLuceneInvertedWriter;
using CLuceneInvertedWriterPtr = std::unique_ptr<CLuceneInvertedWriter>;

class FileSystem;

class CLuceneFileWriter final {
public:
    explicit CLuceneFileWriter(std::string index_path, std::shared_ptr<FileSystem> fs,
                               const std::shared_ptr<WritableFileOptions>& opts);
    ~CLuceneFileWriter();

    StatusOr<std::shared_ptr<StarRocksMergingDirectory>> open(const TabletIndex* index_meta);
    Status close();

private:
    static int64_t calculate_header_length(std::map<int64_t, std::vector<FileEntry>>& index_id_to_files);
    static StatusOr<std::unique_ptr<lucene::store::IndexInput>> open_input(
            const FileEntry& file, const std::shared_ptr<lucene::store::Directory>& directory);
    static Status copy_file(const FileEntry& file, const std::shared_ptr<lucene::store::Directory>& directory,
                            const std::unique_ptr<lucene::store::IndexOutput>& output, uint8_t* buffer);
    Status copy_files(const std::map<int64_t, std::vector<FileEntry>>& files,
                      const std::unique_ptr<lucene::store::IndexOutput>& output);

    std::map<int64_t, std::vector<FileEntry>> prepare_files();
    Status write_headers(int64_t offset, const std::unique_ptr<lucene::store::IndexOutput>& output,
                         const std::map<int64_t, std::vector<FileEntry>>& files);

    Status write();

    std::map<int64_t, std::shared_ptr<lucene::store::Directory>> _index_to_dir;

    std::string _index_path;
    std::string _tmp_dir;

    std::shared_ptr<FileSystem> _fs;
    std::shared_ptr<WritableFileOptions> _opts;

    FileSystem* _local_fs;

    constexpr static int64_t BUFFER_SIZE = 16 * 1024;
};

} // namespace starrocks
