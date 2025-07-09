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
#include <utility>

#include "common/statusor.h"
#include "storage/index/inverted/clucene/clucene_common.h"

namespace lucene::store {
class Directory;
class IndexOutput;
} // namespace lucene::store

namespace starrocks {

class TabletIndex;

struct FileInfo;

class StarRocksFSDirectory;
class WritableFile;
struct WritableFileOptions;

class CLuceneInvertedWriter;
using CLuceneInvertedWriterPtr = std::unique_ptr<CLuceneInvertedWriter>;

struct FileMetadata {
    int64_t index_id;
    std::string filename;
    int64_t offset;
    int64_t length;
    lucene::store::Directory* directory;

    FileMetadata(const int64_t id, std::string file, const int64_t off, const int64_t len,
                 lucene::store::Directory* dir)
            : index_id(id), filename(file), offset(off), length(len), directory(dir) {}
};

class FileSystem;

class CLuceneFileWriter final {
public:
    CLuceneFileWriter(std::shared_ptr<FileSystem> fs, std::string index_path,
                      std::unique_ptr<WritableFile> file_writer = nullptr, bool can_use_ram_dir = true);

    ~CLuceneFileWriter();

    Status delete_index(const TabletIndex* index_meta);
    Status initialize(InvertedIndexDirectoryMap& indices_dirs);

    StatusOr<std::shared_ptr<StarRocksFSDirectory>> open(const TabletIndex* index_meta);
    Status write();
    Status close();

    int64_t get_index_file_total_size() const;

    const FileSystem* get_fs() const;
    void set_file_writer_opts(const std::shared_ptr<WritableFileOptions>& opts);

    std::string debug_string() const;

private:
    static void sort_files(std::vector<FileInfo>& file_infos);
    static std::vector<FileInfo> prepare_sorted_files(const std::shared_ptr<lucene::store::Directory>& directory);
    static void copy_files_data(lucene::store::IndexOutput* output, const std::vector<FileMetadata>& file_metadata);
    static void copy_file(const std::string& fileName, lucene::store::Directory* dir,
                          lucene::store::IndexOutput* output, uint8_t* buffer, int64_t bufferLength);
    static void write_index_headers_and_metadata(lucene::store::IndexOutput* output,
                                                 const std::vector<FileMetadata>& file_metadata);

    int64_t headerLength();

    StatusOr<std::pair<std::shared_ptr<lucene::store::Directory>, std::unique_ptr<lucene::store::IndexOutput>>>
    create_output_stream() const;

    void write_indices_count(lucene::store::IndexOutput* output) const;

    std::vector<FileMetadata> prepare_file_metadata(int64_t& current_offset);
    Status _insert_directory_into_map(int64_t index_id, std::shared_ptr<StarRocksFSDirectory> dir);
    // Member variables...
    InvertedIndexDirectoryMap _indices_dirs;

    std::string _index_path;
    std::string _tmp_dir;

    std::shared_ptr<FileSystem> _fs;
    FileSystem* _local_fs;

    // write to disk or stream
    std::unique_ptr<WritableFile> _index_writer = nullptr;
    std::shared_ptr<WritableFileOptions> _opts;

    int64_t _total_file_size = 0;

    // only once
    bool _closed = false;
    bool _can_use_ram_dir = true;
};

} // namespace starrocks
