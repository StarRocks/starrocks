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

#include "clucene_file_manager.h"

#include "fs/fs.h"
#include "storage/index/inverted/clucene/clucene_file_reader.h"
#include "storage/index/inverted/clucene/clucene_file_writer.h"

namespace starrocks {

CLuceneFileManager& CLuceneFileManager::getInstance() {
    static CLuceneFileManager instance;
    return instance;
}

StatusOr<std::shared_ptr<CLuceneFileWriter>> CLuceneFileManager::get_or_create_clucene_file_writer(
        const std::string& path) {
    std::unique_lock l(_write_mutex);
    if (!_path_to_file_writers.contains(path)) {
        VLOG(1) << "create clucene file writer for " << path;
        const auto opts = std::make_shared<WritableFileOptions>();
        opts->mode = FileSystem::OpenMode::CREATE_OR_OPEN;
        opts->sync_on_close = true;

        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(path));
        auto index_file_writer = std::make_shared<CLuceneFileWriter>(fs, path);
        index_file_writer->set_file_writer_opts(opts);

        _path_to_file_writers.emplace(path, index_file_writer);
    }
    return _path_to_file_writers.at(path);
}

Status CLuceneFileManager::remove_clucene_file_writer(const std::string& path) {
    std::unique_lock l(_write_mutex);
    VLOG(1) << "erase clucene file writer for " << path;
    _path_to_file_writers.erase(path);
    return Status::OK();
}

StatusOr<std::shared_ptr<CLuceneFileReader>> CLuceneFileManager::get_or_create_clucene_file_reader(
        const std::string& path) {
    VLOG(1) << "create clucene file reader for " << path;
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(path));
    const auto& reader = std::make_shared<CLuceneFileReader>(std::move(fs), path);
    RETURN_IF_ERROR(reader->init());
    return reader;
}

} // namespace starrocks