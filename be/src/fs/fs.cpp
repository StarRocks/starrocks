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

#include "fs/fs.h"

#include <ctime>
#include <deque>
#include <mutex>
#include <unordered_map>

#include "base/format.h"
#include "common/config_local_io_fwd.h"
#include "fs/bundle_file.h"
#include "fs/encrypt_file.h"

namespace starrocks {

std::unique_ptr<SequentialFile> SequentialFile::from(std::unique_ptr<io::SeekableInputStream> stream,
                                                     const std::string& name, const FileEncryptionInfo& info) {
    if (info.is_encrypted()) {
        return std::make_unique<SequentialFile>(std::make_unique<EncryptSeekableInputStream>(std::move(stream), info),
                                                name);
    } else {
        return std::make_unique<SequentialFile>(std::move(stream), name);
    }
}

std::unique_ptr<RandomAccessFile> RandomAccessFile::from(std::unique_ptr<io::SeekableInputStream> stream,
                                                         const std::string& name, bool is_cache_hit,
                                                         const FileEncryptionInfo& info) {
    if (info.is_encrypted()) {
        return std::make_unique<RandomAccessFile>(std::make_unique<EncryptSeekableInputStream>(std::move(stream), info),
                                                  name, is_cache_hit);
    } else {
        return std::make_unique<RandomAccessFile>(std::move(stream), name, is_cache_hit);
    }
}

StatusOr<std::unique_ptr<RandomAccessFile>> FileSystem::new_random_access_file_with_bundling(
        const RandomAccessFileOptions& opts, const FileInfo& file_info) {
    if (file_info.bundle_file_offset.has_value() && file_info.bundle_file_offset.value() >= 0) {
        // If the file is a shared file, we need to create a new random access file with the offset.
        // Notice, we CAN'T pass file_info to new_random_access_file, because size in file_info is not the size of the
        // total bundle file.
        ASSIGN_OR_RETURN(auto file, new_random_access_file(opts, file_info.path));
        auto bundle_file = std::make_unique<BundleSeekableInputStream>(
                file->stream(), file_info.bundle_file_offset.value(), file_info.size.value());
        RETURN_IF_ERROR(bundle_file->init());
        // Pass the slice's base offset to the outer RandomAccessFile so its page_cache_key folds
        // it in. The outer wrapper otherwise sees only `path` and would produce identical keys
        // for every slice of the same physical file.
        return std::make_unique<RandomAccessFile>(std::move(bundle_file), file->filename(), file->is_cache_hit(),
                                                  file_info.bundle_file_offset.value());
    } else {
        return new_random_access_file(opts, file_info);
    }
}

static std::deque<FileWriteStat> file_write_history;
static std::unordered_map<uint64_t, FileWriteStat> file_writes;
static std::mutex file_writes_mutex;

void FileSystem::get_file_write_history(std::vector<FileWriteStat>* stats) {
    std::lock_guard<std::mutex> l(file_writes_mutex);
    stats->assign(file_write_history.begin(), file_write_history.end());
    for (auto& it : file_writes) {
        stats->push_back(it.second);
    }
}

void FileSystem::on_file_write_open(WritableFile* file) {
    std::lock_guard<std::mutex> l(file_writes_mutex);
    FileWriteStat stat;
    stat.path = file->filename();
    stat.open_time = time(nullptr);
    file_writes[reinterpret_cast<uint64_t>(file)] = stat;
}

void FileSystem::on_file_write_close(WritableFile* file) {
    std::lock_guard<std::mutex> l(file_writes_mutex);
    auto it = file_writes.find(reinterpret_cast<uint64_t>(file));
    if (it == file_writes.end()) {
        return;
    }
    it->second.close_time = time(nullptr);
    it->second.size = file->size();
    file_write_history.push_back(it->second);
    file_writes.erase(it);
    while (file_write_history.size() > config::file_write_history_size) {
        file_write_history.pop_front();
    }
}

} // namespace starrocks

auto fmt::formatter<starrocks::FileSystem::OpenMode>::format(const starrocks::FileSystem::OpenMode value,
                                                             format_context& ctx) const -> format_context::iterator {
    return formatter<std::underlying_type_t<starrocks::FileSystem::OpenMode>>::format(
            starrocks::enum_to_underlying_type(value), ctx);
}
