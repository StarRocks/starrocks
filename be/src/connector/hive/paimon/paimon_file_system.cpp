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

#include "connector/hive/paimon/paimon_file_system.h"

#include <fmt/format.h>

#include <limits>
#include <mutex>
#include <string_view>
#include <utility>

#include "base/time/time.h"
#include "formats/scan_context.h"
#include "fs/fs.h"

namespace starrocks {
namespace {

paimon::Status to_paimon_io_status(const std::string& operation, const std::string& path, const Status& status) {
    if (status.is_not_found()) {
        return paimon::Status::NotExist(fmt::format("{} {} failed: {}", operation, path, status.to_string()));
    }
    return paimon::Status::IOError(fmt::format("{} {} failed: {}", operation, path, status.to_string()));
}

std::string join_path(const std::string& directory, std::string_view child) {
    if (directory.empty() || child.empty()) {
        return directory + std::string(child);
    }
    if (directory.back() == '/') {
        return directory + std::string(child);
    }
    return directory + "/" + std::string(child);
}

int64_t to_epoch_millis(int64_t timestamp) {
    // StarRocks POSIX, HDFS, and S3 file systems expose modification times in
    // seconds, while paimon::FileStatus requires milliseconds. Keep values
    // that are already in milliseconds unchanged.
    constexpr int64_t kMinPlausibleEpochMillis = 100000000000LL;
    if (timestamp > 0 && timestamp < kMinPlausibleEpochMillis &&
        timestamp <= std::numeric_limits<int64_t>::max() / 1000) {
        return timestamp * 1000;
    }
    return timestamp;
}

class PaimonInputStream final : public paimon::InputStream {
public:
    PaimonInputStream(std::unique_ptr<RandomAccessFile> file, FormatScannerStats* fs_stats,
                      FormatScannerStats* app_stats)
            : _file(std::move(file)), _fs_stats(fs_stats), _app_stats(app_stats) {}
    ~PaimonInputStream() override = default;

    paimon::Status Close() override { return paimon::Status::OK(); }

    paimon::Status Seek(int64_t offset, paimon::SeekOrigin origin) override {
        std::lock_guard<std::mutex> lock(_read_mutex);
        int64_t new_position = offset;
        if (origin == paimon::FS_SEEK_CUR) {
            new_position += _position;
        } else if (origin == paimon::FS_SEEK_END) {
            auto result = Length();
            if (!result.ok()) {
                return result.status();
            }
            new_position += result.value();
        }
        if (new_position < 0) {
            return paimon::Status::Invalid(
                    fmt::format("negative seek position {} for {}", new_position, _file->filename()));
        }
        _position = new_position;
        return paimon::Status::OK();
    }

    paimon::Result<int64_t> GetPos() const override {
        std::lock_guard<std::mutex> lock(_read_mutex);
        return _position;
    }

    paimon::Result<int64_t> Read(char* buffer, int64_t size) override {
        std::lock_guard<std::mutex> lock(_read_mutex);
        const int64_t start_ns = MonotonicNanos();
        auto result = _file->read_at(_position, buffer, size);
        if (!result.ok()) {
            return to_paimon_io_status("read", _file->filename(), result.status());
        }
        _position += result.value();
        update_stats(result.value(), MonotonicNanos() - start_ns);
        return result.value();
    }

    paimon::Result<int64_t> Read(char* buffer, int64_t size, int64_t offset) override {
        std::lock_guard<std::mutex> lock(_read_mutex);
        const int64_t start_ns = MonotonicNanos();
        auto result = _file->read_at(offset, buffer, size);
        if (!result.ok()) {
            return to_paimon_io_status("read", _file->filename(), result.status());
        }
        update_stats(result.value(), MonotonicNanos() - start_ns);
        return result.value();
    }

    void ReadAsync(char* buffer, int64_t size, int64_t offset,
                   std::function<void(paimon::Status)>&& callback) override {
        auto result = Read(buffer, size, offset);
        if (!result.ok()) {
            callback(result.status());
            return;
        }
        if (result.value() != size) {
            callback(paimon::Status::IOError(fmt::format("short read for {}: expected {} bytes, got {}",
                                                         _file->filename(), size, result.value())));
            return;
        }
        callback(paimon::Status::OK());
    }

    paimon::Result<std::string> GetUri() const override { return _file->filename(); }

    paimon::Result<int64_t> Length() const override {
        auto result = _file->get_size();
        if (!result.ok()) {
            return to_paimon_io_status("get size for", _file->filename(), result.status());
        }
        return result.value();
    }

private:
    void update_stats(int64_t bytes_read, int64_t elapsed_ns) {
        if (_fs_stats != nullptr) {
            ++_fs_stats->io_count;
            _fs_stats->bytes_read += bytes_read;
            _fs_stats->io_ns += elapsed_ns;
        }
        if (_app_stats != nullptr) {
            ++_app_stats->io_count;
            _app_stats->bytes_read += bytes_read;
            _app_stats->io_ns += elapsed_ns;
        }
    }

    std::unique_ptr<RandomAccessFile> _file;
    FormatScannerStats* _fs_stats;
    FormatScannerStats* _app_stats;
    mutable std::mutex _read_mutex;
    int64_t _position = 0;
};

class PaimonBasicFileStatus final : public paimon::BasicFileStatus {
public:
    PaimonBasicFileStatus(std::string path, bool is_directory) : _path(std::move(path)), _is_directory(is_directory) {}
    bool IsDir() const override { return _is_directory; }
    std::string GetPath() const override { return _path; }

private:
    std::string _path;
    bool _is_directory;
};

class PaimonFileStatus final : public paimon::FileStatus {
public:
    PaimonFileStatus(std::string path, int64_t length, int64_t modification_time, bool is_directory)
            : _path(std::move(path)),
              _length(length),
              _modification_time(modification_time),
              _is_directory(is_directory) {}
    int64_t GetLen() const override { return _length; }
    bool IsDir() const override { return _is_directory; }
    std::string GetPath() const override { return _path; }
    int64_t GetModificationTime() const override { return _modification_time; }

private:
    std::string _path;
    int64_t _length;
    int64_t _modification_time;
    bool _is_directory;
};

} // namespace

paimon::Result<std::unique_ptr<paimon::InputStream>> PaimonFileSystem::Open(const std::string& path) const {
    auto result = _file_system->new_random_access_file(path);
    if (!result.ok()) {
        return to_paimon_io_status("open", path, result.status());
    }
    return std::make_unique<PaimonInputStream>(std::move(result).value(), _fs_stats, _app_stats);
}

paimon::Result<std::unique_ptr<paimon::InputStream>> PaimonFileSystem::Open(const std::string& path,
                                                                            int64_t file_size) const {
    if (file_size < 0) {
        return paimon::Status::Invalid(fmt::format("negative file size {} for {}", file_size, path));
    }
    FileInfo file_info{.path = path, .size = file_size};
    auto result = _file_system->new_random_access_file(file_info);
    if (!result.ok()) {
        return to_paimon_io_status("open", path, result.status());
    }
    return std::make_unique<PaimonInputStream>(std::move(result).value(), _fs_stats, _app_stats);
}

paimon::Result<std::unique_ptr<paimon::OutputStream>> PaimonFileSystem::Create(const std::string& path,
                                                                               bool overwrite) const {
    return paimon::Status::NotImplemented(
            fmt::format("creating {} (overwrite={}) is not supported by the StarRocks Paimon reader", path, overwrite));
}

paimon::Status PaimonFileSystem::Mkdirs(const std::string& path) const {
    return paimon::Status::NotImplemented(
            fmt::format("creating directory {} is not supported by the StarRocks Paimon reader", path));
}

paimon::Status PaimonFileSystem::Rename(const std::string& src, const std::string& dst) const {
    return paimon::Status::NotImplemented(
            fmt::format("renaming {} to {} is not supported by the StarRocks Paimon reader", src, dst));
}

paimon::Status PaimonFileSystem::Delete(const std::string& path, bool recursive) const {
    return paimon::Status::NotImplemented(
            fmt::format("deleting {} (recursive={}) is not supported by the StarRocks Paimon reader", path, recursive));
}

paimon::Result<std::unique_ptr<paimon::FileStatus>> PaimonFileSystem::GetFileStatus(const std::string& path) const {
    auto directory_result = _file_system->is_directory(path);
    if (!directory_result.ok()) {
        if (!directory_result.status().is_not_supported()) {
            return to_paimon_io_status("stat", path, directory_result.status());
        }
        const Status exists_status = _file_system->path_exists(path);
        if (!exists_status.ok()) {
            return to_paimon_io_status("stat", path, exists_status);
        }
        // File systems without directory stat support can still serve exact
        // metadata and data file paths.
        directory_result = false;
    }

    int64_t length = 0;
    if (!directory_result.value()) {
        auto size_result = _file_system->get_file_size(path);
        if (size_result.ok()) {
            length = static_cast<int64_t>(size_result.value());
        } else if (size_result.status().is_not_supported()) {
            auto file_result = _file_system->new_random_access_file(path);
            if (!file_result.ok()) {
                return to_paimon_io_status("open", path, file_result.status());
            }
            auto stream_size_result = file_result.value()->get_size();
            if (!stream_size_result.ok()) {
                return to_paimon_io_status("get size for", path, stream_size_result.status());
            }
            length = stream_size_result.value();
        } else {
            return to_paimon_io_status("get size for", path, size_result.status());
        }
    }

    int64_t modification_time = 0;
    auto modification_time_result = _file_system->get_file_modified_time(path);
    if (modification_time_result.ok()) {
        modification_time = to_epoch_millis(static_cast<int64_t>(modification_time_result.value()));
    } else if (!modification_time_result.status().is_not_supported()) {
        return to_paimon_io_status("get modification time for", path, modification_time_result.status());
    }
    return std::make_unique<PaimonFileStatus>(path, length, modification_time, directory_result.value());
}

paimon::Status PaimonFileSystem::ListDir(
        const std::string& directory, std::vector<std::unique_ptr<paimon::BasicFileStatus>>* file_status_list) const {
    if (file_status_list == nullptr) {
        return paimon::Status::Invalid("file_status_list must not be null");
    }
    file_status_list->clear();
    const Status status = _file_system->iterate_dir2(directory, [&](const DirEntry& entry) {
        file_status_list->emplace_back(std::make_unique<PaimonBasicFileStatus>(join_path(directory, entry.name),
                                                                               entry.is_dir.value_or(false)));
        return true;
    });
    return status.ok() ? paimon::Status::OK() : to_paimon_io_status("list", directory, status);
}

paimon::Status PaimonFileSystem::ListFileStatus(
        const std::string& path, std::vector<std::unique_ptr<paimon::FileStatus>>* file_status_list) const {
    if (file_status_list == nullptr) {
        return paimon::Status::Invalid("file_status_list must not be null");
    }
    file_status_list->clear();

    auto directory_result = _file_system->is_directory(path);
    if (!directory_result.ok()) {
        if (directory_result.status().is_not_supported()) {
            auto status_result = GetFileStatus(path);
            if (!status_result.ok()) {
                return status_result.status();
            }
            file_status_list->emplace_back(std::move(status_result).value());
            return paimon::Status::OK();
        }
        return to_paimon_io_status("stat", path, directory_result.status());
    }
    if (!directory_result.value()) {
        auto status_result = GetFileStatus(path);
        if (!status_result.ok()) {
            return status_result.status();
        }
        file_status_list->emplace_back(std::move(status_result).value());
        return paimon::Status::OK();
    }

    const Status status = _file_system->iterate_dir2(path, [&](const DirEntry& entry) {
        file_status_list->emplace_back(std::make_unique<PaimonFileStatus>(
                join_path(path, entry.name), entry.size.value_or(0), to_epoch_millis(entry.mtime.value_or(0)),
                entry.is_dir.value_or(false)));
        return true;
    });
    return status.ok() ? paimon::Status::OK() : to_paimon_io_status("list", path, status);
}

paimon::Result<bool> PaimonFileSystem::Exists(const std::string& path) const {
    const Status status = _file_system->path_exists(path);
    if (status.ok()) {
        return true;
    }
    if (status.is_not_found()) {
        return false;
    }
    if (status.is_not_supported()) {
        auto directory_result = _file_system->is_directory(path);
        if (directory_result.ok()) {
            // A successful directory probe establishes existence for both a
            // directory (true) and an exact object/file (false).
            return true;
        }
        if (directory_result.status().is_not_found()) {
            return false;
        }
        return to_paimon_io_status("check existence of", path, directory_result.status());
    }
    return to_paimon_io_status("check existence of", path, status);
}

} // namespace starrocks
