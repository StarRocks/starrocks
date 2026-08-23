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

#include <memory>
#include <mutex>
#include <string_view>
#include <utility>

#include "base/time/time.h"
#include "cache/scan/cache_input_stream.h"
#include "cache/scan/shared_buffered_input_stream.h"
#include "common/config_cache_fwd.h"
#include "common/config_scan_io_fwd.h"
#include "fs/fs.h"

namespace starrocks {

void PaimonFileSystemStats::record_app_read(ReadType type, int64_t bytes, int64_t elapsed_ns) {
    switch (type) {
    case ReadType::SEQUENTIAL:
        _sequential_read_count.fetch_add(1, std::memory_order_relaxed);
        _sequential_read_bytes.fetch_add(bytes, std::memory_order_relaxed);
        _sequential_read_ns.fetch_add(elapsed_ns, std::memory_order_relaxed);
        return;
    case ReadType::POSITIONAL:
        _positional_read_count.fetch_add(1, std::memory_order_relaxed);
        _positional_read_bytes.fetch_add(bytes, std::memory_order_relaxed);
        _positional_read_ns.fetch_add(elapsed_ns, std::memory_order_relaxed);
        return;
    case ReadType::ASYNC:
        _async_read_count.fetch_add(1, std::memory_order_relaxed);
        _async_read_bytes.fetch_add(bytes, std::memory_order_relaxed);
        _async_read_ns.fetch_add(elapsed_ns, std::memory_order_relaxed);
        return;
    }
}

void PaimonFileSystemStats::record_fs_read(int64_t bytes, int64_t elapsed_ns) {
    _fs_io_count.fetch_add(1, std::memory_order_relaxed);
    _fs_io_bytes.fetch_add(bytes, std::memory_order_relaxed);
    _fs_io_ns.fetch_add(elapsed_ns, std::memory_order_relaxed);
}

void PaimonFileSystemStats::record_stream_stats(const CacheInputStream::Stats& cache_stats,
                                                const SharedBufferedStats& shared_buffered_stats) {
    std::lock_guard<std::mutex> lock(_stream_stats_mutex);
    _cache_stats.read_block_cache_ns += cache_stats.read_block_cache_ns;
    _cache_stats.write_block_cache_ns += cache_stats.write_block_cache_ns;
    _cache_stats.read_block_cache_count += cache_stats.read_block_cache_count;
    _cache_stats.write_block_cache_count += cache_stats.write_block_cache_count;
    _cache_stats.write_mem_cache_bytes += cache_stats.write_mem_cache_bytes;
    _cache_stats.write_disk_cache_bytes += cache_stats.write_disk_cache_bytes;
    _cache_stats.read_block_cache_bytes += cache_stats.read_block_cache_bytes;
    _cache_stats.read_mem_cache_bytes += cache_stats.read_mem_cache_bytes;
    _cache_stats.read_disk_cache_bytes += cache_stats.read_disk_cache_bytes;
    _cache_stats.read_peer_cache_bytes += cache_stats.read_peer_cache_bytes;
    _cache_stats.read_peer_cache_count += cache_stats.read_peer_cache_count;
    _cache_stats.read_peer_cache_ns += cache_stats.read_peer_cache_ns;
    _cache_stats.write_block_cache_bytes += cache_stats.write_block_cache_bytes;
    _cache_stats.skip_read_cache_count += cache_stats.skip_read_cache_count;
    _cache_stats.skip_read_cache_bytes += cache_stats.skip_read_cache_bytes;
    _cache_stats.skip_read_peer_cache_count += cache_stats.skip_read_peer_cache_count;
    _cache_stats.skip_read_peer_cache_bytes += cache_stats.skip_read_peer_cache_bytes;
    _cache_stats.skip_write_cache_count += cache_stats.skip_write_cache_count;
    _cache_stats.skip_write_cache_bytes += cache_stats.skip_write_cache_bytes;
    _cache_stats.write_cache_fail_count += cache_stats.write_cache_fail_count;
    _cache_stats.write_cache_fail_bytes += cache_stats.write_cache_fail_bytes;
    _cache_stats.read_block_buffer_bytes += cache_stats.read_block_buffer_bytes;
    _cache_stats.read_block_buffer_count += cache_stats.read_block_buffer_count;

    _shared_buffered_stats.shared_io_count += shared_buffered_stats.shared_io_count;
    _shared_buffered_stats.shared_io_bytes += shared_buffered_stats.shared_io_bytes;
    _shared_buffered_stats.shared_align_io_bytes += shared_buffered_stats.shared_align_io_bytes;
    _shared_buffered_stats.shared_io_timer += shared_buffered_stats.shared_io_timer;
    _shared_buffered_stats.direct_io_count += shared_buffered_stats.direct_io_count;
    _shared_buffered_stats.direct_io_bytes += shared_buffered_stats.direct_io_bytes;
    _shared_buffered_stats.direct_io_timer += shared_buffered_stats.direct_io_timer;
}

PaimonFileSystemStats::Snapshot PaimonFileSystemStats::snapshot() const {
    std::lock_guard<std::mutex> lock(_stream_stats_mutex);
    return {
            .sequential_read_count = _sequential_read_count.load(std::memory_order_relaxed),
            .sequential_read_bytes = _sequential_read_bytes.load(std::memory_order_relaxed),
            .sequential_read_ns = _sequential_read_ns.load(std::memory_order_relaxed),
            .positional_read_count = _positional_read_count.load(std::memory_order_relaxed),
            .positional_read_bytes = _positional_read_bytes.load(std::memory_order_relaxed),
            .positional_read_ns = _positional_read_ns.load(std::memory_order_relaxed),
            .async_read_count = _async_read_count.load(std::memory_order_relaxed),
            .async_read_bytes = _async_read_bytes.load(std::memory_order_relaxed),
            .async_read_ns = _async_read_ns.load(std::memory_order_relaxed),
            .fs_io_count = _fs_io_count.load(std::memory_order_relaxed),
            .fs_io_bytes = _fs_io_bytes.load(std::memory_order_relaxed),
            .fs_io_ns = _fs_io_ns.load(std::memory_order_relaxed),
            .datacache = _cache_stats,
            .shared_buffered = _shared_buffered_stats,
    };
}

namespace {

class PaimonCountedSeekableInputStream final : public io::SeekableInputStreamWrapper {
public:
    PaimonCountedSeekableInputStream(std::shared_ptr<io::SeekableInputStream> stream,
                                     std::shared_ptr<PaimonFileSystemStats> stats)
            : io::SeekableInputStreamWrapper(stream.get(), kDontTakeOwnership),
              _stream(std::move(stream)),
              _stats(std::move(stats)) {}

    StatusOr<int64_t> read(void* data, int64_t size) override {
        const int64_t start_ns = MonotonicNanos();
        auto result = _stream->read(data, size);
        _stats->record_fs_read(result.ok() ? result.value() : 0, MonotonicNanos() - start_ns);
        return result;
    }

    Status read_at_fully(int64_t offset, void* data, int64_t size) override {
        const int64_t start_ns = MonotonicNanos();
        auto status = _stream->read_at_fully(offset, data, size);
        _stats->record_fs_read(status.ok() ? size : 0, MonotonicNanos() - start_ns);
        return status;
    }

    StatusOr<int64_t> read_at(int64_t offset, void* data, int64_t size) override {
        const int64_t start_ns = MonotonicNanos();
        auto result = _stream->read_at(offset, data, size);
        _stats->record_fs_read(result.ok() ? result.value() : 0, MonotonicNanos() - start_ns);
        return result;
    }

private:
    std::shared_ptr<io::SeekableInputStream> _stream;
    std::shared_ptr<PaimonFileSystemStats> _stats;
};

PaimonFileSystemStats::SharedBufferedStats snapshot_shared_buffered_stats(const SharedBufferedInputStream* stream) {
    return {
            .shared_io_count = stream->shared_io_count(),
            .shared_io_bytes = stream->shared_io_bytes(),
            .shared_align_io_bytes = stream->shared_align_io_bytes(),
            .shared_io_timer = stream->shared_io_timer(),
            .direct_io_count = stream->direct_io_count(),
            .direct_io_bytes = stream->direct_io_bytes(),
            .direct_io_timer = stream->direct_io_timer(),
    };
}

class PaimonInputStream final : public paimon::InputStream {
public:
    PaimonInputStream(std::unique_ptr<RandomAccessFile> file, std::shared_ptr<PaimonFileSystemStats> stats,
                      CacheInputStream* cache_stream = nullptr,
                      SharedBufferedInputStream* shared_buffered_stream = nullptr)
            : _file(std::move(file)),
              _stats(std::move(stats)),
              _cache_stream(cache_stream),
              _shared_buffered_stream(shared_buffered_stream) {}
    ~PaimonInputStream() override { (void)Close(); }

    paimon::Status Close() override {
        _finalize_stream_stats();
        _file.reset();
        return paimon::Status::OK();
    }

    paimon::Status Seek(int64_t offset, paimon::SeekOrigin origin) override {
        int64_t new_position = offset;
        if (origin == paimon::FS_SEEK_CUR) {
            auto result = GetPos();
            if (!result.ok()) {
                return result.status();
            }
            new_position += result.value();
        } else if (origin == paimon::FS_SEEK_END) {
            auto result = Length();
            if (!result.ok()) {
                return result.status();
            }
            new_position += result.value();
        }
        const Status status = _file->seek(new_position);
        if (!status.ok()) {
            return paimon::Status::IOError(
                    fmt::format("Failed to seek file {}, reason: {}", _file->filename(), status.detailed_message()));
        }
        return paimon::Status::OK();
    }

    paimon::Result<int64_t> GetPos() const override {
        auto result = _file->position();
        if (!result.ok()) {
            return paimon::Status::IOError(fmt::format("Failed to get position for file {}, reason: {}",
                                                       _file->filename(), result.status().detailed_message()));
        }
        return result.value();
    }

    paimon::Result<int64_t> Read(char* buffer, int64_t size) override {
        if (size < 0) {
            return paimon::Status::Invalid(fmt::format("negative read size {} for {}", size, _file->filename()));
        }
        const int64_t start_ns = MonotonicNanos();
        auto result = _file->read(buffer, size);
        _stats->record_app_read(PaimonFileSystemStats::ReadType::SEQUENTIAL, result.ok() ? result.value() : 0,
                                MonotonicNanos() - start_ns);
        if (!result.ok()) {
            return paimon::Status::IOError(fmt::format("Failed to read file {}, reason: {}", _file->filename(),
                                                       result.status().detailed_message()));
        }
        return result.value();
    }

    paimon::Result<int64_t> Read(char* buffer, int64_t size, int64_t offset) override {
        if (size < 0 || offset < 0) {
            return paimon::Status::Invalid(
                    fmt::format("invalid positional read for {}: size={}, offset={}", _file->filename(), size, offset));
        }
        return _read_at(buffer, size, offset, PaimonFileSystemStats::ReadType::POSITIONAL);
    }

    void ReadAsync(char* buffer, int64_t size, int64_t offset,
                   std::function<void(paimon::Status)>&& callback) override {
        if (size < 0 || offset < 0) {
            callback(paimon::Status::Invalid(
                    fmt::format("invalid async read for {}: size={}, offset={}", _file->filename(), size, offset)));
            return;
        }
        auto result = _read_at(buffer, size, offset, PaimonFileSystemStats::ReadType::ASYNC);
        callback(result.ok() ? paimon::Status::OK() : result.status());
    }

    paimon::Result<std::string> GetUri() const override { return _file->filename(); }

    paimon::Result<int64_t> Length() const override {
        auto result = _file->get_size();
        if (!result.ok()) {
            return paimon::Status::IOError(fmt::format("Failed to get length for file {}, reason: {}",
                                                       _file->filename(), result.status().detailed_message()));
        }
        return result.value();
    }

private:
    void _finalize_stream_stats() {
        if (_cache_stream != nullptr && _shared_buffered_stream != nullptr) {
            _stats->record_stream_stats(_cache_stream->stats(),
                                        snapshot_shared_buffered_stats(_shared_buffered_stream));
        }
        _cache_stream = nullptr;
        _shared_buffered_stream = nullptr;
    }

    paimon::Result<int64_t> _read_at(char* buffer, int64_t size, int64_t offset,
                                     PaimonFileSystemStats::ReadType read_type) {
        // SeekableInputStream's default read_at_fully uses seek + read, so preserve the sequential cursor here.
        auto position_result = _file->position();
        if (!position_result.ok()) {
            return paimon::Status::IOError(fmt::format("Failed to get position for file {}, reason: {}",
                                                       _file->filename(), position_result.status().detailed_message()));
        }
        const int64_t start_ns = MonotonicNanos();
        const Status status = _file->read_at_fully(offset, buffer, size);
        _stats->record_app_read(read_type, status.ok() ? size : 0, MonotonicNanos() - start_ns);
        auto current_position_result = _file->position();
        if (!current_position_result.ok()) {
            return paimon::Status::IOError(fmt::format("Failed to get position for file {}, reason: {}",
                                                       _file->filename(),
                                                       current_position_result.status().detailed_message()));
        }
        Status restore_status;
        if (current_position_result.value() != position_result.value()) {
            restore_status = _file->seek(position_result.value());
        }
        if (!status.ok()) {
            return paimon::Status::IOError(
                    fmt::format("Failed to read file {}, reason: {}", _file->filename(), status.detailed_message()));
        }
        if (!restore_status.ok()) {
            return paimon::Status::IOError(fmt::format("Failed to restore position for file {}, reason: {}",
                                                       _file->filename(), restore_status.detailed_message()));
        }
        return size;
    }

    std::unique_ptr<RandomAccessFile> _file;
    std::shared_ptr<PaimonFileSystemStats> _stats;
    CacheInputStream* _cache_stream = nullptr;
    SharedBufferedInputStream* _shared_buffered_stream = nullptr;
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
    return Open(path, -1);
}

paimon::Result<std::unique_ptr<paimon::InputStream>> PaimonFileSystem::Open(const std::string& path,
                                                                            int64_t file_size) const {
    RandomAccessFileOptions options;
    auto result = _file_system->new_random_access_file(options, path);
    if (!result.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to open file {}, reason: {}", path, result.status().detailed_message()));
    }

    auto raw_file = std::move(result).value();
    const std::string& filename = raw_file->filename();
    std::shared_ptr<io::SeekableInputStream> input_stream = raw_file->stream();
    input_stream = std::make_shared<PaimonCountedSeekableInputStream>(input_stream, _stats);

    if (!_datacache_options.enable_datacache) {
        auto counted_file = std::make_unique<RandomAccessFile>(input_stream, filename);
        if (file_size >= 0) {
            counted_file->set_size(file_size);
        }
        return std::make_unique<PaimonInputStream>(std::move(counted_file), _stats);
    }

    if (file_size < 0) {
        auto size_result = raw_file->get_size();
        if (!size_result.ok()) {
            return paimon::Status::IOError(fmt::format("Failed to get file size for {}, reason: {}", path,
                                                       size_result.status().detailed_message()));
        }
        file_size = size_result.value();
    }

    auto shared_buffered_input_stream = std::make_shared<SharedBufferedInputStream>(input_stream, filename, file_size);
    const SharedBufferedInputStream::CoalesceOptions coalesce_options = {
            .max_dist_size = config::io_coalesce_read_max_distance_size,
            .max_buffer_size = config::io_coalesce_read_max_buffer_size};
    shared_buffered_input_stream->set_coalesce_options(coalesce_options);

    auto cache_input_stream = std::make_shared<CacheInputStream>(shared_buffered_input_stream, filename, file_size, 0);
    cache_input_stream->set_enable_populate_cache(_datacache_options.enable_populate_datacache);
    cache_input_stream->set_enable_async_populate_mode(_datacache_options.enable_datacache_async_populate_mode);
    cache_input_stream->set_enable_cache_io_adaptor(_datacache_options.enable_datacache_io_adaptor);
    cache_input_stream->set_priority(_datacache_options.datacache_priority);
    cache_input_stream->set_ttl_seconds(_datacache_options.datacache_ttl_seconds);
    cache_input_stream->set_enable_block_buffer(config::datacache_block_buffer_enable);
    shared_buffered_input_stream->set_align_size(cache_input_stream->get_align_size());

    auto cache_file = std::make_unique<RandomAccessFile>(cache_input_stream, filename);
    cache_file->set_size(file_size);
    return std::make_unique<PaimonInputStream>(std::move(cache_file), _stats, cache_input_stream.get(),
                                               shared_buffered_input_stream.get());
}

paimon::Result<std::unique_ptr<paimon::OutputStream>> PaimonFileSystem::Create(const std::string&, bool) const {
    return paimon::Status::NotImplemented("Paimon reader file system does not support Create");
}

paimon::Status PaimonFileSystem::Mkdirs(const std::string&) const {
    return paimon::Status::NotImplemented("Paimon reader file system does not support Mkdirs");
}

paimon::Status PaimonFileSystem::Rename(const std::string&, const std::string&) const {
    return paimon::Status::NotImplemented("Paimon reader file system does not support Rename");
}

paimon::Status PaimonFileSystem::Delete(const std::string&, bool) const {
    return paimon::Status::NotImplemented("Paimon reader file system does not support Delete");
}

paimon::Result<std::unique_ptr<paimon::FileStatus>> PaimonFileSystem::GetFileStatus(const std::string& path) const {
    const Status exists_status = _file_system->path_exists(path);
    if (!exists_status.ok()) {
        if (exists_status.is_not_found()) {
            return paimon::Status::NotExist(fmt::format("Path {} is not exist.", path));
        }
        return paimon::Status::IOError(
                fmt::format("Get file status for {} failed, reason: {}", path, exists_status.detailed_message()));
    }
    auto directory_result = _file_system->is_directory(path);
    if (!directory_result.ok()) {
        return paimon::Status::IOError(fmt::format(
                "Get file status but failed to check whether path {} is directory or not from get status, reason: {}",
                path, directory_result.status().detailed_message()));
    }
    auto size_result = _file_system->get_file_size(path);
    if (!size_result.ok()) {
        return paimon::Status::IOError(fmt::format("Failed to get file size for {}, reason: {}", path,
                                                   size_result.status().detailed_message()));
    }
    auto modification_time_result = _file_system->get_file_modified_time(path);
    if (!modification_time_result.ok()) {
        return paimon::Status::IOError(fmt::format("Failed to get file modified time for {}, reason: {}", path,
                                                   modification_time_result.status().detailed_message()));
    }
    return std::make_unique<PaimonFileStatus>(path, size_result.value(), modification_time_result.value(),
                                              directory_result.value());
}

paimon::Status PaimonFileSystem::ListDir(
        const std::string& directory, std::vector<std::unique_ptr<paimon::BasicFileStatus>>* file_status_list) const {
    if (directory.empty()) {
        return paimon::Status::IOError("dir is empty.");
    }
    if (file_status_list == nullptr) {
        return paimon::Status::Invalid("file_status_list must not be null");
    }
    auto directory_result = _file_system->is_directory(directory);
    if (!directory_result.ok()) {
        return paimon::Status::IOError(fmt::format("Failed to check {} is directory or not from list dir, reason: {}",
                                                   directory, directory_result.status().detailed_message()));
    }
    if (!directory_result.value()) {
        return paimon::Status::IOError(
                fmt::format("Cannot get status for {}, because it is not a directory.", directory));
    }
    const Status status = _file_system->iterate_dir2(directory, [&](const DirEntry& entry) {
        file_status_list->emplace_back(
                std::make_unique<PaimonBasicFileStatus>(std::string(entry.name), entry.is_dir.value_or(false)));
        return true;
    });
    if (!status.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to get status for {}, reason: {}", directory, status.detailed_message()));
    }
    return paimon::Status::OK();
}

paimon::Status PaimonFileSystem::ListFileStatus(
        const std::string& path, std::vector<std::unique_ptr<paimon::FileStatus>>* file_status_list) const {
    if (path.empty()) {
        return paimon::Status::IOError("path is empty.");
    }
    if (file_status_list == nullptr) {
        return paimon::Status::Invalid("file_status_list must not be null");
    }

    auto directory_result = _file_system->is_directory(path);
    if (!directory_result.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to check {} is directory or not from list status, reason: {}", path,
                            directory_result.status().detailed_message()));
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
                std::string(entry.name), entry.size.value_or(0), entry.mtime.value_or(0), entry.is_dir.value_or(true)));
        return true;
    });
    if (!status.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to list file status for {}, reason: {}", path, status.detailed_message()));
    }
    return paimon::Status::OK();
}

paimon::Result<bool> PaimonFileSystem::Exists(const std::string& path) const {
    const Status status = _file_system->path_exists(path);
    if (status.ok()) {
        return true;
    }
    if (status.is_not_found()) {
        return false;
    }
    return paimon::Status::IOError(
            fmt::format("Error occurs while checking path {} exist, reason: {}", path, status.detailed_message()));
}

} // namespace starrocks
