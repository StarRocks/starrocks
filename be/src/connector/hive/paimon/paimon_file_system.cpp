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

std::vector<std::string> PaimonWriteFileTracker::owned_files() const {
    std::lock_guard<std::mutex> lock(_mutex);
    std::vector<std::string> paths;
    for (const auto& [path, state] : _files) {
        if (state.owned) {
            paths.emplace_back(path);
        }
    }
    return paths;
}

Status PaimonWriteFileTracker::mark_published(const std::vector<std::string>& paths) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (_rolled_back) {
        return Status::InvalidArgument("Cannot publish a rolled back Paimon write attempt");
    }
    for (const auto& path : paths) {
        auto it = _files.find(path);
        if (it == _files.end() || (!it->second.owned && !it->second.published) || it->second.open ||
            !it->second.closed_ok) {
            return Status::InvalidArgument(fmt::format("Paimon file is not successfully closed and owned: {}", path));
        }
    }
    for (const auto& path : paths) {
        auto& state = _files.at(path);
        state.owned = false;
        state.published = true;
    }
    return Status::OK();
}

void PaimonWriteFileTracker::_stream_closed(const std::string& path, bool success) {
    std::lock_guard<std::mutex> lock(_mutex);
    auto& state = _files.at(path);
    state.open = false;
    state.closed_ok = success;
}

Status PaimonWriteFileTracker::rollback() {
    std::lock_guard<std::mutex> lock(_mutex);
    _rolled_back = true;
    Status result;
    for (auto& [path, state] : _files) {
        if (!state.owned) {
            continue;
        }
        if (state.open) {
            result.update(Status::InvalidArgument(fmt::format("Close Paimon output before rollback: {}", path)));
            continue;
        }
        auto status = _file_system->delete_file(path);
        if (status.ok() || status.is_not_found()) {
            state.owned = false;
        } else {
            result.update(status.clone_and_prepend(fmt::format("Failed to roll back Paimon file {}: ", path)));
        }
    }
    return result;
}

// WritableFile::append is all-or-error, unlike paimon's byte-count API. Verify
// its reported position too: an OK short append must never become a successful
// data file. Keep the first failure sticky through Close and always close once.
class PaimonOutputStream final : public paimon::OutputStream {
public:
    PaimonOutputStream(std::unique_ptr<WritableFile> file, std::string path,
                       std::shared_ptr<PaimonWriteFileTracker> tracker, bool flush_closes_file)
            : _file(std::move(file)),
              _path(std::move(path)),
              _tracker(std::move(tracker)),
              _flush_closes_file(flush_closes_file) {}
    ~PaimonOutputStream() override { (void)Close(); }

    paimon::Result<int64_t> Write(const char* buffer, int64_t size) override {
        if (_closed) {
            return paimon::Status::Invalid(fmt::format("Paimon output is closed: {}", _path));
        }
        if (!_error.ok()) {
            return _error;
        }
        if (size < 0 || (buffer == nullptr && size != 0) || size > std::numeric_limits<int64_t>::max() - _pos ||
            static_cast<uint64_t>(size) > std::numeric_limits<size_t>::max()) {
            return paimon::Status::Invalid(fmt::format("Invalid write size/buffer for {}: {}", _path, size));
        }
        if (size == 0) {
            return int64_t{0};
        }
        auto status = _file->append(Slice(buffer, static_cast<size_t>(size)));
        if (!status.ok()) {
            _error = paimon::Status::IOError(
                    fmt::format("Failed to write Paimon file {}, reason: {}", _path, status.detailed_message()));
            return _error;
        }
        const auto expected_size = static_cast<uint64_t>(_pos + size);
        if (_file->size() != expected_size) {
            _error = paimon::Status::IOError(fmt::format("Incomplete write to Paimon file {}: expected size {}, got {}",
                                                         _path, expected_size, _file->size()));
            return _error;
        }
        _pos += size;
        return size;
    }

    paimon::Status Flush() override {
        if (_closed || !_error.ok()) {
            return _error;
        }
        // SR's S3/Azure WritableFile flush finalizes the object. Close through the
        // normal lifecycle once instead, then reject further writes rather than
        // silently buffering data after an upload has already completed.
        if (_flush_closes_file) {
            return Close();
        }
        auto status = _file->flush(WritableFile::FLUSH_SYNC);
        if (!status.ok()) {
            _error = paimon::Status::IOError(
                    fmt::format("Failed to flush Paimon file {}, reason: {}", _path, status.detailed_message()));
        }
        return _error;
    }

    paimon::Result<int64_t> GetPos() const override {
        if (!_error.ok()) {
            return _error;
        }
        return _pos;
    }

    paimon::Result<std::string> GetUri() const override { return _path; }

    paimon::Status Close() override {
        if (_closed) {
            return _error;
        }
        _closed = true;
        auto status = _file->close();
        if (!status.ok()) {
            auto message = fmt::format("Failed to close Paimon file {}, reason: {}", _path, status.detailed_message());
            if (!_error.ok()) {
                message = fmt::format("{}; {}", _error.ToString(), message);
            }
            _error = paimon::Status::IOError(message);
        }
        _file.reset();
        _tracker->_stream_closed(_path, _error.ok());
        return _error;
    }

private:
    std::unique_ptr<WritableFile> _file;
    std::string _path;
    std::shared_ptr<PaimonWriteFileTracker> _tracker;
    int64_t _pos = 0;
    bool _closed = false;
    bool _flush_closes_file;
    paimon::Status _error;
};

namespace {

paimon::Status to_paimon_write_status(const Status& status, const std::string& operation) {
    if (status.ok()) {
        return paimon::Status::OK();
    }
    const auto message = fmt::format("{}, reason: {}", operation, status.detailed_message());
    if (status.is_already_exist()) {
        return paimon::Status::Exist(message);
    }
    if (status.is_not_found()) {
        return paimon::Status::NotExist(message);
    }
    if (status.is_not_supported()) {
        return paimon::Status::NotImplemented(message);
    }
    if (status.is_invalid_argument()) {
        return paimon::Status::Invalid(message);
    }
    return paimon::Status::IOError(message);
}

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

// paimon expects listing entries to carry full paths (see LocalFileSystem in paimon-cpp),
// while StarRocks DirEntry only carries the bare name.
std::string join_path(const std::string& dir, std::string_view name) {
    if (!dir.empty() && dir.back() == '/') {
        return fmt::format("{}{}", dir, name);
    }
    return fmt::format("{}/{}", dir, name);
}

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

// The paimon::InputStream contract requires positional reads to behave like pread():
// they must not disturb the sequential position. Some underlying streams implement
// read_at_fully() as seek()+read_fully(), so the sequential position is owned here
// (_pos) instead of trusting the wrapped stream's position.
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
        if (origin == paimon::SeekOrigin::FS_SEEK_CUR) {
            new_position += _pos;
        } else if (origin == paimon::SeekOrigin::FS_SEEK_END) {
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
        _pos = new_position;
        return paimon::Status::OK();
    }

    paimon::Result<int64_t> GetPos() const override { return _pos; }

    paimon::Result<int64_t> Read(char* buffer, int64_t size) override {
        if (size < 0) {
            return paimon::Status::Invalid(fmt::format("negative read size {} for {}", size, _file->filename()));
        }
        const int64_t start_ns = MonotonicNanos();
        // paimon-cpp treats a short Read as a hard error (e.g. StreamUtils::ReadFully reads a
        // whole file with a single Read call), but read_at may legally return fewer bytes than
        // requested (an HDFS pread stops at a block boundary), so loop until size bytes or EOF.
        int64_t total_read = 0;
        while (total_read < size) {
            auto result = _file->read_at(_pos + total_read, buffer + total_read, size - total_read);
            if (!result.ok()) {
                _stats->record_app_read(PaimonFileSystemStats::ReadType::SEQUENTIAL, total_read,
                                        MonotonicNanos() - start_ns);
                return paimon::Status::IOError(fmt::format("Failed to read file {}, reason: {}", _file->filename(),
                                                           result.status().detailed_message()));
            }
            if (result.value() == 0) {
                break;
            }
            total_read += result.value();
        }
        _stats->record_app_read(PaimonFileSystemStats::ReadType::SEQUENTIAL, total_read, MonotonicNanos() - start_ns);
        _pos += total_read;
        return total_read;
    }

    paimon::Result<int64_t> Read(char* buffer, int64_t size, int64_t offset) override {
        if (size < 0 || offset < 0) {
            return paimon::Status::Invalid(
                    fmt::format("invalid positional read for {}: size={}, offset={}", _file->filename(), size, offset));
        }
        const int64_t start_ns = MonotonicNanos();
        const Status status = _file->read_at_fully(offset, buffer, size);
        _stats->record_app_read(PaimonFileSystemStats::ReadType::POSITIONAL, status.ok() ? size : 0,
                                MonotonicNanos() - start_ns);
        if (!status.ok()) {
            return paimon::Status::IOError(fmt::format("Failed to read file {} at offset {}, reason: {}",
                                                       _file->filename(), offset, status.detailed_message()));
        }
        return size;
    }

    void ReadAsync(char* buffer, int64_t size, int64_t offset,
                   std::function<void(paimon::Status)>&& callback) override {
        if (size < 0 || offset < 0) {
            callback(paimon::Status::Invalid(
                    fmt::format("invalid async read for {}: size={}, offset={}", _file->filename(), size, offset)));
            return;
        }
        const int64_t start_ns = MonotonicNanos();
        const Status status = _file->read_at_fully(offset, buffer, size);
        _stats->record_app_read(PaimonFileSystemStats::ReadType::ASYNC, status.ok() ? size : 0,
                                MonotonicNanos() - start_ns);
        if (!status.ok()) {
            callback(paimon::Status::IOError(fmt::format("Failed to read file {} at offset {}, reason: {}",
                                                         _file->filename(), offset, status.detailed_message())));
            return;
        }
        callback(paimon::Status::OK());
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

    std::unique_ptr<RandomAccessFile> _file;
    std::shared_ptr<PaimonFileSystemStats> _stats;
    // Sequential position of this stream; see the class comment.
    int64_t _pos = 0;
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
        return std::make_unique<PaimonInputStream>(std::move(counted_file), _stats);
    }

    auto size_result = raw_file->get_size();
    if (!size_result.ok()) {
        return paimon::Status::IOError(fmt::format("Failed to get file size for {}, reason: {}", path,
                                                   size_result.status().detailed_message()));
    }
    const int64_t file_size = size_result.value();

    auto shared_buffered_input_stream = std::make_shared<SharedBufferedInputStream>(input_stream, filename, file_size);
    const SharedBufferedInputStream::CoalesceOptions coalesce_options = {
            .max_dist_size = config::io_coalesce_read_max_distance_size,
            .max_buffer_size = config::io_coalesce_read_max_buffer_size};
    shared_buffered_input_stream->set_coalesce_options(coalesce_options);

    auto cache_input_stream = std::make_shared<CacheInputStream>(shared_buffered_input_stream, filename, file_size,
                                                                 _datacache_options.modification_time);
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

std::shared_ptr<PaimonFileSystem> PaimonFileSystem::create_for_write(std::shared_ptr<PaimonWriteFileTracker> tracker) {
    if (tracker == nullptr || tracker->_file_system == nullptr) {
        return nullptr;
    }
    auto file_system = std::make_shared<PaimonFileSystem>(tracker->_file_system, DataCacheOptions{});
    file_system->_write_tracker = std::move(tracker);
    return file_system;
}

paimon::Result<std::unique_ptr<paimon::OutputStream>> PaimonFileSystem::Create(const std::string& path,
                                                                               bool overwrite) const {
    if (_write_tracker == nullptr) {
        return paimon::Status::NotImplemented("Paimon reader file system does not support Create");
    }
    if (path.empty() || path.back() == '/') {
        return paimon::Status::Invalid("Paimon output path must name a file");
    }
    std::lock_guard<std::mutex> lock(_write_tracker->_mutex);
    if (_write_tracker->_rolled_back) {
        return paimon::Status::Invalid("Cannot create a file in a rolled back Paimon write attempt");
    }
    auto it = _write_tracker->_files.find(path);
    const bool owned = it != _write_tracker->_files.end() && it->second.owned;
    if (it != _write_tracker->_files.end()) {
        if (it->second.open) {
            return paimon::Status::Invalid(fmt::format("Paimon output is already open: {}", path));
        }
        if (!overwrite || !owned) {
            return paimon::Status::Exist(fmt::format("Cannot reuse a Paimon attempt file: {}", path));
        }
    }
    if (!owned) {
        auto exists = Exists(path);
        if (!exists.ok()) {
            return exists.status();
        }
        if (exists.value()) {
            // Even overwrite=true must not turn an existing, unowned file into
            // an attempt-owned file that SDK Abort()/rollback could delete.
            return paimon::Status::Exist(fmt::format("Refusing to overwrite an unowned Paimon file: {}", path));
        }
    }
    WritableFileOptions options;
    options.mode = owned ? starrocks::FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE : starrocks::FileSystem::MUST_CREATE;
    // MUST_CREATE is atomic on POSIX, but not all SR remote providers honor it
    // atomically. The caller must allocate unique attempt data paths; this is
    // not a conditional-create implementation for commit metadata.
    if (owned) {
        // An unsuccessful truncate/open can still have modified the old file.
        it->second.closed_ok = false;
    }
    auto result = _file_system->new_writable_file(options, path);
    if (!result.ok()) {
        return to_paimon_write_status(result.status(), fmt::format("Failed to create Paimon file {}", path));
    }
    _write_tracker->_files[path] = {};
    const bool flush_closes_file =
            _file_system->type() == starrocks::FileSystem::S3 || _file_system->type() == starrocks::FileSystem::AZBLOB;
    return std::make_unique<PaimonOutputStream>(std::move(result).value(), path, _write_tracker, flush_closes_file);
}

paimon::Status PaimonFileSystem::Mkdirs(const std::string& path) const {
    if (_write_tracker == nullptr) {
        return paimon::Status::NotImplemented("Paimon reader file system does not support Mkdirs");
    }
    std::lock_guard<std::mutex> lock(_write_tracker->_mutex);
    if (_write_tracker->_rolled_back) {
        return paimon::Status::Invalid("Cannot create directories in a rolled back Paimon write attempt");
    }
    return to_paimon_write_status(_file_system->create_dir_recursive(path),
                                  fmt::format("Failed to create Paimon directories {}", path));
}

paimon::Status PaimonFileSystem::Rename(const std::string& src, const std::string& dst) const {
    if (_write_tracker == nullptr) {
        return paimon::Status::NotImplemented("Paimon reader file system does not support Rename");
    }
    std::lock_guard<std::mutex> lock(_write_tracker->_mutex);
    if (_write_tracker->_rolled_back) {
        return paimon::Status::Invalid("Cannot rename a file in a rolled back Paimon write attempt");
    }
    auto it = _write_tracker->_files.find(src);
    if (it == _write_tracker->_files.end() || !it->second.owned || it->second.open || !it->second.closed_ok) {
        return paimon::Status::Invalid(
                fmt::format("Can only rename a successfully closed, owned Paimon file: {}", src));
    }
    if (src == dst) {
        return paimon::Status::OK();
    }
    if (dst.empty() || dst.back() == '/' || _write_tracker->_files.count(dst) != 0) {
        return paimon::Status::Invalid(fmt::format("Invalid or previously used Paimon rename target: {}", dst));
    }
    auto exists = Exists(dst);
    if (!exists.ok()) {
        return exists.status();
    }
    if (exists.value()) {
        return paimon::Status::Exist(fmt::format("Refusing to replace an existing Paimon rename target: {}", dst));
    }
    auto status = _file_system->rename_file(src, dst);
    if (!status.ok()) {
        // A non-atomic backend can leave a partial destination on error. Do not
        // claim/delete that path: SR has no conditional ownership/identity API.
        return to_paimon_write_status(status, fmt::format("Failed to rename Paimon file {} to {}", src, dst));
    }
    _write_tracker->_files.emplace(dst, it->second);
    it->second.owned = false;
    return paimon::Status::OK();
}

paimon::Status PaimonFileSystem::Delete(const std::string& path, bool /*recursive*/) const {
    if (_write_tracker == nullptr) {
        return paimon::Status::NotImplemented("Paimon reader file system does not support Delete");
    }
    std::lock_guard<std::mutex> lock(_write_tracker->_mutex);
    auto it = _write_tracker->_files.find(path);
    if (it == _write_tracker->_files.end()) {
        return paimon::Status::Invalid(fmt::format("Refusing to delete an unowned Paimon file or directory: {}", path));
    }
    if (it->second.published) {
        return paimon::Status::Invalid(fmt::format("Refusing to delete a published Paimon file: {}", path));
    }
    if (!it->second.owned) {
        // Tombstones make repeated SDK cleanup idempotent without touching a
        // different file that might subsequently appear at this path.
        return paimon::Status::OK();
    }
    if (it->second.open) {
        return paimon::Status::Invalid(fmt::format("Close Paimon output before deleting it: {}", path));
    }
    auto status = _file_system->delete_file(path);
    if (!status.ok() && !status.is_not_found()) {
        return to_paimon_write_status(status, fmt::format("Failed to delete Paimon file {}", path));
    }
    it->second.owned = false;
    return paimon::Status::OK();
}

paimon::Status PaimonFileSystem::WriteFile(const std::string& path, const std::string& content, bool overwrite) {
    if (content.size() > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return paimon::Status::Invalid("Paimon file content exceeds the stream size limit");
    }
    auto result = Create(path, overwrite);
    if (!result.ok()) {
        return result.status();
    }
    auto output = std::move(result).value();
    auto write_result = output->Write(content.data(), static_cast<int64_t>(content.size()));
    paimon::Status status = write_result.ok() ? paimon::Status::OK() : write_result.status();
    if (status.ok() && write_result.value() != static_cast<int64_t>(content.size())) {
        status = paimon::Status::IOError(fmt::format("Short write to Paimon file {}", path));
    }
    if (status.ok()) {
        status = output->Flush();
    }
    auto close_status = output->Close();
    // The stream retains write/flush failures and appends any close failure.
    return close_status.ok() ? status : close_status;
}

paimon::Status PaimonFileSystem::AtomicStore(const std::string&, const std::string&) {
    return paimon::Status::NotImplemented("Paimon data-file adapter does not provide atomic metadata publication");
}

paimon::Result<std::unique_ptr<paimon::FileStatus>> PaimonFileSystem::GetFileStatus(const std::string& path) const {
    auto exists = Exists(path);
    if (!exists.ok()) {
        return exists.status();
    }
    if (!exists.value()) {
        return paimon::Status::NotExist(fmt::format("Path {} is not exist.", path));
    }
    auto directory_result = _file_system->is_directory(path);
    if (!directory_result.ok()) {
        return paimon::Status::IOError(fmt::format(
                "Get file status but failed to check whether path {} is directory or not from get status, reason: {}",
                path, directory_result.status().detailed_message()));
    }
    // Directory size is undefined in both FS contracts; S3 has no object to
    // stat for a directory prefix. Never issue get_file_size for directories.
    int64_t size = 0;
    if (!directory_result.value()) {
        auto size_result = _file_system->get_file_size(path);
        if (!size_result.ok()) {
            return paimon::Status::IOError(fmt::format("Failed to get file size for {}, reason: {}", path,
                                                       size_result.status().detailed_message()));
        }
        size = size_result.value();
    }
    auto modification_time_result = _file_system->get_file_modified_time(path);
    if (!modification_time_result.ok()) {
        return paimon::Status::IOError(fmt::format("Failed to get file modified time for {}, reason: {}", path,
                                                   modification_time_result.status().detailed_message()));
    }
    // StarRocks file systems report modification time in seconds, paimon::FileStatus wants ms.
    return std::make_unique<PaimonFileStatus>(path, size, modification_time_result.value() * 1000,
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
    paimon::Status entry_status = paimon::Status::OK();
    const Status status = _file_system->iterate_dir2(directory, [&](const DirEntry& entry) {
        std::string full_path = join_path(directory, entry.name);
        bool is_dir;
        if (entry.is_dir.has_value()) {
            is_dir = entry.is_dir.value();
        } else {
            auto is_dir_result = _file_system->is_directory(full_path);
            if (!is_dir_result.ok()) {
                entry_status =
                        paimon::Status::IOError(fmt::format("Failed to check whether {} is a directory, reason: {}",
                                                            full_path, is_dir_result.status().detailed_message()));
                return false;
            }
            is_dir = is_dir_result.value();
        }
        file_status_list->emplace_back(std::make_unique<PaimonBasicFileStatus>(std::move(full_path), is_dir));
        return true;
    });
    if (!entry_status.ok()) {
        return entry_status;
    }
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

    paimon::Status entry_status = paimon::Status::OK();
    const Status status = _file_system->iterate_dir2(path, [&](const DirEntry& entry) {
        std::string full_path = join_path(path, entry.name);
        if (entry.size.has_value() && entry.mtime.has_value() && entry.is_dir.has_value()) {
            // DirEntry.mtime is in seconds, paimon::FileStatus wants ms.
            file_status_list->emplace_back(std::make_unique<PaimonFileStatus>(
                    std::move(full_path), entry.size.value(), entry.mtime.value() * 1000, entry.is_dir.value()));
            return true;
        }
        // Not every filesystem fills all DirEntry fields; fall back to a full status lookup.
        auto status_result = GetFileStatus(full_path);
        if (!status_result.ok()) {
            entry_status = status_result.status();
            return false;
        }
        file_status_list->emplace_back(std::move(status_result).value());
        return true;
    });
    if (!entry_status.ok()) {
        return entry_status;
    }
    if (!status.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to list file status for {}, reason: {}", path, status.detailed_message()));
    }
    return paimon::Status::OK();
}

paimon::Result<bool> PaimonFileSystem::Exists(const std::string& path) const {
    // Built on is_directory() rather than path_exists(): S3 leaves path_exists() unimplemented, while
    // is_directory() answers NotFound for a missing path on every file system StarRocks reads Paimon
    // from (POSIX stat, HDFS getPathInfo, S3 prefix listing).
    const Status status = _file_system->is_directory(path).status();
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
