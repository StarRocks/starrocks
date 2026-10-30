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

#include <paimon/fs/file_system.h>

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "cache/cache_options.h"
#include "cache/scan/cache_input_stream.h"
#include "common/status.h"

namespace starrocks {

class SharedBufferedInputStream;
class FileSystem;
class RandomAccessFile;

class PaimonFileSystemStats {
public:
    struct SharedBufferedStats {
        int64_t shared_io_count = 0;
        int64_t shared_io_bytes = 0;
        int64_t shared_align_io_bytes = 0;
        int64_t shared_io_timer = 0;
        int64_t direct_io_count = 0;
        int64_t direct_io_bytes = 0;
        int64_t direct_io_timer = 0;
    };

    struct Snapshot {
        int64_t sequential_read_count = 0;
        int64_t sequential_read_bytes = 0;
        int64_t sequential_read_ns = 0;
        int64_t positional_read_count = 0;
        int64_t positional_read_bytes = 0;
        int64_t positional_read_ns = 0;
        int64_t async_read_count = 0;
        int64_t async_read_bytes = 0;
        int64_t async_read_ns = 0;
        int64_t fs_io_count = 0;
        int64_t fs_io_bytes = 0;
        int64_t fs_io_ns = 0;
        CacheInputStream::Stats datacache;
        SharedBufferedStats shared_buffered;

        int64_t app_io_count() const { return sequential_read_count + positional_read_count + async_read_count; }
        int64_t app_io_bytes() const { return sequential_read_bytes + positional_read_bytes + async_read_bytes; }
        int64_t app_io_ns() const { return sequential_read_ns + positional_read_ns + async_read_ns; }
    };

    enum class ReadType {
        SEQUENTIAL,
        POSITIONAL,
        ASYNC,
    };

    void record_app_read(ReadType type, int64_t bytes, int64_t elapsed_ns);
    void record_fs_read(int64_t bytes, int64_t elapsed_ns);
    void record_stream_stats(const CacheInputStream::Stats& cache_stats,
                             const SharedBufferedStats& shared_buffered_stats);
    Snapshot snapshot() const;

private:
    std::atomic<int64_t> _sequential_read_count{0};
    std::atomic<int64_t> _sequential_read_bytes{0};
    std::atomic<int64_t> _sequential_read_ns{0};
    std::atomic<int64_t> _positional_read_count{0};
    std::atomic<int64_t> _positional_read_bytes{0};
    std::atomic<int64_t> _positional_read_ns{0};
    std::atomic<int64_t> _async_read_count{0};
    std::atomic<int64_t> _async_read_bytes{0};
    std::atomic<int64_t> _async_read_ns{0};
    std::atomic<int64_t> _fs_io_count{0};
    std::atomic<int64_t> _fs_io_bytes{0};
    std::atomic<int64_t> _fs_io_ns{0};
    mutable std::mutex _stream_stats_mutex;
    CacheInputStream::Stats _cache_stats;
    SharedBufferedStats _shared_buffered_stats;
};

// One tracker per write attempt, bound to the credential-bearing StarRocks FS.
// The FS must outlive the tracker and every stream. Paths must be attempt-unique
// SDK data/temp files, never snapshot/commit metadata. SR's remote MUST_CREATE
// implementations do not all provide atomic no-clobber; an existence precheck
// cannot protect against another client concurrently creating the same path.
// Only files successfully created by this attempt can be overwritten/deleted.
// Directories and preexisting files are never removed by cleanup.
class PaimonWriteFileTracker {
public:
    explicit PaimonWriteFileTracker(starrocks::FileSystem* file_system) : _file_system(file_system) {}

    std::vector<std::string> owned_files() const;
    // Call only after external publication succeeds. All paths must have closed
    // successfully. Idempotent; published paths cannot be mutated or rolled back.
    Status mark_published(const std::vector<std::string>& paths);
    // Close/destroy SDK writers first. Seals the attempt against further writes;
    // retries failed deletions, continues past errors and never recursively deletes.
    Status rollback();

private:
    friend class PaimonFileSystem;
    friend class PaimonOutputStream;
    struct FileState {
        bool open = true;
        bool closed_ok = false;
        bool owned = true;
        bool published = false;
    };

    void _stream_closed(const std::string& path, bool success);

    starrocks::FileSystem* _file_system;
    mutable std::mutex _mutex;
    std::map<std::string, FileState> _files;
    bool _rolled_back = false;
};

// Adapts an existing StarRocks FS without configuring a second client. The
// constructor is read-only; only create_for_write explicitly enables writes.
class PaimonFileSystem final : public paimon::FileSystem {
public:
    // `FileSystem` must be qualified inside this class: the injected-class-name of the
    // base paimon::FileSystem shadows the starrocks::FileSystem forward declaration.
    PaimonFileSystem(starrocks::FileSystem* file_system, const DataCacheOptions& datacache_options)
            : _file_system(file_system),
              _datacache_options(datacache_options),
              _stats(std::make_shared<PaimonFileSystemStats>()) {}
    ~PaimonFileSystem() override = default;

    // Returns null for a null tracker/FS. Writer reads bypass datacache so they
    // work without scan context/cache initialization and cannot read stale
    // cached content for files being written by this attempt. On S3/Azure,
    // Flush finalizes the object; subsequent writes are rejected (SR contract).
    static std::shared_ptr<PaimonFileSystem> create_for_write(std::shared_ptr<PaimonWriteFileTracker> tracker);

    paimon::Result<std::unique_ptr<paimon::InputStream>> Open(const std::string& path) const override;
    paimon::Result<std::unique_ptr<paimon::OutputStream>> Create(const std::string& path,
                                                                 bool overwrite) const override;
    paimon::Status Mkdirs(const std::string& path) const override;
    paimon::Status Rename(const std::string& src, const std::string& dst) const override;
    paimon::Status Delete(const std::string& path, bool recursive) const override;
    paimon::Result<std::unique_ptr<paimon::FileStatus>> GetFileStatus(const std::string& path) const override;
    paimon::Status ListDir(const std::string& directory,
                           std::vector<std::unique_ptr<paimon::BasicFileStatus>>* file_status_list) const override;
    paimon::Status ListFileStatus(const std::string& path,
                                  std::vector<std::unique_ptr<paimon::FileStatus>>* file_status_list) const override;
    paimon::Result<bool> Exists(const std::string& path) const override;
    // v0.3's default helper discards Close() errors; keep them observable here.
    paimon::Status WriteFile(const std::string& path, const std::string& content, bool overwrite) override;
    // SR rename is not an atomic publication primitive (e.g. S3 copy+delete).
    paimon::Status AtomicStore(const std::string& path, const std::string& content) override;

    bool datacache_enabled() const { return _datacache_options.enable_datacache; }
    PaimonFileSystemStats::Snapshot get_stats() const { return _stats->snapshot(); }

private:
    starrocks::FileSystem* _file_system;
    DataCacheOptions _datacache_options;
    std::shared_ptr<PaimonFileSystemStats> _stats;
    std::shared_ptr<PaimonWriteFileTracker> _write_tracker;
};

} // namespace starrocks
