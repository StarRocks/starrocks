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

#ifdef USE_STAROS
#include "fs/fs_starlet.h"

#include <bvar/bvar.h>
#include <fmt/core.h>
#include <fslib/configuration.h>
#include <fslib/file.h>
#include <fslib/file_system.h>
#include <fslib/fslib_all_initializer.h>
#include <fslib/stat.h>
#include <fslib/stream.h>
#include <starlet.h>
#include <sys/stat.h>
#include <worker.h>

#include "common/config.h"
#include "fs/output_stream_adapter.h"
#include "gutil/strings/util.h"
#include "io/input_stream.h"
#include "io/output_stream.h"
#include "io/seekable_input_stream.h"
#include "io/throttled_output_stream.h"
#include "io/throttled_seekable_input_stream.h"
#include "service/staros_worker.h"
#include "storage/olap_common.h"
#include "util/string_parser.hpp"

namespace starrocks {

bvar::Adder<int64_t> g_starlet_io_read;       // number of bytes read through Starlet
bvar::Adder<int64_t> g_starlet_io_write;      // number of bytes wrote through Starlet
bvar::Adder<int64_t> g_starlet_io_num_reads;  // number of Starlet's read API invocations
bvar::Adder<int64_t> g_starlet_io_num_writes; // number of Starlet's write API invocations

bvar::PerSecond<bvar::Adder<int64_t>> g_starlet_read_second("starlet_io_read_bytes_second", &g_starlet_io_read);
bvar::PerSecond<bvar::Adder<int64_t>> g_starlet_write_second("starlet_io_write_bytes_second", &g_starlet_io_write);
bvar::PerSecond<bvar::Adder<int64_t>> g_starlet_num_reads_second("starlet_io_read_second", &g_starlet_io_num_reads);
bvar::PerSecond<bvar::Adder<int64_t>> g_starlet_num_writes_second("starlet_io_write_second", &g_starlet_io_num_writes);

using FileSystemFactory = staros::starlet::fslib::FileSystemFactory;
using WriteOptions = staros::starlet::fslib::WriteOptions;
using ReadOptions = staros::starlet::fslib::ReadOptions;
using Configuration = staros::starlet::fslib::Configuration;
using FileSystemPtr = std::unique_ptr<staros::starlet::fslib::FileSystem>;
using ReadOnlyFilePtr = std::unique_ptr<staros::starlet::fslib::ReadOnlyFile>;
using WritableFilePtr = std::unique_ptr<staros::starlet::fslib::WritableFile>;
using Anchor = staros::starlet::fslib::Stream::Anchor;
using EntryStat = staros::starlet::fslib::EntryStat;

bool is_starlet_uri(std::string_view uri) {
    return HasPrefixString(uri, "staros://");
}

std::string build_starlet_uri(int64_t shard_id, std::string_view path) {
    while (!path.empty() && path.front() == '/') {
        path.remove_prefix(1);
    }
    return path.empty() ? fmt::format("staros://{}", shard_id) : fmt::format("staros://{}/{}", shard_id, path);
}

// Expected format of uri: staros://ShardID/path/to/file
StatusOr<std::pair<std::string, int64_t>> parse_starlet_uri(std::string_view uri) {
    std::string_view path = uri;
    if (!HasPrefixString(path, "staros://")) {
        return Status::InvalidArgument(fmt::format("Invalid starlet URI: {}", uri));
    }
    path.remove_prefix(sizeof("staros://") - 1);
    auto end_shard_id = path.find('/');
    if (end_shard_id == std::string::npos) {
        end_shard_id = path.size();
    }

    StringParser::ParseResult result;
    auto shard_id = StringParser::string_to_int<int64_t>(path.data(), end_shard_id, &result);
    if (result != StringParser::PARSE_SUCCESS) {
        return Status::InvalidArgument(fmt::format("Invalid starlet URI: {}", uri));
    }
    path.remove_prefix(std::min<size_t>(path.size(), end_shard_id + 1));
    return std::make_pair(std::string(path), shard_id);
};

class StarletInputStream : public starrocks::io::SeekableInputStream {
public:
    explicit StarletInputStream(ReadOnlyFilePtr file_ptr) : _file_ptr(std::move(file_ptr)){};
    ~StarletInputStream() override = default;
    StarletInputStream(const StarletInputStream&) = delete;
    void operator=(const StarletInputStream&) = delete;
    StarletInputStream(StarletInputStream&&) = delete;
    void operator=(StarletInputStream&&) = delete;

    Status seek(int64_t position) override {
        auto stream_st = _file_ptr->stream();
        if (!stream_st.ok()) {
            return to_status(stream_st.status());
        }
        return to_status((*stream_st)->seek(position, Anchor::BEGIN).status());
    }

    StatusOr<int64_t> position() override {
        auto stream_st = _file_ptr->stream();
        if (!stream_st.ok()) {
            return to_status(stream_st.status());
        }
        if ((*stream_st)->support_tell()) {
            auto tell_st = (*stream_st)->tell();
            if (tell_st.ok()) {
                return *tell_st;
            } else {
                return to_status(tell_st.status());
            }
        } else {
            return Status::NotSupported("StarletInputStream::position");
        }
    }

    StatusOr<int64_t> get_size() override {
        //note: starlet s3 filesystem do not return not found err when file not exists;
        auto st = _file_ptr->size();
        if (st.ok()) {
            return *st;
        } else {
            return to_status(st.status());
        }
    }

    StatusOr<int64_t> read(void* data, int64_t count) override {
        auto stream_st = _file_ptr->stream();
        if (!stream_st.ok()) {
            return to_status(stream_st.status());
        }
        auto res = (*stream_st)->read(data, count);
        if (res.ok()) {
            g_starlet_io_num_reads << 1;
            g_starlet_io_read << *res;
            return *res;
        } else {
            return to_status(res.status());
        }
    }

    StatusOr<std::string> read_all() override {
        auto stream_st = _file_ptr->stream();
        if (!stream_st.ok()) {
            return to_status(stream_st.status());
        }
        auto res = (*stream_st)->read_all();
        if (res.ok()) {
            g_starlet_io_num_reads << 1;
            g_starlet_io_read << res.value().size();
            return std::move(res).value();
        } else {
            return to_status(res.status());
        }
    }

    StatusOr<std::unique_ptr<io::NumericStatistics>> get_numeric_statistics() override {
        auto stream_st = _file_ptr->stream();
        if (!stream_st.ok()) {
            return to_status(stream_st.status());
        }

        const auto& read_stats = (*stream_st)->get_read_stats();
        auto stats = std::make_unique<io::NumericStatistics>();
        stats->reserve(9);
        stats->append(kBytesReadLocalDisk, read_stats.bytes_read_local_disk);
        stats->append(kBytesReadRemote, read_stats.bytes_read_remote);
        stats->append(kIOCountLocalDisk, read_stats.io_count_local_disk);
        stats->append(kIOCountRemote, read_stats.io_count_remote);
        stats->append(kIONsLocalDisk, read_stats.io_ns_local_disk);
        stats->append(kIONsRemote, read_stats.io_ns_remote);
        stats->append(kPrefetchHitCount, read_stats.prefetch_hit_count);
        stats->append(kPrefetchWaitFinishNs, read_stats.prefetch_wait_finish_ns);
        stats->append(kPrefetchPendingNs, read_stats.prefetch_pending_ns);
        return std::move(stats);
    }

private:
    ReadOnlyFilePtr _file_ptr;
};

class StarletOutputStream : public starrocks::io::OutputStream {
public:
    explicit StarletOutputStream(WritableFilePtr file_ptr) : _file_ptr(std::move(file_ptr)){};
    ~StarletOutputStream() override = default;
    StarletOutputStream(const StarletOutputStream&) = delete;
    void operator=(const StarletOutputStream&) = delete;
    StarletOutputStream(StarletOutputStream&&) = delete;
    void operator=(StarletOutputStream&&) = delete;
    Status skip(int64_t count) override { return Status::NotSupported("StarletOutputStream::skip"); }
    StatusOr<Buffer> get_direct_buffer() override {
        return Status::NotSupported("StarletOutputStream::get_direct_buffer");
    }

    StatusOr<Position> get_direct_buffer_and_advance(int64_t size) override {
        return Status::NotSupported("StarletOutputStream::get_direct_buffer_and_advance");
    }

    Status write(const void* data, int64_t size) override {
        auto stream_st = _file_ptr->stream();
        if (!stream_st.ok()) {
            return to_status(stream_st.status());
        }
        auto left = size;
        while (left > 0) {
            auto* p = static_cast<const char*>(data) + size - left;
            auto res = (*stream_st)->write(p, left);
            if (!res.ok()) {
                return to_status(res.status());
            }
            left -= *res;
        }
        g_starlet_io_num_writes << 1;
        g_starlet_io_write << size;
        return Status::OK();
    }

    bool allows_aliasing() const override { return false; }

    Status write_aliased(const void* data, int64_t size) override {
        return Status::NotSupported("StarletOutputStream::write_aliased");
    }

    Status close() override {
        auto stream_st = _file_ptr->stream();
        if (!stream_st.ok()) {
            return to_status(stream_st.status());
        }
        return to_status((*stream_st)->close());
    }

private:
    WritableFilePtr _file_ptr;
};

class StarletFileSystem : public FileSystem {
public:
    StarletFileSystem() { staros::starlet::fslib::register_builtin_filesystems(); }
    ~StarletFileSystem() override = default;

    StarletFileSystem(const StarletFileSystem&) = delete;
    void operator=(const StarletFileSystem&) = delete;
    StarletFileSystem(StarletFileSystem&&) = delete;
    void operator=(StarletFileSystem&&) = delete;

    Type type() const override { return STARLET; }

    using FileSystem::new_sequential_file;
    using FileSystem::new_random_access_file;

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& file_name) override {
        FileInfo info{.path = file_name};
        return new_random_access_file(opts, info);
    }

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const FileInfo& info) override {
        ASSIGN_OR_RETURN(auto pair, parse_starlet_uri(info.path));
        auto fs_st = get_shard_filesystem(pair.second);
        if (!fs_st.ok()) {
            return to_status(fs_st.status());
        }
        auto opt = ReadOptions();
        opt.skip_fill_local_cache = opts.skip_fill_local_cache;
        opt.buffer_size = opts.buffer_size;
        if (info.size.has_value()) {
            opt.file_size = info.size.value();
        }
        auto file_st = (*fs_st)->open(pair.first, std::move(opt));

        if (!file_st.ok()) {
            return to_status(file_st.status());
        }

        bool is_cache_hit = (*file_st)->is_cache_hit();
        std::unique_ptr<io::SeekableInputStream> istream = std::make_unique<StarletInputStream>(std::move(*file_st));
        if (!is_cache_hit && config::experimental_lake_wait_per_get_ms > 0) {
            istream = std::make_unique<io::ThrottledSeekableInputStream>(std::move(istream),
                                                                         config::experimental_lake_wait_per_get_ms);
        }
        return std::make_unique<RandomAccessFile>(std::move(istream), info.path, is_cache_hit);
    }

    StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const SequentialFileOptions& opts,
                                                                  const std::string& path) override {
        ASSIGN_OR_RETURN(auto pair, parse_starlet_uri(path));

        auto fs_st = get_shard_filesystem(pair.second);
        if (!fs_st.ok()) {
            return to_status(fs_st.status());
        }
        auto opt = ReadOptions();
        opt.skip_fill_local_cache = opts.skip_fill_local_cache;
        opt.buffer_size = opts.buffer_size;
        auto file_st = (*fs_st)->open(pair.first, std::move(opt));

        if (!file_st.ok()) {
            return to_status(file_st.status());
        }
        auto istream = std::make_shared<StarletInputStream>(std::move(*file_st));
        return std::make_unique<SequentialFile>(std::move(istream), path);
    }

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const std::string& path) override {
        return new_writable_file(WritableFileOptions(), path);
    }

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const WritableFileOptions& opts,
                                                              const std::string& path) override {
        ASSIGN_OR_RETURN(auto pair, parse_starlet_uri(path));
        auto fs_st = get_shard_filesystem(pair.second);
        if (!fs_st.ok()) {
            return to_status(fs_st.status());
        }
        staros::starlet::fslib::WriteOptions fslib_opts;
        fslib_opts.create_missing_parent = true;
        fslib_opts.skip_fill_local_cache = opts.skip_fill_local_cache;
        auto file_st = (*fs_st)->create(pair.first, fslib_opts);
        if (!file_st.ok()) {
            return to_status(file_st.status());
        }

        std::unique_ptr<io::OutputStream> os = std::make_unique<StarletOutputStream>(std::move(*file_st));
        if (config::experimental_lake_wait_per_put_ms > 0) {
            os = std::make_unique<io::ThrottledOutputStream>(std::move(os), config::experimental_lake_wait_per_put_ms);
        }
        return std::make_unique<starrocks::OutputStreamAdapter>(std::move(os), path);
    }

    Status delete_file(const std::string& path) override {
        ASSIGN_OR_RETURN(auto pair, parse_starlet_uri(path));
        auto fs_st = get_shard_filesystem(pair.second);
        if (!fs_st.ok()) {
            return to_status(fs_st.status());
        }
        auto st = (*fs_st)->delete_file(pair.first);
        return to_status(st);
    }

    Status iterate_dir(const std::string& dir, const std::function<bool(std::string_view)>& cb) override {
        ASSIGN_OR_RETURN(auto pair, parse_starlet_uri(dir));
        auto fs_st = get_shard_filesystem(pair.second);
        if (!fs_st.ok()) {
            return to_status(fs_st.status());
        }
        auto st = (*fs_st)->list_dir(pair.first, false, [&](EntryStat stat) { return cb(stat.name); });
        return to_status(st);
    }

    Status iterate_dir2(const std::string& dir, const std::function<bool(DirEntry)>& cb) override {
        ASSIGN_OR_RETURN(auto pair, parse_starlet_uri(dir));
        auto fs_st = get_shard_filesystem(pair.second);
        if (!fs_st.ok()) {
            return to_status(fs_st.status());
        }
        auto st = (*fs_st)->list_dir(pair.first, false, [&](EntryStat e) {
            DirEntry entry{.name = e.name,
                           .mtime = std::move(e.mtime),
                           .size = std::move(e.size),
                           .is_dir = std::move(e.is_dir)};
            return cb(entry);
        });
        return to_status(st);
    }

    Status create_dir(const std::string& dirname) override {
        auto st = is_directory(dirname);
        if (st.ok() && st.value()) {
            return Status::AlreadyExist(dirname);
        }
        ASSIGN_OR_RETURN(auto pair, parse_starlet_uri(dirname));
        auto fs_st = get_shard_filesystem(pair.second);
        if (!fs_st.ok()) {
            return to_status(fs_st.status());
        }

        auto res = (*fs_st)->mkdir(pair.first);
        return to_status(res);
    }

    Status create_dir_if_missing(const std::string& dirname, bool* created) override {
        auto st = create_dir(dirname);
        if (created != nullptr) {
            *created = st.ok();
        }
        if (st.is_already_exist()) {
            st = Status::OK();
        }
        return st;
    }

    Status create_dir_recursive(const std::string& dirname) override { return create_dir_if_missing(dirname, nullptr); }

    Status delete_dir(const std::string& dirname) override {
        ASSIGN_OR_RETURN(auto pair, parse_starlet_uri(dirname));
        auto fs_st = get_shard_filesystem(pair.second);
        if (!fs_st.ok()) {
            return to_status(fs_st.status());
        }
        // TODO: leave this check to starlet.
        bool dir_empty = true;
        auto cb = [&dir_empty](EntryStat stat) {
            dir_empty = false;
            // break the iteration
            return false;
        };
        auto st = (*fs_st)->list_dir(pair.first, false, cb);
        if (!st.ok()) {
            return to_status(st);
        }
        if (!dir_empty) {
            return Status::InternalError(fmt::format("dir {} is not empty", pair.first));
        }
        auto res = (*fs_st)->delete_dir(pair.first, false);
        return to_status(res);
    }

    Status delete_dir_recursive(const std::string& dirname) override {
        ASSIGN_OR_RETURN(auto pair, parse_starlet_uri(dirname));
        auto fs_st = get_shard_filesystem(pair.second);
        if (!fs_st.ok()) {
            return to_status(fs_st.status());
        }
        auto st = (*fs_st)->delete_dir(pair.first, true);
        return to_status(st);
    }

    // determine given path is a directory or not
    // returns
    //  * true - exists and is a directory
    //  * false - exists but is not a directory
    //  * error status: not exist or other errors
    StatusOr<bool> is_directory(const std::string& path) override {
        ASSIGN_OR_RETURN(auto pair, parse_starlet_uri(path));
        auto fs_st = get_shard_filesystem(pair.second);

        if (!fs_st.ok()) {
            return to_status(fs_st.status());
        }

        auto fst = (*fs_st)->stat(pair.first);
        if (!fst.ok()) {
            return to_status(fst.status());
        }
        return S_ISDIR(fst->mode);
    }

    Status sync_dir(const std::string& dirname) override {
        ASSIGN_OR_RETURN(const bool is_dir, is_directory(dirname));
        if (is_dir) return Status::OK();
        return Status::NotFound(fmt::format("{} not directory", dirname));
    }

    StatusOr<SpaceInfo> space(const std::string& path) override {
        const Status status = is_directory(path).status();
        if (!status.ok()) {
            return status;
        }
        return SpaceInfo{.capacity = std::numeric_limits<int64_t>::max(),
                         .free = std::numeric_limits<int64_t>::max(),
                         .available = std::numeric_limits<int64_t>::max()};
    }

    Status path_exists(const std::string& path) override {
        ASSIGN_OR_RETURN(auto pair, parse_starlet_uri(path));
        auto fs_st = get_shard_filesystem(pair.second);
        if (!fs_st.ok()) {
            return to_status(fs_st.status());
        }

        auto exists_or = (*fs_st)->exists(pair.first);
        if (!exists_or.ok()) {
            return to_status(exists_or.status());
        }
        return *exists_or ? Status::OK() : Status::NotFound(path);
    }

    Status get_children(const std::string& dir, std::vector<std::string>* file) override {
        return Status::NotSupported("StarletFileSystem::get_children");
    }

    Status canonicalize(const std::string& path, std::string* file) override {
        return Status::NotSupported("StarletFileSystem::canonicalize");
    }

    StatusOr<uint64_t> get_file_size(const std::string& path) override {
        return Status::NotSupported("StarletFileSystem::get_file_size");
    }

    StatusOr<uint64_t> get_file_modified_time(const std::string& path) override {
        ASSIGN_OR_RETURN(auto pair, parse_starlet_uri(path));
        auto fs_st = get_shard_filesystem(pair.second);

        if (!fs_st.ok()) {
            return to_status(fs_st.status());
        }

        auto fst = (*fs_st)->stat(pair.first);
        if (!fst.ok()) {
            return to_status(fst.status());
        }
        return fst->mtime;
    }

    Status rename_file(const std::string& src, const std::string& target) override {
        return Status::NotSupported("StarletFileSystem::rename_file");
    }

    Status link_file(const std::string& old_path, const std::string& new_path) override {
        return Status::NotSupported("StarletFileSystem::link_file");
    }

    Status drop_local_cache(const std::string& path) override {
        ASSIGN_OR_RETURN(auto pair, parse_starlet_uri(path));
        auto fs_st = get_shard_filesystem(pair.second);
        if (!fs_st.ok()) {
            return to_status(fs_st.status());
        }
        return to_status((*fs_st)->drop_cache(pair.first));
    }

    Status delete_files(const std::vector<std::string>& paths) override {
        if (paths.empty()) {
            return Status::OK();
        }

        std::vector<std::string> parsed_paths;
        parsed_paths.reserve(paths.size());
        std::shared_ptr<staros::starlet::fslib::FileSystem> fs = nullptr;
        int64_t shard_id;
        for (auto&& path : paths) {
            ASSIGN_OR_RETURN(auto pair, parse_starlet_uri(path));
            auto fs_st = get_shard_filesystem(pair.second);
            if (!fs_st.ok()) {
                return to_status(fs_st.status());
            }
            if (fs == nullptr) {
                shard_id = pair.second;
                fs = *fs_st;
            }
            if (shard_id != pair.second) {
                return Status::InternalError("Not all paths have the same scheme");
            }
            parsed_paths.emplace_back(std::move(pair.first));
        }
        return to_status(fs->delete_files(parsed_paths));
    }

private:
    absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>> get_shard_filesystem(int64_t shard_id) {
        return g_worker->get_shard_filesystem(shard_id, _conf);
    }

private:
    staros::starlet::fslib::Configuration _conf;
};

std::unique_ptr<FileSystem> new_fs_starlet() {
    return std::make_unique<StarletFileSystem>();
}
} // namespace starrocks

#endif // USE_STAROS
