//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors

#include <dirent.h>
#include <fcntl.h>
#include <fmt/format.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <cstdio>
#include <filesystem>
#include <memory>

#include "common/config.h"
#include "common/logging.h"
#include "fs/fd_cache.h"
#include "fs/fs.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "gutil/port.h"
#include "gutil/strings/substitute.h"
#include "gutil/strings/util.h"
#include "io/fd_input_stream.h"
#include "util/errno.h"
#include "util/slice.h"

#ifdef USE_STAROS
#include "fslib/metric_key.h"
#include "metrics/metrics.h"
#endif

#ifdef USE_STAROS
namespace {
static const staros::starlet::metrics::Labels kSrPosixFsLables({{"fstype", "srposix"}});

DEFINE_HISTOGRAM_METRIC_KEY_WITH_TAG_BUCKET(s_sr_posix_write_iosize, staros::starlet::fslib::kMKWriteIOSize,
                                            kSrPosixFsLables, staros::starlet::metrics::MetricsSystem::kIOSizeBuckets);
DEFINE_HISTOGRAM_METRIC_KEY_WITH_TAG_BUCKET(s_sr_posix_write_iolatency, staros::starlet::fslib::kMKWriteIOLatency,
                                            kSrPosixFsLables,
                                            staros::starlet::metrics::MetricsSystem::kIOLatencyBuckets);
} // namespace
#endif

namespace starrocks {

using std::string;
using strings::Substitute;

// Close file descriptor when object goes out of scope.
class ScopedFdCloser {
public:
    explicit ScopedFdCloser(int fd) : fd_(fd) {}

    ~ScopedFdCloser() {
        int err;
        RETRY_ON_EINTR(err, ::close(fd_));
        if (PREDICT_FALSE(err != 0)) {
            LOG(WARNING) << "Failed to close fd " << fd_;
        }
    }

private:
    const int fd_;
};

class CachedFdInputStream : public io::FdInputStream {
public:
    explicit CachedFdInputStream(FdCache::Handle* h) : io::FdInputStream(FdCache::fd(h)), _h(h) {}

    ~CachedFdInputStream() override { FdCache::Instance()->release(_h); }

private:
    FdCache::Handle* _h;
};

inline bool enable_fd_cache(std::string_view path) {
    // .dat is the suffix of segment file name
    return HasSuffixString(path, ".dat");
}

static Status io_error(const std::string& context, int err_number) {
    switch (err_number) {
    case 0:
        return Status::OK();
    case ENOENT:
        return Status::NotFound(fmt::format("{}: {}", context, std::strerror(err_number)));
    case EEXIST:
        return Status::AlreadyExist(fmt::format("{}: {}", context, std::strerror(err_number)));
    default:
        return Status::IOError(fmt::format("{}: {}", context, std::strerror(err_number)));
    }
}

static Status do_sync(int fd, const string& filename) {
    if (fdatasync(fd) < 0) {
        return io_error(filename, errno);
    }
    return Status::OK();
}

static Status do_open(const string& filename, FileSystem::OpenMode mode, int* fd) {
    int flags = O_RDWR;
    switch (mode) {
    case FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE:
        flags |= O_CREAT | O_TRUNC;
        break;
    case FileSystem::CREATE_OR_OPEN:
        flags |= O_CREAT;
        break;
    case FileSystem::MUST_CREATE:
        flags |= O_CREAT | O_EXCL;
        break;
    case FileSystem::MUST_EXIST:
        break;
    default:
        return Status::NotSupported(strings::Substitute("Unknown create mode $0", mode));
    }
    int f;
    RETRY_ON_EINTR(f, open(filename.c_str(), flags, 0666));
    if (f < 0) {
        return io_error(filename, errno);
    }
    *fd = f;
    return Status::OK();
}

static Status do_writev_at(int fd, const string& filename, uint64_t offset, const Slice* data, size_t data_cnt,
                           size_t* bytes_written) {
    // Convert the results into the iovec vector to request
    // and calculate the total bytes requested.
    size_t bytes_req = 0;
    struct iovec iov[data_cnt];
    for (size_t i = 0; i < data_cnt; i++) {
        const Slice& result = data[i];
        bytes_req += result.size;
        iov[i] = {result.data, result.size};
    }

    uint64_t cur_offset = offset;
    size_t completed_iov = 0;
    size_t rem = bytes_req;
    while (rem > 0) {
        // Never request more than IOV_MAX in one request.
        size_t iov_count = std::min(data_cnt - completed_iov, static_cast<size_t>(IOV_MAX));
        ssize_t w;
        RETRY_ON_EINTR(w, pwritev(fd, iov + completed_iov, iov_count, cur_offset));
        if (PREDICT_FALSE(w < 0)) {
            // An error: return a non-ok status.
            return io_error(filename, errno);
        }

        if (PREDICT_TRUE(w == rem)) {
            // All requested bytes were read. This is almost always the case.
            rem = 0;
            break;
        }
        // Adjust iovec vector based on bytes read for the next request.
        ssize_t bytes_rem = w;
        for (size_t i = completed_iov; i < data_cnt; i++) {
            if (bytes_rem >= iov[i].iov_len) {
                // The full length of this iovec was written.
                completed_iov++;
                bytes_rem -= iov[i].iov_len;
            } else {
                // Partially wrote this result.
                // Adjust the iov_len and iov_base to write only the missing data.
                iov[i].iov_base = static_cast<uint8_t*>(iov[i].iov_base) + bytes_rem;
                iov[i].iov_len -= bytes_rem;
                break; // Don't need to adjust remaining iovec's.
            }
        }
        cur_offset += w;
        rem -= w;
    }
    DCHECK_EQ(0, rem);
    *bytes_written = bytes_req;
    return Status::OK();
}

#ifdef USE_STAROS
#endif

class PosixWritableFile : public WritableFile {
public:
    PosixWritableFile(std::string filename, int fd, uint64_t filesize, bool sync_on_close)
            : _filename(std::move(filename)), _fd(fd), _sync_on_close(sync_on_close), _filesize(filesize) {
        FileSystem::on_file_write_open(this);
    }

    ~PosixWritableFile() override { WARN_IF_ERROR(close(), "Failed to close file, file=" + _filename); }

    Status append(const Slice& data) override { return appendv(&data, 1); }

    Status appendv(const Slice* data, size_t cnt) override {
#ifdef USE_STAROS
        staros::starlet::metrics::TimeObserver<prometheus::Histogram> write_latency(s_sr_posix_write_iolatency);
#endif
        size_t bytes_written = 0;
        RETURN_IF_ERROR(do_writev_at(_fd, _filename, _filesize, data, cnt, &bytes_written));
        _filesize += bytes_written;
        _pending_sync = true;
#ifdef USE_STAROS
        s_sr_posix_write_iosize.Observe(bytes_written);
#endif
        return Status::OK();
    }

    Status pre_allocate(uint64_t size) override {
        uint64_t offset = std::max(_filesize, _pre_allocated_size);
        int ret;
        RETRY_ON_EINTR(ret, fallocate(_fd, 0, offset, size));
        if (ret != 0) {
            if (errno == EOPNOTSUPP) {
                LOG(WARNING) << "The filesystem does not support fallocate().";
            } else if (errno == ENOSYS) {
                LOG(WARNING) << "The kernel does not implement fallocate().";
            } else {
                return io_error(_filename, errno);
            }
        }
        _pre_allocated_size = offset + size;
        return Status::OK();
    }

    Status close() override {
        if (_closed) {
            return Status::OK();
        }
        FileSystem::on_file_write_close(this);
        Status s;

        // If we've allocated more space than we used, truncate to the
        // actual size of the file and perform Sync().
        if (_filesize < _pre_allocated_size) {
            int ret;
            RETRY_ON_EINTR(ret, ftruncate(_fd, _filesize));
            if (ret != 0) {
                s = io_error(_filename, errno);
                _pending_sync = true;
            }
        }

        if (_sync_on_close) {
            Status sync_status = sync();
            if (!sync_status.ok()) {
                LOG(ERROR) << "Unable to Sync " << _filename << ": " << sync_status.to_string();
                if (s.ok()) {
                    s = sync_status;
                }
            }
        }

        int ret;
        RETRY_ON_EINTR(ret, ::close(_fd));
        _closed = true;
        if (ret < 0) {
            if (s.ok()) {
                s = io_error(_filename, errno);
            }
        }
        return s;
    }

    Status flush(FlushMode mode) override {
#if defined(__linux__)
        int flags = SYNC_FILE_RANGE_WRITE;
        if (mode == FLUSH_SYNC) {
            flags |= SYNC_FILE_RANGE_WAIT_BEFORE;
            flags |= SYNC_FILE_RANGE_WAIT_AFTER;
        }
        if (sync_file_range(_fd, 0, 0, flags) < 0) {
            return io_error(_filename, errno);
        }
#else
        if (mode == FLUSH_SYNC && fsync(_fd) < 0) {
            return io_error(_filename, errno);
        }
#endif
        return Status::OK();
    }

    Status sync() override {
        if (_pending_sync) {
            _pending_sync = false;
            RETURN_IF_ERROR(do_sync(_fd, _filename));
        }
        return Status::OK();
    }

    uint64_t size() const override { return _filesize; }
    const string& filename() const override { return _filename; }

private:
    std::string _filename;
    int _fd;
    const bool _sync_on_close = false;
    bool _pending_sync = false;
    bool _closed = false;
    uint64_t _filesize = 0;
    uint64_t _pre_allocated_size = 0;
};

class PosixFileSystem : public FileSystem {
public:
    ~PosixFileSystem() override = default;

    Type type() const override { return POSIX; }

    using FileSystem::new_sequential_file;
    using FileSystem::new_random_access_file;

    StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const SequentialFileOptions& opts,
                                                                  const string& fname) override {
        (void)opts;
        int fd;
        RETRY_ON_EINTR(fd, ::open(fname.c_str(), O_RDONLY));
        if (fd < 0) {
            return io_error(fname, errno);
        }
        auto stream = std::make_shared<io::FdInputStream>(fd);
        stream->set_close_on_delete(true);
        return std::make_unique<SequentialFile>(std::move(stream), fname);
    }

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& fname) override {
        if (config::file_descriptor_cache_capacity > 0 && enable_fd_cache(fname)) {
            FdCache::Handle* h = FdCache::Instance()->lookup(fname);
            if (h == nullptr) {
                int fd;
                RETRY_ON_EINTR(fd, ::open(fname.c_str(), O_RDONLY));
                if (fd < 0) {
                    return io_error(fname, errno);
                }
                h = FdCache::Instance()->insert(fname, fd);
            }
            auto stream = std::make_shared<CachedFdInputStream>(h);
            stream->set_close_on_delete(false);
            return std::make_unique<RandomAccessFile>(std::move(stream), fname);
        } else {
            int fd;
            RETRY_ON_EINTR(fd, ::open(fname.c_str(), O_RDONLY));
            if (fd < 0) {
                return io_error(fname, errno);
            }
            auto stream = std::make_shared<io::FdInputStream>(fd);
            stream->set_close_on_delete(true);
            return std::make_unique<RandomAccessFile>(std::move(stream), fname);
        }
    }

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const string& fname) override {
        return new_writable_file(WritableFileOptions(), fname);
    }

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const WritableFileOptions& opts,
                                                              const string& fname) override {
        int fd = 0;
        RETURN_IF_ERROR(do_open(fname, opts.mode, &fd));

        uint64_t file_size = 0;
        if (opts.mode == MUST_EXIST) {
            ASSIGN_OR_RETURN(file_size, get_file_size(fname));
        }
        return std::make_unique<PosixWritableFile>(fname, fd, file_size, opts.sync_on_close);
    }

    Status path_exists(const std::string& fname) override {
        if (access(fname.c_str(), F_OK) != 0) {
            return io_error(fname, errno);
        }
        return Status::OK();
    }

    Status get_children(const std::string& dir, std::vector<std::string>* result) override {
        result->clear();
        return iterate_dir(dir, [&](std::string_view name) -> bool {
            result->emplace_back(name);
            return true;
        });
    }

    Status iterate_dir(const std::string& dir, const std::function<bool(std::string_view)>& cb) override {
        DIR* d = opendir(dir.c_str());
        if (d == nullptr) {
            return io_error(dir, errno);
        }
        errno = 0;
        struct dirent* entry;
        while ((entry = readdir(d)) != nullptr) {
            std::string_view name(entry->d_name);
            if (name == "." || name == "..") {
                continue;
            }
            // callback returning false means to terminate iteration
            if (!cb(name)) {
                break;
            }
        }
        closedir(d);
        if (errno != 0) return io_error(dir, errno);
        return Status::OK();
    }

    Status iterate_dir2(const std::string& dir, const std::function<bool(DirEntry)>& cb) override {
        DIR* d = opendir(dir.c_str());
        if (d == nullptr) {
            return io_error(dir, errno);
        }
        errno = 0;
        struct dirent* entry;
        while ((entry = readdir(d)) != nullptr) {
            std::string_view name(entry->d_name);
            if (name == "." || name == "..") {
                continue;
            }
            struct stat child_stat;
            std::string child_path = fmt::format("{}/{}", dir, name);
            if (stat(child_path.c_str(), &child_stat) != 0) {
                break;
            }
            DirEntry de;
            de.name = name;
            de.is_dir = S_ISDIR(child_stat.st_mode);
            de.mtime = static_cast<int64_t>(child_stat.st_mtime);
            de.size = child_stat.st_size;
            // callback returning false means to terminate iteration
            if (!cb(de)) {
                break;
            }
        }
        closedir(d);
        if (errno != 0) return io_error(dir, errno);
        return Status::OK();
    }

    Status delete_file(const std::string& fname) override {
        if (config::file_descriptor_cache_capacity > 0 && enable_fd_cache(fname)) {
            FdCache::Instance()->erase(fname);
        }
        if (unlink(fname.c_str()) != 0) {
            return io_error(fname, errno);
        }
        return Status::OK();
    }

    Status create_dir(const std::string& name) override {
        if (mkdir(name.c_str(), 0755) != 0) {
            return io_error(name, errno);
        }
        return Status::OK();
    }

    Status create_dir_if_missing(const string& dirname, bool* created = nullptr) override {
        Status s = create_dir(dirname);
        if (created != nullptr) {
            *created = s.ok();
        }

        // Check that dirname is actually a directory.
        if (s.is_already_exist()) {
            ASSIGN_OR_RETURN(const bool is_dir, is_directory(dirname));
            if (is_dir) {
                return Status::OK();
            } else {
                return s.clone_and_append("path already exists but not a dir");
            }
        }
        return s;
    }

    Status create_dir_recursive(const std::string& dirname) override {
        std::error_code ec;
        // If `dirname` already exist and is a directory, the return value would be false and ec.value() would be 0
        (void)std::filesystem::create_directories(dirname, ec);
        if (ec.value() != 0) {
            return io_error(fmt::format("create {} recursive", dirname), ec.value());
        }
        return Status::OK();
    }

    // Delete the specified directory.
    Status delete_dir(const std::string& dirname) override {
        if (rmdir(dirname.c_str()) != 0) {
            return io_error(dirname, errno);
        }
        return Status::OK();
    }

    Status delete_dir_recursive(const std::string& dirname) override {
        auto res = is_directory(dirname);
        if (res.status().is_not_found()) return Status::OK();
        if (!res.ok()) return res.status();
        bool is_dir = *res;
        if (is_dir) {
            Status st;
            RETURN_IF_ERROR(iterate_dir(dirname, [&](std::string_view name) -> bool {
                st = delete_dir_recursive(fmt::format("{}/{}", dirname, name));
                return st.ok() ? true : false;
            }));
            RETURN_IF_ERROR(st);
            return delete_dir(dirname);
        } else {
            return delete_file(dirname);
        }
    }

    Status sync_dir(const string& dirname) override {
        int dir_fd;
        RETRY_ON_EINTR(dir_fd, open(dirname.c_str(), O_DIRECTORY | O_RDONLY));
        if (dir_fd < 0) {
            return io_error(dirname, errno);
        }
        ScopedFdCloser fd_closer(dir_fd);
        if (fsync(dir_fd) != 0) {
            return io_error(dirname, errno);
        }
        return Status::OK();
    }

    StatusOr<bool> is_directory(const std::string& path) override {
        struct stat path_stat;
        if (stat(path.c_str(), &path_stat) != 0) {
            return io_error(path, errno);
        } else {
            return S_ISDIR(path_stat.st_mode);
        }
    }

    Status canonicalize(const std::string& path, std::string* result) override {
        // NOTE: we must use free() to release the buffer retruned by realpath(),
        // because the buffer is allocated by malloc(), see `man 3 realpath`.
        std::unique_ptr<char[], FreeDeleter> r(realpath(path.c_str(), nullptr));
        if (r == nullptr) {
            return io_error(strings::Substitute("Unable to canonicalize $0", path), errno);
        }
        *result = std::string(r.get());
        return Status::OK();
    }

    StatusOr<uint64_t> get_file_size(const string& fname) override {
        struct stat sbuf;
        if (stat(fname.c_str(), &sbuf) != 0) {
            return io_error(fname, errno);
        } else {
            return sbuf.st_size;
        }
    }

    StatusOr<uint64_t> get_file_modified_time(const std::string& fname) override {
        struct stat s;
        if (stat(fname.c_str(), &s) != 0) {
            return io_error(fname, errno);
        }
        return static_cast<uint64_t>(s.st_mtime);
    }

    Status rename_file(const std::string& src, const std::string& target) override {
        if (config::file_descriptor_cache_capacity > 0) {
            if (enable_fd_cache(src)) FdCache::Instance()->erase(src);
            if (enable_fd_cache(target)) FdCache::Instance()->erase(target);
        }
        if (rename(src.c_str(), target.c_str()) != 0) {
            return io_error(src, errno);
        }
        return Status::OK();
    }

    Status link_file(const std::string& old_path, const std::string& new_path) override {
        if (link(old_path.c_str(), new_path.c_str()) != 0) {
            return io_error(old_path, errno);
        }
        return Status::OK();
    }

    StatusOr<SpaceInfo> space(const std::string& path) override {
        try {
            std::filesystem::space_info path_info = std::filesystem::space(path);
            SpaceInfo info{.capacity = static_cast<int64_t>(path_info.capacity),
                           .free = static_cast<int64_t>(path_info.free),
                           .available = static_cast<int64_t>(path_info.available)};
            return info;
        } catch (std::filesystem::filesystem_error& e) {
            return Status::IOError(fmt::format("fail to get space info of path {}: {}", path, e.what()));
        }
    }
};

// Default Posix FileSystem
FileSystem* FileSystem::Default() {
    static PosixFileSystem default_fs;
    return &default_fs;
}

std::unique_ptr<FileSystem> new_fs_posix() {
    return std::make_unique<PosixFileSystem>();
}

} // end namespace starrocks
