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

#include "fs/hdfs/fs_hdfs.h"

#include <fmt/format.h>
#include <hdfs/hdfs.h>

#include <utility>

#include "fs/fs_util.h"
#include "fs/hdfs/hdfs_fs_cache.h"
#include "gutil/strings/substitute.h"
#include "runtime/file_result_writer.h"
#include "udf/java/utils.h"
#include "util/hdfs_util.h"

using namespace fmt::literals;

namespace starrocks {

// ==================================  HdfsInputStream  ==========================================

// TODO: move this class to directory 'be/srcio/'
// class for remote read hdfs file
// Now this is not thread-safe.
class HdfsInputStream : public io::SeekableInputStream {
public:
    HdfsInputStream(hdfsFS fs, hdfsFile file, std::string file_name)
            : _fs(fs), _file(file), _file_name(std::move(file_name)) {}

    ~HdfsInputStream() override;

    StatusOr<int64_t> read(void* data, int64_t size) override;
    StatusOr<int64_t> get_size() override;
    StatusOr<int64_t> position() override { return _offset; }
    StatusOr<std::unique_ptr<io::NumericStatistics>> get_numeric_statistics() override;
    Status seek(int64_t offset) override;
    void set_size(int64_t size) override;

private:
    hdfsFS _fs;
    hdfsFile _file;
    std::string _file_name;
    int64_t _offset{0};
    int64_t _file_size{0};
};

HdfsInputStream::~HdfsInputStream() {
    auto ret = call_hdfs_scan_function_in_pthread([this]() {
        int r = hdfsCloseFile(this->_fs, this->_file);
        if (r == -1) {
            auto error_msg = fmt::format("Fail to close file {}: {}", _file_name, get_hdfs_err_msg());
            LOG(WARNING) << error_msg;
            return Status::IOError(error_msg);
        }
        return Status::OK();
    });
    Status st = ret->get_future().get();
    PLOG_IF(ERROR, !st.ok()) << "close " << _file_name << " failed";
}

StatusOr<int64_t> HdfsInputStream::read(void* data, int64_t size) {
    if (UNLIKELY(size > std::numeric_limits<tSize>::max())) {
        size = std::numeric_limits<tSize>::max();
    }
    tSize r = hdfsPread(_fs, _file, _offset, data, static_cast<tSize>(size));
    if (r == -1) {
        return Status::IOError(fmt::format("fail to hdfsPread {}: {}", _file_name, get_hdfs_err_msg()));
    }
    _offset += r;
    return r;
}

Status HdfsInputStream::seek(int64_t offset) {
    if (offset < 0) return Status::InvalidArgument(fmt::format("Invalid offset {}", offset));
    _offset = offset;
    return Status::OK();
}

StatusOr<int64_t> HdfsInputStream::get_size() {
    if (_file_size == 0) {
        auto ret = call_hdfs_scan_function_in_pthread([this] {
            auto info = hdfsGetPathInfo(_fs, _file_name.c_str());
            if (UNLIKELY(info == nullptr)) {
                return Status::IOError(fmt::format("Fail to get path info of {}: {}", _file_name, get_hdfs_err_msg()));
            }
            this->_file_size = info->mSize;
            hdfsFreeFileInfo(info, 1);
            return Status::OK();
        });
        Status st = ret->get_future().get();
        if (!st.ok()) return st;
    }
    return _file_size;
}

void HdfsInputStream::set_size(int64_t value) {
    _file_size = value;
}

StatusOr<std::unique_ptr<io::NumericStatistics>> HdfsInputStream::get_numeric_statistics() {
    // `GetReadStatistics` is only supported in HDFS input stream
    if (!fs::is_hdfs_uri(_file_name)) {
        return nullptr;
    }

    auto statistics = std::make_unique<io::NumericStatistics>();
    io::NumericStatistics* stats = statistics.get();
    auto ret = call_hdfs_scan_function_in_pthread([this, stats] {
        struct hdfsReadStatistics* hdfs_statistics = nullptr;
        auto r = hdfsFileGetReadStatistics(_file, &hdfs_statistics);
        if (r == -1) {
            return Status::IOError(fmt::format("Fail to get read statistics of {}: {}", r, get_hdfs_err_msg()));
        }
        stats->append("TotalBytesRead", hdfs_statistics->totalBytesRead);
        stats->append("TotalLocalBytesRead", hdfs_statistics->totalLocalBytesRead);
        stats->append("TotalShortCircuitBytesRead", hdfs_statistics->totalShortCircuitBytesRead);
        stats->append("TotalZeroCopyBytesRead", hdfs_statistics->totalZeroCopyBytesRead);
        hdfsFileFreeReadStatistics(hdfs_statistics);

        struct hdfsHedgedReadMetrics* hdfs_hedged_read_statistics = nullptr;
        r = hdfsGetHedgedReadMetrics(_fs, &hdfs_hedged_read_statistics);
        if (r == 0) {
            stats->append("TotalHedgedReadOps", hdfs_hedged_read_statistics->hedgedReadOps);
            stats->append("TotalHedgedReadOpsInCurThread", hdfs_hedged_read_statistics->hedgedReadOpsInCurThread);
            stats->append("TotalHedgedReadOpsWin", hdfs_hedged_read_statistics->hedgedReadOpsWin);
            hdfsFreeHedgedReadMetrics(hdfs_hedged_read_statistics);
        }
        return Status::OK();
    });
    Status st = ret->get_future().get();
    if (!st.ok()) return st;
    return std::move(statistics);
}

class HDFSWritableFile : public WritableFile {
public:
    HDFSWritableFile(hdfsFS fs, hdfsFile file, std::string path, size_t offset)
            : _fs(fs), _file(file), _path(std::move(path)), _offset(offset) {
        FileSystem::on_file_write_open(this);
    }

    ~HDFSWritableFile() override { (void)HDFSWritableFile::close(); }

    Status append(const Slice& data) override;

    Status appendv(const Slice* data, size_t cnt) override;

    Status close() override;

    Status pre_allocate(uint64_t size) override { return Status::NotSupported("HDFS file pre_allocate not supported"); }

    Status flush(FlushMode mode) override {
        int status = hdfsHFlush(_fs, _file);
        return status == 0 ? Status::OK()
                           : Status::IOError(fmt::format("Fail to flush {}: {}", _path, get_hdfs_err_msg()));
    }

    Status sync() override {
        int status = hdfsHSync(_fs, _file);
        return status == 0 ? Status::OK()
                           : Status::IOError(fmt::format("Fail to sync {}: {}", _path, get_hdfs_err_msg()));
    }

    uint64_t size() const override { return _offset; }

    const std::string& filename() const override { return _path; }

private:
    hdfsFS _fs;
    hdfsFile _file;
    std::string _path;
    size_t _offset;
    bool _closed{false};
};

Status HDFSWritableFile::append(const Slice& data) {
    tSize r = hdfsWrite(_fs, _file, data.data, data.size);
    if (r == -1) { // error
        auto error_msg = fmt::format("Fail to append {}: {}", _path, get_hdfs_err_msg());
        LOG(WARNING) << error_msg;
        return Status::IOError(error_msg);
    }
    if (r != data.size) {
        auto error_msg =
                fmt::format("Fail to append {}, expect written size: {}, actual written size {} ", _path, data.size, r);
        LOG(WARNING) << error_msg;
        return Status::IOError(error_msg);
    }
    _offset += data.size;
    return Status::OK();
}

Status HDFSWritableFile::appendv(const Slice* data, size_t cnt) {
    for (size_t i = 0; i < cnt; i++) {
        RETURN_IF_ERROR(append(data[i]));
    }
    return Status::OK();
}

Status HDFSWritableFile::close() {
    if (_closed) {
        return Status::OK();
    }
    FileSystem::on_file_write_close(this);
    auto ret = call_hdfs_scan_function_in_pthread([this]() {
        int r = hdfsHSync(_fs, _file);
        if (r == -1) {
            auto error_msg = fmt::format("Fail to sync file {}: {}", _path, get_hdfs_err_msg());
            LOG(WARNING) << error_msg;
            return Status::IOError(error_msg);
        }

        r = hdfsCloseFile(_fs, _file);
        if (r == -1) {
            auto error_msg = fmt::format("Fail to close file {}: {}", _path, get_hdfs_err_msg());
            LOG(WARNING) << error_msg;
            return Status::IOError(error_msg);
        }

        return Status::OK();
    });
    Status st = ret->get_future().get();
    PLOG_IF(ERROR, !st.ok()) << "close " << _path << " failed";
    _closed = true;
    return st;
}

class HdfsFileSystem : public FileSystem {
public:
    HdfsFileSystem(const FSOptions& options) : _options(options) {}
    ~HdfsFileSystem() override = default;

    HdfsFileSystem(const HdfsFileSystem&) = delete;
    void operator=(const HdfsFileSystem&) = delete;
    HdfsFileSystem(HdfsFileSystem&&) = delete;
    void operator=(HdfsFileSystem&&) = delete;

    Type type() const override { return HDFS; }

    using FileSystem::new_sequential_file;
    using FileSystem::new_random_access_file;

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& path) override;

    StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const SequentialFileOptions& opts,
                                                                  const std::string& path) override;

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const std::string& path) override;

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const WritableFileOptions& opts,
                                                              const std::string& path) override;

    Status path_exists(const std::string& path) override;

    Status get_children(const std::string& dir, std::vector<std::string>* file) override {
        return Status::NotSupported("HdfsFileSystem::get_children");
    }

    Status iterate_dir(const std::string& dir, const std::function<bool(std::string_view)>& cb) override;

    Status iterate_dir2(const std::string& dir, const std::function<bool(DirEntry)>& cb) override;

    Status delete_file(const std::string& path) override { return Status::NotSupported("HdfsFileSystem::delete_file"); }

    Status create_dir(const std::string& dirname) override {
        return Status::NotSupported("HdfsFileSystem::create_dir");
    }

    Status create_dir_if_missing(const std::string& dirname, bool* created) override {
        return Status::NotSupported("HdfsFileSystem::create_dir_if_missing");
    }

    Status create_dir_recursive(const std::string& dirname) override {
        return Status::NotSupported("HdfsFileSystem::create_dir_recursive");
    }

    Status delete_dir(const std::string& dirname) override {
        return Status::NotSupported("HdfsFileSystem::delete_dir");
    }

    Status delete_dir_recursive(const std::string& dirname) override {
        return Status::NotSupported("HdfsFileSystem::delete_dir_recursive");
    }

    Status sync_dir(const std::string& dirname) override { return Status::NotSupported("HdfsFileSystem::sync_dir"); }

    StatusOr<bool> is_directory(const std::string& path) override {
        return Status::NotSupported("HdfsFileSystem::is_directory");
    }

    Status canonicalize(const std::string& path, std::string* file) override {
        return Status::NotSupported("HdfsFileSystem::canonicalize");
    }

    StatusOr<uint64_t> get_file_size(const std::string& path) override {
        return Status::NotSupported("HdfsFileSystem::get_file_size");
    }

    StatusOr<uint64_t> get_file_modified_time(const std::string& path) override {
        return Status::NotSupported("HdfsFileSystem::get_file_modified_time");
    }

    Status rename_file(const std::string& src, const std::string& target) override;

    Status link_file(const std::string& old_path, const std::string& new_path) override {
        return Status::NotSupported("HdfsFileSystem::link_file");
    }

private:
    Status _path_exists(hdfsFS fs, const std::string& path);

    FSOptions _options;
};

Status HdfsFileSystem::path_exists(const std::string& path) {
    std::string namenode;
    RETURN_IF_ERROR(get_namenode_from_path(path, &namenode));
    std::shared_ptr<HdfsFsClient> hdfs_client;
    RETURN_IF_ERROR(HdfsFsCache::instance()->get_connection(namenode, hdfs_client, _options));
    return _path_exists(hdfs_client->hdfs_fs, path);
}

Status HdfsFileSystem::iterate_dir(const std::string& dir, const std::function<bool(std::string_view)>& cb) {
    std::string namenode;
    RETURN_IF_ERROR(get_namenode_from_path(dir, &namenode));
    std::shared_ptr<HdfsFsClient> hdfs_client;
    RETURN_IF_ERROR(HdfsFsCache::instance()->get_connection(namenode, hdfs_client, _options));
    Status status = _path_exists(hdfs_client->hdfs_fs, dir);
    if (!status.ok()) {
        return status;
    }

    hdfsFileInfo* fileinfo;
    int numEntries;
    fileinfo = hdfsListDirectory(hdfs_client->hdfs_fs, dir.data(), &numEntries);
    if (fileinfo == nullptr) {
        return Status::InvalidArgument(fmt::format("hdfs list directory error {}", dir));
    }
    for (int i = 0; i < numEntries && fileinfo; ++i) {
        // obj_key.data() + uri.key().size(), obj_key.size() - uri.key().size()
        int32_t dir_size;
        if (dir[dir.size() - 1] == '/') {
            dir_size = dir.size();
        } else {
            dir_size = dir.size() + 1;
        }
        std::string_view name(fileinfo[i].mName + dir_size);
        if (!cb(name)) {
            break;
        }
    }
    if (fileinfo) {
        hdfsFreeFileInfo(fileinfo, numEntries);
    }
    return Status::OK();
}

Status HdfsFileSystem::iterate_dir2(const std::string& dir, const std::function<bool(DirEntry)>& cb) {
    std::string namenode;
    RETURN_IF_ERROR(get_namenode_from_path(dir, &namenode));
    std::shared_ptr<HdfsFsClient> hdfs_client;
    RETURN_IF_ERROR(HdfsFsCache::instance()->get_connection(namenode, hdfs_client, _options));
    Status status = _path_exists(hdfs_client->hdfs_fs, dir);
    if (!status.ok()) {
        return status;
    }

    hdfsFileInfo* fileinfo;
    int numEntries;
    fileinfo = hdfsListDirectory(hdfs_client->hdfs_fs, dir.data(), &numEntries);
    if (fileinfo == nullptr) {
        return Status::InvalidArgument(fmt::format("hdfs list directory error {}", dir));
    }
    for (int i = 0; i < numEntries && fileinfo; ++i) {
        // obj_key.data() + uri.key().size(), obj_key.size() - uri.key().size()
        int32_t dir_size;
        if (dir[dir.size() - 1] == '/') {
            dir_size = dir.size();
        } else {
            dir_size = dir.size() + 1;
        }

        const std::string local_fs("file:/");
        if (dir.compare(0, local_fs.length(), local_fs) == 0) {
            std::string mName(fileinfo[i].mName);
            std::size_t found = mName.rfind("/");
            if (found == std::string::npos) {
                return Status::InvalidArgument(fmt::format("parse path fail {}", dir));
            }

            dir_size = found + 1;
        }

        std::string_view name(fileinfo[i].mName + dir_size);
        DirEntry entry{.name = name,
                       .mtime = fileinfo[i].mLastMod,
                       .size = fileinfo[i].mSize,
                       .is_dir = fileinfo[i].mKind == tObjectKind::kObjectKindDirectory};
        if (!cb(entry)) {
            break;
        }
    }
    if (fileinfo) {
        hdfsFreeFileInfo(fileinfo, numEntries);
    }
    return Status::OK();
}

Status HdfsFileSystem::_path_exists(hdfsFS fs, const std::string& path) {
    int status = hdfsExists(fs, path.data());
    return status == 0 ? Status::OK() : Status::NotFound(path);
}

StatusOr<std::unique_ptr<WritableFile>> HdfsFileSystem::new_writable_file(const std::string& path) {
    return HdfsFileSystem::new_writable_file(WritableFileOptions(), path);
}

StatusOr<std::unique_ptr<WritableFile>> HdfsFileSystem::new_writable_file(const WritableFileOptions& opts,
                                                                          const std::string& path) {
    std::string namenode;
    RETURN_IF_ERROR(get_namenode_from_path(path, &namenode));
    std::shared_ptr<HdfsFsClient> hdfs_client;
    RETURN_IF_ERROR(HdfsFsCache::instance()->get_connection(namenode, hdfs_client, _options));
    int flags = O_WRONLY;
    if (opts.mode == FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE) {
        if (auto st = _path_exists(hdfs_client->hdfs_fs, path); st.ok()) {
            return Status::NotSupported(fmt::format("Cannot truncate a file by hdfs writer, path={}", path));
        }
    } else if (opts.mode == MUST_CREATE) {
        if (auto st = _path_exists(hdfs_client->hdfs_fs, path); st.ok()) {
            return Status::AlreadyExist(path);
        }
    } else if (opts.mode == MUST_EXIST) {
        return Status::NotSupported("Open with MUST_EXIST not supported by hdfs writer");
    } else if (opts.mode == CREATE_OR_OPEN) {
        return Status::NotSupported("Open with CREATE_OR_OPEN not supported by hdfs writer");
    } else {
        auto msg = strings::Substitute("Unsupported open mode $0", opts.mode);
        return Status::NotSupported(msg);
    }

    flags |= O_CREAT;

    int hdfs_write_buffer_size = 0;
    // pass zero to hdfsOpenFile will use the default hdfs_write_buffer_size
    if (_options.result_file_options != nullptr) {
        hdfs_write_buffer_size = _options.result_file_options->write_buffer_size_kb;
    }
    if (_options.export_sink != nullptr && _options.export_sink->__isset.hdfs_write_buffer_size_kb) {
        hdfs_write_buffer_size = _options.export_sink->hdfs_write_buffer_size_kb;
    }
    if (_options.upload != nullptr && _options.upload->__isset.hdfs_write_buffer_size_kb) {
        hdfs_write_buffer_size = _options.upload->__isset.hdfs_write_buffer_size_kb;
    }

    hdfsFile file = hdfsOpenFile(hdfs_client->hdfs_fs, path.c_str(), flags, hdfs_write_buffer_size, 0, 0);
    if (file == nullptr) {
        if (errno == ENOENT) {
            return Status::RemoteFileNotFound(fmt::format("hdfsOpenFile failed, file={}", path));
        } else {
            return Status::InternalError(fmt::format("hdfsOpenFile failed, file={}", path));
        }
    }
    return std::make_unique<HDFSWritableFile>(hdfs_client->hdfs_fs, file, path, 0);
}

StatusOr<std::unique_ptr<SequentialFile>> HdfsFileSystem::new_sequential_file(const SequentialFileOptions& opts,
                                                                              const std::string& path) {
    (void)opts;
    std::string namenode;
    RETURN_IF_ERROR(get_namenode_from_path(path, &namenode));
    std::shared_ptr<HdfsFsClient> hdfs_client;
    RETURN_IF_ERROR(HdfsFsCache::instance()->get_connection(namenode, hdfs_client, _options));
    // pass zero to hdfsOpenFile will use the default hdfs_read_buffer_size
    int hdfs_read_buffer_size = 0;
    if (_options.scan_range_params != nullptr && _options.scan_range_params->__isset.hdfs_read_buffer_size_kb) {
        hdfs_read_buffer_size = _options.scan_range_params->hdfs_read_buffer_size_kb;
    }
    if (_options.download != nullptr && _options.download->__isset.hdfs_read_buffer_size_kb) {
        hdfs_read_buffer_size = _options.download->hdfs_read_buffer_size_kb;
    }
    hdfsFile file = hdfsOpenFile(hdfs_client->hdfs_fs, path.c_str(), O_RDONLY, hdfs_read_buffer_size, 0, 0);
    if (file == nullptr) {
        if (errno == ENOENT) {
            return Status::RemoteFileNotFound(fmt::format("hdfsOpenFile failed, file={}", path));
        } else {
            return Status::InternalError(fmt::format("hdfsOpenFile failed, file={}", path));
        }
    }
    auto stream = std::make_shared<HdfsInputStream>(hdfs_client->hdfs_fs, file, path);
    return std::make_unique<SequentialFile>(std::move(stream), path);
}

StatusOr<std::unique_ptr<RandomAccessFile>> HdfsFileSystem::new_random_access_file(const RandomAccessFileOptions& opts,
                                                                                   const std::string& path) {
    std::string namenode;
    RETURN_IF_ERROR(get_namenode_from_path(path, &namenode));
    std::shared_ptr<HdfsFsClient> hdfs_client;
    RETURN_IF_ERROR(HdfsFsCache::instance()->get_connection(namenode, hdfs_client, _options));
    // pass zero to hdfsOpenFile will use the default hdfs_read_buffer_size
    int hdfs_read_buffer_size = 0;
    if (_options.scan_range_params != nullptr && _options.scan_range_params->__isset.hdfs_read_buffer_size_kb) {
        hdfs_read_buffer_size = _options.scan_range_params->hdfs_read_buffer_size_kb;
    }
    if (_options.download != nullptr && _options.download->__isset.hdfs_read_buffer_size_kb) {
        hdfs_read_buffer_size = _options.download->hdfs_read_buffer_size_kb;
    }
    hdfsFile file = hdfsOpenFile(hdfs_client->hdfs_fs, path.c_str(), O_RDONLY, hdfs_read_buffer_size, 0, 0);
    if (file == nullptr) {
        if (errno == ENOENT) {
            return Status::RemoteFileNotFound(fmt::format("hdfsOpenFile failed, file={}", path));
        } else {
            return Status::InternalError(fmt::format("hdfsOpenFile failed, file={}", path));
        }
    }
    auto stream = std::make_shared<HdfsInputStream>(hdfs_client->hdfs_fs, file, path);
    return std::make_unique<RandomAccessFile>(std::move(stream), path);
}

Status HdfsFileSystem::rename_file(const std::string& src, const std::string& target) {
    std::string namenode;
    RETURN_IF_ERROR(get_namenode_from_path(src, &namenode));
    std::shared_ptr<HdfsFsClient> hdfs_client;
    RETURN_IF_ERROR(HdfsFsCache::instance()->get_connection(namenode, hdfs_client, _options));
    int ret = hdfsRename(hdfs_client->hdfs_fs, src.data(), target.data());
    if (ret != 0) {
        return Status::InvalidArgument(fmt::format("rename file from {} to {} error", src, target));
    }
    return Status::OK();
}

std::unique_ptr<FileSystem> new_fs_hdfs(const FSOptions& options) {
    return std::make_unique<HdfsFileSystem>(options);
}

} // namespace starrocks
