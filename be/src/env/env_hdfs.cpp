// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "env/env_hdfs.h"

#include <fmt/format.h>
#include <hdfs/hdfs.h>

#include <atomic>

#include "runtime/hdfs/hdfs_fs_cache.h"
#include "util/hdfs_util.h"

namespace starrocks {

// ==================================  HdfsInputStream  ==========================================

// TODO: move this class to directory 'be/srcio/'
// class for remote read hdfs file
// Now this is not thread-safe.
class HdfsInputStream : public io::SeekableInputStream {
public:
    HdfsInputStream(hdfsFS fs, hdfsFile file, const std::string& file_name)
            : _fs(fs), _file(file), _file_name(file_name), _offset(0), _file_size(0) {}

    ~HdfsInputStream() override;

    StatusOr<int64_t> read(void* data, int64_t size) override;
    StatusOr<int64_t> get_size() override;
    StatusOr<int64_t> position() override { return _offset; }
    StatusOr<std::unique_ptr<io::NumericStatistics>> get_numeric_statistics() override;
    Status seek(int64_t offset) override;

private:
    hdfsFS _fs;
    hdfsFile _file;
    std::string _file_name;
    int64_t _offset;
    int64_t _file_size;
};

HdfsInputStream::~HdfsInputStream() {
    int r = hdfsCloseFile(_fs, _file);
    PLOG_IF(ERROR, r != 0) << "close " << _file_name << " failed";
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
        auto info = hdfsGetPathInfo(_fs, _file_name.c_str());
        if (UNLIKELY(info == nullptr)) {
            return Status::InternalError(fmt::format("hdfsGetPathInfo failed, file={}", _file_name));
        }
        _file_size = info->mSize;
        hdfsFreeFileInfo(info, 1);
    }
    return _file_size;
}

StatusOr<std::unique_ptr<io::NumericStatistics>> HdfsInputStream::get_numeric_statistics() {
    struct hdfsReadStatistics* hdfs_statistics = nullptr;
    auto r = hdfsFileGetReadStatistics(_file, &hdfs_statistics);
    if (r != 0) return Status::InternalError(fmt::format("hdfsFileGetReadStatistics failed: {}", r));
    auto statistics = std::make_unique<io::NumericStatistics>();
    statistics->reserve(4);
    statistics->append("TotalBytesRead", hdfs_statistics->totalBytesRead);
    statistics->append("TotalLocalBytesRead", hdfs_statistics->totalLocalBytesRead);
    statistics->append("TotalShortCircuitBytesRead", hdfs_statistics->totalShortCircuitBytesRead);
    statistics->append("TotalZeroCopyBytesRead", hdfs_statistics->totalZeroCopyBytesRead);
    hdfsFileFreeReadStatistics(hdfs_statistics);
    return std::move(statistics);
}

class EnvHdfs : public Env {
public:
    EnvHdfs() {}
    ~EnvHdfs() override = default;

    EnvHdfs(const EnvHdfs&) = delete;
    void operator=(const EnvHdfs&) = delete;
    EnvHdfs(EnvHdfs&&) = delete;
    void operator=(EnvHdfs&&) = delete;

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const std::string& path) override;

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& path) override;

    StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const std::string& path) override {
        return Status::NotSupported("EnvHdfs::new_sequential_file");
    }

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const std::string& path) override {
        return Status::NotSupported("EnvHdfs::new_writable_file");
    }

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const WritableFileOptions& opts,
                                                              const std::string& path) override {
        return Status::NotSupported("EnvHdfs::new_writable_file");
    }

    StatusOr<std::unique_ptr<RandomRWFile>> new_random_rw_file(const std::string& path) override {
        return Status::NotSupported("EnvHdfs::new_random_rw_file");
    }

    StatusOr<std::unique_ptr<RandomRWFile>> new_random_rw_file(const RandomRWFileOptions& opts,
                                                               const std::string& path) override {
        return Status::NotSupported("EnvHdfs::new_random_rw_file");
    }

    Status path_exists(const std::string& path) override { return Status::NotSupported("EnvHdfs::path_exists"); }

    Status get_children(const std::string& dir, std::vector<std::string>* file) override {
        return Status::NotSupported("EnvHdfs::get_children");
    }

    Status iterate_dir(const std::string& dir, const std::function<bool(std::string_view)>& cb) override {
        return Status::NotSupported("EnvHdfs::iterate_dir");
    }

    Status delete_file(const std::string& path) override { return Status::NotSupported("EnvHdfs::delete_file"); }

    Status create_dir(const std::string& dirname) override { return Status::NotSupported("EnvHdfs::create_dir"); }

    Status create_dir_if_missing(const std::string& dirname, bool* created) override {
        return Status::NotSupported("EnvHdfs::create_dir_if_missing");
    }

    Status create_dir_recursive(const std::string& dirname) override {
        return Status::NotSupported("EnvHdfs::create_dir_recursive");
    }

    Status delete_dir(const std::string& dirname) override { return Status::NotSupported("EnvHdfs::delete_dir"); }

    Status delete_dir_recursive(const std::string& dirname) override {
        return Status::NotSupported("EnvHdfs::delete_dir_recursive");
    }

    Status sync_dir(const std::string& dirname) override { return Status::NotSupported("EnvHdfs::sync_dir"); }

    StatusOr<bool> is_directory(const std::string& path) override {
        return Status::NotSupported("EnvHdfs::is_directory");
    }

    Status canonicalize(const std::string& path, std::string* file) override {
        return Status::NotSupported("EnvHdfs::canonicalize");
    }

    StatusOr<uint64_t> get_file_size(const std::string& path) override {
        return Status::NotSupported("EnvHdfs::get_file_size");
    }

    StatusOr<uint64_t> get_file_modified_time(const std::string& path) override {
        return Status::NotSupported("EnvHdfs::get_file_modified_time");
    }

    Status rename_file(const std::string& src, const std::string& target) override {
        return Status::NotSupported("EnvHdfs::rename_file");
    }

    Status link_file(const std::string& old_path, const std::string& new_path) override {
        return Status::NotSupported("EnvHdfs::link_file");
    }
};

StatusOr<std::unique_ptr<RandomAccessFile>> EnvHdfs::new_random_access_file(const std::string& path) {
    return EnvHdfs::new_random_access_file(RandomAccessFileOptions(), path);
}

StatusOr<std::unique_ptr<RandomAccessFile>> EnvHdfs::new_random_access_file(const RandomAccessFileOptions& opts,
                                                                            const std::string& path) {
    std::string namenode;
    RETURN_IF_ERROR(get_namenode_from_path(path, &namenode));
    HdfsFsHandle handle;
    RETURN_IF_ERROR(HdfsFsCache::instance()->get_connection(namenode, &handle));
    if (handle.type != HdfsFsHandle::Type::HDFS) {
        return Status::InvalidArgument("invalid hdfs path");
    }
    hdfsFile file = hdfsOpenFile(handle.hdfs_fs, path.c_str(), O_RDONLY, 0, 0, 0);
    if (file == nullptr) {
        return Status::InternalError(fmt::format("hdfsOpenFile failed, file={}", path));
    }
    auto stream = std::make_shared<HdfsInputStream>(handle.hdfs_fs, file, path);
    return std::make_unique<RandomAccessFile>(std::move(stream), path);
}

std::unique_ptr<Env> new_env_hdfs() {
    return std::make_unique<EnvHdfs>();
}

} // namespace starrocks
