// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "env/env_hdfs.h"

#include <hdfs/hdfs.h>

#include <atomic>

#include "fmt/format.h"
#include "runtime/hdfs/hdfs_fs_cache.h"
#include "util/hdfs_util.h"

namespace starrocks {

// ==================================  HdfsRandomAccessFile  ==========================================

// class for remote read hdfs file
// Now this is not thread-safe.
class HdfsRandomAccessFile : public RandomAccessFile {
public:
    HdfsRandomAccessFile(hdfsFS fs, hdfsFile file, const std::string& file_name)
            : _fs(fs), _file(file), _file_name(file_name), _file_size(0) {}

    ~HdfsRandomAccessFile() override;

    StatusOr<int64_t> read_at(int64_t offset, void* data, int64_t size) const override;
    Status read_at_fully(int64_t offset, void* data, int64_t size) const override;
    Status readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const override;
    StatusOr<uint64_t> get_size() const override;
    const std::string& filename() const override { return _file_name; }
    StatusOr<std::unique_ptr<NumericStatistics>> get_numeric_statistics() override;

private:
    hdfsFS _fs;
    hdfsFile _file;
    std::string _file_name;
    mutable uint64_t _file_size;
};

HdfsRandomAccessFile::~HdfsRandomAccessFile() {
    int r = hdfsCloseFile(_fs, _file);
    PLOG_IF(ERROR, r != 0) << "close " << _file_name << " failed";
}

StatusOr<int64_t> HdfsRandomAccessFile::read_at(int64_t offset, void* data, int64_t size) const {
    if (UNLIKELY(size > std::numeric_limits<tSize>::max())) {
        return Status::NotSupported("read size is greater than std::numeric_limits<tSize>::max()");
    }
    tSize r = hdfsPread(_fs, _file, offset, data, static_cast<tSize>(size));
    if (UNLIKELY(r == -1)) {
        return Status::IOError(fmt::format("fail to hdfsPread {}: {}", _file_name, get_hdfs_err_msg()));
    }
    return r;
}

Status HdfsRandomAccessFile::read_at_fully(int64_t offset, void* data, int64_t size) const {
    if (UNLIKELY(size > std::numeric_limits<tSize>::max())) {
        return Status::NotSupported("read size is greater than std::numeric_limits<tSize>::max()");
    }
    tSize r = hdfsPreadFully(_fs, _file, offset, data, static_cast<tSize>(size));
    if (UNLIKELY(r == -1)) {
        return Status::IOError(fmt::format("fail to hdfsPreadFully {}: {}", _file_name, get_hdfs_err_msg()));
    }
    return Status::OK();
}

StatusOr<uint64_t> HdfsRandomAccessFile::get_size() const {
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

StatusOr<std::unique_ptr<NumericStatistics>> HdfsRandomAccessFile::get_numeric_statistics() {
    struct hdfsReadStatistics* hdfs_statistics = nullptr;
    auto r = hdfsFileGetReadStatistics(_file, &hdfs_statistics);
    if (r != 0) return Status::InternalError(fmt::format("hdfsFileGetReadStatistics failed: {}", r));
    auto statistics = std::make_unique<NumericStatistics>();
    statistics->reserve(4);
    statistics->append("TotalBytesRead", hdfs_statistics->totalBytesRead);
    statistics->append("TotalLocalBytesRead", hdfs_statistics->totalLocalBytesRead);
    statistics->append("TotalShortCircuitBytesRead", hdfs_statistics->totalShortCircuitBytesRead);
    statistics->append("TotalZeroCopyBytesRead", hdfs_statistics->totalZeroCopyBytesRead);
    hdfsFileFreeReadStatistics(hdfs_statistics);
    return std::move(statistics);
}

Status HdfsRandomAccessFile::readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const {
    for (size_t i = 0; i < res_cnt; i++) {
        RETURN_IF_ERROR(HdfsRandomAccessFile::read_at_fully(offset, res[i].data, res[i].size));
        offset += res[i].size;
    }
    return Status::OK();
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
    return std::make_unique<HdfsRandomAccessFile>(handle.hdfs_fs, file, path);
}

std::unique_ptr<Env> new_env_hdfs() {
    return std::make_unique<EnvHdfs>();
}

} // namespace starrocks
