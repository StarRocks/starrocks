// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "env/env_hdfs.h"

#include <hdfs/hdfs.h>

#include <atomic>

#include "fmt/format.h"
#include "runtime/hdfs/hdfs_fs_cache.h"
#include "udf/java/utils.h"
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
    Status size(uint64_t* size) const override;
    const std::string& filename() const override { return _file_name; }
    StatusOr<std::unique_ptr<NumericStatistics>> get_numeric_statistics() override;

private:
    hdfsFS _fs;
    hdfsFile _file;
    std::string _file_name;
    mutable uint64_t _file_size;
};

HdfsRandomAccessFile::~HdfsRandomAccessFile() {
    auto ret = call_hdfs_scan_function_in_pthread([this]() {
        int r = hdfsCloseFile(this->_fs, this->_file);
        if (r == 0) {
            return Status::OK();
        } else {
            return Status::IOError("");
        }
    });
    Status st = ret->get_future().get();
    PLOG_IF(ERROR, !st.ok()) << "close " << _file_name << " failed";
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

Status HdfsRandomAccessFile::size(uint64_t* size) const {
    if (_file_size == 0) {
        auto ret = call_hdfs_scan_function_in_pthread([this] {
            auto info = hdfsGetPathInfo(_fs, _file_name.c_str());
            if (UNLIKELY(info == nullptr)) {
                return Status::InternalError(fmt::format("hdfsGetPathInfo failed, file={}", _file_name));
            }
            this->_file_size = info->mSize;
            hdfsFreeFileInfo(info, 1);
            return Status::OK();
        });
        Status st = ret->get_future().get();
        if (!st.ok()) return st;
    }
    *size = _file_size;
    return Status::OK();
}

StatusOr<std::unique_ptr<NumericStatistics>> HdfsRandomAccessFile::get_numeric_statistics() {
    auto statistics = std::make_unique<NumericStatistics>();
    NumericStatistics* stats = statistics.get();
    auto ret = call_hdfs_scan_function_in_pthread([this, stats] {
        struct hdfsReadStatistics* hdfs_statistics = nullptr;
        auto r = hdfsFileGetReadStatistics(_file, &hdfs_statistics);
        if (r != 0) return Status::InternalError(fmt::format("hdfsFileGetReadStatistics failed: {}", r));
        stats->reserve(4);
        stats->append("TotalBytesRead", hdfs_statistics->totalBytesRead);
        stats->append("TotalLocalBytesRead", hdfs_statistics->totalLocalBytesRead);
        stats->append("TotalShortCircuitBytesRead", hdfs_statistics->totalShortCircuitBytesRead);
        stats->append("TotalZeroCopyBytesRead", hdfs_statistics->totalZeroCopyBytesRead);
        hdfsFileFreeReadStatistics(hdfs_statistics);
        return Status::OK();
    });
    Status st = ret->get_future().get();
    if (!st.ok()) return st;
    return std::move(statistics);
}

Status HdfsRandomAccessFile::readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const {
    for (size_t i = 0; i < res_cnt; i++) {
        RETURN_IF_ERROR(HdfsRandomAccessFile::read_at_fully(offset, res[i].data, res[i].size));
        offset += res[i].size;
    }
    return Status::OK();
}

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

} // namespace starrocks
