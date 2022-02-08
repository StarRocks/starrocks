// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "env/env_hdfs.h"

#include "env/env.h"
#include "fmt/core.h"
#include "gutil/strings/substitute.h"
#include "hdfs/hdfs.h"
#include "util/hdfs_util.h"

namespace starrocks {

// class for remote read hdfs file
// Now this is not thread-safe.
class HdfsRandomAccessFile final : public io::RandomAccessFile {
public:
    HdfsRandomAccessFile(hdfsFS fs, hdfsFile file, int64_t size) : _fs(fs), _file(file), _size(size), _offset(0) {}
    ~HdfsRandomAccessFile() override;

    StatusOr<int64_t> read(void* data, int64_t count) override;

    StatusOr<int64_t> read_at(int64_t offset, void* data, int64_t count) override;

    StatusOr<int64_t> seek(int64_t offset, int whence) override;

    Status skip(int64_t count) override;

    bool allows_peak() const override;

    StatusOr<std::string_view> peak(int64_t nbytes) override;

    StatusOr<int64_t> get_size() override;

    StatusOr<int64_t> position() override;

    StatusOr<std::unique_ptr<io::NumericStatistics>> get_numeric_statistics() override;

private:
    hdfsFS _fs;
    hdfsFile _file;
    int64_t _size;
    int64_t _offset;
};

HdfsRandomAccessFile::~HdfsRandomAccessFile() {
    (void)hdfsCloseFile(_fs, _file);
}

StatusOr<int64_t> HdfsRandomAccessFile::read(void* data, int64_t count) {
    int r = hdfsPread(_fs, _file, _offset, data, count);
    if (UNLIKELY(r == -1)) {
        return Status::IOError(strings::Substitute("hdfsPread file, failed: $0", get_hdfs_err_msg()));
    }
    _offset += r;
    return r;
}

StatusOr<int64_t> HdfsRandomAccessFile::read_at(int64_t offset, void* data, int64_t count) {
    int r = hdfsPread(_fs, _file, _offset, data, count);
    if (UNLIKELY(r == -1)) {
        return Status::IOError(strings::Substitute("fail to hdfsPread file. error=$0", get_hdfs_err_msg()));
    }
    return r;
}

StatusOr<int64_t> HdfsRandomAccessFile::seek(int64_t offset, int whence) {
    ASSIGN_OR_RETURN(auto size, get_size());
    int64_t pos;
    if (whence == SEEK_SET) {
        pos = offset;
    } else if (whence == SEEK_CUR) {
        pos = _offset + offset;
    } else if (whence == SEEK_END) {
        pos = size + offset;
    } else {
        return Status::InvalidArgument("invalid whence");
    }
    if (pos < 0 || pos > size) {
        return Status::InvalidArgument("out-of-bounds offset");
    }
    _offset = pos;
    return pos;
}

Status HdfsRandomAccessFile::skip(int64_t count) {
    ASSIGN_OR_RETURN(auto size, get_size());
    return seek(std::min(count, size - _offset), SEEK_CUR).status();
}

bool HdfsRandomAccessFile::allows_peak() const {
    return false;
}

StatusOr<std::string_view> HdfsRandomAccessFile::peak(int64_t nbytes) {
    return Status::NotSupported("HdfsRandomAccessFile::peak");
}

StatusOr<int64_t> HdfsRandomAccessFile::get_size() {
    return _size;
}

StatusOr<int64_t> HdfsRandomAccessFile::position() {
    return _offset;
}

StatusOr<std::unique_ptr<io::NumericStatistics>> HdfsRandomAccessFile::get_numeric_statistics() {
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

StatusOr<std::unique_ptr<io::RandomAccessFile>> EnvHdfs::new_random_access_file(const std::string& fname) {
    return new_random_access_file(RandomAccessFileOptions(), fname);
}

StatusOr<std::unique_ptr<io::RandomAccessFile>> EnvHdfs::new_random_access_file(const RandomAccessFileOptions& opts,
                                                                                const std::string& fname) {
    auto info = hdfsGetPathInfo(_fs, fname.c_str());
    if (UNLIKELY(info == nullptr)) {
        return Status::InternalError(fmt::format("hdfsGetPathInfo failed, file={}", fname));
    }
    auto size = info->mSize;
    hdfsFreeFileInfo(info, 1);
    hdfsFile file = hdfsOpenFile(_fs, fname.c_str(), O_RDONLY, 0, 0, 0);
    if (UNLIKELY(file == nullptr)) {
        return Status::InternalError(fmt::format("open file failed, file={}", fname));
    }
    return std::make_unique<HdfsRandomAccessFile>(_fs, file, size);
}

} // namespace starrocks
