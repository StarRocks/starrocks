// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "env/env_hdfs.h"

#include "env/env.h"
#include "fmt/core.h"
#include "gutil/strings/substitute.h"
#include "hdfs/hdfs.h"
#include "util/hdfs_util.h"

namespace starrocks {

// ==================================  HdfsRandomAccessFile  ==========================================

HdfsRandomAccessFile::HdfsRandomAccessFile(hdfsFS fs, const std::string& file_name, size_t file_size, bool usePread)
        : _opened(false), _fs(fs), _file(nullptr), _file_name(file_name), _file_size(file_size), _usePread(usePread) {}

HdfsRandomAccessFile::~HdfsRandomAccessFile() noexcept {
    close();
}

Status HdfsRandomAccessFile::open() {
    DCHECK(!_opened);
    if (_fs) {
        _file = hdfsOpenFile(_fs, _file_name.c_str(), O_RDONLY, 0, 0, 0);
        if (_file == nullptr) {
            return Status::InternalError(fmt::format("open file failed, file={}", _file_name));
        }
    }
    _opened = true;
    return Status::OK();
}

void HdfsRandomAccessFile::close() {
    if (_opened) {
        if (_fs && _file) {
            hdfsCloseFile(_fs, _file);
        }
        _opened = false;
    }
}

Status HdfsRandomAccessFile::_read_at(int64_t offset, char* data, size_t size, size_t* read_size) const {
    if (_usePread) {
        *read_size = size;
        if (hdfsPreadFully(_fs, _file, offset, data, size) == -1) {
            return Status::IOError(strings::Substitute("fail to hdfsPreadFully file, file=$0, error=$1", _file_name,
                                                       get_hdfs_err_msg()));
        }
    } else {
        auto cur_offset = hdfsTell(_fs, _file);
        if (cur_offset == -1) {
            return Status::IOError(
                    strings::Substitute("fail to get offset, file=$0, error=$1", _file_name, get_hdfs_err_msg()));
        }
        if (cur_offset != offset) {
            if (hdfsSeek(_fs, _file, offset)) {
                return Status::IOError(strings::Substitute("fail to seek offset, file=$0, offset=$1, error=$2",
                                                           _file_name, offset, get_hdfs_err_msg()));
            }
        }
        size_t bytes_read = 0;
        while (bytes_read < size) {
            size_t to_read = size - bytes_read;
            auto hdfs_res = hdfsRead(_fs, _file, data + bytes_read, to_read);
            if (hdfs_res < 0) {
                return Status::IOError(strings::Substitute("fail to hdfsRead file, file=$0, error=$1", _file_name,
                                                           get_hdfs_err_msg()));
            } else if (hdfs_res == 0) {
                break;
            }
            bytes_read += hdfs_res;
        }
        *read_size = bytes_read;
    }
    return Status::OK();
}

Status HdfsRandomAccessFile::read(uint64_t offset, Slice* res) const {
    DCHECK(_opened);
    size_t read_size = 0;
    Status st = _read_at(offset, res->data, res->size, &read_size);
    if (!st.ok()) return st;
    res->size = read_size;
    return Status::OK();
}

Status HdfsRandomAccessFile::read_at(uint64_t offset, const Slice& res) const {
    size_t read_size = 0;
    RETURN_IF_ERROR(_read_at(offset, res.data, res.size, &read_size));
    if (read_size != res.size) {
        return Status::InternalError(
                strings::Substitute("fail to read enough data, file=$0, offset=$1, size=$2, expect=$3", _file_name,
                                    offset, read_size, res.size));
    }
    return Status::OK();
}

Status HdfsRandomAccessFile::readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const {
    // TODO: implement
    return Status::InternalError("HdfsRandomAccessFile::readv_at not implement");
}

// ===================================  S3RandomAccessFile  =========================================

S3RandomAccessFile::S3RandomAccessFile(S3Client* client, const std::string& bucket, const std::string& object,
                                       size_t object_size)
        : _client(client), _bucket(bucket), _object(object), _object_size(object_size) {
    _file_name = "s3://" + _bucket + "/" + _object;
}

Status S3RandomAccessFile::read(uint64_t offset, Slice* res) const {
    size_t read_size = 0;
    Status st = _client->get_object_range(_bucket, _object, offset, res->size, res->data, &read_size);
    if (!st.ok()) return st;
    res->size = read_size;
    return st;
}

Status S3RandomAccessFile::read_at(uint64_t offset, const Slice& res) const {
    size_t read_size = 0;
    RETURN_IF_ERROR(_client->get_object_range(_bucket, _object, offset, res.size, res.data, &read_size));
    if (read_size != res.size) {
        return Status::InternalError(
                strings::Substitute("fail to read enough data, file=$0, offset=$1, size=$2, expect=$3", _file_name,
                                    offset, read_size, res.size));
    }
    return Status::OK();
}

Status S3RandomAccessFile::readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const {
    // TODO: implement
    return Status::InternalError("S3RandomAccessFile::readv_at not implement");
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

std::shared_ptr<RandomAccessFile> create_random_access_hdfs_file(const HdfsFsHandle& handle,
                                                                 const std::string& file_path, size_t file_size,
                                                                 bool usePread) {
    if (handle.type == HdfsFsHandle::Type::HDFS) {
        return std::make_shared<HdfsRandomAccessFile>(handle.hdfs_fs, file_path, file_size, usePread);
    } else if (handle.type == HdfsFsHandle::Type::S3) {
        const std::string& nn = handle.namenode;
        const std::string bucket = get_bucket_from_namenode(nn);
        const std::string object = file_path.substr(nn.size(), file_path.size() - nn.size());
        return std::make_shared<S3RandomAccessFile>(handle.s3_client, bucket, object, file_size);
    } else {
        CHECK(false) << strings::Substitute("Unknown HdfsFsHandle::Type $0", static_cast<int>(handle.type));
        __builtin_unreachable();
        return nullptr;
    }
}

} // namespace starrocks
