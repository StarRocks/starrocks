// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "env/env_hdfs.h"

#include "env/env.h"
#include "fmt/core.h"
#include "gutil/strings/substitute.h"
#include "hdfs/hdfs.h"
#include "util/hdfs_util.h"

namespace starrocks {

// ==================================  HdfsRandomAccessFile  ==========================================

HdfsRandomAccessFile::HdfsRandomAccessFile(const HdfsFsHandle& handle, const std::string& file_name, size_t file_size,
                                           bool usePread)
        : _opened(false),
          _fs(handle.hdfs_fs),
          _file(nullptr),
          _file_name(file_name),
          _file_size(file_size),
          _usePread(usePread) {}

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
    // TODO: implement
    // We don't need to maintain the `read` API.
    return Status::InternalError("HdfsRandomAccessFile::read not implement");
}

Status HdfsRandomAccessFile::read_at(uint64_t offset, const Slice& res) const {
    DCHECK(_opened);
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

S3RandomAccessFile::S3RandomAccessFile(const HdfsFsHandle& handle, const std::string& file_name, size_t file_size)
        : _opened(false), _client(handle.s3_client), _file_name(file_name), _file_size(file_size) {
    _init(handle);
}

S3RandomAccessFile::~S3RandomAccessFile() noexcept {
    close();
}

void S3RandomAccessFile::_init(const HdfsFsHandle& handle) {
    const std::string& nn = handle.namenode;
    _bucket = get_bucket_from_namenode(nn);
    _object = _file_name.substr(nn.size(), _file_name.size() - nn.size());
    VLOG_FILE << "[S3] filename = " << _file_name << ", bucket = " << _bucket << ", object = " << _object;
}

Status S3RandomAccessFile::open() {
    return Status::OK();
}

void S3RandomAccessFile::close() {}

Status S3RandomAccessFile::read(uint64_t offset, Slice* res) const {
    // TODO: implement
    return Status::InternalError("S3RandomAccessFile::read not implement");
}

Status S3RandomAccessFile::read_at(uint64_t offset, const Slice& res) const {
    size_t read = 0;
    return _client->get_object_range(_bucket, _object, res.data, offset, res.size, &read);
    if (read != res.size) {
        return Status::InternalError(
                strings::Substitute("fail to read enough data, file=$0, offset=$1, size=$2, expect=$3", _file_name,
                                    offset, read, res.size));
    }
    return Status::OK();
}

Status S3RandomAccessFile::readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const {
    // TODO: implement
    return Status::InternalError("S3RandomAccessFile::readv_at not implement");
}

} // namespace starrocks
