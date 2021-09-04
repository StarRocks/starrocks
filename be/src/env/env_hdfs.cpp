// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "env/env_hdfs.h"

#include "env/env.h"
#include "gutil/strings/substitute.h"
#include "util/hdfs_util.h"

namespace starrocks {

HdfsRandomAccessFile::HdfsRandomAccessFile(hdfsFS fs, hdfsFile file, std::string filename)
        : _fs(fs), _file(file), _filename(std::move(filename)) {}

static Status read_at_internal(hdfsFS fs, hdfsFile file, const std::string& file_name, int64_t offset, Slice* res) {
    auto cur_offset = hdfsTell(fs, file);
    if (cur_offset == -1) {
        return Status::IOError(
                strings::Substitute("fail to get offset, file=$0, error=$1", file_name, get_hdfs_err_msg()));
    }
    if (cur_offset != offset) {
        if (hdfsSeek(fs, file, offset)) {
            return Status::IOError(strings::Substitute("fail to seek offset, file=$0, offset=$1, error=$1", file_name,
                                                       offset, get_hdfs_err_msg()));
        }
    }
    size_t bytes_read = 0;
    while (bytes_read < res->size) {
        size_t to_read = res->size - bytes_read;
        auto hdfs_res = hdfsRead(fs, file, res->data + bytes_read, to_read);
        if (hdfs_res < 0) {
            return Status::IOError(
                    strings::Substitute("fail to read file, file=$0, error=$1", file_name, get_hdfs_err_msg()));
        } else if (hdfs_res == 0) {
            break;
        }
        bytes_read += hdfs_res;
    }
    res->size = bytes_read;
    return Status::OK();
}

Status HdfsRandomAccessFile::read(uint64_t offset, Slice* res) const {
    RETURN_IF_ERROR(read_at_internal(_fs, _file, _filename, offset, res));
    return Status::OK();
}

Status HdfsRandomAccessFile::read_at(uint64_t offset, const Slice& res) const {
    Slice slice = res;
    RETURN_IF_ERROR(read_at_internal(_fs, _file, _filename, offset, &slice));
    if (slice.size != res.size) {
        return Status::InternalError(
                strings::Substitute("fail to read enough data, file=$0, offset=$1, size=$2, expect=$3", _filename,
                                    offset, slice.size, res.size));
    }
    return Status::OK();
}

Status HdfsRandomAccessFile::readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const {
    // TODO: implement
    return Status::InternalError("HdfsRandomAccessFile::readv_at not implement");
}

Status HdfsRandomAccessFile::size(uint64_t* size) const {
    // TODO: implement
    return Status::InternalError("HdfsRandomAccessFile::size not implement");
}
} // namespace starrocks
