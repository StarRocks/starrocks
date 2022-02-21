// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#ifdef STARROCKS_WITH_AWS

#include "io/s3_random_access_file.h"

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <fmt/format.h>

namespace starrocks::io {

StatusOr<int64_t> S3RandomAccessFile::read(void* data, int64_t count) {
    ASSIGN_OR_RETURN(auto nread, S3RandomAccessFile::read_at(_offset, data, count));
    _offset += nread;
    return nread;
}

StatusOr<int64_t> S3RandomAccessFile::read_at(int64_t offset, void* out, int64_t count) {
    if (UNLIKELY(_size == -1)) {
        ASSIGN_OR_RETURN(_size, S3RandomAccessFile::get_size());
    }
    if (offset >= _size) {
        return 0;
    }
    auto range = fmt::format("bytes={}-{}", offset, std::min<int64_t>(offset + count, _size));
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(_bucket);
    request.SetKey(_object);
    request.SetRange(std::move(range));

    Aws::S3::Model::GetObjectOutcome outcome = _s3client->GetObject(request);
    if (outcome.IsSuccess()) {
        Aws::IOStream& body = outcome.GetResult().GetBody();
        body.read(static_cast<char*>(out), count);
        return body.gcount();
    } else {
        return Status::IOError(outcome.GetError().GetMessage());
    }
}

Status S3RandomAccessFile::skip(int64_t count) {
    if (UNLIKELY(_size == -1)) {
        ASSIGN_OR_RETURN(_size, S3RandomAccessFile::get_size());
    }
    _offset = std::min(_offset + count, _size);
    return Status::OK();
}

StatusOr<int64_t> S3RandomAccessFile::seek(int64_t offset, int whence) {
    if (whence == SEEK_CUR) {
        _offset = _offset + offset;
    } else if (whence == SEEK_SET) {
        _offset = offset;
    } else if (whence == SEEK_END) {
        ASSIGN_OR_RETURN(auto length, S3RandomAccessFile::get_size());
        _offset = length + offset;
    } else {
        return Status::InvalidArgument("Invalid `whence` passed to seek");
    }
    return _offset;
}

StatusOr<int64_t> S3RandomAccessFile::position() {
    return _offset;
}

StatusOr<int64_t> S3RandomAccessFile::get_size() {
    if (_size == -1) {
        Aws::S3::Model::HeadObjectRequest request;
        request.SetBucket(_bucket);
        request.SetKey(_object);
        Aws::S3::Model::HeadObjectOutcome outcome = _s3client->HeadObject(request);
        if (outcome.IsSuccess()) {
            _size = outcome.GetResult().GetContentLength();
        } else {
            return Status::IOError(outcome.GetError().GetMessage());
        }
    }
    return _size;
}

} // namespace starrocks::io

#endif
