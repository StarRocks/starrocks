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

#include "io/s3_output_stream.h"

#include <aws/s3/S3Client.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <fmt/format.h>

#include "common/logging.h"

namespace starrocks::io {

S3OutputStream::S3OutputStream(std::shared_ptr<Aws::S3::S3Client> client, std::string bucket, std::string object,
                               int64_t max_single_part_size, int64_t min_upload_part_size)
        : _client(std::move(client)),
          _bucket(std::move(bucket)),
          _object(std::move(object)),
          _max_single_part_size(max_single_part_size),
          _min_upload_part_size(min_upload_part_size),
          _buffer(),
          _upload_id(),
          _etags() {
    CHECK(_client != nullptr);
}

Status S3OutputStream::write(const void* data, int64_t size) {
    _buffer.append(static_cast<const char*>(data), size);
    if (_upload_id.empty() && _buffer.size() > _max_single_part_size) {
        RETURN_IF_ERROR(create_multipart_upload());
        DCHECK(!_upload_id.empty());
    }
    if (!_upload_id.empty() && _buffer.size() >= _min_upload_part_size) {
        RETURN_IF_ERROR(multipart_upload());
        _buffer.clear();
    }
    return Status::OK();
}

Status S3OutputStream::write_aliased(const void* data, int64_t size) {
    return Status::NotSupported("S3OutputStream::write_aliased");
}

Status S3OutputStream::skip(int64_t count) {
    _buffer.resize(_buffer.size() + count);
    // Calling `write()` will trigger the uploading checks.
    return S3OutputStream::write("", 0);
}

StatusOr<OutputStream::Buffer> S3OutputStream::get_direct_buffer() {
    return Buffer();
}

StatusOr<OutputStream::Position> S3OutputStream::get_direct_buffer_and_advance(int64_t size) {
    auto old_size = _buffer.size();
    _buffer.resize(old_size + size);
    return reinterpret_cast<uint8_t*>(_buffer.data()) + old_size;
}

Status S3OutputStream::close() {
    if (_client == nullptr) {
        return Status::OK();
    }

    if (_upload_id.empty()) {
        RETURN_IF_ERROR(singlepart_upload());
    } else {
        RETURN_IF_ERROR(multipart_upload());
        RETURN_IF_ERROR(complete_multipart_upload());
    }
    _client = nullptr;
    return Status::OK();
}

Status S3OutputStream::create_multipart_upload() {
    Aws::S3::Model::CreateMultipartUploadRequest req;
    req.SetBucket(_bucket);
    req.SetKey(_object);
    Aws::S3::Model::CreateMultipartUploadOutcome outcome = _client->CreateMultipartUpload(req);
    if (outcome.IsSuccess()) {
        _upload_id = outcome.GetResult().GetUploadId();
        return Status::OK();
    }
    return Status::IOError(fmt::format("S3: Fail to create multipart upload for object {}/{}: {}", _bucket, _object,
                                       outcome.GetError().GetMessage()));
}

Status S3OutputStream::singlepart_upload() {
    VLOG(12) << "Uploading s3://" << _bucket << "/" << _object << " via singlepart upload";
    DCHECK(_upload_id.empty());
    Aws::S3::Model::PutObjectRequest req;
    req.SetBucket(_bucket);
    req.SetKey(_object);
    req.SetContentLength(static_cast<int64_t>(_buffer.size()));
    req.SetBody(std::make_shared<Aws::StringStream>(_buffer));
    Aws::S3::Model::PutObjectOutcome outcome = _client->PutObject(req);
    if (!outcome.IsSuccess()) {
        std::string error_msg =
                fmt::format("S3: Fail to put object {}/{}, msg: {}", _bucket, _object, outcome.GetError().GetMessage());
        LOG(WARNING) << error_msg;
        return Status::IOError(error_msg);
    }
    return Status::OK();
}

Status S3OutputStream::multipart_upload() {
    VLOG(12) << "Uploading s3://" << _bucket << "/" << _object << " via multipart upload";
    if (_buffer.empty()) {
        return Status::OK();
    }
    Aws::S3::Model::UploadPartRequest req;
    req.SetBucket(_bucket);
    req.SetKey(_object);
    req.SetPartNumber(static_cast<int>(_etags.size() + 1));
    req.SetUploadId(_upload_id);
    req.SetContentLength(static_cast<int64_t>(_buffer.size()));
    req.SetBody(std::make_shared<Aws::StringStream>(_buffer));
    auto outcome = _client->UploadPart(req);
    if (outcome.IsSuccess()) {
        _etags.push_back(outcome.GetResult().GetETag());
        return Status::OK();
    }
    return Status::IOError(
            fmt::format("S3: Fail to upload part of {}/{}: {}", _bucket, _object, outcome.GetError().GetMessage()));
}

Status S3OutputStream::complete_multipart_upload() {
    VLOG(12) << "Completing multipart upload s3://" << _bucket << "/" << _object;
    DCHECK(!_upload_id.empty());
    DCHECK(!_etags.empty());
    if (UNLIKELY(_etags.size() > std::numeric_limits<int>::max())) {
        return Status::NotSupported("Too many S3 upload parts");
    }
    Aws::S3::Model::CompleteMultipartUploadRequest req;
    req.SetBucket(_bucket);
    req.SetKey(_object);
    req.SetUploadId(_upload_id);
    Aws::S3::Model::CompletedMultipartUpload multipart_upload;
    for (int i = 0, sz = static_cast<int>(_etags.size()); i < sz; ++i) {
        Aws::S3::Model::CompletedPart part;
        multipart_upload.AddParts(part.WithETag(_etags[i]).WithPartNumber(i + 1));
    }
    req.SetMultipartUpload(multipart_upload);
    auto outcome = _client->CompleteMultipartUpload(req);
    if (outcome.IsSuccess()) {
        return Status::OK();
    }
    std::string error_msg = fmt::format("S3: Fail to complete multipart upload for object {}/{}, msg: {}", _bucket,
                                        _object, outcome.GetError().GetMessage());
    LOG(WARNING) << error_msg;
    return Status::IOError(error_msg);
}

} // namespace starrocks::io
