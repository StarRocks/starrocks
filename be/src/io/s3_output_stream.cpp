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
#include "runtime/exec_env.h"

namespace starrocks::io {

S3OutputStream::S3OutputStream(std::shared_ptr<Aws::S3::S3Client> client, std::string bucket, std::string object,
                               int64_t max_single_part_size, int64_t min_upload_part_size, bool background_write)
        : _client(std::move(client)),
          _bucket(std::move(bucket)),
          _object(std::move(object)),
          _max_single_part_size(max_single_part_size),
          _min_upload_part_size(min_upload_part_size),
          _buffer(),
          _upload_id(),
          _background_write(background_write),
          _etags() {
    _io_executor = ExecEnv::GetInstance()->pipeline_sink_io_pool();
    CHECK(_client != nullptr);
    CHECK(_io_executor != nullptr);
}

Status S3OutputStream::write(const void* data, int64_t size) {
    if (size == 0) {
        return Status::OK();
    }

    if (_upload_id.empty()) {
        RETURN_IF_ERROR(create_multipart_upload());
    }

    if (_buffer.size() > 0) {
        auto old_size = _buffer.size();
        _buffer.resize(old_size + size);
        memcpy(_buffer.data() + old_size, data, size);
    } else {
        // TODO: avoid copy if ownership can be acquired
        _buffer.resize(size);
        memcpy(_buffer.data(), data, size);
    }

    if (_buffer.size() > _min_upload_part_size) {
        RETURN_IF_ERROR(upload_part(std::move(_buffer)));
    }

    return Status::OK();
}

Status S3OutputStream::write_aliased(const void* data, int64_t size) {
    return Status::NotSupported("S3OutputStream::write_aliased");
}

Status S3OutputStream::skip(int64_t count) {
    return Status::NotSupported("S3OutputStream::skip");
}

StatusOr<OutputStream::Buffer> S3OutputStream::get_direct_buffer() {
    return Status::NotSupported("S3OutputStream::get_direct_buffer");
}

StatusOr<OutputStream::Position> S3OutputStream::get_direct_buffer_and_advance(int64_t size) {
    return Status::NotSupported("S3OutputStream::get_direct_buffer_and_advance");
}

Status S3OutputStream::close() {
    if (_client == nullptr) {
        return Status::OK();
    }

    if (_upload_id.empty()) {
        return Status::OK();
    }

    // upload remaining bytes if exists
    if (_buffer.size() > 0) {
        RETURN_IF_ERROR(upload_part(std::move(_buffer)));
    }

    if (_io_status.ok()) {
        RETURN_IF_ERROR(complete_multipart_upload());
    } else {
        return _io_status;
    }

    _client = nullptr;
    return Status::OK();
}

Status S3OutputStream::create_multipart_upload() {
    LOG(INFO) << "create s3://" << _bucket << "/" << _object << " multipart upload";
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

Status S3OutputStream::upload_part(std::vector<uint8_t> buffer) {
    LOG(INFO) << "Uploading s3://" << _bucket << "/" << _object << " via multipart upload"
              << " part = " << _part_id - 1;
    Aws::S3::Model::UploadPartRequest req;
    req.SetBucket(_bucket);
    req.SetKey(_object);
    req.SetPartNumber(_part_id++);
    req.SetUploadId(_upload_id);
    req.SetContentLength(static_cast<int64_t>(buffer.size()));
    req.SetBody(std::make_shared<StringViewStream>(buffer.data(), buffer.size()));

    if (!_background_write) {
        auto outcome = _client->UploadPart(req);
        if (outcome.IsSuccess()) {
            LOG(INFO) << "Uploaded s3://" << _bucket << "/" << _object << " via multipart upload"
                      << " part = " << req.GetPartNumber() << " length = " << req.GetContentLength();
            size_t slot = req.GetPartNumber() - 1;
            if (slot >= _etags.size()) {
                _etags.resize(slot + 1);
            }
            _etags[slot] = outcome.GetResult().GetETag();
            ++_completed_parts;
            return Status::OK();
        } else {
            LOG(ERROR) << outcome.GetError();
            _io_status.update(Status::IOError(outcome.GetError().GetMessage()));
            return _io_status;
        }
    }

    bool ok = _io_executor->try_offer([&, req = std::move(req), buffer = std::move(buffer)] {
        auto outcome = _client->UploadPart(req);
        std::unique_lock<std::mutex> lock(_mutex);
        if (outcome.IsSuccess()) {
            LOG(INFO) << "Uploaded s3://" << _bucket << "/" << _object << " via multipart upload"
                      << " part = " << req.GetPartNumber() << " length = " << req.GetContentLength();
            size_t slot = req.GetPartNumber() - 1;
            if (slot >= _etags.size()) {
                _etags.resize(slot + 1);
            }
            _etags[slot] = outcome.GetResult().GetETag();
            ++_completed_parts;
        } else {
            LOG(ERROR) << outcome.GetError();
            _io_status.update(Status::IOError(outcome.GetError().GetMessage()));
        }
        _cv.notify_all();
    });

    if (!ok) {
        return Status::ResourceBusy("submit io task failed");
    }
    return Status::OK();
}

Status S3OutputStream::complete_multipart_upload() {
    LOG(INFO) << "Completing multipart upload s3://" << _bucket << "/" << _object;

    std::unique_lock<std::mutex> lock(_mutex);
    _cv.wait(lock, [&] { return _part_id - 1 == _completed_parts; });

    LOG(INFO) << "Parts all upload s3://" << _bucket << "/" << _object;

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
        LOG(INFO) << "Completed multipart upload s3://" << _bucket << "/" << _object;
        return Status::OK();
    }
    std::string error_msg = fmt::format("S3: Fail to complete multipart upload for object {}/{}, msg: {}", _bucket,
                                        _object, outcome.GetError().GetMessage());
    LOG(WARNING) << error_msg;
    return Status::IOError(error_msg);
}

} // namespace starrocks::io
