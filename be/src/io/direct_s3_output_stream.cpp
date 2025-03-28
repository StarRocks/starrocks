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

#include "io/direct_s3_output_stream.h"

#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <fmt/format.h>

#include "common/logging.h"
#include "io/io_profiler.h"
#include "util/failpoint/fail_point.h"
#include "util/stopwatch.hpp"

namespace starrocks::io {

class StringViewStream : Aws::Utils::Stream::PreallocatedStreamBuf, public std::iostream {
public:
    StringViewStream(const void* data, int64_t nbytes)
            : Aws::Utils::Stream::PreallocatedStreamBuf(reinterpret_cast<unsigned char*>(const_cast<void*>(data)),
                                                        static_cast<size_t>(nbytes)),
              std::iostream(this) {}
};

DirectS3OutputStream::DirectS3OutputStream(std::shared_ptr<Aws::S3::S3Client> client, std::string bucket,
                                           std::string object)
        : _client(std::move(client)), _bucket(std::move(bucket)), _object(std::move(object)) {
    DCHECK(_client != nullptr);
}

Status DirectS3OutputStream::write(const void* data, int64_t size) {
    if (_upload_id.empty()) {
        RETURN_IF_ERROR(create_multipart_upload());
        DCHECK(!_upload_id.empty());
    }

    if (size == 0) {
        return Status::OK();
    }

    MonotonicStopWatch watch;
    watch.start();

    Aws::S3::Model::UploadPartRequest req;
    req.SetBucket(_bucket);
    req.SetKey(_object);
    req.SetPartNumber(static_cast<int>(_etags.size() + 1));
    req.SetUploadId(_upload_id);
    req.SetContentLength(size);
    req.SetBody(std::make_shared<StringViewStream>(data, size));
    FAIL_POINT_TRIGGER_RETURN(output_stream_io_error, Status::IOError("injected output_stream_io_error"));
    auto outcome = _client->UploadPart(req);
    if (!outcome.IsSuccess()) {
        return Status::IOError(
                fmt::format("S3: Fail to upload part of {}/{}: {}", _bucket, _object, outcome.GetError().GetMessage()));
    }

    _etags.push_back(outcome.GetResult().GetETag());
    IOProfiler::add_write(size, watch.elapsed_time());
    return Status::OK();
}

Status DirectS3OutputStream::close() {
    if (_client == nullptr) {
        return Status::OK();
    }

    if (!_upload_id.empty() && !_etags.empty()) {
        MonotonicStopWatch watch;
        watch.start();
        RETURN_IF_ERROR(complete_multipart_upload());
        IOProfiler::add_sync(watch.elapsed_time());
    }

    _client = nullptr;
    return Status::OK();
}

Status DirectS3OutputStream::create_multipart_upload() {
    Aws::S3::Model::CreateMultipartUploadRequest req;
    req.SetBucket(_bucket);
    req.SetKey(_object);
    FAIL_POINT_TRIGGER_RETURN(output_stream_io_error, Status::IOError("injected output_stream_io_error"));
    Aws::S3::Model::CreateMultipartUploadOutcome outcome = _client->CreateMultipartUpload(req);
    if (outcome.IsSuccess()) {
        _upload_id = outcome.GetResult().GetUploadId();
        return Status::OK();
    }
    return Status::IOError(fmt::format("S3: Fail to create multipart upload for object {}/{}: {}", _bucket, _object,
                                       outcome.GetError().GetMessage()));
}

Status DirectS3OutputStream::complete_multipart_upload() {
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
    FAIL_POINT_TRIGGER_RETURN(output_stream_io_error, Status::IOError("injected output_stream_io_error"));
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
