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

#include "io/s3_input_stream.h"

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <fmt/format.h>

#ifdef USE_STAROS
#include "fslib/metric_key.h"
#include "metrics/metrics.h"
#endif

namespace starrocks::io {

inline Status make_error_status(const Aws::S3::S3Error& error) {
    return Status::IOError(fmt::format(
            "BE access S3 file failed, SdkResponseCode={}, SdkErrorType={}, SdkErrorMessage={}",
            static_cast<int>(error.GetResponseCode()), static_cast<int>(error.GetErrorType()), error.GetMessage()));
}

StatusOr<int64_t> S3InputStream::read(void* out, int64_t count) {
    if (UNLIKELY(_size == -1)) {
        ASSIGN_OR_RETURN(_size, S3InputStream::get_size());
    }
    if (_offset >= _size) {
        return 0;
    }

    auto range = fmt::format("bytes={}-{}", _offset, std::min<int64_t>(_offset + count, _size));
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(_bucket);
    request.SetKey(_object);
    request.SetRange(std::move(range));

    Aws::S3::Model::GetObjectOutcome outcome = _s3client->GetObject(request);
    if (outcome.IsSuccess()) {
        Aws::IOStream& body = outcome.GetResult().GetBody();
        body.read(static_cast<char*>(out), count);
        _offset += body.gcount();
        return body.gcount();
    } else {
        return make_error_status(outcome.GetError());
    }
}

Status S3InputStream::seek(int64_t offset) {
    if (offset < 0) return Status::InvalidArgument(fmt::format("Invalid offset {}", offset));
    _offset = offset;
    return Status::OK();
}

StatusOr<int64_t> S3InputStream::position() {
    return _offset;
}

StatusOr<int64_t> S3InputStream::get_size() {
    if (_size == -1) {
        Aws::S3::Model::HeadObjectRequest request;
        request.SetBucket(_bucket);
        request.SetKey(_object);
        Aws::S3::Model::HeadObjectOutcome outcome = _s3client->HeadObject(request);
        if (outcome.IsSuccess()) {
            _size = outcome.GetResult().GetContentLength();
        } else {
            return make_error_status(outcome.GetError());
        }
    }
    return _size;
}

void S3InputStream::set_size(int64_t value) {
    _size = value;
}

StatusOr<std::string> S3InputStream::read_all() {
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(_bucket);
    request.SetKey(_object);
    Aws::S3::Model::GetObjectOutcome outcome = _s3client->GetObject(request);
    if (outcome.IsSuccess()) {
        Aws::IOStream& body = outcome.GetResult().GetBody();
        return std::string(std::istreambuf_iterator<char>(body), {});
    } else {
        return make_error_status(outcome.GetError());
    }
}

} // namespace starrocks::io
