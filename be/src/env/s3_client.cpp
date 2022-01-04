// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "env/s3_client.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/s3/model/BucketLocationConstraint.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/transfer/TransferHandle.h>

#include <fstream>

#include "common/logging.h"
#include "gutil/strings/substitute.h"

namespace starrocks {

static inline Aws::String to_aws_string(const std::string& s) {
    return Aws::String(s.data(), s.size());
}

static inline Aws::String to_string(const Aws::String& s) {
    return std::string(s.data(), s.size());
}

// If the first char of the string is the specified character, then return a
// // string that has the first character removed.
static inline std::string ltrim_if(const std::string& s, char c) {
    if (s.length() > 0 && s[0] == c) {
        return s.substr(1);
    }
    return s;
}

// If s doesn't end with '/', it appends it.
// Special case: if s is empty, we don't append '/'
static inline std::string ensure_ends_with_pathsep(std::string s) {
    if (!s.empty() && s.back() != '/') {
        s += '/';
    }
    return s;
}

S3Client::S3Client(const S3ClientConfiguration& config) : _config(config) {
    const char* access_key_id =
            config.access_key_id.empty() ? getenv("AWS_ACCESS_KEY_ID") : config.access_key_id.c_str();
    const char* secret_access_key =
            config.secret_access_key.empty() ? getenv("AWS_SECRET_ACCESS_KEY") : config.secret_access_key.c_str();
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials =
            std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(access_key_id, secret_access_key);

    Aws::Client::ClientConfiguration conf;
    conf.region = config.region;
    conf.maxConnections = config.max_connections;
    conf.httpRequestTimeoutMs = config.http_request_timeout_ms;

    _client = std::make_shared<Aws::S3::S3Client>(credentials, conf);
    if (config.use_transfer_manager) {
        Aws::Transfer::TransferManagerConfiguration transfer_config(_get_transfer_manager_executor());
        transfer_config.s3Client = _client;
        transfer_config.transferBufferMaxHeapSize = _transfer_manager_max_buffer_size;
        transfer_config.bufferSize = _transfer_manager_single_buffer_size;
        _transfer_manager = Aws::Transfer::TransferManager::Create(transfer_config);
    } else {
        _transfer_manager = nullptr;
    }
}

S3Client::~S3Client() {}

Status S3Client::create_bucket(const std::string& bucket_name) {
    Aws::S3::Model::BucketLocationConstraint constraint =
            Aws::S3::Model::BucketLocationConstraintMapper::GetBucketLocationConstraintForName(_config.region);
    Aws::S3::Model::CreateBucketConfiguration bucket_config;
    bucket_config.SetLocationConstraint(constraint);

    Aws::S3::Model::CreateBucketRequest request;
    request.SetCreateBucketConfiguration(bucket_config);
    request.SetBucket(to_aws_string(bucket_name));

    Aws::S3::Model::CreateBucketOutcome outcome = _client->CreateBucket(request);

    if (outcome.IsSuccess()) {
        return Status::OK();
    } else {
        std::string error =
                strings::Substitute("Create Bucket $0 failed. $1.", bucket_name, outcome.GetError().GetMessage());
        LOG(ERROR) << error;
        return Status::IOError(error);
    }
}

Status S3Client::delete_bucket(const std::string& bucket_name) {
    Aws::S3::Model::DeleteBucketRequest request;
    request.SetBucket(to_aws_string(bucket_name));

    Aws::S3::Model::DeleteBucketOutcome outcome = _client->DeleteBucket(request);

    if (outcome.IsSuccess()) {
        return Status::OK();
    } else {
        std::string error =
                strings::Substitute("Delete Bucket $0 failed. $1.", bucket_name, outcome.GetError().GetMessage());
        LOG(ERROR) << error;
        return Status::IOError(error);
    }
}

Status S3Client::put_object(const std::string& bucket_name, const std::string& object_key,
                            const std::string& object_path) {
    if (_transfer_manager) {
        auto handle = _transfer_manager->UploadFile(to_aws_string(object_path), to_aws_string(bucket_name),
                                                    to_aws_string(object_key), Aws::DEFAULT_CONTENT_TYPE,
                                                    Aws::Map<Aws::String, Aws::String>());
        handle->WaitUntilFinished();
        if (handle->GetStatus() != Aws::Transfer::TransferStatus::COMPLETED) {
            // TODO: log error
            return Status::IOError(strings::Substitute("Put Object $0 failed.", object_key));
        } else {
            return Status::OK();
        }
    } else {
        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(to_aws_string(bucket_name));
        request.SetKey(to_aws_string(object_key));
        std::shared_ptr<Aws::IOStream> stream = Aws::MakeShared<Aws::FStream>(
                Aws::Utils::ARRAY_ALLOCATION_TAG, object_path.c_str(), std::ios_base::in | std::ios_base::binary);
        if (!stream->good()) {
            std::string error =
                    strings::Substitute("Put Object $0 failed, fail to open local file $1.", object_key, object_path);
            LOG(ERROR) << error;
            return Status::IOError(error);
        }
        request.SetBody(stream);
        Aws::S3::Model::PutObjectOutcome outcome = _client->PutObject(request);

        if (outcome.IsSuccess()) {
            return Status::OK();
        } else {
            std::string error =
                    strings::Substitute("Put Object $0 failed. $1.", object_key, outcome.GetError().GetMessage());
            LOG(ERROR) << error;
            return Status::IOError(error);
        }
    }
}

Status S3Client::put_string_object(const std::string& bucket_name, const std::string& object_key,
                                   const std::string& object_value) {
    std::shared_ptr<Aws::IOStream> stream = Aws::MakeShared<Aws::StringStream>("", object_value);

    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(to_aws_string(bucket_name));
    request.SetKey(to_aws_string(object_key));
    request.SetBody(stream);

    Aws::S3::Model::PutObjectOutcome outcome = _client->PutObject(request);

    if (outcome.IsSuccess()) {
        return Status::OK();
    } else {
        std::string error =
                strings::Substitute("Put Object $0 failed. $1.", object_key, outcome.GetError().GetMessage());
        LOG(ERROR) << error;
        return Status::IOError(error);
    }
}

Status S3Client::get_object_range(const std::string& bucket_name, const std::string& object_key,
                                  std::string* object_value, size_t offset, size_t length, size_t* read_bytes) {
    object_value->resize(length);
    return get_object_range(bucket_name, object_key, (char*)object_value->data(), offset, length, read_bytes);
}

Status S3Client::get_object_range(const std::string& bucket_name, const std::string& object_key, char* object_value,
                                  size_t offset, size_t length, size_t* read_bytes) {
    *read_bytes = 0;
    length = (length ? length : 1);
    char buffer[128];
    int ret = snprintf(buffer, sizeof(buffer), "bytes=%lu-%lu", offset, offset + length - 1);
    if (ret < 0) {
        std::string error = strings::Substitute("Get Object Range $0 failed, fail to set range.", object_key);
        LOG(ERROR) << error;
        return Status::IOError(error);
    }
    Aws::String range(buffer);

    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(to_aws_string(bucket_name));
    request.SetKey(to_aws_string(object_key));
    request.SetRange(range);

    Aws::S3::Model::GetObjectOutcome outcome = _client->GetObject(request);

    if (outcome.IsSuccess()) {
        if (object_value) {
            Aws::IOStream& body = outcome.GetResult().GetBody();
            body.read(object_value, length);
            *read_bytes = body.gcount();
            if (body.gcount() != length) {
                std::string error = strings::Substitute("Get Object Range $0 failed. expected($1), read($2).",
                                                        object_key, length, *read_bytes);
                LOG(ERROR) << error;
                return Status::IOError(error);
            }
        }
        return Status::OK();
    } else {
        std::string error =
                strings::Substitute("Get Object Range $0 failed. $1.", object_key, outcome.GetError().GetMessage());
        LOG(ERROR) << error;
        return Status::IOError(error);
    }
}

Status S3Client::get_object(const std::string& bucket_name, const std::string& object_key,
                            const std::string& object_path) {
    if (object_path.empty()) {
        return Status::IOError(strings::Substitute("Get Object $0 failed, path empty.", object_key));
    }
    if (_transfer_manager) {
        Aws::Transfer::CreateDownloadStreamCallback stream;
        if (!object_path.empty()) {
            stream = [=]() -> Aws::IOStream* {
                return Aws::New<Aws::FStream>(Aws::Utils::ARRAY_ALLOCATION_TAG, object_path,
                                              std::ios_base::out | std::ios_base::trunc);
            };
        } else {
            stream = [=]() -> Aws::IOStream* { return Aws::New<Aws::StringStream>(""); };
        }
        auto handle = _transfer_manager->DownloadFile(to_aws_string(bucket_name), to_aws_string(object_key),
                                                      std::move(stream));
        handle->WaitUntilFinished();
        if (handle->GetStatus() != Aws::Transfer::TransferStatus::COMPLETED) {
            // TODO: log error
            return Status::IOError(strings::Substitute("Get Object $0 failed.", object_key));
        } else {
            return Status::OK();
        }
    } else {
        Aws::S3::Model::GetObjectRequest request;
        request.SetBucket(to_aws_string(bucket_name));
        request.SetKey(to_aws_string(object_key));

        if (!object_path.empty()) {
            auto stream = [=]() -> Aws::IOStream* {
                return Aws::New<Aws::FStream>(Aws::Utils::ARRAY_ALLOCATION_TAG, object_path,
                                              std::ios_base::out | std::ios_base::trunc);
            };
            request.SetResponseStreamFactory(std::move(stream));
        }

        Aws::S3::Model::GetObjectOutcome outcome = _client->GetObject(request);

        if (outcome.IsSuccess()) {
            return Status::OK();
        } else {
            std::string error =
                    strings::Substitute("Get Object $0 failed. $1.", object_key, outcome.GetError().GetMessage());
            LOG(ERROR) << error;
            return Status::IOError(error);
        }
    }
}

Status S3Client::_head_object(const std::string& bucket_name, const std::string& object_key, size_t* size) {
    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(to_aws_string(bucket_name));
    request.SetKey(to_aws_string(object_key));

    Aws::S3::Model::HeadObjectOutcome outcome = _client->HeadObject(request);
    if (!outcome.IsSuccess()) {
        if (_is_not_found(outcome.GetError().GetErrorType())) {
            return Status::NotFound(strings::Substitute("Object $0 not found.", object_key));
        } else {
            std::string error =
                    strings::Substitute("Head Object $0 failed. $1.", object_key, outcome.GetError().GetMessage());
            LOG(ERROR) << error;
            return Status::IOError(error);
        }
    } else {
        if (size != nullptr) {
            *size = outcome.GetResult().GetContentLength();
        }
        return Status::OK();
    }
}

Status S3Client::exist_object(const std::string& bucket_name, const std::string& object_key) {
    return _head_object(bucket_name, object_key, nullptr /* size */);
}

Status S3Client::get_object_size(const std::string& bucket_name, const std::string& object_key, size_t* size) {
    return _head_object(bucket_name, object_key, size);
}

Status S3Client::delete_object(const std::string& bucket_name, const std::string& object_key) {
    Aws::S3::Model::DeleteObjectRequest request;
    request.SetBucket(to_aws_string(bucket_name));
    request.SetKey(to_aws_string(object_key));

    Aws::S3::Model::DeleteObjectOutcome outcome = _client->DeleteObject(request);

    if (outcome.IsSuccess()) {
        return Status::OK();
    } else {
        std::string error =
                strings::Substitute("Delete Object $0 failed. $1.", object_key, outcome.GetError().GetMessage());
        LOG(ERROR) << error;
        return Status::IOError(error);
    }
}

Status S3Client::list_objects(const std::string& bucket_name, const std::string& object_prefix,
                              std::vector<std::string>* result) {
    result->clear();

    // S3 paths don't start with '/'
    std::string prefix = ltrim_if(object_prefix, '/');
    // S3 paths better end with '/', otherwise we might also get a list of files
    // in a directory for which our path is a prefix
    prefix = ensure_ends_with_pathsep(std::move(prefix));
    // the starting object marker
    Aws::String marker;

    // get info of bucket+object
    while (1) {
        Aws::S3::Model::ListObjectsRequest request;
        request.SetBucket(to_aws_string(bucket_name));
        request.SetMaxKeys(1000); // TODO: make it configurable
        request.SetPrefix(to_aws_string(prefix));
        request.SetMarker(marker);

        Aws::S3::Model::ListObjectsOutcome outcome = _client->ListObjects(request);
        if (!outcome.IsSuccess()) {
            if (_is_not_found(outcome.GetError().GetErrorType())) {
                return Status::OK();
            }
            std::string error = strings::Substitute("List Objects prefix $0 failed. $1.", object_prefix,
                                                    outcome.GetError().GetMessage());
            LOG(ERROR) << error;
            return Status::IOError(error);
        }
        const Aws::S3::Model::ListObjectsResult& res = outcome.GetResult();
        const Aws::Vector<Aws::S3::Model::Object>& objs = res.GetContents();
        for (auto o : objs) {
            const Aws::String& key = o.GetKey();
            // Our path should be a prefix of the fetched value
            std::string keystr(key.c_str(), key.size());
            if (keystr.find(prefix) != 0) {
                return Status::IOError(strings::Substitute("List Objects prefix $0 not match.", object_prefix));
            }
            const std::string fname = keystr.substr(prefix.size());
            result->push_back(fname);
        }

        // If there are no more entries, then we are done.
        if (!res.GetIsTruncated()) {
            break;
        }
        // The new starting point
        marker = res.GetNextMarker();
        if (marker.empty()) {
            // If response does not include the NextMaker and it is
            // truncated, you can use the value of the last Key in the response
            // as the marker in the subsequent request because all objects
            // are returned in alphabetical order
            marker = objs.back().GetKey();
        }
    }
    return Status::OK();
}

} // namespace starrocks
