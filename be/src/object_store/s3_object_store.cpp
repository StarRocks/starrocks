// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#ifdef STARROCKS_WITH_AWS

#include "object_store/s3_object_store.h"

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

#include "common/config.h"
#include "common/logging.h"
#include "gutil/strings/strip.h"
#include "gutil/strings/substitute.h"
#include "io/s3_output_stream.h"
#include "io/s3_random_access_file.h"

namespace starrocks {

static inline Aws::String to_aws_string(const std::string& s) {
    return Aws::String(s.data(), s.size());
}

S3ObjectStore::S3ObjectStore(const Aws::Client::ClientConfiguration& config) : _config(config) {}

Status S3ObjectStore::init(bool use_transfer_manager) {
    const char* env_access_key_id = getenv("AWS_ACCESS_KEY_ID");
    const char* env_secret_access_key = getenv("AWS_SECRET_ACCESS_KEY");
    const string be_conf_access_key_id = config::object_storage_access_key_id;
    const string be_conf_secret_access_key = config::object_storage_secret_access_key;
    if (!be_conf_access_key_id.empty() && !be_conf_secret_access_key.empty()) {
        std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials =
                std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(be_conf_access_key_id, be_conf_secret_access_key);
        _client = std::make_shared<Aws::S3::S3Client>(credentials, _config);
    } else if (strlen(env_access_key_id) > 0 && strlen(env_secret_access_key) > 0) {
        std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials =
                std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(env_secret_access_key, env_secret_access_key);
        _client = std::make_shared<Aws::S3::S3Client>(credentials, _config);
    } else {
        // if not cred provided, we can use default cred in aws profile.
        _client = std::make_shared<Aws::S3::S3Client>(_config);
    }

    if (use_transfer_manager) {
        Aws::Transfer::TransferManagerConfiguration transfer_config(_get_transfer_manager_executor());
        transfer_config.s3Client = _client;
        transfer_config.transferBufferMaxHeapSize = kTransferManagerMaxBufferSize;
        transfer_config.bufferSize = kTransferManagerSingleBufferSize;
        _transfer_manager = Aws::Transfer::TransferManager::Create(transfer_config);
    } else {
        _transfer_manager = nullptr;
    }
    return Status::OK();
}

Status S3ObjectStore::create_bucket(const std::string& bucket) {
    Aws::S3::Model::BucketLocationConstraint constraint =
            Aws::S3::Model::BucketLocationConstraintMapper::GetBucketLocationConstraintForName(_config.region);
    Aws::S3::Model::CreateBucketConfiguration bucket_config;
    bucket_config.SetLocationConstraint(constraint);

    Aws::S3::Model::CreateBucketRequest request;
    request.SetCreateBucketConfiguration(bucket_config);
    request.SetBucket(to_aws_string(bucket));

    Aws::S3::Model::CreateBucketOutcome outcome = _client->CreateBucket(request);

    if (outcome.IsSuccess()) {
        return Status::OK();
    } else {
        std::string error =
                strings::Substitute("Create Bucket $0 failed. $1.", bucket, outcome.GetError().GetMessage());
        LOG(ERROR) << error;
        return Status::IOError(error);
    }
}

Status S3ObjectStore::delete_bucket(const std::string& bucket) {
    Aws::S3::Model::DeleteBucketRequest request;
    request.SetBucket(to_aws_string(bucket));

    Aws::S3::Model::DeleteBucketOutcome outcome = _client->DeleteBucket(request);

    if (outcome.IsSuccess()) {
        return Status::OK();
    } else {
        std::string error =
                strings::Substitute("Delete Bucket $0 failed. $1.", bucket, outcome.GetError().GetMessage());
        LOG(ERROR) << error;
        return Status::IOError(error);
    }
}

Status S3ObjectStore::put_object(const std::string& bucket, const std::string& object, const std::string& object_path) {
    if (_transfer_manager) {
        auto handle =
                _transfer_manager->UploadFile(to_aws_string(object_path), to_aws_string(bucket), to_aws_string(object),
                                              Aws::DEFAULT_CONTENT_TYPE, Aws::Map<Aws::String, Aws::String>());
        handle->WaitUntilFinished();
        if (handle->GetStatus() != Aws::Transfer::TransferStatus::COMPLETED) {
            // TODO: log error
            return Status::IOError(strings::Substitute("Put Object $0 failed.", object));
        } else {
            return Status::OK();
        }
    } else {
        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(to_aws_string(bucket));
        request.SetKey(to_aws_string(object));
        std::shared_ptr<Aws::IOStream> stream = Aws::MakeShared<Aws::FStream>(
                Aws::Utils::ARRAY_ALLOCATION_TAG, object_path.c_str(), std::ios_base::in | std::ios_base::binary);
        if (!stream->good()) {
            std::string error =
                    strings::Substitute("Put Object $0 failed, fail to open local file $1.", object, object_path);
            LOG(ERROR) << error;
            return Status::IOError(error);
        }
        request.SetBody(stream);
        Aws::S3::Model::PutObjectOutcome outcome = _client->PutObject(request);

        if (outcome.IsSuccess()) {
            return Status::OK();
        } else {
            std::string error =
                    strings::Substitute("Put Object $0 failed. $1.", object, outcome.GetError().GetMessage());
            LOG(ERROR) << error;
            return Status::IOError(error);
        }
    }
}

StatusOr<std::unique_ptr<io::OutputStream>> S3ObjectStore::put_object(const std::string& bucket,
                                                                      const std::string& object) {
    return std::make_unique<io::S3OutputStream>(_client, bucket, object, config::experimental_s3_max_single_part_size,
                                                config::experimental_s3_min_upload_part_size);
}

StatusOr<std::unique_ptr<io::RandomAccessFile>> S3ObjectStore::get_object(const std::string& bucket,
                                                                          const std::string& object) {
    return std::make_unique<io::S3RandomAccessFile>(_client, bucket, object);
}

Status S3ObjectStore::get_object(const std::string& bucket, const std::string& object, const std::string& object_path) {
    if (object_path.empty()) {
        return Status::IOError(strings::Substitute("Get Object $0 failed, path empty.", object));
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
        auto handle = _transfer_manager->DownloadFile(to_aws_string(bucket), to_aws_string(object), std::move(stream));
        handle->WaitUntilFinished();
        if (handle->GetStatus() != Aws::Transfer::TransferStatus::COMPLETED) {
            // TODO: log error
            return Status::IOError(strings::Substitute("Get Object $0 failed.", object));
        } else {
            return Status::OK();
        }
    } else {
        Aws::S3::Model::GetObjectRequest request;
        request.SetBucket(to_aws_string(bucket));
        request.SetKey(to_aws_string(object));

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
                    strings::Substitute("Get Object $0 failed. $1.", object, outcome.GetError().GetMessage());
            LOG(ERROR) << error;
            return Status::IOError(error);
        }
    }
}

Status S3ObjectStore::_head_object(const std::string& bucket, const std::string& object, size_t* size) {
    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(to_aws_string(bucket));
    request.SetKey(to_aws_string(object));

    Aws::S3::Model::HeadObjectOutcome outcome = _client->HeadObject(request);
    if (!outcome.IsSuccess()) {
        if (_is_not_found(outcome.GetError().GetErrorType())) {
            return Status::NotFound(strings::Substitute("Object $0 not found.", object));
        } else {
            std::string error =
                    strings::Substitute("Head Object $0 failed. $1.", object, outcome.GetError().GetMessage());
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

Status S3ObjectStore::exist_object(const std::string& bucket, const std::string& object) {
    return _head_object(bucket, object, nullptr /* size */);
}

Status S3ObjectStore::get_object_size(const std::string& bucket, const std::string& object, size_t* size) {
    return _head_object(bucket, object, size);
}

Status S3ObjectStore::delete_object(const std::string& bucket, const std::string& object) {
    Aws::S3::Model::DeleteObjectRequest request;
    request.SetBucket(to_aws_string(bucket));
    request.SetKey(to_aws_string(object));

    Aws::S3::Model::DeleteObjectOutcome outcome = _client->DeleteObject(request);

    if (outcome.IsSuccess()) {
        return Status::OK();
    } else {
        std::string error =
                strings::Substitute("Delete Object $0 failed. $1.", object, outcome.GetError().GetMessage());
        LOG(ERROR) << error;
        return Status::IOError(error);
    }
}

Status S3ObjectStore::list_objects(const std::string& bucket, const std::string& object_prefix,
                                   std::vector<std::string>* result) {
    result->clear();
    // S3 paths don't start with '/'
    std::string prefix = StripPrefixString(object_prefix, "/");
    // S3 paths better end with '/', otherwise we might also get a list of files
    // in a directory for which our path is a prefix
    if (prefix.size() > 0 && prefix.back() != '/') {
        prefix.push_back('/');
    }
    // the starting object marker
    Aws::String marker;

    // get info of bucket+object
    while (1) {
        Aws::S3::Model::ListObjectsRequest request;
        request.SetBucket(to_aws_string(bucket));
        request.SetMaxKeys(kListObjectMaxKeys);
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

#endif
