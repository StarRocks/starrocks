// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#ifdef STARROCKS_WITH_AWS

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/BucketLocationConstraint.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/transfer/TransferHandle.h>
#include <aws/transfer/TransferManager.h>
#include <fmt/format.h>

#include <fstream>
#include <memory>

#include "common/config.h"
#include "common/logging.h"
#include "gutil/strings/strip.h"
#include "gutil/strings/substitute.h"
#include "io/s3_output_stream.h"
#include "io/s3_random_access_file.h"
#include "object_store/object_store.h"
#include "object_store/s3_uri.h"

namespace starrocks {

class S3ObjectStore final : public ObjectStore {
public:
    S3ObjectStore(std::shared_ptr<Aws::S3::S3Client> client, bool use_transfer_manager);
    ~S3ObjectStore() = default;

    Status create_bucket(const std::string& bucket) override;

    Status delete_bucket(const std::string& bucket) override;

    StatusOr<std::unique_ptr<io::OutputStream>> put_object(const std::string& bucket,
                                                           const std::string& object) override;

    StatusOr<std::unique_ptr<io::RandomAccessFile>> get_object(const std::string& bucket,
                                                               const std::string& object) override;

    Status upload_object(const std::string& bucket, const std::string& object, const std::string& local_path) override;

    Status download_object(const std::string& bucket, const std::string& object,
                           const std::string& local_path) override;

    Status exist_object(const std::string& bucket, const std::string& object) override;

    StatusOr<int64_t> get_object_size(const std::string& bucket, const std::string& object) override;

    Status delete_object(const std::string& bucket, const std::string& object) override;

    Status list_objects(const std::string& bucket, const std::string& object_prefix,
                        std::vector<std::string>* result) override;

    StatusOr<std::unique_ptr<io::OutputStream>> put_object(const std::string& uri) override;

    StatusOr<std::unique_ptr<io::RandomAccessFile>> get_object(const std::string& uri) override;

    StatusOr<int64_t> get_object_size(const std::string& uri) override;

    Status exist_object(const std::string& uri) override;

    Status delete_object(const std::string& uri) override;

    Status upload_object(const std::string& uri, const std::string& local_path) override;

    Status download_object(const std::string& uri, const std::string& local_path) override;

private:
    // transfer manager's thread pool.
    static const int kThreadPoolNumber = 16;
    // maximum size of the transfer manager's working buffer to use.
    static const int kTransferManagerMaxBufferSize = 512 * 1024 * 1024; // 256MB
    // maximum size that transfer manager will process in a single request.
    static const int kTransferManagerSingleBufferSize = 32 * 1024 * 1024; // 32MB
    // return how many keys each time call list_object.
    static const int kListObjectMaxKeys = 1000;

    static Aws::Utils::Threading::Executor* _get_transfer_manager_executor() {
        static Aws::Utils::Threading::PooledThreadExecutor executor(kThreadPoolNumber);
        return &executor;
    }

    Status head_object(const std::string& bucket, const std::string& object, size_t* size);

    bool is_not_found(const Aws::S3::S3Errors& err) {
        return (err == Aws::S3::S3Errors::NO_SUCH_BUCKET || err == Aws::S3::S3Errors::NO_SUCH_KEY ||
                err == Aws::S3::S3Errors::RESOURCE_NOT_FOUND);
    }

    std::shared_ptr<Aws::S3::S3Client> _client;
    std::shared_ptr<Aws::Transfer::TransferManager> _transfer_manager;
};

static inline Aws::String to_aws_string(const std::string& s) {
    return Aws::String(s.data(), s.size());
}

S3ObjectStore::S3ObjectStore(std::shared_ptr<Aws::S3::S3Client> client, bool use_transfer_manager)
        : _client(std::move(client)), _transfer_manager(nullptr) {
    if (use_transfer_manager) {
        Aws::Transfer::TransferManagerConfiguration transfer_config(_get_transfer_manager_executor());
        transfer_config.s3Client = _client;
        transfer_config.transferBufferMaxHeapSize = kTransferManagerMaxBufferSize;
        transfer_config.bufferSize = kTransferManagerSingleBufferSize;
        _transfer_manager = Aws::Transfer::TransferManager::Create(transfer_config);
    }
}

Status S3ObjectStore::create_bucket(const std::string& bucket) {
    auto constraint =
            Aws::S3::Model::BucketLocationConstraintMapper::GetBucketLocationConstraintForName(config::aws_s3_region);
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

Status S3ObjectStore::upload_object(const std::string& bucket, const std::string& object,
                                    const std::string& local_path) {
    if (_transfer_manager) {
        auto handle =
                _transfer_manager->UploadFile(to_aws_string(local_path), to_aws_string(bucket), to_aws_string(object),
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
                Aws::Utils::ARRAY_ALLOCATION_TAG, local_path.c_str(), std::ios_base::in | std::ios_base::binary);
        if (!stream->good()) {
            std::string error =
                    strings::Substitute("Put Object $0 failed, fail to open local file $1.", object, local_path);
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

Status S3ObjectStore::upload_object(const std::string& uri, const std::string& local_path) {
    S3URI parsed_uri(uri);
    if (!parsed_uri.parse()) {
        return Status::InvalidArgument(fmt::format("Invalid S3 URI {}", uri));
    }
    return upload_object(std::string(parsed_uri.bucket()), std::string(parsed_uri.key()), local_path);
}

StatusOr<std::unique_ptr<io::OutputStream>> S3ObjectStore::put_object(const std::string& bucket,
                                                                      const std::string& object) {
    return std::make_unique<io::S3OutputStream>(_client, bucket, object, config::experimental_s3_max_single_part_size,
                                                config::experimental_s3_min_upload_part_size);
}

StatusOr<std::unique_ptr<io::OutputStream>> S3ObjectStore::put_object(const std::string& uri) {
    S3URI parsed_uri(uri);
    if (!parsed_uri.parse()) {
        return Status::InvalidArgument(fmt::format("Invalid S3 URI {}", uri));
    }
    return std::make_unique<io::S3OutputStream>(
            _client, std::string(parsed_uri.bucket()), std::string(parsed_uri.key()),
            config::experimental_s3_max_single_part_size, config::experimental_s3_min_upload_part_size);
}

StatusOr<std::unique_ptr<io::RandomAccessFile>> S3ObjectStore::get_object(const std::string& bucket,
                                                                          const std::string& object) {
    return std::make_unique<io::S3RandomAccessFile>(_client, bucket, object);
}

StatusOr<std::unique_ptr<io::RandomAccessFile>> S3ObjectStore::get_object(const std::string& uri) {
    S3URI parsed_uri(uri);
    if (!parsed_uri.parse()) {
        return Status::InvalidArgument(fmt::format("Invalid S3 URI {}", uri));
    }
    return std::make_unique<io::S3RandomAccessFile>(_client, std::string(parsed_uri.bucket()),
                                                    std::string(parsed_uri.key()));
}

Status S3ObjectStore::download_object(const std::string& bucket, const std::string& object,
                                      const std::string& local_path) {
    if (local_path.empty()) {
        return Status::IOError(strings::Substitute("Get Object $0 failed, path empty.", object));
    }
    if (_transfer_manager) {
        Aws::Transfer::CreateDownloadStreamCallback stream;
        if (!local_path.empty()) {
            stream = [=]() -> Aws::IOStream* {
                return Aws::New<Aws::FStream>(Aws::Utils::ARRAY_ALLOCATION_TAG, local_path,
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

        if (!local_path.empty()) {
            auto stream = [=]() -> Aws::IOStream* {
                return Aws::New<Aws::FStream>(Aws::Utils::ARRAY_ALLOCATION_TAG, local_path,
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

Status S3ObjectStore::download_object(const std::string& uri, const std::string& local_path) {
    S3URI parsed_uri(uri);
    if (!parsed_uri.parse()) {
        return Status::InvalidArgument(fmt::format("Invalid S3 URI {}", uri));
    }
    return download_object(std::string(parsed_uri.bucket()), std::string(parsed_uri.key()), local_path);
}

Status S3ObjectStore::head_object(const std::string& bucket, const std::string& object, size_t* size) {
    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(to_aws_string(bucket));
    request.SetKey(to_aws_string(object));

    Aws::S3::Model::HeadObjectOutcome outcome = _client->HeadObject(request);
    if (!outcome.IsSuccess()) {
        if (is_not_found(outcome.GetError().GetErrorType())) {
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
    return head_object(bucket, object, nullptr /* size */);
}

Status S3ObjectStore::exist_object(const std::string& uri) {
    S3URI parsed_uri(uri);
    if (!parsed_uri.parse()) {
        return Status::InvalidArgument(fmt::format("Invalid S3 URI {}", uri));
    }
    return head_object(std::string(parsed_uri.bucket()), std::string(parsed_uri.key()), nullptr /* size */);
}

StatusOr<int64_t> S3ObjectStore::get_object_size(const std::string& bucket, const std::string& object) {
    size_t size;
    RETURN_IF_ERROR(head_object(bucket, object, &size));
    return static_cast<int64_t>(size);
}

StatusOr<int64_t> S3ObjectStore::get_object_size(const std::string& uri) {
    S3URI parsed_uri(uri);
    if (!parsed_uri.parse()) {
        return Status::InvalidArgument(fmt::format("Invalid S3 URI {}", uri));
    }
    return get_object_size(std::string(parsed_uri.bucket()), std::string(parsed_uri.key()));
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

Status S3ObjectStore::delete_object(const std::string& uri) {
    S3URI parsed_uri(uri);
    if (!parsed_uri.parse()) {
        return Status::InvalidArgument(fmt::format("Invalid S3 URI {}", uri));
    }
    return delete_object(std::string(parsed_uri.bucket()), std::string(parsed_uri.key()));
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
            if (is_not_found(outcome.GetError().GetErrorType())) {
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

StatusOr<std::unique_ptr<ObjectStore>> new_s3_object_store() {
    Aws::Client::ClientConfiguration config;
    config.scheme = Aws::Http::Scheme::HTTP;
    config.maxConnections = config::aws_s3_max_connection;
    if (!config::aws_s3_endpoint.empty()) {
        config.endpointOverride = config::aws_s3_endpoint;
    }
    if (!config::aws_s3_region.empty()) {
        config.region = config::aws_s3_region;
    }
    auto credentials = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(config::aws_access_key_id.c_str(),
                                                                                 config::aws_secret_access_key.c_str());
    auto s3client = std::make_shared<Aws::S3::S3Client>(credentials, config);
    auto store = std::make_unique<S3ObjectStore>(std::move(s3client), false);
    return std::move(store);
}

class S3Register {
public:
    explicit S3Register(std::string_view pattern) {
        ObjectStore::register_store(pattern, [](std::string_view /*uri*/) -> StatusOr<std::unique_ptr<ObjectStore>> {
            return new_s3_object_store();
        });
    }
};

void s3_global_init() {
    // Simply define these variables as global variables won't work, because object store
    // is compiled into a static library, and global variables defined in static library will
    // not be initialized.
    static S3Register reg0("s3://.*");
    static S3Register reg1("s3a://.*");
    static S3Register reg2("s3n://.*");
    static S3Register reg3("oss://.*");
}

} // namespace starrocks

#endif
