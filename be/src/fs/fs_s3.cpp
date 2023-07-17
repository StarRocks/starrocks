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

#include "fs/fs_s3.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/identity-management/auth/STSAssumeRoleCredentialsProvider.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/sts/STSClient.h>
#include <fmt/core.h>

#include <ctime>
#include <limits>

#include "common/config.h"
#include "common/s3_uri.h"
#include "fs/output_stream_adapter.h"
#include "gutil/casts.h"
#include "gutil/strings/util.h"
#include "io/s3_input_stream.h"
#include "io/s3_output_stream.h"
#include "util/hdfs_util.h"
#include "util/random.h"

namespace starrocks {

static Status to_status(Aws::S3::S3Errors error, const std::string& msg) {
    switch (error) {
    case Aws::S3::S3Errors::BUCKET_ALREADY_EXISTS:
        return Status::AlreadyExist(fmt::format("bucket already exists: {}", msg));
    case Aws::S3::S3Errors::BUCKET_ALREADY_OWNED_BY_YOU:
        return Status::AlreadyExist(fmt::format("bucket already owned by you: {}", msg));
    case Aws::S3::S3Errors::NO_SUCH_BUCKET:
        return Status::NotFound(fmt::format("no such bucket: {}", msg));
    case Aws::S3::S3Errors::NO_SUCH_KEY:
        return Status::NotFound(fmt::format("no such key: {}", msg));
    case Aws::S3::S3Errors::NO_SUCH_UPLOAD:
        return Status::NotFound(fmt::format("no such upload: {}", msg));
    default:
        return Status::InternalError(msg);
    }
}

bool operator==(const Aws::Client::ClientConfiguration& lhs, const Aws::Client::ClientConfiguration& rhs) {
    return lhs.endpointOverride == rhs.endpointOverride && lhs.region == rhs.region &&
           lhs.maxConnections == rhs.maxConnections && lhs.scheme == rhs.scheme;
}

class S3ClientFactory {
public:
    using ClientConfiguration = Aws::Client::ClientConfiguration;
    using S3Client = Aws::S3::S3Client;
    using S3ClientPtr = std::shared_ptr<S3Client>;

    static S3ClientFactory& instance() {
        static S3ClientFactory obj;
        return obj;
    }

    ~S3ClientFactory() = default;

    S3ClientFactory(const S3ClientFactory&) = delete;
    void operator=(const S3ClientFactory&) = delete;
    S3ClientFactory(S3ClientFactory&&) = delete;
    void operator=(S3ClientFactory&&) = delete;

    S3ClientPtr new_client(const TCloudConfiguration& cloud_configuration);
    S3ClientPtr new_client(const ClientConfiguration& config, const FSOptions& opts);

    static ClientConfiguration& getClientConfig() {
        // We cached config here and make a deep copy each time.Since aws sdk has changed the
        // Aws::Client::ClientConfiguration default constructor to search for the region
        // (where as before 1.8 it has been hard coded default of "us-east-1").
        // Part of that change is looking through the ec2 metadata, which can take a long time.
        // For more details, please refer https://github.com/aws/aws-sdk-cpp/issues/1440
        static ClientConfiguration instance;
        return instance;
    }

private:
    S3ClientFactory();

    static std::shared_ptr<Aws::Auth::AWSCredentialsProvider> _get_aws_credentials_provider(
            const AWSCloudCredential& aws_cloud_credential);

    class ClientCacheKey {
    public:
        ClientConfiguration config;
        AWSCloudConfiguration aws_cloud_configuration;

        bool operator==(const ClientCacheKey& rhs) const {
            return config == rhs.config && aws_cloud_configuration == rhs.aws_cloud_configuration;
        }
    };

    constexpr static int kMaxItems = 8;

    std::mutex _lock;
    int _items{0};
    // _client_cache_keys[i] is the client cache key of |_clients[i].
    ClientCacheKey _client_cache_keys[kMaxItems];
    S3ClientPtr _clients[kMaxItems];
    Random _rand;
};

S3ClientFactory::S3ClientFactory() : _rand((int)::time(nullptr)) {}

// Get an AWSCredentialsProvider based on CloudCredential
std::shared_ptr<Aws::Auth::AWSCredentialsProvider> S3ClientFactory::_get_aws_credentials_provider(
        const AWSCloudCredential& aws_cloud_credential) {
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credential_provider = nullptr;
    // Create a base credentials provider
    if (aws_cloud_credential.use_aws_sdk_default_behavior) {
        credential_provider = std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
    } else if (aws_cloud_credential.use_instance_profile) {
        credential_provider = std::make_shared<Aws::Auth::InstanceProfileCredentialsProvider>();
    } else if (!aws_cloud_credential.access_key.empty() && !aws_cloud_credential.secret_key.empty()) {
        credential_provider = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(
                aws_cloud_credential.access_key, aws_cloud_credential.secret_key, aws_cloud_credential.session_token);
    } else {
        DCHECK(false) << "Unreachable!";
        credential_provider = std::make_shared<Aws::Auth::AnonymousAWSCredentialsProvider>();
    }

    if (!aws_cloud_credential.iam_role_arn.empty()) {
        // Do assume role
        auto sts = std::make_shared<Aws::STS::STSClient>(credential_provider);
        credential_provider = std::make_shared<Aws::Auth::STSAssumeRoleCredentialsProvider>(
                aws_cloud_credential.iam_role_arn, Aws::String(), aws_cloud_credential.external_id,
                Aws::Auth::DEFAULT_CREDS_LOAD_FREQ_SECONDS, sts);
    }
    return credential_provider;
}

S3ClientFactory::S3ClientPtr S3ClientFactory::new_client(const TCloudConfiguration& t_cloud_configuration) {
    const AWSCloudConfiguration aws_cloud_configuration = CloudConfigurationFactory::create_aws(t_cloud_configuration);

    Aws::Client::ClientConfiguration config = S3ClientFactory::getClientConfig();
    AWSCloudCredential aws_cloud_credential = aws_cloud_configuration.aws_cloud_credential;
    // Set into config
    bool path_style_access = aws_cloud_configuration.enable_path_style_access;
    if (aws_cloud_configuration.enable_ssl) {
        config.scheme = Aws::Http::Scheme::HTTPS;
    } else {
        config.scheme = Aws::Http::Scheme::HTTP;
    }

    if (!aws_cloud_credential.region.empty()) {
        config.region = aws_cloud_credential.region;
    }
    if (!aws_cloud_credential.endpoint.empty()) {
        config.endpointOverride = aws_cloud_credential.endpoint;
    }
    config.maxConnections = config::object_storage_max_connection;
    if (config::object_storage_connect_timeout_ms > 0) {
        config.connectTimeoutMs = config::object_storage_connect_timeout_ms;
    }
    if (config::object_storage_request_timeout_ms >= 0) {
        config.requestTimeoutMs = config::object_storage_request_timeout_ms;
    }

    ClientCacheKey client_cache_key{config, aws_cloud_configuration};
    {
        // Duplicate code for cache s3 client
        std::lock_guard l(_lock);
        for (size_t i = 0; i < _items; i++) {
            if (_client_cache_keys[i] == client_cache_key) return _clients[i];
        }
    }

    auto credential_provider = _get_aws_credentials_provider(aws_cloud_credential);

    S3ClientPtr client = std::make_shared<Aws::S3::S3Client>(
            credential_provider, config, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, !path_style_access);

    {
        std::lock_guard l(_lock);
        if (UNLIKELY(_items >= kMaxItems)) {
            int idx = _rand.Uniform(kMaxItems);
            _client_cache_keys[idx] = client_cache_key;
            _clients[idx] = client;
        } else {
            _client_cache_keys[_items] = client_cache_key;
            _clients[_items] = client;
            _items++;
        }
    }
    return client;
}

S3ClientFactory::S3ClientPtr S3ClientFactory::new_client(const ClientConfiguration& config, const FSOptions& opts) {
    std::lock_guard l(_lock);

    ClientCacheKey client_cache_key{config, AWSCloudConfiguration{}};
    for (size_t i = 0; i < _items; i++) {
        if (_client_cache_keys[i] == client_cache_key) return _clients[i];
    }

    S3ClientPtr client;
    string access_key_id;
    string secret_access_key;
    bool path_style_access = config::object_storage_endpoint_path_style_access;
    const THdfsProperties* hdfs_properties = opts.hdfs_properties();
    if (hdfs_properties != nullptr) {
        if (hdfs_properties->__isset.access_key) {
            access_key_id = hdfs_properties->access_key;
        }
        if (hdfs_properties->__isset.secret_key) {
            secret_access_key = hdfs_properties->secret_key;
        }
    } else {
        access_key_id = config::object_storage_access_key_id;
        secret_access_key = config::object_storage_secret_access_key;
    }
    if (!access_key_id.empty() && !secret_access_key.empty()) {
        auto credentials = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(access_key_id, secret_access_key);
        client = std::make_shared<Aws::S3::S3Client>(credentials, config,
                                                     /* signPayloads */
                                                     Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                                                     /* useVirtualAddress */
                                                     !path_style_access);
    } else {
        // if not cred provided, we can use default cred in aws profile.
        client = std::make_shared<Aws::S3::S3Client>(config,
                                                     /* signPayloads */
                                                     Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                                                     /* useVirtualAddress */
                                                     !path_style_access);
    }

    if (UNLIKELY(_items >= kMaxItems)) {
        int idx = _rand.Uniform(kMaxItems);
        _client_cache_keys[idx] = client_cache_key;
        _clients[idx] = client;
    } else {
        _client_cache_keys[_items] = client_cache_key;
        _clients[_items] = client;
        _items++;
    }
    return client;
}

// If you find yourself change this code, see also `bool operator==(const Aws::Client::ClientConfiguration&, const Aws::Client::ClientConfiguration&)`
static std::shared_ptr<Aws::S3::S3Client> new_s3client(const S3URI& uri, const FSOptions& opts) {
    Aws::Client::ClientConfiguration config = S3ClientFactory::getClientConfig();
    const THdfsProperties* hdfs_properties = opts.hdfs_properties();
    // TODO(SmithCruise) If CloudType is DEFAULT, we should use hadoop sdk to access file,
    // otherwise user's core-site.xml will not take effect in s3 sdk
    if ((hdfs_properties != nullptr && hdfs_properties->__isset.cloud_configuration) ||
        (opts.cloud_configuration != nullptr && opts.cloud_configuration->cloud_type != TCloudType::DEFAULT)) {
        // Use CloudConfiguration instead of original logic
        const TCloudConfiguration& tCloudConfiguration = (opts.cloud_configuration != nullptr)
                                                                 ? *opts.cloud_configuration
                                                                 : hdfs_properties->cloud_configuration;
        return S3ClientFactory::instance().new_client(tCloudConfiguration);
    } else if (hdfs_properties != nullptr) {
        DCHECK(hdfs_properties->__isset.end_point);
        if (hdfs_properties->__isset.end_point) {
            config.endpointOverride = hdfs_properties->end_point;
        }
        if (hdfs_properties->__isset.region) {
            config.region = hdfs_properties->region;
        }
        if (hdfs_properties->__isset.ssl_enable && hdfs_properties->ssl_enable) {
            config.scheme = Aws::Http::Scheme::HTTPS;
        }
        if (hdfs_properties->__isset.max_connection) {
            config.maxConnections = hdfs_properties->max_connection;
        } else {
            config.maxConnections = config::object_storage_max_connection;
        }
    } else {
        if (!uri.endpoint().empty()) {
            config.endpointOverride = uri.endpoint();
        } else if (!config::object_storage_endpoint.empty()) {
            config.endpointOverride = config::object_storage_endpoint;
        } else if (config::object_storage_endpoint_use_https) {
            config.scheme = Aws::Http::Scheme::HTTPS;
        } else {
            config.scheme = Aws::Http::Scheme::HTTP;
        }
        if (!config::object_storage_region.empty()) {
            config.region = config::object_storage_region;
        }
        config.maxConnections = config::object_storage_max_connection;
    }
    if (config::object_storage_connect_timeout_ms > 0) {
        config.connectTimeoutMs = config::object_storage_connect_timeout_ms;
    }
    // 0 is meaningful for object_storage_request_timeout_ms
    if (config::object_storage_request_timeout_ms >= 0) {
        config.requestTimeoutMs = config::object_storage_request_timeout_ms;
    }
    return S3ClientFactory::instance().new_client(config, opts);
}

class S3FileSystem : public FileSystem {
public:
    S3FileSystem(const FSOptions& options) : _options(options) {}
    ~S3FileSystem() override = default;

    S3FileSystem(const S3FileSystem&) = delete;
    void operator=(const S3FileSystem&) = delete;
    S3FileSystem(S3FileSystem&&) = delete;
    void operator=(S3FileSystem&&) = delete;

    Type type() const override { return S3; }

    using FileSystem::new_sequential_file;
    using FileSystem::new_random_access_file;

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& path) override;

    StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const SequentialFileOptions& opts,
                                                                  const std::string& path) override;

    // FIXME: `new_writable_file()` will not truncate an already-exist file/object, which does not satisfy
    // the API requirement.
    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const std::string& path) override;

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const WritableFileOptions& opts,
                                                              const std::string& path) override;

    Status path_exists(const std::string& path) override { return Status::NotSupported("S3FileSystem::path_exists"); }

    Status get_children(const std::string& dir, std::vector<std::string>* file) override {
        return Status::NotSupported("S3FileSystem::get_children");
    }

    Status iterate_dir(const std::string& dir, const std::function<bool(std::string_view)>& cb) override;

    Status iterate_dir2(const std::string& dir, const std::function<bool(DirEntry)>& cb) override;

    Status delete_file(const std::string& path) override;

    Status create_dir(const std::string& dirname) override;

    Status create_dir_if_missing(const std::string& dirname, bool* created) override;

    Status create_dir_recursive(const std::string& dirname) override;

    Status delete_dir(const std::string& dirname) override;

    Status delete_dir_recursive(const std::string& dirname) override;

    Status sync_dir(const std::string& dirname) override;

    StatusOr<bool> is_directory(const std::string& path) override;

    Status canonicalize(const std::string& path, std::string* file) override {
        return Status::NotSupported("S3FileSystem::canonicalize");
    }

    StatusOr<uint64_t> get_file_size(const std::string& path) override {
        return Status::NotSupported("S3FileSystem::get_file_size");
    }

    StatusOr<uint64_t> get_file_modified_time(const std::string& path) override {
        return Status::NotSupported("S3FileSystem::get_file_modified_time");
    }

    Status rename_file(const std::string& src, const std::string& target) override;

    Status link_file(const std::string& old_path, const std::string& new_path) override {
        return Status::NotSupported("S3FileSystem::link_file");
    }

    StatusOr<SpaceInfo> space(const std::string& path) override;

private:
    FSOptions _options;
};

StatusOr<std::unique_ptr<RandomAccessFile>> S3FileSystem::new_random_access_file(const RandomAccessFileOptions& opts,
                                                                                 const std::string& path) {
    S3URI uri;
    if (!uri.parse(path)) {
        return Status::InvalidArgument(fmt::format("Invalid S3 URI: {}", path));
    }
    auto client = new_s3client(uri, _options);
    auto input_stream = std::make_shared<io::S3InputStream>(std::move(client), uri.bucket(), uri.key());
    return std::make_unique<RandomAccessFile>(std::move(input_stream), path);
}

StatusOr<std::unique_ptr<SequentialFile>> S3FileSystem::new_sequential_file(const SequentialFileOptions& opts,
                                                                            const std::string& path) {
    (void)opts;
    S3URI uri;
    if (!uri.parse(path)) {
        return Status::InvalidArgument(fmt::format("Invalid S3 URI: {}", path));
    }
    auto client = new_s3client(uri, _options);
    auto input_stream = std::make_shared<io::S3InputStream>(std::move(client), uri.bucket(), uri.key());
    return std::make_unique<SequentialFile>(std::move(input_stream), path);
}

StatusOr<std::unique_ptr<WritableFile>> S3FileSystem::new_writable_file(const std::string& fname) {
    return new_writable_file(WritableFileOptions(), fname);
}

// NOTE: Unlike the posix fs, we can create files under a non-exist directory.
StatusOr<std::unique_ptr<WritableFile>> S3FileSystem::new_writable_file(const WritableFileOptions& opts,
                                                                        const std::string& fname) {
    if (!fname.empty() && fname.back() == '/') {
        return Status::NotSupported(fmt::format("S3: cannot create file with name ended with '/': {}", fname));
    }
    S3URI uri;
    if (!uri.parse(fname)) {
        return Status::InvalidArgument(fmt::format("Invalid S3 URI {}", fname));
    }
    // NOTE: if the open mode is MUST_CREATE, technology we should send a head object request first
    // before creating the S3OutputStream, but since this API only used in test environment now,
    // here we assume that the caller can ensure that the file does not exist by themself.
    if (opts.mode != FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE && opts.mode != FileSystem::MUST_CREATE) {
        return Status::NotSupported(fmt::format("S3FileSystem does not support open mode {}", opts.mode));
    }
    auto client = new_s3client(uri, _options);
    auto ostream = std::make_unique<io::S3OutputStream>(std::move(client), uri.bucket(), uri.key(),
                                                        config::experimental_s3_max_single_part_size,
                                                        config::experimental_s3_min_upload_part_size);
    return std::make_unique<OutputStreamAdapter>(std::move(ostream), fname);
}

Status S3FileSystem::rename_file(const std::string& src, const std::string& target) {
    S3URI src_uri;
    S3URI dest_uri;
    if (!src_uri.parse(src)) {
        return Status::InvalidArgument(fmt::format("Invalid src S3 URI: {}", src));
    }
    if (!dest_uri.parse(target)) {
        return Status::InvalidArgument(fmt::format("Invalid target S3 URI: {}", target));
    }
    auto client = new_s3client(src_uri, _options);
    Aws::S3::Model::CopyObjectRequest copy_request;
    copy_request.WithCopySource(src_uri.bucket() + "/" + src_uri.key());
    copy_request.WithBucket(dest_uri.bucket());
    copy_request.WithKey(dest_uri.key());
    Aws::S3::Model::CopyObjectOutcome copy_outcome = client->CopyObject(copy_request);
    if (!copy_outcome.IsSuccess()) {
        return Status::InvalidArgument(fmt::format("Fail to copy from src {} to target {}, msg: {}", src, target,
                                                   copy_outcome.GetError().GetMessage()));
    }

    Aws::S3::Model::DeleteObjectRequest delete_request;
    delete_request.WithBucket(src_uri.bucket()).WithKey(src_uri.key());
    Aws::S3::Model::DeleteObjectOutcome delete_outcome = client->DeleteObject(delete_request);
    if (!delete_outcome.IsSuccess()) {
        return Status::InvalidArgument(
                fmt::format("Fail to delte src {}, msg: {}", src, delete_outcome.GetError().GetMessage()));
    }

    return Status::OK();
}

StatusOr<SpaceInfo> S3FileSystem::space(const std::string& path) {
    // call `is_directory()` to check if 'path' is an valid path
    const Status status = S3FileSystem::is_directory(path).status();
    if (!status.ok()) {
        return status;
    }
    return SpaceInfo{.capacity = std::numeric_limits<int64_t>::max(),
                     .free = std::numeric_limits<int64_t>::max(),
                     .available = std::numeric_limits<int64_t>::max()};
}

Status S3FileSystem::iterate_dir(const std::string& dir, const std::function<bool(std::string_view)>& cb) {
    S3URI uri;
    if (!uri.parse(dir)) {
        return Status::InvalidArgument(fmt::format("Invalid S3 URI {}", dir));
    }
    if (!uri.key().empty() && !HasSuffixString(uri.key(), "/")) {
        uri.key().reserve(uri.key().size() + 1);
        uri.key().push_back('/');
    }
    // `uri.key().empty()` is true means this is a root directory.
    bool directory_exist = uri.key().empty() ? true : false;
    auto client = new_s3client(uri, _options);
    Aws::S3::Model::ListObjectsV2Request request;
    Aws::S3::Model::ListObjectsV2Result result;
    request.WithBucket(uri.bucket()).WithPrefix(uri.key()).WithDelimiter("/");
#ifdef BE_TEST
    request.SetMaxKeys(1);
#endif
    do {
        auto outcome = client->ListObjectsV2(request);
        if (!outcome.IsSuccess()) {
            return Status::IOError(fmt::format("S3: fail to list {}: {}", dir, outcome.GetError().GetMessage()));
        }
        result = outcome.GetResultWithOwnership();
        request.SetContinuationToken(result.GetNextContinuationToken());
        directory_exist |= !result.GetCommonPrefixes().empty();
        directory_exist |= !result.GetContents().empty();
        for (auto&& cp : result.GetCommonPrefixes()) {
            DCHECK(HasPrefixString(cp.GetPrefix(), uri.key())) << cp.GetPrefix() << " " << uri.key();
            DCHECK(HasSuffixString(cp.GetPrefix(), "/")) << cp.GetPrefix();
            const auto& full_name = cp.GetPrefix();
            std::string_view name(full_name.data() + uri.key().size(), full_name.size() - uri.key().size() - 1);
            if (!cb(name)) {
                return Status::OK();
            }
        }
        for (auto&& obj : result.GetContents()) {
            if (obj.GetKey() == uri.key()) {
                continue;
            }
            DCHECK(HasPrefixString(obj.GetKey(), uri.key()));
            std::string_view obj_key(obj.GetKey());
            if (obj_key.back() == '/') {
                obj_key = std::string_view(obj_key.data(), obj_key.size() - 1);
            }
            std::string_view name(obj_key.data() + uri.key().size(), obj_key.size() - uri.key().size());
            if (!cb(name)) {
                return Status::OK();
            }
        }
    } while (result.GetIsTruncated());
    return directory_exist ? Status::OK() : Status::NotFound(dir);
}

Status S3FileSystem::iterate_dir2(const std::string& dir, const std::function<bool(DirEntry)>& cb) {
    S3URI uri;
    if (!uri.parse(dir)) {
        return Status::InvalidArgument(fmt::format("Invalid S3 URI {}", dir));
    }
    if (!uri.key().empty() && !HasSuffixString(uri.key(), "/")) {
        uri.key().reserve(uri.key().size() + 1);
        uri.key().push_back('/');
    }
    // `uri.key().empty()` is true means this is a root directory.
    bool directory_exist = uri.key().empty() ? true : false;
    auto client = new_s3client(uri, _options);
    Aws::S3::Model::ListObjectsV2Request request;
    Aws::S3::Model::ListObjectsV2Result result;
    request.WithBucket(uri.bucket()).WithPrefix(uri.key()).WithDelimiter("/");

    do {
        auto outcome = client->ListObjectsV2(request);
        if (!outcome.IsSuccess()) {
            return Status::IOError(fmt::format("S3: fail to list {}: {}", dir, outcome.GetError().GetMessage()));
        }
        result = outcome.GetResultWithOwnership();
        request.SetContinuationToken(result.GetNextContinuationToken());
        directory_exist |= !result.GetCommonPrefixes().empty();
        directory_exist |= !result.GetContents().empty();
        for (auto&& cp : result.GetCommonPrefixes()) {
            DCHECK(HasPrefixString(cp.GetPrefix(), uri.key())) << cp.GetPrefix() << " " << uri.key();
            DCHECK(HasSuffixString(cp.GetPrefix(), "/")) << cp.GetPrefix();
            const auto& full_name = cp.GetPrefix();

            std::string_view name(full_name.data() + uri.key().size(), full_name.size() - uri.key().size() - 1);
            DirEntry entry{.name = name, .is_dir = {true}};
            if (!cb(entry)) {
                return Status::OK();
            }
        }
        for (auto&& obj : result.GetContents()) {
            if (obj.GetKey() == uri.key()) {
                continue;
            }
            DCHECK(HasPrefixString(obj.GetKey(), uri.key()));

            std::string_view obj_key(obj.GetKey());
            DirEntry entry;
            if (obj.LastModifiedHasBeenSet()) {
                entry.mtime = obj.GetLastModified().Seconds();
            }
            if (obj_key.back() == '/') {
                obj_key = std::string_view(obj_key.data(), obj_key.size() - 1);
                entry.is_dir = true;
            } else {
                DCHECK(obj.SizeHasBeenSet());
                entry.is_dir = false;
                if (obj.SizeHasBeenSet()) {
                    entry.size = obj.GetSize();
                }
            }

            std::string_view name(obj_key.data() + uri.key().size(), obj_key.size() - uri.key().size());
            entry.name = name;

            if (!cb(entry)) {
                return Status::OK();
            }
        }
    } while (result.GetIsTruncated());
    return directory_exist ? Status::OK() : Status::NotFound(dir);
}

// Creating directory is actually creating an object of key "dirname/"
Status S3FileSystem::create_dir(const std::string& dirname) {
    auto st = S3FileSystem::is_directory(dirname).status();
    if (st.ok()) {
        return Status::AlreadyExist(dirname);
    }
    S3URI uri;
    if (!uri.parse(dirname)) {
        return Status::InvalidArgument(fmt::format("Invalid S3 URI: {}", dirname));
    }
    if (uri.key().empty()) { // root directory '/'
        return Status::AlreadyExist(fmt::format("root directory {} already exists", dirname));
    }
    Aws::S3::Model::PutObjectRequest req;
    req.SetBucket(uri.bucket());
    if (uri.key().back() == '/') {
        req.SetKey(uri.key());
    } else {
        req.SetKey(fmt::format("{}/", uri.key()));
    }
    req.SetContentLength(0);
    auto client = new_s3client(uri, _options);
    auto outcome = client->PutObject(req);
    if (outcome.IsSuccess()) {
        return Status::OK();
    } else {
        return to_status(outcome.GetError().GetErrorType(), outcome.GetError().GetMessage());
    }
}

Status S3FileSystem::create_dir_if_missing(const std::string& dirname, bool* created) {
    auto st = create_dir(dirname);
    if (created != nullptr) {
        *created = st.ok();
    }
    if (st.is_already_exist()) {
        st = Status::OK();
    }
    return st;
}

Status S3FileSystem::create_dir_recursive(const std::string& dirname) {
    return create_dir_if_missing(dirname, nullptr);
}

StatusOr<bool> S3FileSystem::is_directory(const std::string& path) {
    S3URI uri;
    if (!uri.parse(path)) {
        return Status::InvalidArgument(fmt::format("Invalid S3 URI {}", path));
    }
    if (uri.key().empty()) { // root directory '/'
        return true;
    }
    auto client = new_s3client(uri, _options);
    Aws::S3::Model::ListObjectsV2Request request;
    Aws::S3::Model::ListObjectsV2Result result;
    request.WithBucket(uri.bucket()).WithPrefix(uri.key()).WithMaxKeys(1);
    auto outcome = client->ListObjectsV2(request);
    if (!outcome.IsSuccess()) {
        return Status::IOError(fmt::format("fail to list {}: {}", path, outcome.GetError().GetMessage()));
    }
    result = outcome.GetResultWithOwnership();
    request.SetContinuationToken(result.GetNextContinuationToken());
    std::string dirname = uri.key() + "/";
    for (auto&& obj : result.GetContents()) {
        if (HasPrefixString(obj.GetKey(), dirname)) {
            return true;
        }
        if (obj.GetKey() == uri.key()) {
            return false;
        }
    }
    return Status::NotFound(path);
}

Status S3FileSystem::delete_file(const std::string& path) {
    S3URI uri;
    if (!uri.parse(path)) {
        return Status::InvalidArgument(fmt::format("Invalid S3 URI {}", path));
    }
    if (UNLIKELY(!uri.key().empty() && uri.key().back() == '/')) {
        return Status::InvalidArgument(fmt::format("object key ended with slash: {}", path));
    }
    Aws::S3::Model::DeleteObjectRequest request;
    request.WithBucket(uri.bucket()).WithKey(uri.key());
    auto client = new_s3client(uri, _options);
    auto outcome = client->DeleteObject(request);
    // NOTE: If the object does not exist, outcome.IsSuccess() is true and OK is returned, which
    // is different from the behavior of posix fs.
    if (outcome.IsSuccess()) {
        return Status::OK();
    } else {
        return to_status(outcome.GetError().GetErrorType(), outcome.GetError().GetMessage());
    }
}

Status S3FileSystem::delete_dir(const std::string& dirname) {
    S3URI uri;
    if (!uri.parse(dirname)) {
        return Status::InvalidArgument(fmt::format("Invalid S3 URI: {}", dirname));
    }
    if (uri.key().empty()) {
        return Status::NotSupported("Cannot delete root directory of S3");
    }
    if (!HasSuffixString(uri.key(), "/")) {
        uri.key().push_back('/');
    }

    auto client = new_s3client(uri, _options);

    // Check if the directory is empty
    Aws::S3::Model::ListObjectsV2Request request;
    Aws::S3::Model::ListObjectsV2Result result;
    request.WithBucket(uri.bucket()).WithPrefix(uri.key()).WithDelimiter("/").WithMaxKeys(2);
    auto outcome = client->ListObjectsV2(request);
    if (!outcome.IsSuccess()) {
        return to_status(outcome.GetError().GetErrorType(), outcome.GetError().GetMessage());
    }
    result = outcome.GetResultWithOwnership();
    if (!result.GetCommonPrefixes().empty()) {
        return Status::IOError(fmt::format("directory {} not empty", dirname));
    }
    if (result.GetContents().empty()) {
        return Status::NotFound(fmt::format("directory {} not exist", dirname));
    }
    if (result.GetContents().size() > 1 || result.GetContents()[0].GetKey() != uri.key()) {
        return Status::IOError(fmt::format("directory {} not empty", dirname));
    }

    // The directory is empty, delete it now
    Aws::S3::Model::DeleteObjectRequest del_request;
    del_request.WithBucket(uri.bucket()).WithKey(uri.key());
    auto del_outcome = client->DeleteObject(del_request);
    if (del_outcome.IsSuccess()) {
        return Status::OK();
    } else {
        return to_status(del_outcome.GetError().GetErrorType(), del_outcome.GetError().GetMessage());
    }
}

Status S3FileSystem::sync_dir(const std::string& dirname) {
    // The only thing we need to do is check whether the directory exist or not.
    ASSIGN_OR_RETURN(const bool is_dir, is_directory(dirname));
    if (is_dir) return Status::OK();
    return Status::NotFound(fmt::format("{} not directory", dirname));
}

Status S3FileSystem::delete_dir_recursive(const std::string& dirname) {
    S3URI uri;
    if (!uri.parse(dirname)) {
        return Status::InvalidArgument(fmt::format("Invalid S3 URI {}", dirname));
    }
    if (uri.key().empty()) {
        return Status::NotSupported(fmt::format("S3 URI with an empty key: {}", dirname));
    }
    if (uri.key().back() != '/') {
        uri.key().push_back('/');
    }
    bool directory_exist = false;
    auto client = new_s3client(uri, _options);
    Aws::S3::Model::ListObjectsV2Request request;
    Aws::S3::Model::ListObjectsV2Result result;
    request.WithBucket(uri.bucket()).WithPrefix(uri.key());
#ifdef BE_TEST
    request.SetMaxKeys(1);
#endif

    Aws::S3::Model::DeleteObjectsRequest delete_request;
    delete_request.SetBucket(uri.bucket());
    do {
        auto outcome = client->ListObjectsV2(request);
        if (!outcome.IsSuccess()) {
            return Status::IOError(fmt::format("S3: fail to list {}: {}", dirname, outcome.GetError().GetMessage()));
        }
        result = outcome.GetResultWithOwnership();
        directory_exist |= !result.GetContents().empty();
        Aws::Vector<Aws::S3::Model::ObjectIdentifier> objects;
        objects.reserve(result.GetContents().size());
        for (auto&& obj : result.GetContents()) {
            objects.emplace_back().SetKey(obj.GetKey());
        }
        if (!objects.empty()) {
            Aws::S3::Model::Delete d;
            d.WithObjects(std::move(objects)).WithQuiet(true);
            delete_request.SetDelete(std::move(d));
            auto delete_outcome = client->DeleteObjects(delete_request);
            if (!delete_outcome.IsSuccess()) {
                return Status::IOError(
                        fmt::format("fail to batch delete {}: {}", dirname, delete_outcome.GetError().GetMessage()));
            }
            if (!delete_outcome.GetResult().GetErrors().empty()) {
                auto&& e = delete_outcome.GetResult().GetErrors()[0];
                return Status::IOError(fmt::format("fail to delete {}: {}", e.GetKey(), e.GetMessage()));
            }
        }
    } while (result.GetIsTruncated());
    return directory_exist ? Status::OK() : Status::NotFound(dirname);
}

std::unique_ptr<FileSystem> new_fs_s3(const FSOptions& options) {
    return std::make_unique<S3FileSystem>(options);
}

} // namespace starrocks
