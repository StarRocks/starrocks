// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "env/env_s3.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/DeleteObjectsResult.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <fmt/core.h>
#include <time.h>

#include <limits>

#include "common/config.h"
#include "common/s3_uri.h"
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
        return Status::InternalError(fmt::format(msg));
    }
}

// Wrap a `starrocks::io::SeekableInputStream` into `starrocks::RandomAccessFile`.
// this wrapper will be deleted after we merged `starrocks::RandomAccessFile` and
// `starrocks::io:RandomAccessFile` into one class,
class RandomAccessFileAdapter : public RandomAccessFile {
public:
    explicit RandomAccessFileAdapter(std::unique_ptr<io::SeekableInputStream> input, std::string path)
            : _input(std::move(input)), _object_path(std::move(path)), _object_size(0) {}

    ~RandomAccessFileAdapter() override = default;

    StatusOr<int64_t> read_at(int64_t offset, void* data, int64_t size) const override;
    Status read_at_fully(int64_t offset, void* data, int64_t size) const override;
    Status readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const override;
    StatusOr<uint64_t> get_size() const override;
    const std::string& filename() const override { return _object_path; }

private:
    std::unique_ptr<io::SeekableInputStream> _input;
    std::string _object_path;
    mutable uint64_t _object_size;
};

StatusOr<int64_t> RandomAccessFileAdapter::read_at(int64_t offset, void* data, int64_t size) const {
    return _input->read_at(offset, data, size);
}

Status RandomAccessFileAdapter::read_at_fully(int64_t offset, void* data, int64_t size) const {
    return _input->read_at_fully(offset, data, size);
}

Status RandomAccessFileAdapter::readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const {
    for (size_t i = 0; i < res_cnt; i++) {
        RETURN_IF_ERROR(RandomAccessFileAdapter::read_at_fully(offset, res[i].data, res[i].size));
        offset += res[i].size;
    }
    return Status::OK();
}

StatusOr<uint64_t> RandomAccessFileAdapter::get_size() const {
    if (_object_size == 0) {
        ASSIGN_OR_RETURN(_object_size, _input->get_size());
    }
    return _object_size;
}

// // Wrap a `starrocks::io::OutputStream` into `starrocks::WritableFile`.
class OutputStreamAdapter : public WritableFile {
public:
    explicit OutputStreamAdapter(std::unique_ptr<io::OutputStream> os, std::string name)
            : _os(std::move(os)), _name(std::move(name)), _bytes_written(0) {}

    Status append(const Slice& data) override {
        auto st = _os->write(data.data, data.size);
        _bytes_written += st.ok() ? data.size : 0;
        return st;
    }

    Status appendv(const Slice* data, size_t cnt) override {
        for (size_t i = 0; i < cnt; i++) {
            RETURN_IF_ERROR(append(data[i]));
        }
        return Status::OK();
    }

    Status pre_allocate(uint64_t size) override { return Status::NotSupported("OutputStreamAdapter::pre_allocate"); }

    Status close() override { return _os->close(); }

    // NOTE: unlike posix file, the file cannot be writen anymore after `flush`ed.
    Status flush(FlushMode mode) override { return _os->close(); }

    // NOTE: unlike posix file, the file cannot be writen anymore after `sync`ed.
    Status sync() override { return _os->close(); }

    uint64_t size() const { return _bytes_written; }

    const std::string& filename() const override { return _name; }

private:
    std::unique_ptr<io::OutputStream> _os;
    std::string _name;
    uint64_t _bytes_written;
};

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

    S3ClientPtr new_client(const ClientConfiguration& config);

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

    constexpr static int kMaxItems = 8;

    std::mutex _lock;
    int _items;
    // _configs[i] is the client configuration of |_clients[i].
    ClientConfiguration _configs[kMaxItems];
    S3ClientPtr _clients[kMaxItems];
    Random _rand;
};

S3ClientFactory::S3ClientFactory() : _items(0), _rand((int)::time(NULL)) {}

S3ClientFactory::S3ClientPtr S3ClientFactory::new_client(const ClientConfiguration& config) {
    std::lock_guard l(_lock);

    for (size_t i = 0; i < _items; i++) {
        if (_configs[i] == config) return _clients[i];
    }

    S3ClientPtr client;
    const auto& access_key_id = config::object_storage_access_key_id;
    const auto& secret_access_key = config::object_storage_secret_access_key;
    if (!access_key_id.empty() && !secret_access_key.empty()) {
        auto credentials = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(access_key_id, secret_access_key);
        client = std::make_shared<Aws::S3::S3Client>(credentials, config);
    } else {
        // if not cred provided, we can use default cred in aws profile.
        client = std::make_shared<Aws::S3::S3Client>(config);
    }

    if (UNLIKELY(_items >= kMaxItems)) {
        int idx = _rand.Uniform(kMaxItems);
        _configs[idx] = config;
        _clients[idx] = client;
    } else {
        _configs[_items] = config;
        _clients[_items] = client;
        _items++;
    }
    return client;
}

std::shared_ptr<Aws::S3::S3Client> new_s3client(const S3URI& uri) {
    Aws::Client::ClientConfiguration config = S3ClientFactory::getClientConfig();
    config.scheme = Aws::Http::Scheme::HTTP; // TODO: use the scheme in uri
    if (!uri.endpoint().empty()) {
        config.endpointOverride = uri.endpoint();
    } else if (!config::object_storage_endpoint.empty()) {
        config.endpointOverride = config::object_storage_endpoint;
    }
    if (!config::object_storage_region.empty()) {
        config.region = config::object_storage_region;
    }
    config.maxConnections = config::object_storage_max_connection;
    return S3ClientFactory::instance().new_client(config);
}

class EnvS3 : public Env {
public:
    EnvS3() {}
    ~EnvS3() override = default;

    EnvS3(const EnvS3&) = delete;
    void operator=(const EnvS3&) = delete;
    EnvS3(EnvS3&&) = delete;
    void operator=(EnvS3&&) = delete;

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const std::string& path) override;

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& path) override;

    StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const std::string& path) override {
        return Status::NotSupported("EnvS3::new_sequential_file");
    }

    // FIXME: `new_writable_file()` will not truncate an already-exist file/object, which does not satisfy
    // the API requirement.
    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const std::string& path) override;

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const WritableFileOptions& opts,
                                                              const std::string& path) override;

    StatusOr<std::unique_ptr<RandomRWFile>> new_random_rw_file(const std::string& path) override {
        return Status::NotSupported("EnvS3::new_random_rw_file");
    }

    StatusOr<std::unique_ptr<RandomRWFile>> new_random_rw_file(const RandomRWFileOptions& opts,
                                                               const std::string& path) override {
        return Status::NotSupported("EnvS3::new_random_rw_file");
    }

    Status path_exists(const std::string& path) override { return Status::NotSupported("EnvS3::path_exists"); }

    Status get_children(const std::string& dir, std::vector<std::string>* file) override {
        return Status::NotSupported("EnvS3::get_children");
    }

    Status iterate_dir(const std::string& dir, const std::function<bool(std::string_view)>& cb) override;

    Status delete_file(const std::string& path) override;

    Status create_dir(const std::string& dirname) override;

    Status create_dir_if_missing(const std::string& dirname, bool* created) override;

    Status delete_dir(const std::string& dirname) override;

    Status delete_dir_recursive(const std::string& dirname) override;

    Status sync_dir(const std::string& dirname) override;

    StatusOr<bool> is_directory(const std::string& path) override;

    Status canonicalize(const std::string& path, std::string* file) override {
        return Status::NotSupported("EnvS3::canonicalize");
    }

    StatusOr<uint64_t> get_file_size(const std::string& path) override {
        return Status::NotSupported("EnvS3::get_file_size");
    }

    StatusOr<uint64_t> get_file_modified_time(const std::string& path) override {
        return Status::NotSupported("EnvS3::get_file_modified_time");
    }

    Status rename_file(const std::string& src, const std::string& target) override {
        return Status::NotSupported("EnvS3::rename_file");
    }

    Status link_file(const std::string& old_path, const std::string& new_path) override {
        return Status::NotSupported("EnvS3::link_file");
    }

    StatusOr<SpaceInfo> space(const std::string& path) override;
};

StatusOr<std::unique_ptr<RandomAccessFile>> EnvS3::new_random_access_file(const std::string& path) {
    return EnvS3::new_random_access_file(RandomAccessFileOptions(), path);
}

StatusOr<std::unique_ptr<RandomAccessFile>> EnvS3::new_random_access_file(const RandomAccessFileOptions& opts,
                                                                          const std::string& path) {
    S3URI uri;
    if (!uri.parse(path)) {
        return Status::InvalidArgument(fmt::format("Invalid S3 URI: {}", path));
    }
    auto client = new_s3client(uri);
    auto input_stream = std::make_unique<io::S3InputStream>(std::move(client), uri.bucket(), uri.key());
    return std::make_unique<RandomAccessFileAdapter>(std::move(input_stream), path);
}

StatusOr<std::unique_ptr<WritableFile>> EnvS3::new_writable_file(const std::string& fname) {
    return new_writable_file(WritableFileOptions(), fname);
}

// NOTE: Unlike the posix env, we can create files under a non-exist directory.
StatusOr<std::unique_ptr<WritableFile>> EnvS3::new_writable_file(const WritableFileOptions& opts,
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
    if (opts.mode != Env::CREATE_OR_OPEN_WITH_TRUNCATE && opts.mode != Env::MUST_CREATE) {
        return Status::NotSupported(fmt::format("EnvS3 does not support open mode {}", opts.mode));
    }
    auto client = new_s3client(uri);
    auto ostream = std::make_unique<io::S3OutputStream>(std::move(client), uri.bucket(), uri.key(),
                                                        config::experimental_s3_max_single_part_size,
                                                        config::experimental_s3_min_upload_part_size);
    return std::make_unique<OutputStreamAdapter>(std::move(ostream), fname);
}

StatusOr<SpaceInfo> EnvS3::space(const std::string& path) {
    // call `is_directory()` to check if 'path' is an valid path
    const Status status = EnvS3::is_directory(path).status();
    if (!status.ok()) {
        return status;
    }
    return SpaceInfo{.capacity = std::numeric_limits<int64_t>::max(),
                     .free = std::numeric_limits<int64_t>::max(),
                     .available = std::numeric_limits<int64_t>::max()};
}

Status EnvS3::iterate_dir(const std::string& dir, const std::function<bool(std::string_view)>& cb) {
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
    auto client = new_s3client(uri);
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

// Creating directory is actually creating an object of key "dirname/"
Status EnvS3::create_dir(const std::string& dirname) {
    auto st = EnvS3::is_directory(dirname).status();
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
    auto client = new_s3client(uri);
    auto outcome = client->PutObject(req);
    if (outcome.IsSuccess()) {
        return Status::OK();
    } else {
        return to_status(outcome.GetError().GetErrorType(), outcome.GetError().GetMessage());
    }
}

Status EnvS3::create_dir_if_missing(const std::string& dirname, bool* created) {
    auto st = create_dir(dirname);
    if (created != nullptr) {
        *created = st.ok();
    }
    if (st.is_already_exist()) {
        st = Status::OK();
    }
    return st;
}

StatusOr<bool> EnvS3::is_directory(const std::string& path) {
    S3URI uri;
    if (!uri.parse(path)) {
        return Status::InvalidArgument(fmt::format("Invalid S3 URI {}", path));
    }
    if (uri.key().empty()) { // root directory '/'
        return true;
    }
    auto client = new_s3client(uri);
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

Status EnvS3::delete_file(const std::string& path) {
    S3URI uri;
    if (!uri.parse(path)) {
        return Status::InvalidArgument(fmt::format("Invalid S3 URI {}", path));
    }
    if (UNLIKELY(!uri.key().empty() && uri.key().back() == '/')) {
        return Status::InvalidArgument(fmt::format("object key ended with slash: {}", path));
    }
    Aws::S3::Model::DeleteObjectRequest request;
    request.WithBucket(uri.bucket()).WithKey(uri.key());
    auto client = new_s3client(uri);
    auto outcome = client->DeleteObject(request);
    // NOTE: If the object does not exist, outcome.IsSuccess() is true and OK is returned, which
    // is different from the behavior of posix env.
    if (outcome.IsSuccess()) {
        return Status::OK();
    } else {
        return to_status(outcome.GetError().GetErrorType(), outcome.GetError().GetMessage());
    }
}

Status EnvS3::delete_dir(const std::string& dirname) {
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

    auto client = new_s3client(uri);

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

Status EnvS3::sync_dir(const std::string& dirname) {
    // The only thing we need to do is check whether the directory exist or not.
    ASSIGN_OR_RETURN(const bool is_dir, is_directory(dirname));
    if (is_dir) return Status::OK();
    return Status::NotFound(fmt::format("{} not directory", dirname));
}

Status EnvS3::delete_dir_recursive(const std::string& dirname) {
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
    auto client = new_s3client(uri);
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

std::unique_ptr<Env> new_env_s3() {
    return std::make_unique<EnvS3>();
}

} // namespace starrocks
