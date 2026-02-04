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

#include "fs/azure/fs_azblob.h"

#include <fmt/format.h>

#include <azure/identity.hpp>
#include <azure/storage/blobs.hpp>

#include "base/concurrency/stopwatch.hpp"
#include "fs/azure/azblob_uri.h"
#include "fs/azure/utils.h"
#include "fs/credential/cloud_configuration_factory.h"
#include "fs/encrypt_file.h"
#include "fs/output_stream_adapter.h"
#include "io/io_profiler.h"
#include "io/output_stream.h"

namespace starrocks {

using BlobContainerClient = Azure::Storage::Blobs::BlobContainerClient;
using BlobContainerClientPtr = std::shared_ptr<BlobContainerClient>;
using BlockBlobClient = Azure::Storage::Blobs::BlockBlobClient;
using BlockBlobClientUniquePtr = std::unique_ptr<Azure::Storage::Blobs::BlockBlobClient>;

// ------ input stream ------

class AzBlobInputStream : public io::SeekableInputStream {
public:
    explicit AzBlobInputStream(BlockBlobClientUniquePtr client, AzBlobURI uri)
            : _blob_client(std::move(client)), _uri(std::move(uri)) {}
    ~AzBlobInputStream() override = default;

    AzBlobInputStream(const AzBlobInputStream&) = delete;
    void operator=(const AzBlobInputStream&) = delete;
    AzBlobInputStream(AzBlobInputStream&&) = delete;
    void operator=(AzBlobInputStream&&) = delete;

    StatusOr<int64_t> read(void* data, int64_t size) override;
    StatusOr<int64_t> get_size() override;
    StatusOr<int64_t> position() override { return _offset; }
    Status seek(int64_t offset) override;
    void set_size(int64_t size) override { _size = size; }

private:
    BlockBlobClientUniquePtr _blob_client;
    AzBlobURI _uri;
    int64_t _offset{0};
    int64_t _size{-1};
};

StatusOr<int64_t> AzBlobInputStream::read(void* data, int64_t count) {
    if (UNLIKELY(_size == -1)) {
        ASSIGN_OR_RETURN(_size, get_size());
    }

    if (_offset >= _size) { // 0 means reach EOF
        return 0;
    }

    MonotonicStopWatch watch;
    watch.start();

    Azure::Core::Http::HttpRange range;
    range.Offset = _offset;
    range.Length = std::min<int64_t>(count, _size - _offset);

    Azure::Storage::Blobs::DownloadBlobToOptions options;
    options.Range = range;

    int64_t size = 0;
    try {
        auto response = _blob_client->DownloadTo(reinterpret_cast<uint8_t*>(data), count, options);
        if (!response.Value.ContentRange.Length.HasValue()) {
            return Status::InternalError(fmt::format("Download blob object %s error: read length in response is empty",
                                                     _uri.get_blob_uri()));
        }
        size = response.Value.ContentRange.Length.Value();
    } catch (const Azure::Core::RequestFailedException& e) {
        return azure_error_to_status(e.StatusCode,
                                     fmt::format("Download blob object {} error: {}", _uri.get_blob_uri(), e.what()),
                                     _uri.get_blob_uri());
    } catch (const std::exception& e) {
        return Status::InternalError(fmt::format("Download blob object {} error: {}", _uri.get_blob_uri(), e.what()));
    }

    _offset += size;
    IOProfiler::add_read(size, watch.elapsed_time());
    return size;
}

StatusOr<int64_t> AzBlobInputStream::get_size() {
    if (_size == -1) {
        try {
            auto response = _blob_client->GetProperties();
            _size = response.Value.BlobSize;
        } catch (const Azure::Core::RequestFailedException& e) {
            return azure_error_to_status(
                    e.StatusCode, fmt::format("Blob object {} get properties error: {}", _uri.get_blob_uri(), e.what()),
                    _uri.get_blob_uri());
        } catch (const std::exception& e) {
            return Status::InternalError(
                    fmt::format("Blob object {} get properties error: {}", _uri.get_blob_uri(), e.what()));
        }
    }
    return _size;
}

Status AzBlobInputStream::seek(int64_t offset) {
    if (offset < 0) {
        return Status::InvalidArgument(fmt::format("Invalid offset {}", offset));
    }

    _offset = offset;
    return Status::OK();
}

// ------ output stream ------

class AzBlobOutputStream : public io::OutputStream {
public:
    explicit AzBlobOutputStream(BlockBlobClientUniquePtr client, AzBlobURI uri)
            : _blob_client(std::move(client)), _uri(std::move(uri)) {}
    ~AzBlobOutputStream() override = default;

    AzBlobOutputStream(const AzBlobOutputStream&) = delete;
    void operator=(const AzBlobOutputStream&) = delete;
    AzBlobOutputStream(AzBlobOutputStream&&) = delete;
    void operator=(AzBlobOutputStream&&) = delete;

    [[nodiscard]] bool allows_aliasing() const override { return false; }
    Status write_aliased(const void* data, int64_t size) override {
        return Status::NotSupported("AzBlobOutputStream::write_aliased");
    }
    Status skip(int64_t count) override { return Status::NotSupported("AzBlobOutputStream::skip"); }
    StatusOr<Buffer> get_direct_buffer() override {
        return Status::NotSupported("AzBlobOutputStream::get_direct_buffer");
    }
    StatusOr<Position> get_direct_buffer_and_advance(int64_t size) override {
        return Status::NotSupported("AzBlobOutputStream::get_direct_buffer_and_advance");
    }

    Status write(const void* data, int64_t size) override;
    Status close() override;

private:
    Status create_multipart_upload();
    Status multipart_upload();
    Status singlepart_upload();
    Status complete_multipart_upload();

    // Max single object size stored in blob. For object size exceeds
    // _max_single_part_size, multiupload will be used.
    uint64_t _max_single_part_size = 100UL << 20;

    // Upload part size if multiupload enabled.
    uint64_t _min_upload_part_size = 5UL << 20;

    BlockBlobClientUniquePtr _blob_client;
    AzBlobURI _uri;

    bool _multipart_upload{false};
    std::string _buffer;
    std::vector<std::string> _block_ids;
};

Status AzBlobOutputStream::write(const void* data, int64_t size) {
    MonotonicStopWatch watch;
    watch.start();

    _buffer.append(static_cast<const char*>(data), size);
    if (!_multipart_upload && _buffer.size() > _max_single_part_size) {
        RETURN_IF_ERROR(create_multipart_upload());
    }
    if (_multipart_upload && _buffer.size() >= _min_upload_part_size) {
        RETURN_IF_ERROR(multipart_upload());
        _buffer.clear();
    }

    IOProfiler::add_write(size, watch.elapsed_time());
    return Status::OK();
}

Status AzBlobOutputStream::close() {
    if (!_multipart_upload) {
        RETURN_IF_ERROR(singlepart_upload());
    } else {
        RETURN_IF_ERROR(multipart_upload());
        RETURN_IF_ERROR(complete_multipart_upload());
    }

    return Status::OK();
}

Status AzBlobOutputStream::create_multipart_upload() {
    _multipart_upload = true;
    return Status::OK();
}

Status AzBlobOutputStream::singlepart_upload() {
    try {
        _blob_client->UploadFrom(reinterpret_cast<const uint8_t*>(_buffer.data()), _buffer.size());
    } catch (const Azure::Core::RequestFailedException& e) {
        return azure_error_to_status(e.StatusCode,
                                     fmt::format("Upload blob object {} error: {}", _uri.get_blob_uri(), e.what()),
                                     _uri.get_blob_uri());
    } catch (const std::exception& e) {
        return Status::InternalError(fmt::format("Upload blob object {} error: {}", _uri.get_blob_uri(), e.what()));
    }

    return Status::OK();
}

static std::string get_block_id(int64_t id) {
    constexpr size_t kBlockIdLength = 64;
    std::string block_id = std::to_string(id);
    block_id = std::string(kBlockIdLength - block_id.length(), '0') + block_id;
    return Azure::Core::Convert::Base64Encode(std::vector<uint8_t>(block_id.begin(), block_id.end()));
}

Status AzBlobOutputStream::multipart_upload() {
    if (_buffer.empty()) {
        return Status::OK();
    }

    try {
        std::string block_id = get_block_id(_block_ids.size());
        Azure::Core::IO::MemoryBodyStream body_stream(reinterpret_cast<const uint8_t*>(_buffer.data()), _buffer.size());
        _blob_client->StageBlock(block_id, body_stream);
        _block_ids.push_back(block_id);
    } catch (const Azure::Core::RequestFailedException& e) {
        return azure_error_to_status(e.StatusCode,
                                     fmt::format("Stage blob object {} error: {}", _uri.get_blob_uri(), e.what()),
                                     _uri.get_blob_uri());
    } catch (const std::exception& e) {
        return Status::InternalError(fmt::format("Stage blob object {} error: {}", _uri.get_blob_uri(), e.what()));
    }

    return Status::OK();
}

Status AzBlobOutputStream::complete_multipart_upload() {
    try {
        _blob_client->CommitBlockList(_block_ids);
    } catch (const Azure::Core::RequestFailedException& e) {
        return azure_error_to_status(
                e.StatusCode,
                fmt::format("Commit block list for blob object {} error: {}", _uri.get_blob_uri(), e.what()),
                _uri.get_blob_uri());
    } catch (const std::exception& e) {
        return Status::InternalError(
                fmt::format("Commit block list for blob object {} error: {}", _uri.get_blob_uri(), e.what()));
    }

    return Status::OK();
}

// ------ client factory ------

class AzBlobClientFactory {
public:
    AzBlobClientFactory() { srand(time(nullptr)); }
    ~AzBlobClientFactory() = default;

    BlobContainerClientPtr new_blob_container_client(const AzureCloudCredential& azure_cloud_credential,
                                                     const AzBlobURI& blob_uri);

private:
    BlobContainerClientPtr create_blob_container_client(const AzureCloudCredential& azure_cloud_credential,
                                                        const AzBlobURI& blob_uri);

    constexpr static int kMaxItems = 8;

    struct ClientCacheItem {
        AzureCloudCredential azure_cloud_credential;
        BlobContainerClientPtr blob_container_client;
    };

    std::shared_mutex _lock;
    std::vector<ClientCacheItem> _client_cache;
};

BlobContainerClientPtr AzBlobClientFactory::new_blob_container_client(
        const AzureCloudCredential& azure_cloud_credential, const AzBlobURI& blob_uri) {
    std::string container_uri = blob_uri.get_container_uri();
    {
        std::shared_lock guard(_lock);
        for (auto& client : _client_cache) {
            if (container_uri == client.blob_container_client->GetUrl() &&
                azure_cloud_credential == client.azure_cloud_credential) {
                return client.blob_container_client;
            }
        }
    }

    auto container_client = create_blob_container_client(azure_cloud_credential, blob_uri);

    {
        std::unique_lock guard(_lock);

        for (auto& client : _client_cache) {
            if (container_uri == client.blob_container_client->GetUrl() &&
                azure_cloud_credential == client.azure_cloud_credential) {
                return client.blob_container_client;
            }
        }

        auto& client =
                _client_cache.size() < kMaxItems ? _client_cache.emplace_back() : _client_cache[rand() % kMaxItems];
        client.azure_cloud_credential = azure_cloud_credential;
        client.blob_container_client = container_client;
    }

    return container_client;
}

BlobContainerClientPtr AzBlobClientFactory::create_blob_container_client(
        const AzureCloudCredential& azure_cloud_credential, const AzBlobURI& blob_uri) {
    // shared key
    if (!azure_cloud_credential.shared_key.empty()) {
        auto shared_key_credential = std::make_shared<Azure::Storage::StorageSharedKeyCredential>(
                blob_uri.account(), azure_cloud_credential.shared_key);
        return std::make_shared<BlobContainerClient>(blob_uri.get_container_uri(), shared_key_credential);
    }

    // sas token
    if (!azure_cloud_credential.sas_token.empty()) {
        std::string container_uri = blob_uri.get_container_uri();
        if (azure_cloud_credential.sas_token.front() != '?') {
            container_uri += '?';
        }
        container_uri += azure_cloud_credential.sas_token;
        return std::make_shared<BlobContainerClient>(container_uri);
    }

    // client secret service principal
    if (!azure_cloud_credential.client_id.empty() && !azure_cloud_credential.client_secret.empty() &&
        !azure_cloud_credential.tenant_id.empty()) {
        auto client_secret_credential = std::make_shared<Azure::Identity::ClientSecretCredential>(
                azure_cloud_credential.tenant_id, azure_cloud_credential.client_id,
                azure_cloud_credential.client_secret);
        return std::make_shared<BlobContainerClient>(blob_uri.get_container_uri(), client_secret_credential);
    }

    // user assigned managed identity
    if (!azure_cloud_credential.client_id.empty()) {
        auto managed_identity_credential =
                std::make_shared<Azure::Identity::ManagedIdentityCredential>(azure_cloud_credential.client_id);
        return std::make_shared<BlobContainerClient>(blob_uri.get_container_uri(), managed_identity_credential);
    }

    // default
    auto default_credential = std::make_shared<Azure::Identity::DefaultAzureCredential>();
    return std::make_shared<BlobContainerClient>(blob_uri.get_container_uri(), default_credential);
}

// ------ file system ------

class AzBlobFileSystem : public FileSystem {
public:
    explicit AzBlobFileSystem(const FSOptions& options);
    ~AzBlobFileSystem() override = default;

    AzBlobFileSystem(const AzBlobFileSystem&) = delete;
    void operator=(const AzBlobFileSystem&) = delete;
    AzBlobFileSystem(AzBlobFileSystem&&) = delete;
    void operator=(AzBlobFileSystem&&) = delete;

    Type type() const override { return AZBLOB; }

    using FileSystem::new_sequential_file;
    using FileSystem::new_random_access_file;

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& fname) override;

    StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const SequentialFileOptions& opts,
                                                                  const std::string& fname) override;

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const std::string& fname) override {
        return new_writable_file(WritableFileOptions(), fname);
    }

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const WritableFileOptions& opts,
                                                              const std::string& fname) override;

    Status path_exists(const std::string& fname) override {
        return Status::NotSupported("AzBlobFileSystem::path_exists");
    }

    Status get_children(const std::string& dir, std::vector<std::string>* file) override {
        return Status::NotSupported("AzBlobFileSystem::get_children");
    }

    Status iterate_dir(const std::string& dir, const std::function<bool(std::string_view)>& cb) override {
        return Status::NotSupported("AzBlobFileSystem::iterate_dir");
    }

    Status iterate_dir2(const std::string& dir, const std::function<bool(DirEntry)>& cb) override {
        return Status::NotSupported("AzBlobFileSystem::iterate_dir2");
    }

    Status delete_file(const std::string& fname) override {
        return Status::NotSupported("AzBlobFileSystem::delete_file");
    }

    Status create_dir(const std::string& dirname) override {
        return Status::NotSupported("AzBlobFileSystem::create_dir");
    }

    Status create_dir_if_missing(const std::string& dirname, bool* created) override {
        return Status::NotSupported("AzBlobFileSystem::create_dir_if_missing");
    }

    Status create_dir_recursive(const std::string& dirname) override {
        return Status::NotSupported("AzBlobFileSystem::create_dir_recursive");
    }

    Status delete_dir(const std::string& dirname) override {
        return Status::NotSupported("AzBlobFileSystem::delete_dir");
    }

    Status delete_dir_recursive(const std::string& dirname) override {
        return Status::NotSupported("AzBlobFileSystem::delete_dir_recursive");
    }

    Status sync_dir(const std::string& dirname) override { return Status::NotSupported("AzBlobFileSystem::sync_dir"); }

    StatusOr<bool> is_directory(const std::string& path) override {
        return Status::NotSupported("AzBlobFileSystem::is_directory");
    }

    Status canonicalize(const std::string& path, std::string* file) override {
        return Status::NotSupported("AzBlobFileSystem::canonicalize");
    }

    StatusOr<uint64_t> get_file_size(const std::string& fname) override {
        return Status::NotSupported("AzBlobFileSystem::get_file_size");
    }

    StatusOr<uint64_t> get_file_modified_time(const std::string& fname) override {
        return Status::NotSupported("AzBlobFileSystem::get_file_modified_time");
    }

    Status rename_file(const std::string& src, const std::string& target) override {
        return Status::NotSupported("AzBlobFileSystem::rename_file");
    }

    Status link_file(const std::string& old_path, const std::string& new_path) override {
        return Status::NotSupported("AzBlobFileSystem::link_file");
    }

private:
    static AzBlobClientFactory* blob_client_factory() {
        static AzBlobClientFactory factory;
        return &factory;
    }

    StatusOr<BlobContainerClientPtr> new_blob_container_client(const AzBlobURI& uri);
    StatusOr<BlockBlobClientUniquePtr> new_blob_client(const AzBlobURI& uri);

    FSOptions _options;
    AzBlobClientFactory* _factory;
};

AzBlobFileSystem::AzBlobFileSystem(const FSOptions& options)
        : _options(std::move(options)), _factory(blob_client_factory()) {}

StatusOr<BlobContainerClientPtr> AzBlobFileSystem::new_blob_container_client(const AzBlobURI& uri) {
    // Create azure cloud credential from TCloudConfiguration.cloud_properties
    const auto* t_cloud_configuration = _options.get_cloud_configuration();
    if (t_cloud_configuration == nullptr) {
        return Status::InvalidArgument("CloudConfiguration in FSOption is nullptr");
    }

    const auto& azure_cloud_configuration = CloudConfigurationFactory::create_azure(*t_cloud_configuration);
    const auto& azure_cloud_credential = azure_cloud_configuration.azure_cloud_credential;
    return _factory->new_blob_container_client(azure_cloud_credential, uri);
}

StatusOr<BlockBlobClientUniquePtr> AzBlobFileSystem::new_blob_client(const AzBlobURI& uri) {
    ASSIGN_OR_RETURN(auto container_client, new_blob_container_client(uri));
    return std::make_unique<BlockBlobClient>(container_client->GetBlockBlobClient(uri.blob_name()));
}

StatusOr<std::unique_ptr<RandomAccessFile>> AzBlobFileSystem::new_random_access_file(
        const RandomAccessFileOptions& opts, const std::string& fname) {
    AzBlobURI uri;
    if (!uri.parse(fname)) {
        return Status::InvalidArgument(fmt::format("Invalid azure blob URI: {}", fname));
    }

    ASSIGN_OR_RETURN(auto client, new_blob_client(uri));
    auto input_stream = std::make_unique<AzBlobInputStream>(std::move(client), uri);
    return RandomAccessFile::from(std::move(input_stream), fname, false, opts.encryption_info);
}

StatusOr<std::unique_ptr<SequentialFile>> AzBlobFileSystem::new_sequential_file(const SequentialFileOptions& opts,
                                                                                const std::string& fname) {
    AzBlobURI uri;
    if (!uri.parse(fname)) {
        return Status::InvalidArgument(fmt::format("Invalid azure blob URI: {}", fname));
    }

    ASSIGN_OR_RETURN(auto client, new_blob_client(uri));
    auto input_stream = std::make_unique<AzBlobInputStream>(std::move(client), uri);
    return SequentialFile::from(std::move(input_stream), fname, opts.encryption_info);
}

StatusOr<std::unique_ptr<WritableFile>> AzBlobFileSystem::new_writable_file(const WritableFileOptions& opts,
                                                                            const std::string& fname) {
    if (!fname.empty() && fname.back() == '/') {
        return Status::NotSupported(fmt::format("Azure blob cannot create file with name ended with '/': {}", fname));
    }

    if (opts.mode != FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE && opts.mode != FileSystem::MUST_CREATE) {
        return Status::NotSupported(fmt::format("AzureBlobFileSystem does not support open mode {}", opts.mode));
    }

    AzBlobURI uri;
    if (!uri.parse(fname)) {
        return Status::InvalidArgument(fmt::format("Invalid azure blob URI: {}", fname));
    }

    ASSIGN_OR_RETURN(auto client, new_blob_client(uri));
    auto output_stream = std::make_unique<AzBlobOutputStream>(std::move(client), uri);
    return wrap_encrypted(std::make_unique<OutputStreamAdapter>(std::move(output_stream), fname), opts.encryption_info);
}

std::unique_ptr<FileSystem> new_fs_azblob(const FSOptions& options) {
    return std::make_unique<AzBlobFileSystem>(options);
}

} // namespace starrocks
