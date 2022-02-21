// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <aws/s3/S3Client.h>
#include <aws/transfer/TransferManager.h>

#include <memory>

#include "object_store/object_store.h"

namespace starrocks {

class S3Credential {
public:
    std::string access_key_id;
    std::string secret_access_key;
};

class S3ObjectStore final : public ObjectStore {
public:
    S3ObjectStore(const Aws::Client::ClientConfiguration& config, const S3Credential* cred = nullptr,
                  bool use_transfer_manager = false);
    ~S3ObjectStore() = default;

    /*
     *  Bucket Operation
     */
    Status create_bucket(const std::string& bucket_name) override;

    Status delete_bucket(const std::string& bucket_name) override;

    /*
     *  Object Operation
     */
    Status put_object(const std::string& bucket_name, const std::string& object_key,
                      const std::string& object_path) override;

    Status put_string_object(const std::string& bucket_name, const std::string& object_key,
                             const std::string& object_value) override;

    Status get_object(const std::string& bucket_name, const std::string& object_key,
                      const std::string& object_path) override;

    Status get_object_range(const std::string& bucket_name, const std::string& object_key, size_t offset, size_t length,
                            std::string* object_value, size_t* read_bytes) override;

    Status get_object_range(const std::string& bucket_name, const std::string& object_key, size_t offset, size_t length,
                            char* object_value, size_t* read_bytes) override;

    Status exist_object(const std::string& bucket_name, const std::string& object_key) override;

    Status get_object_size(const std::string& bucket_name, const std::string& object_key, size_t* size) override;

    Status delete_object(const std::string& bucket_name, const std::string& object_key) override;

    Status list_objects(const std::string& bucket_name, const std::string& object_prefix,
                        std::vector<std::string>* result) override;

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

    Status _head_object(const std::string& bucket_name, const std::string& object_key, size_t* size);

    bool _is_not_found(const Aws::S3::S3Errors& err) {
        return (err == Aws::S3::S3Errors::NO_SUCH_BUCKET || err == Aws::S3::S3Errors::NO_SUCH_KEY ||
                err == Aws::S3::S3Errors::RESOURCE_NOT_FOUND);
    }

    Aws::Client::ClientConfiguration _config;
    std::shared_ptr<Aws::S3::S3Client> _client;
    std::shared_ptr<Aws::Transfer::TransferManager> _transfer_manager;
};

} // namespace starrocks
