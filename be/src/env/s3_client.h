// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <aws/s3/S3Client.h>
#include <aws/transfer/TransferManager.h>

#include <memory>

#include "env/cloud_storage_client.h"

namespace starrocks {

class S3Client : public CloudStorageClient {
public:
    S3Client(const Aws::Client::ClientConfiguration& config, bool use_transfer_manager = true);
    ~S3Client();

    /*
     *  Bucket Operation
     */
    virtual Status create_bucket(const std::string& bucket_name) override;

    virtual Status delete_bucket(const std::string& bucket_name) override;

    /*
     *  Object Operation
     */
    virtual Status put_object(const std::string& bucket_name, const std::string& object_key,
                              const std::string& object_path) override;

    virtual Status put_string_object(const std::string& bucket_name, const std::string& object_key,
                                     const std::string& object_value) override;

    virtual Status get_object(const std::string& bucket_name, const std::string& object_key,
                              const std::string& object_path) override;

    virtual Status get_object_range(const std::string& bucket_name, const std::string& object_key,
                                    std::string* object_value, size_t offset, size_t length) override;

    virtual Status exist_object(const std::string& bucket_name, const std::string& object_key, bool* exist) override;

    virtual Status delete_object(const std::string& bucket_name, const std::string& object_key) override;

    virtual Status list_objects(const std::string& bucket_name, const std::string& object_prefix,
                                std::vector<std::string>* result) override;

private:
    // transfer manager's thread pool.
    static const int _thread_pool_thread_number = 16;
    // maximum size of the transfer manager's working buffer to use.
    static const int _transfer_manager_max_buffer_size = 512 * 1024 * 1024; // 256MB
    // maximum size that transfer manager will process in a single request.
    static const int _transfer_manager_single_buffer_size = 32 * 1024 * 1024; // 32MB

    static Aws::Utils::Threading::Executor* _get_transfer_manager_executor() {
        static Aws::Utils::Threading::PooledThreadExecutor executor(_thread_pool_thread_number);
        return &executor;
    }

    bool _is_not_found(const Aws::S3::S3Errors& err) {
        return (err == Aws::S3::S3Errors::NO_SUCH_BUCKET || err == Aws::S3::S3Errors::NO_SUCH_KEY ||
                err == Aws::S3::S3Errors::RESOURCE_NOT_FOUND);
    }

    Aws::Client::ClientConfiguration _config;
    std::shared_ptr<Aws::S3::S3Client> _client;
    std::shared_ptr<Aws::Transfer::TransferManager> _transfer_manager;
};

} // namespace starrocks
