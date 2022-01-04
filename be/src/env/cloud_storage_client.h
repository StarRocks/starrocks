// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <string>

#include "common/status.h"

namespace starrocks {

// base class for cloud storage client
class CloudStorageClient {
public:
    CloudStorageClient() = default;
    virtual ~CloudStorageClient() = default;

    /*
     *  Bucket Operation
     */
    virtual Status create_bucket(const std::string& bucket_name) = 0;

    virtual Status delete_bucket(const std::string& bucket_name) = 0;

    /*
     *  Object Operation
     */
    virtual Status put_object(const std::string& bucket_name, const std::string& object_key,
                              const std::string& object_path) = 0;

    virtual Status put_string_object(const std::string& bucket_name, const std::string& object_key,
                                     const std::string& object_value) = 0;

    virtual Status get_object(const std::string& bucket_name, const std::string& object_key,
                              const std::string& object_path) = 0;

    virtual Status get_object_range(const std::string& bucket_name, const std::string& object_key,
                                    std::string* object_value, size_t offset, size_t length, size_t* read_bytes) = 0;

    // `object_value` should already be allocated at least `length` bytes
    virtual Status get_object_range(const std::string& bucket_name, const std::string& object_key,
                                    char* object_value, size_t offset, size_t length, size_t* read_bytes) = 0;

    virtual Status exist_object(const std::string& bucket_name, const std::string& object_key) = 0;

    virtual Status get_object_size(const std::string& bucket_name, const std::string& object_key, size_t* size) = 0;

    virtual Status delete_object(const std::string& bucket_name, const std::string& object_key) = 0;

    virtual Status list_objects(const std::string& bucket_name, const std::string& object_prefix,
                                std::vector<std::string>* result) = 0;
};

} // namespace starrocks
