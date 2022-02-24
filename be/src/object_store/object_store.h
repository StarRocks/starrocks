// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <string>

#include "common/statusor.h"
#include "io/output_stream.h"
#include "io/random_access_file.h"

namespace starrocks {

class ObjectStore {
public:
    ObjectStore() = default;
    virtual ~ObjectStore() = default;

    virtual Status create_bucket(const std::string& bucket_name) = 0;

    virtual Status delete_bucket(const std::string& bucket_name) = 0;

    virtual StatusOr<std::unique_ptr<io::OutputStream>> put_object(const std::string& bucket_name,
                                                                   const std::string& object_key) = 0;

    virtual StatusOr<std::unique_ptr<io::RandomAccessFile>> get_object(const std::string& bucket_name,
                                                                       const std::string& object_key) = 0;

    virtual Status put_object(const std::string& bucket_name, const std::string& object_key,
                              const std::string& object_path) = 0;

    virtual Status get_object(const std::string& bucket_name, const std::string& object_key,
                              const std::string& object_path) = 0;

    virtual Status exist_object(const std::string& bucket_name, const std::string& object_key) = 0;

    virtual Status get_object_size(const std::string& bucket_name, const std::string& object_key, size_t* size) = 0;

    virtual Status delete_object(const std::string& bucket_name, const std::string& object_key) = 0;

    virtual Status list_objects(const std::string& bucket_name, const std::string& object_prefix,
                                std::vector<std::string>* result) = 0;
};

} // namespace starrocks
