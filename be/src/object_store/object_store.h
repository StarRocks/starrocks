// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <string>

#include "common/statusor.h"
#include "io/output_stream.h"
#include "io/random_access_file.h"

namespace starrocks {

class ObjectStore {
public:
    using FactoryFunc = std::function<StatusOr<std::unique_ptr<ObjectStore>>(std::string_view uri)>;

    ObjectStore() = default;
    virtual ~ObjectStore() = default;

    static StatusOr<std::unique_ptr<ObjectStore>> create_unique_from_uri(std::string_view uri);

    // [NOT thread-safe]
    static void register_store(std::string_view pattern, FactoryFunc func);

    virtual Status create_bucket(const std::string& bucket) = 0;

    virtual Status delete_bucket(const std::string& bucket) = 0;

    virtual StatusOr<std::unique_ptr<io::OutputStream>> put_object(const std::string& bucket,
                                                                   const std::string& object) = 0;

    virtual StatusOr<std::unique_ptr<io::RandomAccessFile>> get_object(const std::string& bucket,
                                                                       const std::string& object) = 0;

    // TODO: Replace |local_path| with an input stream of type `io::InputStream`.
    virtual Status upload_object(const std::string& bucket, const std::string& object,
                                 const std::string& local_path) = 0;

    // TODO: Replace |local_path| with an output stream of type `io::OutputStream`.
    virtual Status download_object(const std::string& bucket, const std::string& object,
                                   const std::string& local_path) = 0;

    virtual Status exist_object(const std::string& bucket, const std::string& object) = 0;

    virtual StatusOr<int64_t> get_object_size(const std::string& bucket, const std::string& object) = 0;

    virtual Status delete_object(const std::string& bucket, const std::string& object) = 0;

    virtual Status list_objects(const std::string& bucket, const std::string& object_prefix,
                                std::vector<std::string>* result) = 0;

    /// Methods based on URI

    virtual StatusOr<std::unique_ptr<io::OutputStream>> put_object(const std::string& uri) = 0;

    virtual StatusOr<std::unique_ptr<io::RandomAccessFile>> get_object(const std::string& uri) = 0;

    virtual StatusOr<int64_t> get_object_size(const std::string& uri) = 0;

    virtual Status exist_object(const std::string& uri) = 0;

    virtual Status delete_object(const std::string& uri) = 0;

    // TODO: Replace |local_path| with an input stream of type `io::InputStream`.
    virtual Status upload_object(const std::string& uri, const std::string& local_path) = 0;

    // TODO: Replace |local_path| with an output stream of type `io::OutputStream`.
    virtual Status download_object(const std::string& uri, const std::string& local_path) = 0;
};

} // namespace starrocks
