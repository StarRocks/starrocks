// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "env/env.h"
#include "gen_cpp/Types_types.h"

namespace starrocks {

const static int DEFAULT_TIMEOUT_MS = 10000;

class TBrokerFileStatus;
class TFileBrokerServiceClient;
class TNetworkAddress;

class EnvBroker : public Env {
public:
    // FIXME: |timeout_ms| is unused now.
    EnvBroker(const TNetworkAddress& broker_addr, std::map<std::string, std::string> properties,
              int timeout_ms = DEFAULT_TIMEOUT_MS)
            : _broker_addr(broker_addr), _properties(std::move(properties)), _timeout_ms(timeout_ms) {}

    StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const std::string& path) override;

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const std::string& path) override;

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& path) override;

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const std::string& path) override;

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const WritableFileOptions& opts,
                                                              const std::string& path) override;

    StatusOr<std::unique_ptr<RandomRWFile>> new_random_rw_file(const std::string& path) override;

    StatusOr<std::unique_ptr<RandomRWFile>> new_random_rw_file(const RandomRWFileOptions& opts,
                                                               const std::string& path) override;

    Status path_exists(const std::string& path) override;

    Status get_children(const std::string& dir, std::vector<std::string>* file) override;

    Status iterate_dir(const std::string& dir, const std::function<bool(std::string_view)>& cb) override;

    Status delete_file(const std::string& path) override;

    Status create_dir(const std::string& dirname) override;

    Status create_dir_if_missing(const std::string& dirname, bool* created) override;

    Status delete_dir(const std::string& dirname) override;

    Status delete_dir_recursive(const std::string& dirname) override {
        return Status::NotSupported("EnvBroker::delete_dir_recursive");
    }

    Status sync_dir(const std::string& dirname) override;

    Status is_directory(const std::string& path, bool* is_dir) override;

    Status canonicalize(const std::string& path, std::string* file) override;

    Status get_file_size(const std::string& path, uint64_t* size) override;

    Status get_file_modified_time(const std::string& path, uint64_t* file_mtime) override;

    Status rename_file(const std::string& src, const std::string& target) override;

    Status link_file(const std::string& old_path, const std::string& new_path) override;

#ifdef BE_TEST
    static void TEST_set_broker_client(TFileBrokerServiceClient* client);
#endif

private:
    Status _get_file_size(const std::string& params, uint64_t* size);
    Status _path_exists(const std::string& path);
    Status _delete_file(const std::string& path);
    Status _list_file(const std::string& path, TBrokerFileStatus* stat);

    TNetworkAddress _broker_addr;
    std::map<std::string, std::string> _properties;
    int _timeout_ms;
};

} // namespace starrocks
