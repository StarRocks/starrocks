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

#pragma once

#include "common/config.h"
#include "fs/fs.h"
#include "gen_cpp/Types_types.h"

namespace starrocks {

const static int DEFAULT_TIMEOUT_MS = config::broker_write_timeout_seconds * 1000;

class TBrokerFileStatus;
class TFileBrokerServiceClient;
class TNetworkAddress;

// TODO: Remove BrokerFileSystem
class BrokerFileSystem : public FileSystem {
public:
    // FIXME: |timeout_ms| is unused now.
    BrokerFileSystem(const TNetworkAddress& broker_addr, std::map<std::string, std::string> properties,
                     int timeout_ms = DEFAULT_TIMEOUT_MS)
            : _broker_addr(broker_addr), _properties(std::move(properties)), _timeout_ms(timeout_ms) {}

    Type type() const override { return BROKER; }

    using FileSystem::new_sequential_file;
    using FileSystem::new_random_access_file;

    StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const SequentialFileOptions& opts,
                                                                  const std::string& path) override;

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& path) override;

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const std::string& path) override;

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const WritableFileOptions& opts,
                                                              const std::string& path) override;

    Status path_exists(const std::string& path) override;

    Status get_children(const std::string& dir, std::vector<std::string>* file) override;

    Status iterate_dir(const std::string& dir, const std::function<bool(std::string_view)>& cb) override;

    Status iterate_dir2(const std::string& dir,
                        const std::function<bool(std::string_view, const FileMeta&)>& cb) override;

    Status delete_file(const std::string& path) override;

    Status create_dir(const std::string& dirname) override;

    Status create_dir_if_missing(const std::string& dirname, bool* created) override;

    Status create_dir_recursive(const std::string& dirname) override;

    Status delete_dir(const std::string& dirname) override;

    Status delete_dir_recursive(const std::string& dirname) override;

    Status sync_dir(const std::string& dirname) override;

    StatusOr<bool> is_directory(const std::string& path) override;

    Status canonicalize(const std::string& path, std::string* file) override;

    StatusOr<uint64_t> get_file_size(const std::string& path) override;

    StatusOr<uint64_t> get_file_modified_time(const std::string& path) override;

    Status rename_file(const std::string& src, const std::string& target) override;

    Status link_file(const std::string& old_path, const std::string& new_path) override;

#ifdef BE_TEST
    static void TEST_set_broker_client(TFileBrokerServiceClient* client);
#endif

private:
    Status _path_exists(const std::string& path);
    Status _delete_file(const std::string& path);
    Status _list_file(const std::string& path, TBrokerFileStatus* stat);

    TNetworkAddress _broker_addr;
    std::map<std::string, std::string> _properties;
    int _timeout_ms;
};

} // namespace starrocks
