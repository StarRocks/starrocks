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

#include "common/s3_uri.h"
#include "fs/fs.h"
#include "jindosdk/jdo_api.h"
#include "jindosdk/jdo_options.h"
#include "util/random.h"

namespace starrocks {

struct IsSpace : std::unary_function<int, bool> {
    bool operator()(int ch) const { return (ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t'); }
};

typedef std::unordered_map<std::string, std::string> HashMap;

class JindoSdkConfig {
public:
    JindoSdkConfig() = default;
    virtual ~JindoSdkConfig() = default;

    int loadConfig(const std::string& config);

    HashMap& get_configs();

private:
    // trim from start
    std::string lefttrim(const std::string& s);

    // trim from end
    std::string righttrim(const std::string& s);

    // trim from both ends
    std::string trim(const std::string& s);

private:
    std::vector<std::string> _text;
    HashMap _configs;
};

std::unique_ptr<FileSystem> new_fs_jindo(const FSOptions& options);

class JindoClientFactory {
public:
    static JindoClientFactory& instance() {
        static JindoClientFactory obj;
        return obj;
    }

    ~JindoClientFactory() = default;

    JindoClientFactory(const JindoClientFactory&) = delete;
    void operator=(const JindoClientFactory&) = delete;
    JindoClientFactory(JindoClientFactory&&) = delete;
    void operator=(JindoClientFactory&&) = delete;

    bool option_equals(const JdoOptions_t& left, const JdoOptions_t& right);
    StatusOr<std::string> get_local_user();
    std::tuple<std::string, std::string, std::string> get_credentials(const S3URI& uri, const FSOptions& opts);
    StatusOr<JdoSystem_t> new_client(const S3URI& uri, const FSOptions& opts);

private:
    JindoClientFactory();

    static constexpr const char* OSS_ACCESS_KEY_ID = "fs.oss.accessKeyId";
    static constexpr const char* OSS_ACCESS_KEY_SECRET = "fs.oss.accessKeySecret";
    static constexpr const char* OSS_ENDPOINT_KEY = "fs.oss.endpoint";
    static constexpr int MAX_CLIENTS_ITEMS = 8;

    std::mutex _lock;
    int _items{0};
    // _configs[i] is the client configuration of _clients[i].
    JdoOptions_t _configs[MAX_CLIENTS_ITEMS];
    JdoSystem_t _clients[MAX_CLIENTS_ITEMS];
    Random _rand;

    HashMap _jindo_config_map;
};

class JindoFileSystem : public FileSystem {
public:
    JindoFileSystem(const FSOptions& options) : _options(options) {}
    ~JindoFileSystem() override = default;

    JindoFileSystem(const JindoFileSystem&) = delete;
    void operator=(const JindoFileSystem&) = delete;
    JindoFileSystem(JindoFileSystem&&) = delete;
    void operator=(JindoFileSystem&&) = delete;

    Type type() const override { return OSS; }

    using FileSystem::new_sequential_file;
    using FileSystem::new_random_access_file;

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& path) override;

    StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const SequentialFileOptions& opts,
                                                                  const std::string& path) override;

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const std::string& path) override;

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const WritableFileOptions& opts,
                                                              const std::string& path) override;

    Status path_exists(const std::string& path) override;

    Status get_children(const std::string& dir, std::vector<std::string>* file) override {
        return Status::NotSupported("JindoFileSystem::get_children");
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
        return Status::NotSupported("JindoFileSystem::canonicalize");
    }

    StatusOr<uint64_t> get_file_size(const std::string& path) override;

    StatusOr<uint64_t> get_file_modified_time(const std::string& path) override;

    Status rename_file(const std::string& src, const std::string& target) override;

    Status link_file(const std::string& old_path, const std::string& new_path) override {
        return Status::NotSupported("JindoFileSystem::link_file");
    }

    StatusOr<SpaceInfo> space(const std::string& path) override;

private:
    FSOptions _options;

    Status create_dir_internal(const std::string& dirname, bool recursive);

    Status remove_internal(const std::string& dirname, bool recursive);

    StatusOr<JdoFileStatus_t> get_file_status(const std::string& path);
};

} // namespace starrocks
