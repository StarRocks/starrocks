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

#include "fs/fs_jindo.h"

#include <fmt/format.h>
#include <pwd.h>

#include <filesystem>
#include <fstream>
#include <utility>

#include "common/config.h"
#include "common/s3_uri.h"
#include "common/status.h"
#include "io/jindo_input_stream.h"
#include "io/jindo_output_stream.h"
#include "io/jindo_utils.h"
#include "jindosdk/jdo_api.h"
#include "jindosdk/jdo_cap_def.h"
#include "jindosdk/jdo_file_status.h"
#include "jindosdk/jdo_list_directory_result.h"
#include "jindosdk/jdo_login_user.h"
#include "jindosdk/jdo_options.h"
#include "output_stream_adapter.h"

using namespace fmt::literals;

namespace starrocks {

int JindoSdkConfig::loadConfig(const std::string& config) {
    std::ifstream infile{config};
    std::string line, seg;
    while (std::getline(infile, line)) {
        _text.push_back(line);

        line = trim(line);
        if (line.empty()) {
            // is empty
        } else if (line[0] == '#' || line[0] == ';') {
            // is comment
        } else if (line.size() >= 3 && line.front() == '[' && line.back() == ']') {
            // is section
        } else {
            auto pos = line.find_first_of('=');
            std::string key = trim(line.substr(0, pos));
            std::string value = trim(line.substr(pos + 1));
            _configs[key] = pos == std::string::npos ? std::string() : value;
        }
    }
    return 0;
}

HashMap& JindoSdkConfig::get_configs() {
    return _configs;
}

// trim from start
std::string JindoSdkConfig::lefttrim(const std::string& s) {
    std::string r = s;
    r.erase(r.begin(), std::find_if(r.begin(), r.end(), std::not1(IsSpace())));
    return r;
}

// trim from end
std::string JindoSdkConfig::righttrim(const std::string& s) {
    std::string r = s;
    r.erase(std::find_if(r.rbegin(), r.rend(), std::not1(IsSpace())).base(), r.end());
    return r;
}

// trim from both ends
std::string JindoSdkConfig::trim(const std::string& s) {
    return righttrim(lefttrim(s));
}

bool JindoClientFactory::option_equals(const JdoOptions_t& left, const JdoOptions_t& right) {
    std::string left_endpoint(jdo_getOption(left, OSS_ENDPOINT_KEY, ""));
    std::string right_endpoint(jdo_getOption(right, OSS_ENDPOINT_KEY, ""));
    std::string left_ak_id(jdo_getOption(left, OSS_ACCESS_KEY_ID, ""));
    std::string right_ak_id(jdo_getOption(right, OSS_ACCESS_KEY_ID, ""));
    std::string left_ak_secret(jdo_getOption(left, OSS_ACCESS_KEY_SECRET, ""));
    std::string right_ak_secret(jdo_getOption(right, OSS_ACCESS_KEY_SECRET, ""));
    return left_endpoint == right_endpoint && left_ak_id == right_ak_id && left_ak_secret == right_ak_secret;
}

JindoClientFactory::JindoClientFactory() : _rand((int)::time(nullptr)) {
    std::lock_guard l(_lock);
    std::string jindosdk_conf_path = getenv("STARROCKS_HOME");
    jindosdk_conf_path.append("/conf/jindosdk.cfg");
    if (std::filesystem::exists(jindosdk_conf_path)) {
        std::shared_ptr<JindoSdkConfig> jindo_sdk_config;
        jindo_sdk_config = std::make_shared<JindoSdkConfig>();
        jindo_sdk_config->loadConfig(jindosdk_conf_path);
        _jindo_config_map = jindo_sdk_config->get_configs();
    } else {
        LOG(INFO) << "Jindo config file not found, SDK will be initialized from be.conf";
        _jindo_config_map[OSS_ENDPOINT_KEY] = config::object_storage_endpoint;
        _jindo_config_map[OSS_ACCESS_KEY_ID] = config::object_storage_access_key_id;
        _jindo_config_map[OSS_ACCESS_KEY_SECRET] = config::object_storage_secret_access_key;
    }
}

StatusOr<std::string> JindoClientFactory::get_local_user() {
    uid_t euid;
    int buf_size;
    static struct passwd epwd, *result = nullptr;
    euid = geteuid();
    if ((buf_size = sysconf(_SC_GETPW_R_SIZE_MAX)) == -1) {
        std::string msg = "Invalid input: sysconf function failed to get the configure with key _SC_GETPW_R_SIZE_MAX.";
        return Status::IOError(msg);
    }

    std::vector<char> buffer(buf_size);

    if (getpwuid_r(euid, &epwd, &buffer[0], buf_size, &result) != 0 || !result) {
        std::string msg = "Invalid input: effective user name cannot be found with UID.";
        return Status::IOError(msg);
    }

    static std::string username(epwd.pw_name);
    return username;
}

std::tuple<std::string, std::string, std::string> JindoClientFactory::get_credentials(const S3URI& uri,
                                                                                      const FSOptions& opts) {
    std::string endpoint = "";
    std::string ak_id = "";
    std::string ak_secret = "";
    const THdfsProperties* hdfs_properties = opts.hdfs_properties();
    if ((hdfs_properties != nullptr && hdfs_properties->__isset.cloud_configuration) ||
        (opts.cloud_configuration != nullptr && opts.cloud_configuration->cloud_type != TCloudType::DEFAULT)) {
        const TCloudConfiguration& t_cloud_configuration = (opts.cloud_configuration != nullptr)
                                                                   ? *opts.cloud_configuration
                                                                   : hdfs_properties->cloud_configuration;
        const AliyunCloudConfiguration aliyun_cloud_configuration =
                CloudConfigurationFactory::create_aliyun(t_cloud_configuration);
        const AliyunCloudCredential aliyun_cloud_credential = aliyun_cloud_configuration.aliyun_cloud_credential;
        if (!aliyun_cloud_credential.endpoint.empty()) {
            endpoint = aliyun_cloud_credential.endpoint;
        }
        if (!aliyun_cloud_credential.access_key.empty()) {
            ak_id = aliyun_cloud_credential.access_key;
        }
        if (!aliyun_cloud_credential.secret_key.empty()) {
            ak_secret = aliyun_cloud_credential.secret_key;
        }
    } else if (hdfs_properties != nullptr) {
        if (hdfs_properties->__isset.end_point) {
            endpoint = hdfs_properties->end_point;
        }
        if (hdfs_properties->__isset.access_key) {
            ak_id = hdfs_properties->access_key;
        }
        if (hdfs_properties->__isset.secret_key) {
            ak_secret = hdfs_properties->secret_key;
        }
    }

    if (!uri.endpoint().empty()) {
        endpoint = uri.endpoint();
    }
    return std::make_tuple(endpoint, ak_id, ak_secret);
}

StatusOr<JdoSystem_t> JindoClientFactory::new_client(const S3URI& uri, const FSOptions& opts) {
    std::lock_guard l(_lock);

    auto jdo_options = jdo_createOptions();

    auto [endpoint, access_key, secret_key] = get_credentials(uri, opts);

    if (!access_key.empty() && !secret_key.empty()) {
        jdo_setOption(jdo_options, OSS_ACCESS_KEY_ID, access_key.c_str());
        jdo_setOption(jdo_options, OSS_ACCESS_KEY_SECRET, secret_key.c_str());
    } else {
        for (auto& kv : _jindo_config_map) {
            jdo_setOption(jdo_options, kv.first.c_str(), kv.second.c_str());
        }
    }
    if (!endpoint.empty()) {
        jdo_setOption(jdo_options, OSS_ENDPOINT_KEY, endpoint.c_str());
    }

    std::string uri_prefix = uri.scheme() + "://" + uri.bucket();

    for (size_t i = 0; i < _items; i++) {
        if (option_equals(_configs[i], jdo_options)) {
            LOG(INFO) << "Reuse jindo client for " << uri_prefix << ", index " << i;
            return _clients[i];
        }
    }

    LOG(INFO) << "Creating jindo client for " << uri_prefix;
    JdoSystem_t client = jdo_createSystem(jdo_options, uri_prefix.c_str());
    auto jdo_ctx = jdo_createContext1(client);
    ASSIGN_OR_RETURN(auto user_name, get_local_user())
    auto jdo_login_user = jdo_createLoginUser(user_name.c_str());
    jdo_init(jdo_ctx, jdo_login_user);
    Status init_status = io::check_jindo_status(jdo_ctx);
    if (UNLIKELY(!init_status.ok())) {
        LOG(ERROR) << fmt::format("Failed to init the jindo file system for {} and file {}.", uri_prefix, uri.key());
        if (client != nullptr) {
            LOG(INFO) << "Free invalid jindo client for " << uri_prefix;
            JdoContext_t ctx = jdo_createContext1(client);
            jdo_destroySystem(ctx);
            jdo_freeContext(ctx);
            jdo_freeSystem(client);
        }
        return init_status;
    }
    jdo_freeContext(jdo_ctx);

    if (UNLIKELY(_items >= MAX_CLIENTS_ITEMS)) {
        int idx = _rand.Uniform(MAX_CLIENTS_ITEMS);

        LOG(INFO) << "Free jindo client for " << uri_prefix << ", index " << _items;
        auto old_client = _clients[idx];
        JdoContext_t ctx = jdo_createContext1(old_client);
        jdo_destroySystem(ctx);
        jdo_freeContext(ctx);
        jdo_freeSystem(old_client);

        _configs[idx] = jdo_options;
        _clients[idx] = client;
    } else {
        LOG(INFO) << "Put jindo client for " << uri_prefix << ", index " << _items;
        _configs[_items] = jdo_options;
        _clients[_items] = client;
        _items++;
    }
    return client;
}

StatusOr<std::unique_ptr<RandomAccessFile>> JindoFileSystem::new_random_access_file(const RandomAccessFileOptions& opts,
                                                                                    const std::string& path) {
    S3URI uri;
    if (!uri.parse(path)) {
        return Status::InvalidArgument(fmt::format("Invalid OSS URI: {}", path));
    }

    ASSIGN_OR_RETURN(auto client, JindoClientFactory::instance().new_client(uri, _options))
    auto input_stream = std::make_shared<io::JindoInputStream>(std::move(client), path);
    return std::make_unique<RandomAccessFile>(std::move(input_stream), path);
}

StatusOr<std::unique_ptr<SequentialFile>> JindoFileSystem::new_sequential_file(const SequentialFileOptions& opts,
                                                                               const std::string& path) {
    S3URI uri;
    if (!uri.parse(path)) {
        return Status::InvalidArgument(fmt::format("Invalid OSS URI: {}", path));
    }

    ASSIGN_OR_RETURN(auto client, JindoClientFactory::instance().new_client(uri, _options))
    auto input_stream = std::make_shared<io::JindoInputStream>(std::move(client), path);
    return std::make_unique<SequentialFile>(std::move(input_stream), path);
}

StatusOr<std::unique_ptr<WritableFile>> JindoFileSystem::new_writable_file(const std::string& path) {
    return new_writable_file(WritableFileOptions(), path);
}

StatusOr<std::unique_ptr<WritableFile>> JindoFileSystem::new_writable_file(const WritableFileOptions& opts,
                                                                           const std::string& path) {
    if (!path.empty() && path.back() == '/') {
        return Status::NotSupported(fmt::format("Jindo: cannot create file with name ended with '/': {}", path));
    }
    S3URI uri;
    if (!uri.parse(path)) {
        return Status::InvalidArgument(fmt::format("Invalid OSS URI {}", path));
    }
    ASSIGN_OR_RETURN(auto client, JindoClientFactory::instance().new_client(uri, _options))
    auto output_stream = std::make_unique<io::JindoOutputStream>(std::move(client), path);
    return std::make_unique<OutputStreamAdapter>(std::move(output_stream), path);
}

Status JindoFileSystem::path_exists(const std::string& path) {
    S3URI uri;
    if (!uri.parse(path)) {
        return Status::InvalidArgument(fmt::format("Invalid OSS URI: {}", path));
    }

    ASSIGN_OR_RETURN(auto client, JindoClientFactory::instance().new_client(uri, _options))
    auto jdo_ctx = jdo_createContext1(client);
    bool result = jdo_exists(jdo_ctx, path.c_str());
    Status status = io::check_jindo_status(jdo_ctx);
    if (UNLIKELY(!status.ok())) {
        LOG(ERROR) << "Failed to get jdo_exists for " << path;
        return Status::IOError(path);
    }
    jdo_freeContext(jdo_ctx);
    return result ? Status::OK() : Status::NotFound(path);
}

Status JindoFileSystem::iterate_dir(const std::string& dir, const std::function<bool(std::string_view)>& cb) {
    Status status = path_exists(dir);
    if (!status.ok()) {
        return status;
    }

    S3URI uri;
    if (!uri.parse(dir)) {
        return Status::InvalidArgument(fmt::format("Invalid OSS URI: {}", dir));
    }
    std::string ndir = dir;
    if (ndir.at(ndir.size() - 1) != '/') {
        ndir.append("/");
    }
    JdoListDirectoryResult_t listResult;
    ASSIGN_OR_RETURN(auto client, JindoClientFactory::instance().new_client(uri, _options))
    auto jdo_ctx = jdo_createContext1(client);
    jdo_listDirectory(jdo_ctx, ndir.c_str(), false, &listResult);
    status = io::check_jindo_status(jdo_ctx);
    if (UNLIKELY(!status.ok())) {
        LOG(ERROR) << "Failed to execute jdo_listDirectory for " << dir;
        return Status::IOError(dir);
    }
    jdo_freeContext(jdo_ctx);

    auto num_entries = jdo_getListDirectoryResultSize(listResult);
    for (int i = 0; i < num_entries; i++) {
        auto info = jdo_getListDirectoryFileStatus(listResult, i);
        if (info == nullptr) {
            continue;
        }
        auto full_name = jdo_getFileStatusName(info);
        std::string file_name;
        if (full_name != nullptr) {
            file_name.assign(full_name);
        } else {
            continue;
        }
        file_name = file_name.substr(ndir.size(), file_name.size() - ndir.size());
        if (file_name.back() == '/') {
            file_name.pop_back();
        }
        if (!cb(file_name)) {
            return Status::OK();
        }
    }
    jdo_freeListDirectoryResult(listResult);
    return Status::OK();
}

Status JindoFileSystem::iterate_dir2(const std::string& dir, const std::function<bool(DirEntry)>& cb) {
    Status status = path_exists(dir);
    if (!status.ok()) {
        return status;
    }

    S3URI uri;
    if (!uri.parse(dir)) {
        return Status::InvalidArgument(fmt::format("Invalid OSS URI: {}", dir));
    }
    std::string ndir = dir;
    if (ndir.at(ndir.size() - 1) != '/') {
        ndir.append("/");
    }
    JdoListDirectoryResult_t listResult;
    ASSIGN_OR_RETURN(auto client, JindoClientFactory::instance().new_client(uri, _options))
    auto jdo_ctx = jdo_createContext1(client);
    jdo_listDirectory(jdo_ctx, ndir.c_str(), false, &listResult);
    status = io::check_jindo_status(jdo_ctx);
    if (UNLIKELY(!status.ok())) {
        LOG(ERROR) << "Failed to execute jdo_listDirectory for " << dir;
        return Status::IOError(dir);
    }
    jdo_freeContext(jdo_ctx);

    auto num_entries = jdo_getListDirectoryResultSize(listResult);
    for (int i = 0; i < num_entries; i++) {
        auto info = jdo_getListDirectoryFileStatus(listResult, i);
        if (info == nullptr) {
            continue;
        }
        auto full_name = jdo_getFileStatusName(info);
        std::string file_name;
        if (full_name != nullptr) {
            file_name.assign(full_name);
        } else {
            continue;
        }
        file_name = file_name.substr(ndir.size(), file_name.size() - ndir.size());
        if (file_name.back() == '/') {
            file_name.pop_back();
        }

        DirEntry entry;
        entry.name = file_name;
        entry.size = jdo_getFileStatusFileSize(info);
        entry.mtime = jdo_getFileStatusMtime(info);
        entry.is_dir = jdo_getFileStatusFileType(info) == JDO_FILE_TYPE_DIRECTORY;
        if (!cb(entry)) {
            return Status::OK();
        }
    }
    jdo_freeListDirectoryResult(listResult);
    return Status::OK();
}

Status JindoFileSystem::remove_internal(const std::string& path, bool recursive) {
    Status status = path_exists(path);
    if (!status.ok()) {
        return status;
    }

    S3URI uri;
    if (!uri.parse(path)) {
        return Status::InvalidArgument(fmt::format("Invalid OSS URI: {}", path));
    }
    ASSIGN_OR_RETURN(auto client, JindoClientFactory::instance().new_client(uri, _options))
    auto jdo_ctx = jdo_createContext1(client);
    bool result = jdo_remove(jdo_ctx, path.c_str(), recursive);
    status = io::check_jindo_status(jdo_ctx);
    if (UNLIKELY(!status.ok())) {
        LOG(ERROR) << "Failed to execute jdo_remove for " << path;
        return Status::IOError(path);
    }
    jdo_freeContext(jdo_ctx);
    return result ? Status::OK() : Status::IOError(path);
}

Status JindoFileSystem::delete_file(const std::string& path) {
    return remove_internal(path, false);
}

Status JindoFileSystem::create_dir_internal(const std::string& dirname, bool recursive) {
    Status status = path_exists(dirname);
    if (status.ok()) {
        return Status::AlreadyExist(dirname);
    }

    S3URI uri;
    if (!uri.parse(dirname)) {
        return Status::InvalidArgument(fmt::format("Invalid OSS URI: {}", dirname));
    }
    ASSIGN_OR_RETURN(auto client, JindoClientFactory::instance().new_client(uri, _options))
    auto jdo_ctx = jdo_createContext1(client);
    bool result = jdo_mkdir(jdo_ctx, dirname.c_str(), recursive, 777);
    status = io::check_jindo_status(jdo_ctx);
    if (UNLIKELY(!status.ok())) {
        LOG(ERROR) << "Failed to execute jdo_mkdir for " << dirname;
        return Status::IOError(dirname);
    }
    jdo_freeContext(jdo_ctx);
    return result ? Status::OK() : Status::IOError(dirname);
}

Status JindoFileSystem::create_dir(const std::string& dirname) {
    Status status = create_dir_internal(dirname, false);
    return status;
}

Status JindoFileSystem::create_dir_if_missing(const std::string& dirname, bool* created) {
    Status status = create_dir_internal(dirname, false);
    if (created != nullptr) {
        *created = status.ok();
    }
    if (status.is_already_exist()) {
        status = Status::OK();
    }
    return status;
}

Status JindoFileSystem::create_dir_recursive(const std::string& dirname) {
    Status status = create_dir_internal(dirname, true);
    if (status.is_already_exist()) {
        status = Status::OK();
    }
    return status;
}

Status JindoFileSystem::delete_dir(const std::string& dirname) {
    return remove_internal(dirname, false);
}

Status JindoFileSystem::delete_dir_recursive(const std::string& dirname) {
    return remove_internal(dirname, true);
}

Status JindoFileSystem::sync_dir(const std::string& dirname) {
    // check if 'path' is a directory
    ASSIGN_OR_RETURN(const bool is_dir, is_directory(dirname))
    if (is_dir) {
        return Status::OK();
    } else {
        return Status::IOError(fmt::format("{} is not a directory", dirname));
    }
}

StatusOr<JdoFileStatus_t> JindoFileSystem::get_file_status(const std::string& path) {
    Status status = path_exists(path);
    if (!status.ok()) {
        return status;
    }

    S3URI uri;
    if (!uri.parse(path)) {
        return Status::InvalidArgument(fmt::format("Invalid OSS URI: {}", path));
    }
    ASSIGN_OR_RETURN(auto client, JindoClientFactory::instance().new_client(uri, _options))

    auto jdo_ctx = jdo_createContext1(client);
    bool has_cap_of_symlink = jdo_hasCapOf(jdo_ctx, path.c_str(), JDO_STORE_SYMLINK);
    status = io::check_jindo_status(jdo_ctx);
    if (UNLIKELY(!status.ok())) {
        LOG(ERROR) << "Failed to execute jdo_hasCapOf for " << path;
        return Status::IOError(path);
    }

    JdoFileStatus_t file_status;
    if (!has_cap_of_symlink) {
        jdo_getFileStatus(jdo_ctx, path.c_str(), &file_status);
    } else {
        jdo_getFileLinkStatus(jdo_ctx, path.c_str(), &file_status);
    }
    status = io::check_jindo_status(jdo_ctx);
    if (UNLIKELY(!status.ok())) {
        LOG(ERROR) << "Failed to execute jdo_getFileStatus for " << path;
        return Status::IOError(path);
    }
    jdo_freeContext(jdo_ctx);
    return file_status;
}

StatusOr<bool> JindoFileSystem::is_directory(const std::string& path) {
    ASSIGN_OR_RETURN(auto file_status, get_file_status(path))
    return jdo_getFileStatusFileType(file_status) == JDO_FILE_TYPE_DIRECTORY;
}

StatusOr<uint64_t> JindoFileSystem::get_file_size(const std::string& path) {
    ASSIGN_OR_RETURN(auto file_status, get_file_status(path))
    return jdo_getFileStatusFileSize(file_status);
}

StatusOr<uint64_t> JindoFileSystem::get_file_modified_time(const std::string& path) {
    ASSIGN_OR_RETURN(auto file_status, get_file_status(path))
    return jdo_getFileStatusMtime(file_status);
}

Status JindoFileSystem::rename_file(const std::string& src, const std::string& target) {
    Status status = path_exists(src);
    if (!status.ok()) {
        return status;
    }
    status = path_exists(target);
    if (status.ok()) {
        return Status::AlreadyExist(target);
    }

    S3URI uri;
    if (!uri.parse(src)) {
        return Status::InvalidArgument(fmt::format("Invalid OSS URI: {}", src));
    }
    ASSIGN_OR_RETURN(auto client, JindoClientFactory::instance().new_client(uri, _options))
    auto jdo_ctx = jdo_createContext1(client);
    bool result = jdo_rename(jdo_ctx, src.c_str(), target.c_str());
    status = io::check_jindo_status(jdo_ctx);
    if (UNLIKELY(!status.ok())) {
        LOG(ERROR) << "Failed to execute jdo_rename from " << src << " to " << target;
        return Status::IOError(src);
    }
    jdo_freeContext(jdo_ctx);
    return result ? Status::OK() : Status::IOError(src);
}

StatusOr<SpaceInfo> JindoFileSystem::space(const std::string& path) {
    // check if 'path' is a directory
    ASSIGN_OR_RETURN(const bool is_dir, is_directory(path))
    if (is_dir) {
        return SpaceInfo{.capacity = std::numeric_limits<int64_t>::max(),
                         .free = std::numeric_limits<int64_t>::max(),
                         .available = std::numeric_limits<int64_t>::max()};
    } else {
        return Status::IOError(fmt::format("{} is not a directory", path));
    }
}

std::unique_ptr<FileSystem> new_fs_jindo(const FSOptions& options) {
    return std::make_unique<JindoFileSystem>(options);
}

} // namespace starrocks
