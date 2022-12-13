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

#include <utility>

#include "common/config.h"
#include "common/s3_uri.h"
#include "common/status.h"
#include "io/jindo_input_stream.h"
#include "jindosdk/jdo_api.h"
#include "jindosdk/jdo_login_user.h"
#include "jindosdk/jdo_options.h"

using namespace fmt::literals;

namespace starrocks {

bool JindoClientFactory::option_equals(const JdoOptions_t& left, const JdoOptions_t& right) {
    std::string l(jdo_getOption(left, OSS_ENDPOINT_KEY, "x"));
    std::string r(jdo_getOption(right, OSS_ENDPOINT_KEY, "y"));
    return l == r;
}

JindoClientFactory::JindoClientFactory() : _rand((int)::time(nullptr)) {}

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

StatusOr<JdoSystem_t> JindoClientFactory::new_client(const S3URI& uri) {
    std::lock_guard l(_lock);

    auto jdo_options = jdo_createOptions();
    jdo_setOption(jdo_options, OSS_PROVIDER_KEY, OSS_PROVIDER_VALUE);
    if (!uri.endpoint().empty()) {
        jdo_setOption(jdo_options, OSS_ENDPOINT_KEY, uri.endpoint().c_str());
    } else {
        jdo_setOption(jdo_options, OSS_ENDPOINT_KEY, config::object_storage_endpoint.c_str());
    }

    std::string uri_prefix = uri.scheme() + "://" + uri.bucket();

    for (size_t i = 0; i < _items; i++) {
        if (option_equals(_configs[i], jdo_options)) {
            return _clients[i];
        }
    }

    JdoSystem_t client = jdo_createSystem(jdo_options, uri_prefix.c_str());
    auto jdo_ctx = jdo_createContext1(client);
    ASSIGN_OR_RETURN(auto user_name, get_local_user())
    auto jdo_login_user = jdo_createLoginUser(user_name.c_str());
    jdo_init(jdo_ctx, jdo_login_user);
    Status init_status = io::check_jindo_status(jdo_ctx);
    if (UNLIKELY(!init_status.ok())) {
        LOG(ERROR) << fmt::format("Failed to init the jindo file system for {} and file {}.", uri_prefix, uri.key());
        if (client != nullptr) {
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
        _configs[idx] = jdo_options;
        _clients[idx] = client;
    } else {
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

    ASSIGN_OR_RETURN(auto client, JindoClientFactory::instance().new_client(uri))
    auto input_stream = std::make_shared<io::JindoInputStream>(std::move(client), path);
    return std::make_unique<RandomAccessFile>(std::move(input_stream), path);
}

std::unique_ptr<FileSystem> new_fs_jindo(const FSOptions& options) {
    return std::make_unique<JindoFileSystem>(options);
}

} // namespace starrocks
