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

#include "http/http_auth.h"

#include "base/url_coding.h"
#include "http/http_headers.h"
#include "http/http_request.h"

namespace starrocks {

std::string encode_basic_auth(const std::string& user, const std::string& passwd) {
    std::string auth = user + ":" + passwd;
    std::string encoded_auth;
    base64_encode(auth, &encoded_auth);
    static std::string s_prefix = "Basic ";
    return s_prefix + encoded_auth;
}

bool parse_basic_auth(const HttpRequest& req, std::string* user, std::string* passwd) {
    const char k_basic[] = "Basic ";
    auto& auth = req.header(HttpHeaders::AUTHORIZATION);
    if (auth.compare(0, sizeof(k_basic) - 1, k_basic, sizeof(k_basic) - 1) != 0) {
        return false;
    }
    std::string encoded_str = auth.substr(sizeof(k_basic) - 1);
    std::string decoded_auth;
    if (!base64_decode(encoded_str, &decoded_auth)) {
        return false;
    }
    auto pos = decoded_auth.find(':');
    if (pos == std::string::npos) {
        return false;
    }
    user->assign(decoded_auth.c_str(), pos);
    passwd->assign(decoded_auth.c_str() + pos + 1);

    return true;
}

bool parse_basic_auth(const HttpRequest& req, AuthInfo* auth) {
    std::string full_user;
    if (!parse_basic_auth(req, &full_user, &auth->passwd)) {
        return false;
    }
    auto pos = full_user.find('@');
    if (pos != std::string::npos) {
        auth->user.assign(full_user.data(), pos);
        auth->cluster.assign(full_user.data() + pos + 1);
    } else {
        auth->user = full_user;
    }

    if (req.remote_host() != nullptr) {
        auth->user_ip.assign(req.remote_host());
    } else {
        auth->user_ip.assign("");
    }

    return true;
}

} // namespace starrocks
