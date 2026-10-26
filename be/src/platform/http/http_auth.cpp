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

#include "platform/http/http_auth.h"

#include <boost/algorithm/string.hpp>
#include <cctype>

#include "base/url_coding.h"
#include "platform/http/http_headers.h"
#include "platform/http/http_request.h"

namespace starrocks {

std::string encode_basic_auth(const std::string& user, const std::string& passwd) {
    std::string auth = user + ":" + passwd;
    std::string encoded_auth;
    base64_encode(auth, &encoded_auth);
    static std::string s_prefix = "Basic ";
    return s_prefix + encoded_auth;
}

// Split "<scheme> <value>" the way RFC 7235 reads it: the scheme is case-insensitive, any amount
// of blank separates it from the value, and blanks around the value carry no meaning. Returns false
// when the header does not start with this scheme (so the next scheme can be tried) or has no value.
static bool scheme_value(const std::string& header, const std::string& scheme, std::string* value) {
    std::string h = boost::algorithm::trim_copy(header);
    if (!boost::algorithm::istarts_with(h, scheme) || h.size() == scheme.size() || !std::isspace(h[scheme.size()])) {
        return false; // "Basicxyz" is not the Basic scheme, and a bare "Basic" has no value
    }
    *value = boost::algorithm::trim_copy(h.substr(scheme.size()));
    return !value->empty();
}

bool parse_basic_auth(const HttpRequest& req, std::string* user, std::string* passwd) {
    std::string encoded_str;
    if (!scheme_value(req.header(HttpHeaders::AUTHORIZATION), "Basic", &encoded_str)) {
        return false;
    }
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

// Bearer: the token travels in `passwd` (there is no separate token field on the wire) and there is
// no declared user - the username comes out of the token's claims on the FE side.
static bool parse_bearer_auth(const HttpRequest& req, AuthInfo* auth) {
    std::string token;
    if (!scheme_value(req.header(HttpHeaders::AUTHORIZATION), "Bearer", &token)) {
        return false;
    }
    auth->passwd = std::move(token);
    auth->auth_type = AuthInfo::AUTH_TYPE_BEARER;
    if (req.remote_host() != nullptr) {
        auth->user_ip.assign(req.remote_host());
    } else {
        auth->user_ip.assign("");
    }
    return true;
}

// Dispatch on the Authorization scheme:
//   "Bearer ..."  -> parse_bearer_auth
//   anything else -> parse_basic_auth, leaving auth_type empty, which the FE reads as basic
bool parse_auth(const HttpRequest& req, AuthInfo* auth) {
    if (parse_bearer_auth(req, auth)) {
        return true;
    }
    return parse_basic_auth(req, auth);
}

} // namespace starrocks
