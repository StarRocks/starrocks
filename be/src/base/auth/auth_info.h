// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstdint>
#include <string>

namespace starrocks {

struct AuthInfo {
    std::string user;
    std::string passwd;
    std::string cluster;
    std::string user_ip;
    // -1 as unset
    int64_t auth_code = -1;
    // Wire values for `auth_type`, kept here so the two spellings live in one place on the BE side.
    // The FE keeps the same pair on AuthCredentialKind.
    static constexpr const char* AUTH_TYPE_BASIC = "basic";
    static constexpr const char* AUTH_TYPE_BEARER = "bearer";

    // Credential shape. Empty (the default) is equivalent to AUTH_TYPE_BASIC and means `passwd`
    // holds a plaintext password; AUTH_TYPE_BEARER means `passwd` holds an IdP token instead. Maps
    // 1:1 to the FE-side AuthCredentialKind, and reaches the FE through the optional `auth_type`
    // thrift field on the requests that declare it: load txn begin, stream load put, and
    // TAuthenticateParams.
    std::string auth_type;
    // Optional cluster-internal trust token used by the rejected_records
    // sync daemon to authenticate its merge-commit RPCs without holding
    // a real password. Empty for normal load paths. Only honored by the
    // FE for system-table loads that are explicitly whitelisted (see
    // FrontendServiceImpl::isAuthorizedByInternalToken).
    std::string internal_token;
};

// Fill the credential fields of any request that carries one from one AuthInfo: the load RPCs and
// the BE HTTP gate's TAuthenticateParams. Every field that only some request types declare is guarded
// by a requires-expression, so the same template serves TLoadTxnBeginRequest (has them all),
// TMergeCommitRequest (no auth_code, no cluster) and TAuthenticateParams (client address is `host`).
// auth_type in particular lives here and nowhere else: a call site that set it by hand would work
// for the one request it remembered and silently drop the token for the next one.
template <class T>
void set_request_auth(T* req, const AuthInfo& auth) {
    if constexpr (requires { req->__set_auth_code(auth.auth_code); }) {
        if (auth.auth_code != -1) {
            // If 'auth_code' is set, no need to set other info.
            req->__set_auth_code(auth.auth_code);
            // User name and passwd is unused, but they are required field.
            // so they have to be set.
            req->__set_user("");
            req->__set_passwd("");
            return;
        }
    }
    // Always through the setters: on a request where user/passwd are optional (TMergeCommitRequest)
    // a plain assignment leaves __isset false and the field is never serialized, so the FE would see
    // a null password and refuse the load.
    req->__set_user(auth.user);
    req->__set_passwd(auth.passwd);
    if constexpr (requires { req->__set_cluster(auth.cluster); }) {
        if (!auth.cluster.empty()) {
            req->__set_cluster(auth.cluster);
        }
    }
    if constexpr (requires { req->__set_user_ip(auth.user_ip); }) {
        req->__set_user_ip(auth.user_ip);
    } else if constexpr (requires { req->__set_host(auth.user_ip); }) {
        req->__set_host(auth.user_ip);
    }
    if constexpr (requires { req->__set_auth_type(auth.auth_type); }) {
        if (!auth.auth_type.empty()) {
            req->__set_auth_type(auth.auth_type);
        }
    }
}

} // namespace starrocks
