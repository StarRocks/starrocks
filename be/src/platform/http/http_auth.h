// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
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

#include <string>

#include "base/auth/auth_info.h"

namespace starrocks {

class HttpRequest;

std::string encode_basic_auth(const std::string& user, const std::string& passwd);

// Parse HTTP Basic authorization. Returns true when the request contains valid
// Basic auth credentials.
bool parse_basic_auth(const HttpRequest& req, std::string* user, std::string* passwd);

bool parse_basic_auth(const HttpRequest& req, AuthInfo* auth);

// Parse the Authorization header into AuthInfo, dispatching on the scheme:
//   - "Bearer <token>": the token is carried in AuthInfo::passwd and auth_type is set to
//     AuthInfo::AUTH_TYPE_BEARER; user is left empty (a Bearer credential has no declared user).
//   - "Basic ...": delegates to parse_basic_auth (auth_type left empty == basic).
// Returns false when the header is missing or its scheme is neither Basic nor Bearer.
bool parse_auth(const HttpRequest& req, AuthInfo* auth);

} // namespace starrocks
