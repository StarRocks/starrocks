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

#include <string>

#include "arrow/flight/types.h"

namespace starrocks {

const char kBearerDefaultToken[] = "bearertoken";
const char kBasicPrefix[] = "Basic ";
const char kBearerPrefix[] = "Bearer ";
const char kAuthHeader[] = "authorization";

// Function to look in CallHeaders for a key that has a value starting with prefix and
// return the rest of the value after the prefix.
std::string FindKeyValPrefixInCallHeaders(const arrow::flight::CallHeaders& incoming_headers, const std::string& key,
                                          const std::string& prefix);

void ParseBasicHeader(const arrow::flight::CallHeaders& incoming_headers, std::string& username, std::string& password);

} // namespace starrocks
