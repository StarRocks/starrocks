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

#include <azure/core/http/http_status_code.hpp>

#include "common/status.h"

namespace starrocks {

const std::string kSchemeSeparator("://");
const std::string kHttpScheme("http");
const std::string kHttpsScheme("https");
const std::string kWasbScheme("wasb");
const std::string kWasbsScheme("wasbs");

Status azure_error_to_status(Azure::Core::Http::HttpStatusCode code, std::string_view message, std::string_view object);

} // namespace starrocks
