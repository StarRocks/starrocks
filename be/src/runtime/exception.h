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

#include <stdexcept>

namespace starrocks {
// Demangling is a very time-consuming operation.
// Sometimes we are not concerned with the specific stack trace of certain runtime_errors.
// Therefore, we use a wrapper class to avoid printing the stack trace.
class RuntimeException final : public std::runtime_error {
public:
    RuntimeException(const std::string& cause) : std::runtime_error(cause) {}
    RuntimeException(const char* cause) : std::runtime_error(cause) {}
};

} // namespace starrocks