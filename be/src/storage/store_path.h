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
#include <utility>

#include "gen_cpp/Types_types.h"

namespace starrocks {

struct StorePath {
    StorePath() = default;
    explicit StorePath(std::string path_) : path(std::move(path_)) {}
    std::string path;
    TStorageMedium::type storage_medium{TStorageMedium::HDD};
};

} // namespace starrocks
