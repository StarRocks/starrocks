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

#include <memory>
#include <string_view>

#include "common/statusor.h"
#include "fs/fs_options.h"

namespace starrocks {

class FileSystem;

class FileSystemFactory {
public:
    static StatusOr<std::shared_ptr<FileSystem>> Create(std::string_view uri, const FSOptions& options);
    static StatusOr<std::unique_ptr<FileSystem>> CreateUniqueFromString(std::string_view uri,
                                                                        const FSOptions& options = FSOptions());
    static StatusOr<std::shared_ptr<FileSystem>> CreateSharedFromString(std::string_view uri);
};

} // namespace starrocks
