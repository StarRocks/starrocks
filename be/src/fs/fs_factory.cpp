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

#include "fs/fs_factory.h"

#include "fs/fs.h"
#include "fs/fs_registry.h"

namespace starrocks {

StatusOr<std::shared_ptr<FileSystem>> FileSystemFactory::Create(std::string_view uri, const FSOptions& options) {
    if (!options._fs_options.empty()) {
        return FileSystemFactory::CreateUniqueFromString(uri, options);
    } else {
        return FileSystemFactory::CreateSharedFromString(uri);
    }
}

StatusOr<std::unique_ptr<FileSystem>> FileSystemFactory::CreateUniqueFromString(std::string_view uri,
                                                                                const FSOptions& options) {
    return fs::default_file_system_provider_registry().create_unique(uri, options);
}

StatusOr<std::shared_ptr<FileSystem>> FileSystemFactory::CreateSharedFromString(std::string_view uri) {
    return fs::default_file_system_provider_registry().create_shared(uri);
}

} // namespace starrocks
