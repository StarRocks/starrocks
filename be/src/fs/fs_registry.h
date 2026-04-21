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
#include <mutex>
#include <string_view>
#include <vector>

#include "fs/fs.h"

namespace starrocks::fs {

using FileSystemSharedMatcher = bool (*)(std::string_view uri);
using FileSystemUniqueMatcher = bool (*)(std::string_view uri, const FSOptions& options);
using FileSystemSharedFactory = StatusOr<std::shared_ptr<FileSystem>> (*)(std::string_view uri);
using FileSystemUniqueFactory = StatusOr<std::unique_ptr<FileSystem>> (*)(std::string_view uri,
                                                                          const FSOptions& options);

struct FileSystemProvider {
    // Stable provider identifier. Registration rejects duplicate ids.
    const char* id = "";

    // Providers with lower priority are tried first. Providers with the same
    // priority keep their registration order.
    int priority = 0;

    // A provider may support shared creation, unique creation, or both.
    // Each supported mode must define both its matcher and factory.
    FileSystemSharedMatcher match_shared = nullptr;
    FileSystemSharedFactory create_shared = nullptr;
    FileSystemUniqueMatcher match_unique = nullptr;
    FileSystemUniqueFactory create_unique = nullptr;
};

class FrozenFileSystemProviderRegistry {
public:
    FrozenFileSystemProviderRegistry() = default;

    StatusOr<std::shared_ptr<FileSystem>> create_shared(std::string_view uri) const;
    StatusOr<std::unique_ptr<FileSystem>> create_unique(std::string_view uri, const FSOptions& options) const;

    size_t num_shared_providers() const { return _shared_providers.size(); }
    size_t num_unique_providers() const { return _unique_providers.size(); }

private:
    friend class FileSystemProviderRegistry;

    void reset(std::vector<FileSystemProvider> providers);

    std::vector<FileSystemProvider> _providers;
    std::vector<const FileSystemProvider*> _shared_providers;
    std::vector<const FileSystemProvider*> _unique_providers;
};

class FileSystemProviderRegistry {
public:
    Status register_provider(const FileSystemProvider& provider);
    const FrozenFileSystemProviderRegistry& freeze();

private:
    std::mutex _lock;
    std::vector<FileSystemProvider> _providers;
    std::unique_ptr<FrozenFileSystemProviderRegistry> _frozen;
};

const FrozenFileSystemProviderRegistry& default_file_system_provider_registry();
void install_default_file_system_provider_registry(const FrozenFileSystemProviderRegistry& registry);

} // namespace starrocks::fs
