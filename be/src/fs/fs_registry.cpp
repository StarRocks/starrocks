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

#include "fs/fs_registry.h"

#include <algorithm>
#include <atomic>
#include <utility>

#include "fmt/format.h"
#include "fs/fs_posix.h"

namespace starrocks::fs {

namespace {

Status validate_provider(const FileSystemProvider& provider) {
    if (provider.id == nullptr || provider.id[0] == '\0') {
        return Status::InvalidArgument("filesystem provider id must not be empty");
    }
    if ((provider.match_shared == nullptr) != (provider.create_shared == nullptr)) {
        return Status::InvalidArgument(
                fmt::format("filesystem provider {} must define both shared matcher and shared factory", provider.id));
    }
    if ((provider.match_unique == nullptr) != (provider.create_unique == nullptr)) {
        return Status::InvalidArgument(
                fmt::format("filesystem provider {} must define both unique matcher and unique factory", provider.id));
    }
    if (provider.create_shared == nullptr && provider.create_unique == nullptr) {
        return Status::InvalidArgument(
                fmt::format("filesystem provider {} must support shared or unique creation", provider.id));
    }
    return Status::OK();
}

} // namespace

void FrozenFileSystemProviderRegistry::reset(std::vector<FileSystemProvider> providers) {
    _providers = std::move(providers);
    _shared_providers.clear();
    _unique_providers.clear();

    for (const auto& provider : _providers) {
        if (provider.create_shared != nullptr) {
            _shared_providers.push_back(&provider);
        }
        if (provider.create_unique != nullptr) {
            _unique_providers.push_back(&provider);
        }
    }
}

StatusOr<std::shared_ptr<FileSystem>> FrozenFileSystemProviderRegistry::create_shared(std::string_view uri) const {
    for (const auto* provider : _shared_providers) {
        if (provider->match_shared(uri)) {
            return provider->create_shared(uri);
        }
    }
    return Status::NotSupported(fmt::format("no filesystem provider matched uri {}", uri));
}

StatusOr<std::unique_ptr<FileSystem>> FrozenFileSystemProviderRegistry::create_unique(std::string_view uri,
                                                                                      const FSOptions& options) const {
    for (const auto* provider : _unique_providers) {
        if (provider->match_unique(uri, options)) {
            return provider->create_unique(uri, options);
        }
    }
    return Status::NotSupported(fmt::format("no filesystem provider matched uri {}", uri));
}

Status FileSystemProviderRegistry::register_provider(const FileSystemProvider& provider) {
    RETURN_IF_ERROR(validate_provider(provider));

    std::lock_guard<std::mutex> lg(_lock);
    if (_frozen != nullptr) {
        return Status::InternalError("filesystem provider registry is frozen");
    }
    for (const auto& existing : _providers) {
        if (std::string_view(existing.id) == provider.id) {
            return Status::AlreadyExist(fmt::format("filesystem provider {} already registered", provider.id));
        }
    }
    _providers.push_back(provider);
    return Status::OK();
}

const FrozenFileSystemProviderRegistry& FileSystemProviderRegistry::freeze() {
    std::lock_guard<std::mutex> lg(_lock);
    if (_frozen == nullptr) {
        auto frozen = std::make_unique<FrozenFileSystemProviderRegistry>();
        auto providers = _providers;
        std::stable_sort(providers.begin(), providers.end(),
                         [](const FileSystemProvider& lhs, const FileSystemProvider& rhs) {
                             return lhs.priority < rhs.priority;
                         });
        frozen->reset(std::move(providers));
        _frozen = std::move(frozen);
    }
    return *_frozen;
}

namespace {

const FrozenFileSystemProviderRegistry& posix_file_system_provider_registry() {
    static FileSystemProviderRegistry registry;
    static const FrozenFileSystemProviderRegistry* frozen = [] {
        auto st = registry.register_provider(new_posix_file_system_provider());
        CHECK(st.ok()) << st;
        return &registry.freeze();
    }();
    return *frozen;
}

std::atomic<const FrozenFileSystemProviderRegistry*>& installed_file_system_provider_registry() {
    // The default registry is installed during process bootstrap before worker
    // threads start, so relaxed ordering is enough for this pointer handoff.
    static std::atomic<const FrozenFileSystemProviderRegistry*> registry{&posix_file_system_provider_registry()};
    return registry;
}

} // namespace

const FrozenFileSystemProviderRegistry& default_file_system_provider_registry() {
    return *installed_file_system_provider_registry().load(std::memory_order_relaxed);
}

void install_default_file_system_provider_registry(const FrozenFileSystemProviderRegistry& registry) {
    installed_file_system_provider_registry().store(&registry, std::memory_order_relaxed);
}

} // namespace starrocks::fs
