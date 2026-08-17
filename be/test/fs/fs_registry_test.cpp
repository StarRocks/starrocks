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

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "fs/fs_memory.h"
#include "fs/fs_posix.h"

namespace starrocks {

namespace {

struct RegistryCounters {
    int preferred_shared = 0;
    int preferred_unique = 0;
    int fallback_shared = 0;
    int fallback_unique = 0;
    int shared_mode_hits = 0;
    int unique_mode_hits = 0;
};

RegistryCounters* g_registry_counters = nullptr;

bool match_preferred_shared(std::string_view uri) {
    return uri == "registry://preferred";
}

bool match_preferred_unique(std::string_view uri, const FSOptions&) {
    return uri == "registry://preferred";
}

bool match_fallback_shared(std::string_view uri) {
    return uri == "registry://preferred";
}

bool match_fallback_unique(std::string_view uri, const FSOptions&) {
    return uri == "registry://preferred";
}

bool match_shared_mode(std::string_view uri) {
    return uri == "registry://mode";
}

bool match_unique_mode(std::string_view uri, const FSOptions&) {
    return uri == "registry://mode";
}

StatusOr<std::shared_ptr<FileSystem>> create_preferred_shared(std::string_view) {
    ++g_registry_counters->preferred_shared;
    return std::shared_ptr<FileSystem>(new MemoryFileSystem());
}

StatusOr<std::unique_ptr<FileSystem>> create_preferred_unique(std::string_view, const FSOptions&) {
    ++g_registry_counters->preferred_unique;
    return std::unique_ptr<FileSystem>(new MemoryFileSystem());
}

StatusOr<std::shared_ptr<FileSystem>> create_fallback_shared(std::string_view) {
    ++g_registry_counters->fallback_shared;
    return std::shared_ptr<FileSystem>(new MemoryFileSystem());
}

StatusOr<std::unique_ptr<FileSystem>> create_fallback_unique(std::string_view, const FSOptions&) {
    ++g_registry_counters->fallback_unique;
    return std::unique_ptr<FileSystem>(new MemoryFileSystem());
}

StatusOr<std::shared_ptr<FileSystem>> create_shared_mode(std::string_view) {
    ++g_registry_counters->shared_mode_hits;
    return std::shared_ptr<FileSystem>(new MemoryFileSystem());
}

StatusOr<std::unique_ptr<FileSystem>> create_unique_mode(std::string_view, const FSOptions&) {
    ++g_registry_counters->unique_mode_hits;
    return std::unique_ptr<FileSystem>(new MemoryFileSystem());
}

fs::FileSystemProvider make_shared_only_provider(const char* id, int priority) {
    return {
            .id = id,
            .priority = priority,
            .match_shared = match_shared_mode,
            .create_shared = create_shared_mode,
    };
}

fs::FileSystemProvider make_unique_only_provider(const char* id, int priority) {
    return {
            .id = id,
            .priority = priority,
            .match_unique = match_unique_mode,
            .create_unique = create_unique_mode,
    };
}

class ScopedRegistryCounters {
public:
    explicit ScopedRegistryCounters(RegistryCounters* counters) : _prev(g_registry_counters) {
        g_registry_counters = counters;
    }

    ~ScopedRegistryCounters() { g_registry_counters = _prev; }

private:
    RegistryCounters* _prev;
};

} // namespace

TEST(FSRegistryTest, invalid_provider_is_rejected) {
    fs::FileSystemProviderRegistry registry;

    ASSERT_TRUE(registry.register_provider({}).is_invalid_argument());
    ASSERT_TRUE(registry.register_provider({
                                                   .id = "missing-shared-factory",
                                                   .match_shared = match_shared_mode,
                                           })
                        .is_invalid_argument());
    ASSERT_TRUE(registry.register_provider({
                                                   .id = "missing-unique-matcher",
                                                   .create_unique = create_unique_mode,
                                           })
                        .is_invalid_argument());
    ASSERT_TRUE(registry.register_provider({
                                                   .id = "no-mode",
                                           })
                        .is_invalid_argument());
}

TEST(FSRegistryTest, duplicate_provider_is_rejected) {
    fs::FileSystemProviderRegistry registry;
    ASSERT_OK(registry.register_provider(make_shared_only_provider("duplicate", 10)));

    auto st = registry.register_provider(make_shared_only_provider("duplicate", 20));
    ASSERT_TRUE(st.is_already_exist()) << st;
}

TEST(FSRegistryTest, late_registration_is_rejected_after_freeze) {
    fs::FileSystemProviderRegistry registry;
    ASSERT_OK(registry.register_provider(make_shared_only_provider("shared-only", 10)));
    (void)registry.freeze();

    auto st = registry.register_provider(make_unique_only_provider("late-provider", 20));
    ASSERT_FALSE(st.ok());
}

TEST(FSRegistryTest, priority_resolution_prefers_lower_priority_value) {
    RegistryCounters counters;
    ScopedRegistryCounters scoped_counters(&counters);

    fs::FileSystemProviderRegistry registry;
    ASSERT_OK(registry.register_provider({
            .id = "fallback",
            .priority = 20,
            .match_shared = match_fallback_shared,
            .create_shared = create_fallback_shared,
            .match_unique = match_fallback_unique,
            .create_unique = create_fallback_unique,
    }));
    ASSERT_OK(registry.register_provider({
            .id = "preferred",
            .priority = 10,
            .match_shared = match_preferred_shared,
            .create_shared = create_preferred_shared,
            .match_unique = match_preferred_unique,
            .create_unique = create_preferred_unique,
    }));

    const auto& frozen = registry.freeze();
    ASSERT_OK(frozen.create_shared("registry://preferred").status());
    ASSERT_OK(frozen.create_unique("registry://preferred", FSOptions()).status());

    ASSERT_EQ(1, counters.preferred_shared);
    ASSERT_EQ(1, counters.preferred_unique);
    ASSERT_EQ(0, counters.fallback_shared);
    ASSERT_EQ(0, counters.fallback_unique);
}

TEST(FSRegistryTest, shared_and_unique_modes_use_independent_provider_lists) {
    RegistryCounters counters;
    ScopedRegistryCounters scoped_counters(&counters);

    fs::FileSystemProviderRegistry registry;
    ASSERT_OK(registry.register_provider(make_shared_only_provider("shared-only", 10)));
    ASSERT_OK(registry.register_provider(make_unique_only_provider("unique-only", 20)));

    const auto& frozen = registry.freeze();
    ASSERT_EQ(1, frozen.num_shared_providers());
    ASSERT_EQ(1, frozen.num_unique_providers());
    ASSERT_OK(frozen.create_shared("registry://mode").status());
    ASSERT_OK(frozen.create_unique("registry://mode", FSOptions()).status());

    ASSERT_EQ(1, counters.shared_mode_hits);
    ASSERT_EQ(1, counters.unique_mode_hits);
}

TEST(FSRegistryTest, posix_provider_constructs_shared_and_unique_modes) {
    auto provider = fs::new_posix_file_system_provider(10);
    ASSERT_STREQ("posix", provider.id);
    ASSERT_EQ(10, provider.priority);

    fs::FileSystemProviderRegistry registry;
    ASSERT_OK(registry.register_provider(provider));

    const auto& frozen = registry.freeze();
    ASSERT_EQ(1, frozen.num_shared_providers());
    ASSERT_EQ(1, frozen.num_unique_providers());

    ASSIGN_OR_ABORT(auto shared_fs, frozen.create_shared("posix://"));
    ASSERT_EQ(FileSystem::POSIX, shared_fs->type());

    ASSIGN_OR_ABORT(auto shared_relative_fs, frozen.create_shared("./relative-path"));
    ASSERT_EQ(shared_fs.get(), shared_relative_fs.get());

    ASSIGN_OR_ABORT(auto unique_fs, frozen.create_unique("posix://", FSOptions()));
    ASSERT_EQ(FileSystem::POSIX, unique_fs->type());
    ASSERT_NE(shared_fs.get(), unique_fs.get());

    ASSERT_TRUE(frozen.create_shared("s3://bucket").status().is_not_supported());
}

} // namespace starrocks
