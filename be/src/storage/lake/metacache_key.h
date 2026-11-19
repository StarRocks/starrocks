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

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>

#include "common/statusor.h"

namespace starrocks {
struct FileInfo;
class DelvecPagePB;
} // namespace starrocks

namespace starrocks::lake {
class LocationProvider;

// Metacache accepts only keys made by these factories. Paths are virtual for
// tablet-local objects and resolved for partition-wide objects, independently
// of whether the object was read from a bundled file.
class MetacacheKey {
public:
    // Borrow lvalue paths on the lookup hot path; own temporary paths so keys
    // remain valid when saved by the caller. Borrowed keys must not outlive paths.
    template <typename Path>
    static MetacacheKey tablet_metadata(Path&& path) {
        return from_path(std::forward<Path>(path));
    }
    template <typename Path>
    static MetacacheKey txn_log(Path&& path) {
        return from_path(std::forward<Path>(path));
    }

    static StatusOr<MetacacheKey> combined_txn_log(const LocationProvider& provider, const std::string& path);
    static MetacacheKey shared_initial_metadata(const LocationProvider& provider, const std::string& path);
    static StatusOr<MetacacheKey> bundled_metadata_marker(const LocationProvider& provider, const std::string& root);
    static MetacacheKey latest_tablet_metadata(int64_t tablet_id);
    static MetacacheKey tablet_schema(int64_t tablet_id);
    static MetacacheKey global_schema(int64_t schema_id);
    static MetacacheKey segment(const FileInfo& file);
    static MetacacheKey delvec(int64_t tablet_id, const DelvecPagePB& page);

    std::string_view view() const {
        if (const auto* owned = std::get_if<std::string>(&_value)) {
            return *owned;
        }
        return std::get<std::string_view>(_value);
    }
    bool operator==(const MetacacheKey& other) const { return view() == other.view(); }
    struct Hash {
        size_t operator()(const MetacacheKey& key) const { return std::hash<std::string_view>{}(key.view()); }
    };

#ifdef BE_TEST
    // For tests of raw encodings, collisions and wrong cache value types only.
    template <typename Path>
    static MetacacheKey TEST_from_string(Path&& path) {
        return from_path(std::forward<Path>(path));
    }
#endif

private:
    MetacacheKey(std::in_place_type_t<std::string_view>, std::string_view path) : _value(path) {}
    MetacacheKey(std::in_place_type_t<std::string>, std::string path) : _value(std::move(path)) {}

    template <typename Path>
    static MetacacheKey from_path(Path&& path) {
        if constexpr (std::is_same_v<std::remove_cvref_t<Path>, std::string> && !std::is_lvalue_reference_v<Path>) {
            return MetacacheKey(std::in_place_type<std::string>, std::forward<Path>(path));
        } else {
            return MetacacheKey(std::in_place_type<std::string_view>, std::string_view(path));
        }
    }

    std::variant<std::string_view, std::string> _value;
};
} // namespace starrocks::lake
