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
#include <memory>
#include <optional>
#include <string>

#include "common/statusor.h"
#include "storage/lake/tablet_metadata.h"

namespace starrocks {
class FileSystem;
}

namespace starrocks::lake {

class LocationProvider;

enum class TabletMetadataStorageFormat : uint8_t { kStandalone, kBundle };

struct ExactTabletMetadataReadLimits {
    uint64_t max_metadata_bytes;
    uint64_t max_bundle_footer_bytes;
};

StatusOr<TabletMetadataPtr> read_bundle_tablet_metadata_page(int64_t tablet_id, int64_t version,
                                                             const std::string& bundle_path,
                                                             const std::shared_ptr<FileSystem>& fs,
                                                             ExactTabletMetadataReadLimits limits);

class ExactTabletMetadataReader final {
public:
    ExactTabletMetadataReader(std::shared_ptr<LocationProvider> location_provider, ExactTabletMetadataReadLimits limits,
                              std::shared_ptr<FileSystem> fs = nullptr);

    StatusOr<TabletMetadataPtr> read(int64_t tablet_id, int64_t version, TabletMetadataStorageFormat format) const;

private:
    StatusOr<TabletMetadataPtr> _read_standalone(int64_t tablet_id, int64_t version) const;
    StatusOr<TabletMetadataPtr> _read_standalone_file(const std::string& path,
                                                      std::optional<int64_t> expected_tablet_id,
                                                      int64_t expected_version) const;
    StatusOr<std::shared_ptr<FileSystem>> _filesystem(const std::string& path) const;

    std::shared_ptr<LocationProvider> _location_provider;
    ExactTabletMetadataReadLimits _limits;
    std::shared_ptr<FileSystem> _fs;
};

} // namespace starrocks::lake
