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
#include <string>

#include "common/status.h"
#include "common/statusor.h"

namespace starrocks {

class FileSystem;
class TCloudConfiguration;

// Presigns the URIs of objects in object storage for reading with the credentials of one cloud configuration,
// through FileSystem::presign_read(). It lets modules that must not depend on the file system layer presign
// objects.
//
// Only S3-compatible storage is supported for now: the S3 file system is used for every URI instead of being
// resolved from the scheme of each one.
class ObjectStoragePresigner {
public:
    // |cloud_configuration| must outlive the presigner.
    static StatusOr<std::unique_ptr<ObjectStoragePresigner>> create(const TCloudConfiguration& cloud_configuration);

    ~ObjectStoragePresigner();

    // Returns a URL that grants read access to |uri| without credentials for |expiration_seconds|.
    StatusOr<std::string> presign_read(const std::string& uri, int64_t expiration_seconds) const;

private:
    explicit ObjectStoragePresigner(std::unique_ptr<FileSystem> file_system);

    std::unique_ptr<FileSystem> _fs;
};

} // namespace starrocks
