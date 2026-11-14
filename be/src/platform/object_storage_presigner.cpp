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

#include "platform/object_storage_presigner.h"

#include <fmt/format.h>

#include <utility>

#include "fs/fs.h"
#include "fs/fs_s3.h"
#include "fs/fs_scheme.h"
#include "gen_cpp/CloudConfiguration_types.h"

namespace starrocks {

ObjectStoragePresigner::ObjectStoragePresigner(std::unique_ptr<FileSystem> file_system) : _fs(std::move(file_system)) {}

ObjectStoragePresigner::~ObjectStoragePresigner() = default;

StatusOr<std::unique_ptr<ObjectStoragePresigner>> ObjectStoragePresigner::create(
        const TCloudConfiguration& cloud_configuration) {
    if (!cloud_configuration.__isset.cloud_type || cloud_configuration.cloud_type != TCloudType::AWS) {
        return Status::NotSupported("presigned URLs require an S3-compatible cloud configuration");
    }
    return std::unique_ptr<ObjectStoragePresigner>(
            new ObjectStoragePresigner(new_fs_s3(FSOptions(&cloud_configuration))));
}

StatusOr<std::string> ObjectStoragePresigner::presign_read(const std::string& uri, int64_t expiration_seconds) const {
    // The file system is not resolved from the URI, and it would sign a URI of any scheme as an S3 one.
    if (!fs::is_s3_uri(uri)) {
        return Status::InvalidArgument(
                fmt::format("cannot presign '{}': not a URI of an S3-compatible object storage", uri));
    }
    return _fs->presign_read(uri, expiration_seconds);
}

} // namespace starrocks
