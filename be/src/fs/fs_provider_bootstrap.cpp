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

#include "fs/fs_provider_bootstrap.h"

#ifndef __APPLE__
#include "fs/azure/fs_azblob.h"
#endif
#include "fs/fs_posix.h"
#include "fs/fs_registry.h"
#ifndef __APPLE__
#include "fs/fs_s3.h"
#endif
#ifndef __APPLE__
#include "fs/hdfs/fs_hdfs.h"
#endif
#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
#include "fs/fs_starlet.h"
#endif

namespace starrocks::fs {

Status install_builtin_file_system_providers() {
    static Status status = [] {
        static FileSystemProviderRegistry registry;

        RETURN_IF_ERROR(registry.register_provider(new_posix_file_system_provider()));
#ifndef __APPLE__
        RETURN_IF_ERROR(registry.register_provider(new_hdfs_fallback_file_system_provider()));
#endif
#ifndef __APPLE__
        RETURN_IF_ERROR(registry.register_provider(new_s3_file_system_provider()));
        RETURN_IF_ERROR(registry.register_provider(new_azblob_file_system_provider()));
#endif
#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
        RETURN_IF_ERROR(registry.register_provider(new_starlet_file_system_provider()));
#endif
#ifndef __APPLE__
        RETURN_IF_ERROR(registry.register_provider(new_hdfs_file_system_provider()));
#endif

        install_default_file_system_provider_registry(registry.freeze());
        return Status::OK();
    }();
    return status;
}

} // namespace starrocks::fs
