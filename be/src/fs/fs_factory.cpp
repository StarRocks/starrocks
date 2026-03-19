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
#ifndef __APPLE__
#include "fs/azure/fs_azblob.h"
#endif
#include "fs/fs_options_helper.h"
#include "fs/fs_posix.h"
#ifndef __APPLE__
#include "fs/fs_s3.h"
#endif
#include "fs/fs_util.h"
#ifndef __APPLE__
#include "fs/hdfs/fs_hdfs.h"
#endif
#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
#include "fs/fs_starlet.h"
#endif

namespace starrocks {

static thread_local std::shared_ptr<FileSystem> tls_fs_posix;
#ifndef __APPLE__
static thread_local std::shared_ptr<FileSystem> tls_fs_s3;
#endif
#ifndef __APPLE__
static thread_local std::shared_ptr<FileSystem> tls_fs_hdfs;
#endif
#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
static thread_local std::shared_ptr<FileSystem> tls_fs_starlet;
#endif

#ifndef __APPLE__
inline std::shared_ptr<FileSystem> get_tls_fs_hdfs() {
    if (tls_fs_hdfs == nullptr) {
        tls_fs_hdfs.reset(new_fs_hdfs(FSOptions()).release());
    }
    return tls_fs_hdfs;
}
#endif

inline std::shared_ptr<FileSystem> get_tls_fs_posix() {
    if (tls_fs_posix == nullptr) {
        tls_fs_posix.reset(new_fs_posix().release());
    }
    return tls_fs_posix;
}

#ifndef __APPLE__
inline std::shared_ptr<FileSystem> get_tls_fs_s3() {
    if (tls_fs_s3 == nullptr) {
        tls_fs_s3.reset(new_fs_s3(FSOptions()).release());
    }
    return tls_fs_s3;
}
#endif

#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
inline std::shared_ptr<FileSystem> get_tls_fs_starlet() {
    if (tls_fs_starlet == nullptr) {
        tls_fs_starlet.reset(new_fs_starlet().release());
    }
    return tls_fs_starlet;
}
#endif

StatusOr<std::shared_ptr<FileSystem>> FileSystemFactory::Create(std::string_view uri, const FSOptions& options) {
    if (!options._fs_options.empty()) {
        return FileSystemFactory::CreateUniqueFromString(uri, options);
    } else {
        return FileSystemFactory::CreateSharedFromString(uri);
    }
}

StatusOr<std::unique_ptr<FileSystem>> FileSystemFactory::CreateUniqueFromString(std::string_view uri,
                                                                                const FSOptions& options) {
#ifndef __APPLE__
    if (fs::is_fallback_to_hadoop_fs(uri)) {
        return new_fs_hdfs(options);
    }
#endif
    if (fs::is_posix_uri(uri)) {
        return new_fs_posix();
    }
#ifndef __APPLE__
    if (fs::is_s3_uri(uri)) {
        return new_fs_s3(options);
    }
#endif
#ifndef __APPLE__
    if (FSOptionsHelper::azure_use_native_sdk(options) && fs::is_azblob_uri(uri)) {
        return new_fs_azblob(options);
    }
#endif
#ifndef __APPLE__
    if (fs::is_azure_uri(uri) || fs::is_gcs_uri(uri)) {
        // TODO(SmithCruise):
        // Now Azure storage and Google Cloud Storage both are using LibHdfs, we can use cpp sdk instead in the future.
        return new_fs_hdfs(options);
    }
#endif
#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
    if (is_starlet_uri(uri)) {
        return new_fs_starlet();
    }
#endif
    // Since almost all famous storage are compatible with Hadoop FileSystem, it's always a choice to fallback using
    // Hadoop FileSystem to access storage.
#ifndef __APPLE__
    return new_fs_hdfs(options);
#else
    return Status::NotSupported("HDFS not supported");
#endif
}

StatusOr<std::shared_ptr<FileSystem>> FileSystemFactory::CreateSharedFromString(std::string_view uri) {
#ifndef __APPLE__
    if (fs::is_fallback_to_hadoop_fs(uri)) {
        return get_tls_fs_hdfs();
    }
#endif
    if (fs::is_posix_uri(uri)) {
        return get_tls_fs_posix();
    }
#ifndef __APPLE__
    if (fs::is_s3_uri(uri)) {
        return get_tls_fs_s3();
    }
#endif
#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
    if (is_starlet_uri(uri)) {
        return get_tls_fs_starlet();
    }
#endif
    // Since almost all famous storage are compatible with Hadoop FileSystem, it's always a choice to fallback using
    // Hadoop FileSystem to access storage.
#ifndef __APPLE__
    return get_tls_fs_hdfs();
#else
    return Status::NotSupported("HDFS not supported");
#endif
}

} // namespace starrocks
