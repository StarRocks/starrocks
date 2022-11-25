// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "fs/fs.h"

#include <fmt/format.h>

#include "fs/fs_hdfs.h"
#include "fs/fs_posix.h"
#include "fs/fs_s3.h"
#ifdef USE_STAROS
#include "fs/fs_starlet.h"
#endif

namespace starrocks {

static thread_local std::shared_ptr<FileSystem> tls_fs_posix;
static thread_local std::shared_ptr<FileSystem> tls_fs_s3;
static thread_local std::shared_ptr<FileSystem> tls_fs_hdfs;
#ifdef USE_STAROS
static thread_local std::shared_ptr<FileSystem> tls_fs_starlet;
#endif

inline std::shared_ptr<FileSystem> get_tls_fs_hdfs() {
    if (tls_fs_hdfs == nullptr) {
        tls_fs_hdfs.reset(new_fs_hdfs(FSOptions()).release());
    }
    return tls_fs_hdfs;
}

inline std::shared_ptr<FileSystem> get_tls_fs_posix() {
    if (tls_fs_posix == nullptr) {
        tls_fs_posix.reset(new_fs_posix().release());
    }
    return tls_fs_posix;
}

inline std::shared_ptr<FileSystem> get_tls_fs_s3() {
    if (tls_fs_s3 == nullptr) {
        tls_fs_s3.reset(new_fs_s3(FSOptions()).release());
    }
    return tls_fs_s3;
}

#ifdef USE_STAROS
inline std::shared_ptr<FileSystem> get_tls_fs_starlet() {
    if (tls_fs_starlet == nullptr) {
        tls_fs_starlet.reset(new_fs_starlet().release());
    }
    return tls_fs_starlet;
}
#endif

inline bool starts_with(std::string_view s, std::string_view prefix) {
    return (s.size() >= prefix.size()) && (memcmp(s.data(), prefix.data(), prefix.size()) == 0);
}

inline bool is_s3_uri(std::string_view uri) {
    return starts_with(uri, "oss://") || starts_with(uri, "s3n://") || starts_with(uri, "s3a://") ||
           starts_with(uri, "s3://") || starts_with(uri, "cos://") || starts_with(uri, "cosn://") ||
           starts_with(uri, "obs://") || starts_with(uri, "ks3://") || starts_with(uri, "gs://");
}

/*
inline bool is_hdfs_uri(std::string_view uri) {
    return starts_with(uri, "hdfs://") || starts_with(uri, "viewfs://");
}
 */

inline bool is_posix_uri(std::string_view uri) {
    return (memchr(uri.data(), ':', uri.size()) == nullptr) || starts_with(uri, "posix://");
}

StatusOr<std::unique_ptr<FileSystem>> FileSystem::CreateUniqueFromString(std::string_view uri, FSOptions options) {
    if (is_posix_uri(uri)) {
        return new_fs_posix();
    }
    if (is_s3_uri(uri)) {
        return new_fs_s3(options);
    }
#ifdef USE_STAROS
    if (is_starlet_uri(uri)) {
        return new_fs_starlet();
    }
#endif
    // Since almost all famous storage are compatible with Hadoop FileSystem, it's always a choice to fallback using
    // Hadoop FileSystem to access storage.
    return new_fs_hdfs(options);
}

StatusOr<std::shared_ptr<FileSystem>> FileSystem::CreateSharedFromString(std::string_view uri) {
    if (is_posix_uri(uri)) {
        return get_tls_fs_posix();
    }
    if (is_s3_uri(uri)) {
        return get_tls_fs_s3();
    }
#ifdef USE_STAROS
    if (is_starlet_uri(uri)) {
        return get_tls_fs_starlet();
    }
#endif
    // Since almost all famous storage are compatible with Hadoop FileSystem, it's always a choice to fallback using
    // Hadoop FileSystem to access storage.
    return get_tls_fs_hdfs();
}

} // namespace starrocks
