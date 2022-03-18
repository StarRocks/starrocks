// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "env/env.h"

#include <fmt/format.h>

#include <type_traits>

#include "env/env_hdfs.h"
#include "env/env_posix.h"
#include "env/env_s3.h"

namespace starrocks {

static thread_local std::shared_ptr<Env> tls_env_posix;
static thread_local std::shared_ptr<Env> tls_env_s3;
static thread_local std::shared_ptr<Env> tls_env_hdfs;

inline std::shared_ptr<Env> get_tls_env_hdfs() {
    if (tls_env_hdfs == nullptr) {
        tls_env_hdfs.reset(new_env_hdfs().release());
    }
    return tls_env_hdfs;
}

inline std::shared_ptr<Env> get_tls_env_posix() {
    if (tls_env_posix == nullptr) {
        tls_env_posix.reset(new_env_posix().release());
    }
    return tls_env_posix;
}

inline std::shared_ptr<Env> get_tls_env_s3() {
    if (tls_env_s3 == nullptr) {
        tls_env_s3.reset(new_env_s3().release());
    }
    return tls_env_s3;
}

inline bool starts_with(std::string_view s, std::string_view prefix) {
    return (s.size() >= prefix.size()) && (memcmp(s.data(), prefix.data(), prefix.size()) == 0);
}

inline bool is_s3_uri(std::string_view uri) {
    return starts_with(uri, "oss://") || starts_with(uri, "s3n://") || starts_with(uri, "s3a://") ||
           starts_with(uri, "s3://");
}

inline bool is_hdfs_uri(std::string_view uri) {
    return starts_with(uri, "hdfs://");
}

inline bool is_posix_uri(std::string_view uri) {
    return (memchr(uri.data(), ':', uri.size()) == nullptr) || starts_with(uri, "posix://");
}

StatusOr<std::unique_ptr<Env>> Env::CreateUniqueFromString(std::string_view uri) {
    if (is_posix_uri(uri)) {
        return new_env_posix();
    }
    if (is_hdfs_uri(uri)) {
        return new_env_hdfs();
    }
    if (is_s3_uri(uri)) {
        return new_env_s3();
    }
    return Status::NotSupported(fmt::format("No Env associated with {}", uri));
}

StatusOr<std::shared_ptr<Env>> Env::CreateSharedFromString(std::string_view uri) {
    if (is_posix_uri(uri)) {
        return get_tls_env_posix();
    }
    if (is_hdfs_uri(uri)) {
        return get_tls_env_hdfs();
    }
    if (is_s3_uri(uri)) {
        return get_tls_env_s3();
    }
    return Status::NotSupported(fmt::format("No Env associated with {}", uri));
}

StatusOr<std::unique_ptr<Env>> Env::CreateUniqueFromStringOrDefault(std::string_view uri) {
    if (is_hdfs_uri(uri)) {
        return new_env_hdfs();
    }
    if (is_s3_uri(uri)) {
        return new_env_s3();
    }
    return new_env_posix();
}

StatusOr<std::shared_ptr<Env>> Env::CreateSharedFromStringOrDefault(std::string_view uri) {
    if (is_hdfs_uri(uri)) {
        return get_tls_env_hdfs();
    }
    if (is_s3_uri(uri)) {
        return get_tls_env_s3();
    }
    return get_tls_env_posix();
}

} // namespace starrocks
