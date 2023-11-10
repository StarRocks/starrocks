// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "fs/fs.h"

#include <fmt/format.h>

#include "fs/fs_hdfs.h"
#include "fs/fs_posix.h"
#include "fs/fs_s3.h"
#include "runtime/file_result_writer.h"
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

inline bool is_hdfs_uri(std::string_view uri) {
    return starts_with(uri, "hdfs://");
}

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

const THdfsProperties* FSOptions::hdfs_properties() const {
    if (scan_range_params != nullptr && scan_range_params->__isset.hdfs_properties) {
        return &scan_range_params->hdfs_properties;
    } else if (export_sink != nullptr && export_sink->__isset.hdfs_properties) {
        return &export_sink->hdfs_properties;
    } else if (result_file_options != nullptr) {
        return &result_file_options->hdfs_properties;
    } else if (upload != nullptr && upload->__isset.hdfs_properties) {
        return &upload->hdfs_properties;
    } else if (download != nullptr && download->__isset.hdfs_properties) {
        return &download->hdfs_properties;
    }
    return nullptr;
}

static std::deque<FileWriteStat> file_write_history;
static std::unordered_map<uint64_t, FileWriteStat> file_writes;
static std::mutex file_writes_mutex;

void FileSystem::get_file_write_history(std::vector<FileWriteStat>* stats) {
    std::lock_guard<std::mutex> l(file_writes_mutex);
    stats->assign(file_write_history.begin(), file_write_history.end());
    for (auto& it : file_writes) {
        stats->push_back(it.second);
    }
}

void FileSystem::on_file_write_open(WritableFile* file) {
    std::lock_guard<std::mutex> l(file_writes_mutex);
    FileWriteStat stat;
    stat.path = file->filename();
    stat.open_time = time(nullptr);
    file_writes[reinterpret_cast<uint64_t>(file)] = stat;
}

void FileSystem::on_file_write_close(WritableFile* file) {
    std::lock_guard<std::mutex> l(file_writes_mutex);
    auto it = file_writes.find(reinterpret_cast<uint64_t>(file));
    if (it == file_writes.end()) {
        return;
    }
    it->second.close_time = time(nullptr);
    it->second.size = file->size();
    file_write_history.push_back(it->second);
    file_writes.erase(it);
    while (file_write_history.size() > config::file_write_history_size) {
        file_write_history.pop_front();
    }
}

} // namespace starrocks
