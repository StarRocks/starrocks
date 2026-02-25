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

#include "fs/fs.h"

#include <fmt/format.h>

#ifndef __APPLE__
#include "fs/azure/fs_azblob.h"
#endif
#include "fs/bundle_file.h"
#include "fs/encrypt_file.h"
#include "fs/fs_posix.h"
#ifndef __APPLE__
#include "fs/fs_s3.h"
#endif
#include "fs/fs_util.h"
#ifndef __APPLE__
#include "fs/hdfs/fs_hdfs.h"
#endif
#include "runtime/file_result_writer.h"
#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
#include "fs/fs_starlet.h"
#endif

namespace starrocks {

std::unique_ptr<SequentialFile> SequentialFile::from(std::unique_ptr<io::SeekableInputStream> stream,
                                                     const std::string& name, const FileEncryptionInfo& info) {
    if (info.is_encrypted()) {
        return std::make_unique<SequentialFile>(std::make_unique<EncryptSeekableInputStream>(std::move(stream), info),
                                                name);
    } else {
        return std::make_unique<SequentialFile>(std::move(stream), name);
    }
}

std::unique_ptr<RandomAccessFile> RandomAccessFile::from(std::unique_ptr<io::SeekableInputStream> stream,
                                                         const std::string& name, bool is_cache_hit,
                                                         const FileEncryptionInfo& info) {
    if (info.is_encrypted()) {
        return std::make_unique<RandomAccessFile>(std::make_unique<EncryptSeekableInputStream>(std::move(stream), info),
                                                  name, is_cache_hit);
    } else {
        return std::make_unique<RandomAccessFile>(std::move(stream), name, is_cache_hit);
    }
}

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

StatusOr<std::shared_ptr<FileSystem>> FileSystem::Create(std::string_view uri, const FSOptions& options) {
    if (!options._fs_options.empty()) {
        return FileSystem::CreateUniqueFromString(uri, options);
    } else {
        return FileSystem::CreateSharedFromString(uri);
    }
}

StatusOr<std::unique_ptr<FileSystem>> FileSystem::CreateUniqueFromString(std::string_view uri,
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
    if (options.azure_use_native_sdk() && fs::is_azblob_uri(uri)) {
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

StatusOr<std::shared_ptr<FileSystem>> FileSystem::CreateSharedFromString(std::string_view uri) {
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

StatusOr<std::unique_ptr<RandomAccessFile>> FileSystem::new_random_access_file_with_bundling(
        const RandomAccessFileOptions& opts, const FileInfo& file_info) {
    if (file_info.bundle_file_offset.has_value() && file_info.bundle_file_offset.value() >= 0) {
        // If the file is a shared file, we need to create a new random access file with the offset.
        // Notice, we CAN'T pass file_info to new_random_access_file, because size in file_info is not the size of the
        // total bundle file.
        ASSIGN_OR_RETURN(auto file, new_random_access_file(opts, file_info.path));
        auto bundle_file = std::make_unique<BundleSeekableInputStream>(
                file->stream(), file_info.bundle_file_offset.value(), file_info.size.value());
        RETURN_IF_ERROR(bundle_file->init());
        return std::make_unique<RandomAccessFile>(std::move(bundle_file), file->filename(), file->is_cache_hit());
    } else {
        return new_random_access_file(opts, file_info);
    }
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

const TCloudConfiguration* FSOptions::get_cloud_configuration() const {
    if (cloud_configuration != nullptr) {
        return cloud_configuration;
    }

    const THdfsProperties* t_hdfs_properties = hdfs_properties();
    if (t_hdfs_properties != nullptr) {
        return &(t_hdfs_properties->cloud_configuration);
    }

    return nullptr;
}

bool FSOptions::azure_use_native_sdk() const {
#ifndef __APPLE__
    const auto* t_cloud_configuration = get_cloud_configuration();
    if (t_cloud_configuration == nullptr) {
        return false;
    }

    return t_cloud_configuration->__isset.azure_use_native_sdk && t_cloud_configuration->azure_use_native_sdk;
#else
    return false;
#endif
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
