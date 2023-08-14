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

#include <set>
#include <string>
#include <vector>

#include "fs/fs.h"

namespace starrocks::fs {

StatusOr<std::string> md5sum(const std::string& path);

Status list_dirs_files(const std::string& path, std::set<std::string>* dirs, std::set<std::string>* files);

Status list_dirs_files(FileSystem* fs, const std::string& path, std::set<std::string>* dirs,
                       std::set<std::string>* files);

inline StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const std::string& path) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(path));
    return fs->new_sequential_file(path);
}

inline StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const SequentialFileOptions& opts,
                                                                     const std::string& path) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(path));
    return fs->new_sequential_file(opts, path);
}

inline StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const std::string& path) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(path));
    return fs->new_random_access_file(path);
}

inline StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                          const std::string& path) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(path));
    return fs->new_random_access_file(opts, path);
}

inline StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const std::string& path) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(path));
    return fs->new_writable_file(path);
}

inline StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const WritableFileOptions& opts,
                                                                 const std::string& path) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(path));
    return fs->new_writable_file(opts, path);
}

inline Status create_directories(const std::string& path) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(path));
    return fs->create_dir_recursive(path);
}

inline Status sync_dir(const std::string& path) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(path));
    return fs->sync_dir(path);
}

inline Status delete_file(const std::string& path) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(path));
    return fs->delete_file(path);
}

inline Status remove(const std::string& path) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(path));
    ASSIGN_OR_RETURN(auto is_dir, fs->is_directory(path));
    return is_dir ? fs->delete_dir(path) : fs->delete_file(path);
}

inline Status remove(const std::vector<std::string>& paths) {
    for (auto&& path : paths) {
        RETURN_IF_ERROR(remove(path));
    }
    return Status::OK();
}

inline Status remove_all(const std::string& path) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(path));
    return fs->delete_dir_recursive(path);
}

inline Status get_children(const std::string& path, std::vector<std::string>* files) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(path));
    return fs->get_children(path, files);
}

inline StatusOr<bool> is_directory(const std::string& path) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(path));
    return fs->is_directory(path);
}

inline bool path_exist(const std::string& path) {
    auto fs = FileSystem::CreateSharedFromString(path);
    if (!fs.ok()) return false;
    return (*fs)->path_exists(path).ok();
}

// Return the number of bytes copied on success.
inline StatusOr<int64_t> copy(SequentialFile* src, WritableFile* dest, size_t buff_size = 8192) {
    char* buf = new char[buff_size];
    std::unique_ptr<char[]> guard(buf);
    int64_t ncopy = 0;
    while (true) {
        ASSIGN_OR_RETURN(auto nread, src->read(buf, buff_size));
        if (nread == 0) {
            break;
        }
        ncopy += nread;
        RETURN_IF_ERROR(dest->append(Slice(buf, nread)));
    }
    return ncopy;
}

// copy the file from src path to dest path, it will overwrite the existing files
inline Status copy_file(const std::string& src_path, const std::string& dst_path) {
    WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(auto src_fs, FileSystem::CreateSharedFromString(src_path));
    ASSIGN_OR_RETURN(auto dst_fs, FileSystem::CreateSharedFromString(dst_path));
    ASSIGN_OR_RETURN(auto src_file, src_fs->new_sequential_file(src_path));
    ASSIGN_OR_RETURN(auto dst_file, dst_fs->new_writable_file(opts, dst_path));
    RETURN_IF_ERROR(copy(src_file.get(), dst_file.get()));
    RETURN_IF_ERROR(dst_file->close());
    return Status::OK();
}

inline Status canonicalize(const std::string& path, std::string* real_path) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(path));
    return fs->canonicalize(path, real_path);
}

inline bool starts_with(std::string_view s, std::string_view prefix) {
    return (s.size() >= prefix.size()) && (memcmp(s.data(), prefix.data(), prefix.size()) == 0);
}

inline bool is_s3_uri(std::string_view uri) {
    return starts_with(uri, "oss://") || starts_with(uri, "s3n://") || starts_with(uri, "s3a://") ||
           starts_with(uri, "s3://") || starts_with(uri, "cos://") || starts_with(uri, "cosn://") ||
           starts_with(uri, "obs://") || starts_with(uri, "ks3://") || starts_with(uri, "tos://");
}

inline bool is_azure_uri(std::string_view uri) {
    return starts_with(uri, "wasb://") || starts_with(uri, "wasbs://") || starts_with(uri, "adl://") ||
           starts_with(uri, "abfs://") || starts_with(uri, "abfss://");
}

inline bool is_gcs_uri(std::string_view uri) {
    return starts_with(uri, "gs://");
}

inline bool is_hdfs_uri(std::string_view uri) {
    return starts_with(uri, "hdfs://");
}

inline bool is_posix_uri(std::string_view uri) {
    return (memchr(uri.data(), ':', uri.size()) == nullptr) || starts_with(uri, "posix://");
}

} // namespace starrocks::fs
