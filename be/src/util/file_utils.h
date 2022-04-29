// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/file_utils.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <functional>
#include <set>
#include <string>
#include <vector>

#include "common/statusor.h"

namespace starrocks {

class FileSystem;
class SequentialFile;
class WritableFile;

// Return true if file is '.' or '..'
inline bool is_dot_or_dotdot(std::string_view name) {
    return name == "." || name == "..";
}

class FileUtils {
public:
    // Create directory of dir_path with proper FileSystem,
    // This function will create directory recursively,
    // if dir's parent directory doesn't exist
    //
    // RETURNS:
    //  Status::OK()      if create directory success or directory already exists
    static Status create_dir(const std::string& dir_path);

    static Status create_dir(FileSystem* fs, const std::string& dir_path);

    // Delete file recursively.
    static Status remove_all(const std::string& dir_path);

    static Status remove_all(FileSystem* fs, const std::string& dir_path);

    // Delete dir or file, failed when there are files or dirs under the path
    static Status remove(const std::string& path);

    static Status remove(FileSystem* fs, const std::string& path);

    static Status remove_paths(const std::vector<std::string>& paths);

    static Status remove_paths(FileSystem* fs, const std::vector<std::string>& paths);

    // List all files in the specified directory without '.' and '..'.
    // If you want retreive all files, you can use FileSystem::iterate_dir.
    // All valid files will be stored in given *files.
    static Status list_files(const std::string& dir, std::vector<std::string>* files);

    static Status list_files(FileSystem* fs, const std::string& dir, std::vector<std::string>* files);

    // List all dirs and files in the specified directory
    static Status list_dirs_files(const std::string& path, std::set<std::string>* dirs, std::set<std::string>* files);

    static Status list_dirs_files(FileSystem* fs, const std::string& path, std::set<std::string>* dirs,
                                  std::set<std::string>* files);

    // Get the number of children belong to the specified directory, this
    // funciton also exclude '.' and '..'.
    // Return OK with *count is set to the count, if execute successful.
    static Status get_children_count(const std::string& dir, int64_t* count);

    static Status get_children_count(FileSystem* fs, const std::string& dir, int64_t* count);

    // If the file_path is not exist, or is not a dir, return false.
    static bool is_dir(const std::string& file_path);

    static bool is_dir(FileSystem* fs, const std::string& file_path);

    // check path(file or directory) exist
    static bool check_exist(const std::string& path);

    static bool check_exist(FileSystem* fs, const std::string& path);

    // Get file path from fd
    // Return
    //  file path of this fd referenced
    //  "" if this fd is invalid
    static std::string path_of_fd(int fd);

    // split pathes in configue file to path
    // for example
    // "/home/disk1/;/home/disk2"
    // will split to ['/home/disk1', '/home/disk2']
    static Status split_pathes(const char* path, std::vector<std::string>* path_vec);

    // copy the file from src path to dest path, it will overwrite the existing files
    static Status copy_file(const std::string& src_path, const std::string& dest_path);

    // Return the number of bytes copied on success.
    static StatusOr<int64_t> copy(SequentialFile* src, WritableFile* dest, size_t buff_size = 8192);

    // calc md5sum of a local file
    static Status md5sum(const std::string& file, std::string* md5sum);

    // Canonicalize 'path' by applying the following conversions:
    // - Converts a relative path into an absolute one using the cwd.
    // - Converts '.' and '..' references.
    // - Resolves all symbolic links.
    //
    // All directory entries in 'path' must exist on the filesystem.
    static Status canonicalize(const std::string& path, std::string* real_path);
};

} // namespace starrocks
