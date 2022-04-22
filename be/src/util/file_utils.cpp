// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/file_utils.cpp

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

#include "util/file_utils.h"

#include <fcntl.h>
#include <fmt/format.h>
#include <openssl/md5.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <filesystem>
#include <iomanip>
#include <sstream>

#include "env/env.h"
#include "gutil/strings/split.h"
#include "gutil/strings/strip.h"
#include "gutil/strings/substitute.h"
#include "util/defer_op.h"

namespace starrocks {

using strings::Substitute;

Status FileUtils::create_dir(Env* env, const std::string& path) {
    if (path.empty()) {
        return Status::InvalidArgument(strings::Substitute("Unknown primitive type($0)", path));
    }
    return env->create_dir_recursive(path);
}

Status FileUtils::create_dir(const std::string& path) {
    ASSIGN_OR_RETURN(auto env, Env::CreateSharedFromString(path));
    return create_dir(env.get(), path);
}

Status FileUtils::remove_all(Env* env, const std::string& file_path) {
    return env->delete_dir_recursive(file_path);
}

Status FileUtils::remove_all(const std::string& file_path) {
    ASSIGN_OR_RETURN(auto env, Env::CreateSharedFromString(file_path));
    return remove_all(env.get(), file_path);
}

Status FileUtils::remove(Env* env, const std::string& dir_path) {
    ASSIGN_OR_RETURN(const bool is_dir, env->is_directory(dir_path));
    return is_dir ? env->delete_dir(dir_path) : env->delete_file(dir_path);
}

Status FileUtils::remove(const std::string& path) {
    ASSIGN_OR_RETURN(auto env, Env::CreateSharedFromString(path));
    return remove(env.get(), path);
}

Status FileUtils::remove_paths(const std::vector<std::string>& paths) {
    for (const std::string& p : paths) {
        RETURN_IF_ERROR(remove(p));
    }
    return Status::OK();
}

Status FileUtils::remove_paths(Env* env, const std::vector<std::string>& paths) {
    for (const std::string& p : paths) {
        RETURN_IF_ERROR(remove(env, p));
    }
    return Status::OK();
}

Status FileUtils::list_files(Env* env, const std::string& dir, std::vector<std::string>* files) {
    auto cb = [files](std::string_view name) -> bool {
        if (!is_dot_or_dotdot(name)) {
            files->emplace_back(name);
        }
        return true;
    };
    return env->iterate_dir(dir, cb);
}

Status FileUtils::list_files(const std::string& dir, std::vector<std::string>* files) {
    ASSIGN_OR_RETURN(auto env, Env::CreateSharedFromString(dir));
    return list_files(env.get(), dir, files);
}

Status FileUtils::list_dirs_files(Env* env, const std::string& path, std::set<std::string>* dirs,
                                  std::set<std::string>* files) {
    auto cb = [path, dirs, files, env](std::string_view name) -> bool {
        if (is_dot_or_dotdot(name)) {
            return true;
        }

        std::string temp_path = fmt::format("{}/{}", path, name);

        auto status_or = env->is_directory(temp_path);
        if (status_or.ok()) {
            if (status_or.value()) {
                if (dirs != nullptr) {
                    dirs->emplace(name);
                }
            } else if (files != nullptr) {
                files->emplace(name);
            }
        } else {
            LOG(WARNING) << "check path " << path << "is directory error: " << status_or.status().to_string();
        }

        return true;
    };

    return env->iterate_dir(path, cb);
}

Status FileUtils::list_dirs_files(const std::string& path, std::set<std::string>* dirs, std::set<std::string>* files) {
    ASSIGN_OR_RETURN(auto env, Env::CreateSharedFromString(path));
    return list_dirs_files(env.get(), path, dirs, files);
}

Status FileUtils::get_children_count(Env* env, const std::string& dir, int64_t* count) {
    auto cb = [count](std::string_view name) -> bool {
        if (!is_dot_or_dotdot(name)) {
            *count += 1;
        }
        return true;
    };
    return env->iterate_dir(dir, cb);
}

bool FileUtils::is_dir(Env* env, const std::string& file_path) {
    const auto status_or = env->is_directory(file_path);
    return status_or.ok() ? status_or.value() : false;
}

Status FileUtils::get_children_count(const std::string& dir, int64_t* count) {
    ASSIGN_OR_RETURN(auto env, Env::CreateSharedFromString(dir));
    return get_children_count(env.get(), dir, count);
}

bool FileUtils::is_dir(const std::string& file_path) {
    auto res = Env::CreateSharedFromString(file_path);
    if (!res.ok()) return false;
    return is_dir(res.value().get(), file_path);
}

// Through proc filesystem
std::string FileUtils::path_of_fd(int fd) {
    const int PATH_SIZE = 256;
    char proc_path[PATH_SIZE];
    snprintf(proc_path, PATH_SIZE, "/proc/self/fd/%d", fd);
    char path[PATH_SIZE];
    if (readlink(proc_path, path, PATH_SIZE) < 0) {
        path[0] = '\0';
    }
    return path;
}

Status FileUtils::split_pathes(const char* path, std::vector<std::string>* path_vec) {
    path_vec->clear();
    *path_vec = strings::Split(path, ";", strings::SkipWhitespace());

    for (std::vector<std::string>::iterator it = path_vec->begin(); it != path_vec->end();) {
        StripWhiteSpace(&(*it));

        it->erase(it->find_last_not_of("/") + 1);
        if (it->size() == 0) {
            it = path_vec->erase(it);
        } else {
            ++it;
        }
    }

    // Check if
    std::sort(path_vec->begin(), path_vec->end());
    if (std::unique(path_vec->begin(), path_vec->end()) != path_vec->end()) {
        std::stringstream ss;
        ss << "Same path in path.[path=" << path << "]";
        return Status::InternalError(ss.str());
    }

    if (path_vec->size() == 0) {
        std::stringstream ss;
        ss << "Size of vector after split is zero.[path=" << path << "]";
        return Status::InternalError(ss.str());
    }

    return Status::OK();
}

Status FileUtils::copy_file(const std::string& src_path, const std::string& dst_path) {
    ASSIGN_OR_RETURN(auto src_env, Env::CreateSharedFromString(src_path));
    ASSIGN_OR_RETURN(auto dst_env, Env::CreateSharedFromString(dst_path));
    ASSIGN_OR_RETURN(auto src_file, src_env->new_sequential_file(src_path));
    ASSIGN_OR_RETURN(auto dst_file, dst_env->new_writable_file(dst_path));
    RETURN_IF_ERROR(copy(src_file.get(), dst_file.get()));
    RETURN_IF_ERROR(dst_file->sync());
    RETURN_IF_ERROR(dst_file->close());
    return Status::OK();
}

StatusOr<int64_t> FileUtils::copy(SequentialFile* src, WritableFile* dest, size_t buff_size) {
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

Status FileUtils::md5sum(const std::string& file, std::string* md5sum) {
    int fd = open(file.c_str(), O_RDONLY);
    if (fd < 0) {
        return Status::InternalError("failed to open file");
    }

    struct stat statbuf;
    if (fstat(fd, &statbuf) < 0) {
        close(fd);
        return Status::InternalError("failed to stat file");
    }
    size_t file_len = statbuf.st_size;
    void* buf = mmap(nullptr, file_len, PROT_READ, MAP_SHARED, fd, 0);
    if (buf == MAP_FAILED) {
        close(fd);
        PLOG(WARNING) << "mmap failed";
        return Status::InternalError("mmap failed");
    }

    unsigned char result[MD5_DIGEST_LENGTH];
    MD5((unsigned char*)buf, file_len, result);
    if (munmap(buf, file_len) != 0) {
        close(fd);
        PLOG(WARNING) << "munmap failed";
        return Status::InternalError("munmap failed");
    }

    std::stringstream ss;
    for (unsigned char i : result) {
        ss << std::setfill('0') << std::setw(2) << std::hex << (int)i;
    }
    ss >> *md5sum;

    close(fd);
    return Status::OK();
}

bool FileUtils::check_exist(Env* env, const std::string& path) {
    return env->path_exists(path).ok();
}

bool FileUtils::check_exist(const std::string& path) {
    auto res = Env::CreateSharedFromString(path);
    if (!res.ok()) return false;
    return check_exist(res.value().get(), path);
}

Status FileUtils::canonicalize(const std::string& path, std::string* real_path) {
    return Env::Default()->canonicalize(path, real_path);
}

} // namespace starrocks
