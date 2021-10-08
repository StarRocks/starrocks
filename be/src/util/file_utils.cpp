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
#include <openssl/md5.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <algorithm>
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

Status FileUtils::create_dir(const std::string& path, Env* env) {
    if (path.empty()) {
        return Status::InvalidArgument(strings::Substitute("Unknown primitive type($0)", path));
    }

    std::filesystem::path p(path);

    std::string partial_path;
    for (const auto& it : p) {
        partial_path = partial_path + it.string() + "/";
        bool is_dir = false;

        Status s = env->is_directory(partial_path, &is_dir);

        if (s.ok()) {
            if (is_dir) {
                // It's a normal directory.
                continue;
            }

            // Maybe a file or a symlink. Let's try to follow the symlink.
            std::string real_partial_path;
            RETURN_IF_ERROR(env->canonicalize(partial_path, &real_partial_path));

            RETURN_IF_ERROR(env->is_directory(real_partial_path, &is_dir));
            if (is_dir) {
                // It's a symlink to a directory.
                continue;
            } else {
                return Status::IOError(partial_path + " exists but is not a directory");
            }
        }

        RETURN_IF_ERROR(env->create_dir_if_missing(partial_path));
    }

    return Status::OK();
}

Status FileUtils::create_dir(const std::string& dir_path) {
    return create_dir(dir_path, Env::Default());
}

Status FileUtils::remove_all(const std::string& file_path) {
    std::error_code ec;
    std::filesystem::remove_all(file_path, ec);
    if (ec) {
        std::stringstream ss;
        ss << "remove all(" << file_path << ") failed, because: " << ec;
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

Status FileUtils::remove(const std::string& path, starrocks::Env* env) {
    bool is_dir;
    RETURN_IF_ERROR(env->is_directory(path, &is_dir));

    if (is_dir) {
        return env->delete_dir(path);
    } else {
        return env->delete_file(path);
    }
}

Status FileUtils::remove(const std::string& path) {
    return remove(path, Env::Default());
}

Status FileUtils::remove_paths(const std::vector<std::string>& paths) {
    for (const std::string& p : paths) {
        RETURN_IF_ERROR(remove(p));
    }
    return Status::OK();
}

Status FileUtils::list_files(Env* env, const std::string& dir, std::vector<std::string>* files) {
    auto cb = [files](const char* name) -> bool {
        if (!is_dot_or_dotdot(name)) {
            files->push_back(name);
        }
        return true;
    };
    return env->iterate_dir(dir, cb);
}

Status FileUtils::list_dirs_files(const std::string& path, std::set<std::string>* dirs, std::set<std::string>* files,
                                  Env* env) {
    auto cb = [path, dirs, files, env](const char* name) -> bool {
        if (is_dot_or_dotdot(name)) {
            return true;
        }

        std::string temp_path = path + "/" + name;
        bool is_dir;

        auto st = env->is_directory(temp_path, &is_dir);
        if (st.ok()) {
            if (is_dir) {
                if (dirs != nullptr) {
                    dirs->insert(name);
                }
            } else if (files != nullptr) {
                files->insert(name);
            }
        } else {
            LOG(WARNING) << "check path " << path << "is directory error: " << st.to_string();
        }

        return true;
    };

    return env->iterate_dir(path, cb);
}

Status FileUtils::get_children_count(Env* env, const std::string& dir, int64_t* count) {
    auto cb = [count](const char* name) -> bool {
        if (!is_dot_or_dotdot(name)) {
            *count += 1;
        }
        return true;
    };
    return env->iterate_dir(dir, cb);
}

bool FileUtils::is_dir(const std::string& file_path, Env* env) {
    bool ret;
    if (env->is_directory(file_path, &ret).ok()) {
        return ret;
    }

    return false;
}

bool FileUtils::is_dir(const std::string& path) {
    return is_dir(path, Env::Default());
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

Status FileUtils::copy_file(const std::string& src_path, const std::string& dest_path) {
    std::unique_ptr<SequentialFile> src_file;
    RETURN_IF_ERROR(Env::Default()->new_sequential_file(src_path, &src_file));

    std::unique_ptr<WritableFile> dest_file;
    RETURN_IF_ERROR(Env::Default()->new_writable_file(dest_path, &dest_file));

    return copy(src_file.get(), dest_file.get()).status();
}

StatusOr<int64_t> FileUtils::copy(SequentialFile* src, WritableFile* dest, size_t buff_size) {
    char* buf = new char[buff_size];
    std::unique_ptr<char[]> guard(buf);
    int64_t ncopy = 0;
    while (true) {
        Slice read_buf(buf, buff_size);
        RETURN_IF_ERROR(src->read(&read_buf));
        if (read_buf.size == 0) {
            break;
        }
        ncopy += read_buf.size;
        RETURN_IF_ERROR(dest->append(read_buf));
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
        PLOG(WARNING) << "mmap failed";
        return Status::InternalError("mmap failed");
    }

    unsigned char result[MD5_DIGEST_LENGTH];
    MD5((unsigned char*)buf, file_len, result);
    if (munmap(buf, file_len) != 0) {
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

bool FileUtils::check_exist(const std::string& path) {
    return Env::Default()->path_exists(path).ok();
}

bool FileUtils::check_exist(const std::string& path, Env* env) {
    return env->path_exists(path).ok();
}

Status FileUtils::canonicalize(const std::string& path, std::string* real_path) {
    return Env::Default()->canonicalize(path, real_path);
}

} // namespace starrocks
