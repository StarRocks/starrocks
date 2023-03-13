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

#include "fs/fs_memory.h"

#include <butil/files/file_path.h>
#include <fmt/format.h>

#include "io/array_input_stream.h"
#include "util/raw_container.h"

namespace starrocks {

enum InodeType {
    kNormal = 0,
    kDir,
};

struct Inode {
    Inode(InodeType t, std::string c) : type(t), data(std::move(c)) {}

    InodeType type = kNormal;
    std::string data;
};

using InodePtr = std::shared_ptr<Inode>;

class MemoryFileInputStream : public io::SeekableInputStreamWrapper {
public:
    explicit MemoryFileInputStream(InodePtr inode)
            : io::SeekableInputStreamWrapper(&_stream, kDontTakeOwnership), _inode(std::move(inode)) {
        _stream.reset(_inode->data.data(), static_cast<int64_t>(_inode->data.size()));
    }

private:
    io::ArrayInputStream _stream;
    InodePtr _inode;
};

class MemoryWritableFile final : public WritableFile {
public:
    MemoryWritableFile(std::string path, InodePtr inode) : _path(std::move(path)), _inode(std::move(inode)) {}

    Status append(const Slice& data) override {
        if (_closed) return Status::IOError(fmt::format("{} has been closed", _path));
        _inode->data.append(data.data, data.size);
        return Status::OK();
    }

    Status appendv(const Slice* data, size_t cnt) override {
        for (size_t i = 0; i < cnt; i++) {
            RETURN_IF_ERROR(append(data[i]));
        }
        return Status::OK();
    }

    Status pre_allocate(uint64_t size) override {
        if (_closed) return Status::IOError(fmt::format("{} has been closed", _path));
        _inode->data.reserve(size);
        return Status::OK();
    }

    Status close() override {
        _closed = true;
        return Status::OK();
    }

    Status flush(FlushMode mode) override { return Status::OK(); }

    Status sync() override { return Status::OK(); }

    uint64_t size() const override { return _inode->data.size(); }

    const std::string& filename() const override { return _path; }

private:
    std::string _path;
    InodePtr _inode;
    bool _closed{false};
};

class EnvMemoryImpl {
public:
    EnvMemoryImpl() {
        // init root directory.
        _namespace["/"] = std::make_shared<Inode>(kDir, "");
    }

    StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const butil::FilePath& path) {
        auto iter = _namespace.find(path.value());
        if (iter == _namespace.end()) {
            return Status::NotFound(path.value());
        } else {
            auto stream = std::make_shared<MemoryFileInputStream>(iter->second);
            return std::make_unique<SequentialFile>(std::move(stream), path.value());
        }
    }

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const butil::FilePath& path) {
        auto iter = _namespace.find(path.value());
        if (iter == _namespace.end()) {
            return Status::NotFound(path.value());
        } else {
            auto stream = std::make_unique<MemoryFileInputStream>(iter->second);
            return std::make_unique<RandomAccessFile>(std::move(stream), path.value());
        }
    }

    template <typename DerivedType, typename BaseType>
    StatusOr<std::unique_ptr<BaseType>> new_writable_file(FileSystem::OpenMode mode, const butil::FilePath& path) {
        InodePtr inode = get_inode(path);
        if (mode == FileSystem::MUST_EXIST && inode == nullptr) {
            return Status::NotFound(path.value());
        }
        if (mode == FileSystem::MUST_CREATE && inode != nullptr) {
            return Status::AlreadyExist(path.value());
        }
        if (mode == FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE && inode != nullptr) {
            inode->data.clear();
        }
        if (inode == nullptr && !path_exists(path.DirName()).ok()) {
            return Status::NotFound("parent directory not exist");
        }
        if (inode == nullptr) {
            assert(mode != FileSystem::MUST_EXIST);
            inode = std::make_shared<Inode>(kNormal, "");
            _namespace[path.value()] = inode;
        } else if (inode->type != kNormal) {
            return Status::IOError(path.value() + " is a directory");
        }
        return std::make_unique<DerivedType>(path.value(), std::move(inode));
    }

    Status path_exists(const butil::FilePath& path) {
        return get_inode(path) != nullptr ? Status::OK() : Status::NotFound(path.value());
    }

    Status get_children(const butil::FilePath& path, std::vector<std::string>* file) {
        return iterate_dir(path, [&](std::string_view filename) -> bool {
            file->emplace_back(filename);
            return true;
        });
    }

    Status iterate_dir(const butil::FilePath& path, const std::function<bool(std::string_view)>& cb) {
        auto inode = get_inode(path);
        if (inode == nullptr || inode->type != kDir) {
            return Status::NotFound(path.value());
        }
        DCHECK(path.value().back() != '/' || path.value() == "/");
        std::string s = (path.value() == "/") ? path.value() : path.value() + "/";
        for (auto iter = _namespace.lower_bound(s); iter != _namespace.end(); ++iter) {
            Slice child(iter->first);
            if (!child.starts_with(s)) {
                break;
            }
            // Get the relative path.
            child.remove_prefix(s.size());
            if (child.empty()) {
                continue;
            }
            auto slash = (const char*)memchr(child.data, '/', child.size);
            if (slash != nullptr) {
                continue;
            }
            if (!cb(child.data)) {
                break;
            }
        }
        return Status::OK();
    }

    Status iterate_dir2(const butil::FilePath& path, const std::function<bool(std::string_view, const FileMeta&)>& cb) {
        auto inode = get_inode(path);
        if (inode == nullptr || inode->type != kDir) {
            return Status::NotFound(path.value());
        }
        DCHECK(path.value().back() != '/' || path.value() == "/");
        std::string s = (path.value() == "/") ? path.value() : path.value() + "/";
        for (auto iter = _namespace.lower_bound(s); iter != _namespace.end(); ++iter) {
            Slice child(iter->first);
            if (!child.starts_with(s)) {
                break;
            }
            FileMeta meta;
            ASSIGN_OR_RETURN(bool is_dir, is_directory(butil::FilePath(child.to_string())));
            meta.set_is_dir(is_dir);
            if (!is_dir) {
                ASSIGN_OR_RETURN(int64_t size, get_file_size(butil::FilePath(child.to_string())));
                meta.set_size(size);
            }
            // Get the relative path.
            child.remove_prefix(s.size());
            if (child.empty()) {
                continue;
            }
            auto slash = (const char*)memchr(child.data, '/', child.size);
            if (slash != nullptr) {
                continue;
            }
            if (!cb(child.data, meta)) {
                break;
            }
        }
        return Status::OK();
    }

    Status delete_file(const butil::FilePath& path) {
        auto iter = _namespace.find(path.value());
        if (iter == _namespace.end() || iter->second->type != kNormal) {
            return Status::NotFound(path.value());
        }
        _namespace.erase(iter);
        return Status::OK();
    }

    Status create_dir(const butil::FilePath& dirname) {
        if (get_inode(dirname) != nullptr) {
            return Status::AlreadyExist(dirname.value());
        }
        if (get_inode(dirname.DirName()) == nullptr) {
            return Status::NotFound("parent directory not exist");
        }
        _namespace[dirname.value()] = std::make_shared<Inode>(kDir, "");
        return Status::OK();
    }

    Status create_dir_if_missing(const butil::FilePath& dirname, bool* created) {
        auto inode = get_inode(dirname);
        if (inode != nullptr && inode->type == kDir) {
            *created = false;
            return Status::OK();
        } else if (inode != nullptr) {
            return Status::AlreadyExist(dirname.value());
        } else if (get_inode(dirname.DirName()) == nullptr) {
            return Status::NotFound("parent directory not exist");
        } else {
            *created = true;
            _namespace[dirname.value()] = std::make_shared<Inode>(kDir, "");
            return Status::OK();
        }
    }

    Status create_dir_recursive(const butil::FilePath& dirname) {
        std::vector<std::string> components;
        dirname.GetComponents(&components);
        std::string path;
        bool created;
        for (auto&& e : components) {
            if (path.empty()) {
                path = e;
            } else if (path.back() == '/') {
                path = fmt::format("{}{}", path, e);
            } else {
                path = fmt::format("{}/{}", path, e);
            }
            RETURN_IF_ERROR(create_dir_if_missing(butil::FilePath(path), &created));
        }
        return Status::OK();
    }

    Status delete_dir(const butil::FilePath& dirname) {
        bool empty_dir = true;
        RETURN_IF_ERROR(iterate_dir(dirname, [&](std::string_view) -> bool {
            empty_dir = false;
            return false;
        }));
        if (!empty_dir) {
            return Status::IOError("directory not empty");
        }
        _namespace.erase(dirname.value());
        return Status::OK();
    }

    Status delete_dir_recursive(const butil::FilePath& dirname) {
        auto inode = get_inode(dirname);
        if (inode == nullptr || inode->type != kDir) {
            return Status::NotFound(dirname.value());
        }
        DCHECK(dirname.value().back() != '/' || dirname.value() == "/");
        std::string s = (dirname.value() == "/") ? dirname.value() : dirname.value() + "/";
        for (auto iter = _namespace.lower_bound(s); iter != _namespace.end(); /**/) {
            Slice child(iter->first);
            if (!child.starts_with(s)) {
                break;
            }
            iter = _namespace.erase(iter);
        }
        _namespace.erase(dirname.value());
        return Status::OK();
    }

    StatusOr<bool> is_directory(const butil::FilePath& path) {
        auto inode = get_inode(path);
        if (inode == nullptr) {
            return Status::NotFound(path.value());
        }
        return inode->type == kDir;
    }

    StatusOr<uint64_t> get_file_size(const butil::FilePath& path) {
        auto inode = get_inode(path);
        if (inode == nullptr || inode->type != kNormal) {
            return Status::NotFound("not exist or is a directory");
        }
        return inode->data.size();
    }

    Status rename_file(const butil::FilePath& src, const butil::FilePath& target) {
        Slice s1(src.value());
        Slice s2(target.value());
        if (s2.starts_with(s1) && s2.size != s1.size) {
            return Status::InvalidArgument("cannot make a directory a subdirectory of itself");
        }
        auto src_inode = get_inode(src);
        auto dst_inode = get_inode(target);
        if (src_inode == nullptr) {
            return Status::NotFound(src.value());
        }
        auto dst_parent = get_inode(target.DirName());
        if (dst_parent == nullptr || dst_parent->type != kDir) {
            return Status::NotFound(target.DirName().value());
        }
        if (dst_inode != nullptr) {
            if (src_inode->type == kNormal && dst_inode->type == kDir) {
                return Status::IOError("target is an existing directory, but source is not a directory");
            }
            if (src_inode->type == kDir && dst_inode->type == kNormal) {
                return Status::IOError("source is a directory, but target is not a directory");
            }
            // |src| and |target| referring to the same file
            if (src_inode.get() == dst_inode.get()) {
                return Status::OK();
            }
            if (dst_inode->type == kDir && !_is_directory_empty(target)) {
                return Status::IOError("target is a nonempty directory");
            }
        }
        _namespace[target.value()] = src_inode;
        if (src_inode->type == kDir) {
            std::vector<std::string> children;
            Status st = get_children(src, &children);
            LOG_IF(FATAL, !st.ok()) << st.to_string();
            for (const auto& s : children) {
                butil::FilePath src_child_path = src.Append(s);
                butil::FilePath dst_child_path = target.Append(s);
                st = rename_file(src_child_path, dst_child_path);
                LOG_IF(FATAL, !st.ok()) << st.to_string();
            }
        }
        _namespace.erase(src.value());
        return Status::OK();
    }

    Status link_file(const butil::FilePath& old_path, const butil::FilePath& new_path) {
        auto old_inode = get_inode(old_path);
        auto new_inode = get_inode(new_path);
        if (new_inode != nullptr) {
            return Status::AlreadyExist(new_path.value());
        }
        if (old_inode == nullptr) {
            return Status::NotFound(old_path.value());
        }
        if (get_inode(new_path.DirName()) == nullptr) {
            return Status::NotFound(new_path.value());
        }
        _namespace[new_path.value()] = old_inode;
        return Status::OK();
    }

private:
    // prerequisite: |path| exist and is a directory.
    bool _is_directory_empty(const butil::FilePath& path) {
        bool empty_dir = true;
        Status st = iterate_dir(path, [&](std::string_view) -> bool {
            empty_dir = false;
            return false;
        });
        CHECK(st.ok()) << st.to_string();
        return empty_dir;
    }

    // Returns nullptr if |path| does not exists.
    InodePtr get_inode(const butil::FilePath& path) {
        auto iter = _namespace.find(path.value());
        return iter == _namespace.end() ? nullptr : iter->second;
    }

    template <typename K, typename V>
    using OrderedMap = std::map<K, V>;

    OrderedMap<std::string, InodePtr> _namespace;
};

MemoryFileSystem::MemoryFileSystem() : _impl(new EnvMemoryImpl()) {}

MemoryFileSystem::~MemoryFileSystem() {
    delete _impl;
}

StatusOr<std::unique_ptr<SequentialFile>> MemoryFileSystem::new_sequential_file(const SequentialFileOptions& opts,
                                                                                const std::string& path) {
    (void)opts;
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(path, &new_path));
    return _impl->new_sequential_file(butil::FilePath(new_path));
}

StatusOr<std::unique_ptr<RandomAccessFile>> MemoryFileSystem::new_random_access_file(
        const RandomAccessFileOptions& opts, const std::string& path) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(path, &new_path));
    return _impl->new_random_access_file(opts, butil::FilePath(new_path));
}

StatusOr<std::unique_ptr<WritableFile>> MemoryFileSystem::new_writable_file(const std::string& path) {
    return new_writable_file(WritableFileOptions(), path);
}

StatusOr<std::unique_ptr<WritableFile>> MemoryFileSystem::new_writable_file(const WritableFileOptions& opts,
                                                                            const std::string& path) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(path, &new_path));
    return _impl->new_writable_file<MemoryWritableFile, WritableFile>(opts.mode, butil::FilePath(new_path));
}

Status MemoryFileSystem::path_exists(const std::string& path) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(path, &new_path));
    return _impl->path_exists(butil::FilePath(new_path));
}

Status MemoryFileSystem::get_children(const std::string& dir, std::vector<std::string>* file) {
    std::string new_path;
    file->clear();
    RETURN_IF_ERROR(canonicalize(dir, &new_path));
    return _impl->get_children(butil::FilePath(new_path), file);
}

Status MemoryFileSystem::iterate_dir(const std::string& dir, const std::function<bool(std::string_view)>& cb) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(dir, &new_path));
    return _impl->iterate_dir(butil::FilePath(new_path), cb);
}

Status MemoryFileSystem::iterate_dir2(const std::string& dir,
                                      const std::function<bool(std::string_view, const FileMeta&)>& cb) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(dir, &new_path));
    return _impl->iterate_dir2(butil::FilePath(new_path), cb);
}

Status MemoryFileSystem::delete_file(const std::string& path) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(path, &new_path));
    return _impl->delete_file(butil::FilePath(new_path));
}

Status MemoryFileSystem::create_dir(const std::string& dirname) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(dirname, &new_path));
    return _impl->create_dir(butil::FilePath(new_path));
}

Status MemoryFileSystem::create_dir_if_missing(const std::string& dirname, bool* created) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(dirname, &new_path));
    return _impl->create_dir_if_missing(butil::FilePath(new_path), created);
}

Status MemoryFileSystem::create_dir_recursive(const std::string& dirname) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(dirname, &new_path));
    return _impl->create_dir_recursive(butil::FilePath(new_path));
}

Status MemoryFileSystem::delete_dir(const std::string& dirname) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(dirname, &new_path));
    return _impl->delete_dir(butil::FilePath(new_path));
}

Status MemoryFileSystem::delete_dir_recursive(const std::string& dirname) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(dirname, &new_path));
    return _impl->delete_dir_recursive(butil::FilePath(new_path));
}

Status MemoryFileSystem::sync_dir(const std::string& dirname) {
    return Status::OK();
}

StatusOr<bool> MemoryFileSystem::is_directory(const std::string& path) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(path, &new_path));
    return _impl->is_directory(butil::FilePath(new_path));
}

Status MemoryFileSystem::canonicalize(const std::string& path, std::string* file) {
    if (path.empty() || path[0] != '/') {
        return Status::InvalidArgument("Invalid path");
    }
    // fast path
    if (path.find('.') == std::string::npos && path.find("//") == std::string::npos) {
        *file = path;
        if (file->size() > 1 && file->back() == '/') {
            file->pop_back();
        }
        return Status::OK();
    }
    // slot path
    butil::FilePath file_path(path);
    std::vector<std::string> components;
    file_path.GetComponents(&components);
    std::vector<std::string> normalized_components;
    components.erase(components.begin());
    for (auto& s : components) {
        if (s == "..") {
            if (!normalized_components.empty()) {
                normalized_components.pop_back();
            }
        } else if (s == ".") {
            continue;
        } else {
            normalized_components.emplace_back(std::move(s));
        }
    }
    butil::FilePath final_path("/");
    for (const auto& s : normalized_components) {
        final_path = final_path.Append(s);
    }
    *file = final_path.value();
    return Status::OK();
}

StatusOr<uint64_t> MemoryFileSystem::get_file_size(const std::string& path) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(path, &new_path));
    return _impl->get_file_size(butil::FilePath(new_path));
}

StatusOr<uint64_t> MemoryFileSystem::get_file_modified_time(const std::string& path) {
    return Status::NotSupported("get_file_modified_time");
}

Status MemoryFileSystem::rename_file(const std::string& src, const std::string& target) {
    std::string new_src_path;
    std::string new_dst_path;
    RETURN_IF_ERROR(canonicalize(src, &new_src_path));
    RETURN_IF_ERROR(canonicalize(target, &new_dst_path));
    return _impl->rename_file(butil::FilePath(new_src_path), butil::FilePath(new_dst_path));
}

Status MemoryFileSystem::link_file(const std::string& old_path, const std::string& new_path) {
    std::string new_src_path;
    std::string new_dst_path;
    RETURN_IF_ERROR(canonicalize(old_path, &new_src_path));
    RETURN_IF_ERROR(canonicalize(new_path, &new_dst_path));
    return _impl->link_file(butil::FilePath(new_src_path), butil::FilePath(new_dst_path));
}

Status MemoryFileSystem::create_file(const std::string& path) {
    WritableFileOptions opts{.mode = CREATE_OR_OPEN};
    return new_writable_file(opts, path).status();
}

Status MemoryFileSystem::append_file(const std::string& path, const Slice& content) {
    WritableFileOptions opts{.mode = CREATE_OR_OPEN};
    ASSIGN_OR_RETURN(auto f, new_writable_file(opts, path));
    return f->append(content);
}

Status MemoryFileSystem::read_file(const std::string& path, std::string* content) {
    ASSIGN_OR_RETURN(auto random_access_file, new_random_access_file(RandomAccessFileOptions(), path));
    ASSIGN_OR_RETURN(const uint64_t size, random_access_file->get_size());
    raw::make_room(content, size);
    return random_access_file->read_at_fully(0, content->data(), content->size());
}

} // namespace starrocks
