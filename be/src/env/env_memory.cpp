// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "env/env_memory.h"

#include <butil/files/file_path.h>

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

class MemoryRandomAccessFile final : public RandomAccessFile {
public:
    MemoryRandomAccessFile(std::string path, InodePtr inode) : _path(std::move(path)), _inode(std::move(inode)) {}

    ~MemoryRandomAccessFile() override = default;

    Status read(uint64_t offset, Slice* res) const override {
        const std::string& data = _inode->data;
        if (offset >= data.size()) {
            res->size = 0;
            return Status::OK();
        }
        size_t to_read = std::min<size_t>(res->size, data.size() - offset);
        memcpy(res->data, data.data() + offset, to_read);
        res->size = to_read;
        return Status::OK();
    }

    Status read_at(uint64_t offset, const Slice& result) const override {
        const std::string& data = _inode->data;
        if (offset + result.size > data.size()) {
            return Status::IOError("Cannot read required bytes");
        }
        memcpy(result.data, data.data() + offset, result.size);
        return Status::OK();
    }

    Status readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const override {
        const std::string& data = _inode->data;
        size_t total_size = 0;
        for (int i = 0; i < res_cnt; ++i) {
            total_size += res[i].size;
        }
        if (offset + total_size > data.size()) {
            return Status::IOError("Cannot read required bytes");
        }
        for (int i = 0; i < res_cnt; ++i) {
            memcpy(res[i].data, data.data() + offset, res[i].size);
            offset += res[i].size;
        }
        return Status::OK();
    }

    Status size(uint64_t* size) const override {
        const std::string& data = _inode->data;
        *size = data.size();
        return Status::OK();
    }

    const std::string& file_name() const override { return _path; }

private:
    std::string _path;
    InodePtr _inode;
};

class MemorySequentialFile final : public SequentialFile {
public:
    MemorySequentialFile(std::string path, InodePtr inode) : _random_file(std::move(path), std::move(inode)) {}

    ~MemorySequentialFile() override = default;

    Status read(Slice* res) override {
        Status st = _random_file.read(_offset, res);
        if (st.ok()) {
            _offset += res->size;
        }
        return st;
    }

    const std::string& filename() const override { return _random_file.file_name(); }

    Status skip(uint64_t n) override {
        uint64_t size = 0;
        CHECK(_random_file.size(&size).ok());
        _offset = std::min(_offset + n, size);
        return Status::OK();
    }

private:
    uint64_t _offset = 0;
    MemoryRandomAccessFile _random_file;
};

class MemoryWritableFile final : public WritableFile {
public:
    MemoryWritableFile(std::string path, InodePtr inode) : _path(std::move(path)), _inode(std::move(inode)) {}

    Status append(const Slice& data) override {
        _inode->data.append(data.data, data.size);
        return Status::OK();
    }

    Status appendv(const Slice* data, size_t cnt) override {
        for (size_t i = 0; i < cnt; i++) {
            (void)append(data[i]);
        }
        return Status::OK();
    }

    Status pre_allocate(uint64_t size) override {
        _inode->data.reserve(size);
        return Status::OK();
    }

    Status close() override {
        _inode = nullptr;
        return Status::OK();
    }

    Status flush(FlushMode mode) override { return Status::OK(); }

    Status sync() override { return Status::OK(); }

    uint64_t size() const override { return _inode->data.size(); }

    const std::string& filename() const override { return _path; }

private:
    std::string _path;
    InodePtr _inode;
};

class MemoryRandomRWFile final : public RandomRWFile {
public:
    MemoryRandomRWFile(std::string path, InodePtr inode) : _path(std::move(path)), _inode(std::move(inode)) {}

    Status read_at(uint64_t offset, const Slice& result) const override {
        const std::string& data = _inode->data;
        if (offset + result.size > data.size()) {
            return Status::IOError("invalid offset or buffer size");
        }
        memcpy(result.data, &data[offset], result.size);
        return Status::OK();
    }

    Status readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const override {
        for (size_t i = 0; i < res_cnt; i++) {
            RETURN_IF_ERROR(read_at(offset, res[i]));
            offset += res[i].size;
        }
        return Status::OK();
    }

    Status write_at(uint64_t offset, const Slice& data) override {
        std::string& content = _inode->data;
        if (offset + data.size > content.size()) {
            content.resize(offset + data.size);
        }
        memcpy(&content[offset], data.data, data.size);
        return Status::OK();
    }

    Status writev_at(uint64_t offset, const Slice* data, size_t data_cnt) override {
        for (size_t i = 0; i < data_cnt; i++) {
            (void)write_at(offset, data[i]);
            offset += data[i].size;
        }
        return Status::OK();
    }

    Status flush(FlushMode mode, uint64_t offset, size_t length) override { return Status::OK(); }

    Status sync() override { return Status::OK(); }

    Status close() override {
        _inode = nullptr;
        return Status::OK();
    }

    Status size(uint64_t* size) const override {
        *size = _inode->data.size();
        return Status::OK();
    }

    const std::string& filename() const override { return _path; }

private:
    std::string _path;
    InodePtr _inode;
};

class EnvMemoryImpl {
public:
    EnvMemoryImpl() {
        // init root directory.
        _namespace["/"] = std::make_shared<Inode>(kDir, "");
    }

    Status new_sequential_file(const butil::FilePath& path, std::unique_ptr<SequentialFile>* file) {
        auto iter = _namespace.find(path.value());
        if (iter == _namespace.end()) {
            return Status::NotFound(path.value());
        } else {
            *file = std::make_unique<MemorySequentialFile>(path.value(), iter->second);
            return Status::OK();
        }
    }

    Status new_random_access_file(const butil::FilePath& path, std::unique_ptr<RandomAccessFile>* file) {
        return new_random_access_file(RandomAccessFileOptions(), path, file);
    }

    Status new_random_access_file(const RandomAccessFileOptions& opts, const butil::FilePath& path,
                                  std::unique_ptr<RandomAccessFile>* file) {
        auto iter = _namespace.find(path.value());
        if (iter == _namespace.end()) {
            return Status::NotFound(path.value());
        } else {
            *file = std::make_unique<MemoryRandomAccessFile>(path.value(), iter->second);
            return Status::OK();
        }
    }

    template <typename DerivedType, typename BaseType>
    Status new_writable_file(Env::OpenMode mode, const butil::FilePath& path, std::unique_ptr<BaseType>* file) {
        InodePtr inode = get_inode(path);
        if (mode == Env::MUST_EXIST && inode == nullptr) {
            return Status::NotFound(path.value());
        }
        if (mode == Env::MUST_CREATE && inode != nullptr) {
            return Status::AlreadyExist(path.value());
        }
        if (mode == Env::CREATE_OR_OPEN_WITH_TRUNCATE && inode != nullptr) {
            inode->data.clear();
        }
        if (inode == nullptr && !path_exists(path.DirName()).ok()) {
            return Status::NotFound("parent directory not exist");
        }
        if (inode == nullptr) {
            assert(mode != Env::MUST_EXIST);
            inode = std::make_shared<Inode>(kNormal, "");
            _namespace[path.value()] = inode;
        } else if (inode->type != kNormal) {
            return Status::IOError(path.value() + " is a directory");
        }
        *file = std::make_unique<DerivedType>(path.value(), std::move(inode));
        return Status::OK();
    }

    Status path_exists(const butil::FilePath& path) {
        return get_inode(path) != nullptr ? Status::OK() : Status::NotFound(path.value());
    }

    Status get_children(const butil::FilePath& path, std::vector<std::string>* file) {
        return iterate_dir(path, [&](const char* filename) -> bool {
            file->emplace_back(filename);
            return true;
        });
    }

    Status iterate_dir(const butil::FilePath& path, const std::function<bool(const char*)>& cb) {
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

    Status delete_dir(const butil::FilePath& dirname) {
        bool empty_dir = true;
        RETURN_IF_ERROR(iterate_dir(dirname, [&](const char*) -> bool {
            empty_dir = false;
            return false;
        }));
        if (!empty_dir) {
            return Status::IOError("directory not empty");
        }
        _namespace.erase(dirname.value());
        return Status::OK();
    }

    Status is_directory(const butil::FilePath& path, bool* is_dir) {
        auto inode = get_inode(path);
        if (inode == nullptr) {
            return Status::NotFound(path.value());
        }
        *is_dir = (inode->type == kDir);
        return Status::OK();
    }

    Status get_file_size(const butil::FilePath& path, uint64_t* size) {
        auto inode = get_inode(path);
        if (inode == nullptr || inode->type != kNormal) {
            return Status::NotFound("not exist or is a directory");
        }
        *size = inode->data.size();
        return Status::OK();
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
        Status st = iterate_dir(path, [&](const char*) -> bool {
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

EnvMemory::EnvMemory() : _impl(new EnvMemoryImpl()) {}

EnvMemory::~EnvMemory() {
    delete _impl;
}

Status EnvMemory::new_sequential_file(const std::string& path, std::unique_ptr<SequentialFile>* file) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(path, &new_path));
    return _impl->new_sequential_file(butil::FilePath(new_path), file);
}

Status EnvMemory::new_random_access_file(const std::string& path, std::unique_ptr<RandomAccessFile>* file) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(path, &new_path));
    return _impl->new_random_access_file(butil::FilePath(new_path), file);
}

Status EnvMemory::new_random_access_file(const RandomAccessFileOptions& opts, const std::string& path,
                                         std::unique_ptr<RandomAccessFile>* file) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(path, &new_path));
    return _impl->new_random_access_file(opts, butil::FilePath(new_path), file);
}

Status EnvMemory::new_writable_file(const std::string& path, std::unique_ptr<WritableFile>* file) {
    return new_writable_file(WritableFileOptions(), path, file);
}

Status EnvMemory::new_writable_file(const WritableFileOptions& opts, const std::string& path,
                                    std::unique_ptr<WritableFile>* file) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(path, &new_path));
    return _impl->new_writable_file<MemoryWritableFile>(opts.mode, butil::FilePath(new_path), file);
}

Status EnvMemory::new_random_rw_file(const std::string& path, std::unique_ptr<RandomRWFile>* file) {
    return new_random_rw_file(RandomRWFileOptions(), path, file);
}

Status EnvMemory::new_random_rw_file(const RandomRWFileOptions& opts, const std::string& path,
                                     std::unique_ptr<RandomRWFile>* file) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(path, &new_path));
    return _impl->new_writable_file<MemoryRandomRWFile>(opts.mode, butil::FilePath(new_path), file);
}

Status EnvMemory::path_exists(const std::string& path) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(path, &new_path));
    return _impl->path_exists(butil::FilePath(new_path));
}

Status EnvMemory::get_children(const std::string& dir, std::vector<std::string>* file) {
    std::string new_path;
    file->clear();
    RETURN_IF_ERROR(canonicalize(dir, &new_path));
    return _impl->get_children(butil::FilePath(new_path), file);
}

Status EnvMemory::iterate_dir(const std::string& dir, const std::function<bool(const char*)>& cb) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(dir, &new_path));
    return _impl->iterate_dir(butil::FilePath(new_path), cb);
}

Status EnvMemory::delete_file(const std::string& path) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(path, &new_path));
    return _impl->delete_file(butil::FilePath(new_path));
}

Status EnvMemory::create_dir(const std::string& dirname) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(dirname, &new_path));
    return _impl->create_dir(butil::FilePath(new_path));
}

Status EnvMemory::create_dir_if_missing(const std::string& dirname, bool* created) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(dirname, &new_path));
    return _impl->create_dir_if_missing(butil::FilePath(new_path), created);
}

Status EnvMemory::delete_dir(const std::string& dirname) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(dirname, &new_path));
    return _impl->delete_dir(butil::FilePath(new_path));
}

Status EnvMemory::sync_dir(const std::string& dirname) {
    return Status::OK();
}

Status EnvMemory::is_directory(const std::string& path, bool* is_dir) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(path, &new_path));
    return _impl->is_directory(butil::FilePath(new_path), is_dir);
}

Status EnvMemory::canonicalize(const std::string& path, std::string* file) {
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

Status EnvMemory::get_file_size(const std::string& path, uint64_t* size) {
    std::string new_path;
    RETURN_IF_ERROR(canonicalize(path, &new_path));
    return _impl->get_file_size(butil::FilePath(new_path), size);
}

Status EnvMemory::get_file_modified_time(const std::string& path, uint64_t* file_mtime) {
    return Status::NotSupported("get_file_modified_time");
}

Status EnvMemory::rename_file(const std::string& src, const std::string& target) {
    std::string new_src_path;
    std::string new_dst_path;
    RETURN_IF_ERROR(canonicalize(src, &new_src_path));
    RETURN_IF_ERROR(canonicalize(target, &new_dst_path));
    return _impl->rename_file(butil::FilePath(new_src_path), butil::FilePath(new_dst_path));
}

Status EnvMemory::link_file(const std::string& old_path, const std::string& new_path) {
    std::string new_src_path;
    std::string new_dst_path;
    RETURN_IF_ERROR(canonicalize(old_path, &new_src_path));
    RETURN_IF_ERROR(canonicalize(new_path, &new_dst_path));
    return _impl->link_file(butil::FilePath(new_src_path), butil::FilePath(new_dst_path));
}

Status EnvMemory::create_file(const std::string& path) {
    WritableFileOptions opts{.mode = CREATE_OR_OPEN};
    std::unique_ptr<WritableFile> dummy;
    return new_writable_file(opts, path, &dummy);
}

Status EnvMemory::append_file(const std::string& path, const Slice& content) {
    WritableFileOptions opts{.mode = CREATE_OR_OPEN};
    std::unique_ptr<WritableFile> f;
    RETURN_IF_ERROR(new_writable_file(opts, path, &f));
    return f->append(content);
}

Status EnvMemory::read_file(const std::string& path, std::string* content) {
    std::unique_ptr<RandomAccessFile> f;
    RETURN_IF_ERROR(new_random_access_file(path, &f));
    uint64_t size = 0;
    RETURN_IF_ERROR(f->size(&size));
    raw::make_room(content, size);
    Slice buff(*content);
    return f->read_at(0, buff);
}

} // namespace starrocks