// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "fs_starlet.h"

#include "gutil/strings/util.h"

namespace starrocks {

StatusOr<std::unique_ptr<RandomAccessFile>> StarletFileSystem::new_random_access_file(const std::string& path) {
    return new_random_access_file(RandomAccessFileOptions(), path);
}

StatusOr<std::unique_ptr<RandomAccessFile>> StarletFileSystem::new_random_access_file(
        const RandomAccessFileOptions& opts, const std::string& path) {
    auto uri = format_starlet_path(path);
    auto format_str = format_starlet_path(uri);
    staros::starlet::ObjectStorePtr object_store = g_starlet->get_store(format_str);
    auto st = object_store->new_object(format_str);
    if (!st.ok()) {
        return to_status(st.status());
    }
    auto object = *st;
    auto istream = std::make_shared<StarletInputStream>(object->get_input_stream());
    return std::make_unique<RandomAccessFile>(std::move(istream), path);
}

StatusOr<std::unique_ptr<SequentialFile>> StarletFileSystem::new_sequential_file(const std::string& path) {
    auto uri = format_starlet_path(path);
    auto format_str = format_starlet_path(uri);
    staros::starlet::ObjectStorePtr object_store = g_starlet->get_store(format_str);
    auto st = object_store->new_object(format_str);
    if (!st.ok()) {
        return to_status(st.status());
    }
    auto object = *st;
    auto istream = std::make_shared<StarletInputStream>(object->get_input_stream());
    return std::make_unique<SequentialFile>(std::move(istream), path);
}

StatusOr<std::unique_ptr<WritableFile>> StarletFileSystem::new_writable_file(const std::string& path) {
    return new_writable_file(WritableFileOptions(), path);
}

StatusOr<std::unique_ptr<WritableFile>> StarletFileSystem::new_writable_file(const WritableFileOptions& opts,
                                                                             const std::string& path) {
    if (!path.empty() && path.back() == '/') {
        return Status::NotSupported(fmt::format("Starlet: cannot create file with name ended with '/': {}", path));
    }
    auto uri = format_starlet_path(path);
    auto format_str = format_starlet_path(uri);
    staros::starlet::ObjectStorePtr object_store = g_starlet->get_store(format_str);
    auto st = object_store->new_object(format_str);
    if (!st.ok()) {
        return to_status(st.status());
    }
    auto object = *st;
    auto outputstream = std::make_unique<StarletOutputStream>(object->get_output_stream());
    return std::make_unique<starrocks::OutputStreamAdapter>(std::move(outputstream), path);
}

Status StarletFileSystem::delete_file(const std::string& path) {
    auto uri = format_starlet_path(path);
    if (uri->uri.key().empty()) {
        return Status::InvalidArgument(fmt::format("root object can not be deleted", path));
    }
    if (uri->uri.key().back() == '/') {
        return Status::InvalidArgument(fmt::format("object {} with slash not name a file", path));
    }
    auto format_str = format_starlet_path(uri);
    staros::starlet::ObjectStorePtr object_store = g_starlet->get_store(format_str);
    auto st = object_store->delete_object(format_str);
    LOG(WARNING) << "delete object " << format_str << " return  " << st.ToString();
    return to_status(st);
}

Status StarletFileSystem::iterate_dir(const std::string& dir, const std::function<bool(std::string_view)>& cb) {
    auto uri = format_starlet_path(dir);
    auto format_str = format_starlet_path(uri);

    if (!uri->uri.key().empty() && uri->uri.key().back() != '/') {
        format_str.push_back('/');
    }

    staros::starlet::ObjectStorePtr object_store = g_starlet->get_store(format_str);
    auto st = object_store->iterate_objects(format_str, cb);
    LOG(WARNING) << "iter  dir " << format_str << " return  " << st.ToString();
    return to_status(st);
}

Status StarletFileSystem::create_dir(const std::string& dirname) {
    auto uri = format_starlet_path(dirname);
    auto format_str = format_starlet_path(uri);
    auto st = is_directory(format_str);
    if (st.ok() && st.value()) {
        return Status::AlreadyExist(dirname);
    }
    if (uri->uri.key().empty() || uri->uri.key() == "/") {
        return Status::AlreadyExist(fmt::format("root directory already exist"));
    }

    if (uri->uri.key().back() != '/') {
        format_str.push_back('/');
    }
    staros::starlet::ObjectStorePtr object_store = g_starlet->get_store(format_str);
    auto res = object_store->create_empty_object(format_str);
    LOG(WARNING) << "create dir " << format_str << " return  " << res.ToString();
    return to_status(res);
}

Status StarletFileSystem::create_dir_if_missing(const std::string& dirname, bool* created) {
    auto st = create_dir(dirname);
    if (created != nullptr) {
        *created = st.ok();
    }
    if (st.is_already_exist()) {
        st = Status::OK();
    }
    return st;
}

Status StarletFileSystem::create_dir_recursive(const std::string& dirname) {
    return create_dir_if_missing(dirname, nullptr);
}

Status StarletFileSystem::delete_dir(const std::string& dirname) {
    auto uri = format_starlet_path(dirname);
    auto format_str = format_starlet_path(uri);
    staros::starlet::ObjectStorePtr object_store = g_starlet->get_store(format_str);
    std::set<std::string> files;
    if (uri->uri.key().empty() || uri->uri.key() == "/") {
        return Status::NotSupported("Cannot delete root directory of StarletFS");
    }
    if (uri->uri.key().back() != '/') {
        format_str.push_back('/');
    }
    auto cb = [&files](std::string_view file) {
        files.emplace(file);
        return true;
    };
    auto st = object_store->iterate_objects(format_str, cb);
    LOG(WARNING) << "iter  dir " << format_str << " return  " << st.ToString();
    if (!st.ok()) {
        return Status::IOError(fmt::format("failed to list files in {}", format_str));
    }
    if (files.size() != 0) {
        return Status::InternalError(fmt::format("dir {} is not empty", format_str));
    }
    auto res = object_store->delete_object(format_str);
    LOG(WARNING) << "delete object " << format_str << " return  " << st.ToString();
    return to_status(res);
}

Status StarletFileSystem::delete_dir_recursive(const std::string& dirname) {
    auto uri = format_starlet_path(dirname);
    auto format_str = format_starlet_path(uri);

    staros::starlet::ObjectStorePtr object_store = g_starlet->get_store(format_str);
    std::set<std::string> files;
    if (uri->uri.key().empty() || uri->uri.key() == "/") {
        return Status::NotSupported("Cannot delete root directory of StarletFS");
    }
    if (uri->uri.key().back() != '/') {
        format_str.push_back('/');
    }
    auto st = object_store->delete_objects(format_str);
    LOG(WARNING) << "delete objects " << format_str << " return  " << st.ToString();
    return to_status(st);
}

// in starlet filesystem dir is an object with suffix '/' ;
StatusOr<bool> StarletFileSystem::is_directory(const std::string& path) {
    auto uri = format_starlet_path(path);
    auto format_str = format_starlet_path(uri);
    // root directory
    if (uri->uri.key().empty() || uri->uri.key() == "/") {
        return true;
    }

    staros::starlet::ObjectStorePtr object_store = g_starlet->get_store(format_str);
    auto st = object_store->object_exist(format_str);
    LOG(WARNING) << "object exist" << format_str << " return  " << st.status().ToString();
    if (!st.ok()) {
        return to_status(st.status());
    }
    LOG(WARNING) << "object exist" << format_str << " return  " << *st;
    if (*st) {
        return false;
    }
    format_str.push_back('/');
    st = object_store->object_exist(format_str);
    LOG(WARNING) << "object exist" << format_str << " return  " << st.status().ToString();
    if (!st.ok()) {
        return to_status(st.status());
    }
    LOG(WARNING) << "object exist" << format_str << " return  " << *st;
    if (*st) {
        return true;
    }
    return Status::NotFound(path);
}

Status StarletFileSystem::sync_dir(const std::string& dirname) {
    ASSIGN_OR_RETURN(const bool is_dir, is_directory(dirname));
    if (is_dir) return Status::OK();
    return Status::NotFound(fmt::format("{} not directory", dirname));
}

StatusOr<SpaceInfo> StarletFileSystem::space(const std::string& path) {
    const Status status = is_directory(path).status();
    if (!status.ok()) {
        return status;
    }
    return SpaceInfo{.capacity = std::numeric_limits<int64_t>::max(),
                     .free = std::numeric_limits<int64_t>::max(),
                     .available = std::numeric_limits<int64_t>::max()};
}

const std::string StarletUri::starlet_prefix = "staros://";

std::shared_ptr<StarletUri> format_starlet_path(const std::string& path) {
    auto uri = std::make_shared<StarletUri>();
    if (HasPrefixString(path, StarletUri::starlet_prefix)) {
        uri->uri.parse(path.substr(StarletUri::starlet_prefix.length()));
    } else {
        uri->uri.parse(path);
    }
    return uri;
};

std::string format_starlet_path(std::shared_ptr<StarletUri> uri) {
    //    https://bucket-name.s3.Region.amazonaws.com/key-name
    if (uri->uri.key().front() == '/') {
        return fmt::format("{}://{}.{}{}", uri->uri.scheme(), uri->uri.bucket(), uri->uri.endpoint(), uri->uri.key());
    } else {
        return fmt::format("{}://{}.{}/{}", uri->uri.scheme(), uri->uri.bucket(), uri->uri.endpoint(), uri->uri.key());
    }
};
std::unique_ptr<FileSystem> new_fs_starlet() {
    return std::make_unique<StarletFileSystem>();
}
} // namespace starrocks
