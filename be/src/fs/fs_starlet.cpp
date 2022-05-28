// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include <fmt/core.h>
#include <s3_uri.h>
#include <starlet.h>
#include <worker.h>

#include "fs/output_stream_adapter.h"
#include "gutil/strings/util.h"
#include "io/input_stream.h"
#include "io/output_stream.h"
#include "io/seekable_input_stream.h"

namespace starrocks {

extern staros::starlet::Starlet* g_starlet;

static Status to_status(absl::Status absl_status) {
    switch (absl_status.code()) {
    case absl::StatusCode::kOk:
        return Status::OK();
    case absl::StatusCode::kAlreadyExists:
        return Status::AlreadyExist(fmt::format("starlet err {}", absl_status.message()));
    case absl::StatusCode::kOutOfRange:
        return Status::InvalidArgument(fmt::format("starlet err {}", absl_status.message()));
    case absl::StatusCode::kInvalidArgument:
        return Status::InvalidArgument(fmt::format("starlet err {}", absl_status.message()));
    case absl::StatusCode::kNotFound:
        return Status::NotFound(fmt::format("starlet err {}", absl_status.message()));
    default:
        return Status::InternalError(fmt::format("starlet err {}", absl_status.message()));
    }
}

static const char* const kStarletPrefix = "staros://";
std::shared_ptr<staros::starlet::S3URI> parse_starlet_path(const std::string& path) {
    auto uri = std::make_shared<staros::starlet::S3URI>();
    if (HasPrefixString(path, kStarletPrefix)) {
        uri->parse(path.substr(strlen(kStarletPrefix)));
    } else {
        uri->parse(path);
    }
    return uri;
};

std::string format_starlet_path(std::shared_ptr<staros::starlet::S3URI> uri) {
    //    https://bucket-name.s3.Region.amazonaws.com/key-name
    if (uri->key().front() == '/') {
        return fmt::format("{}://{}.{}{}", uri->scheme(), uri->bucket(), uri->endpoint(), uri->key());
    } else {
        return fmt::format("{}://{}.{}/{}", uri->scheme(), uri->bucket(), uri->endpoint(), uri->key());
    }
};

class StarletInputStream : public starrocks::io::SeekableInputStream {
public:
    explicit StarletInputStream(staros::starlet::SeekableInputStreamPtr ptr) : _ptr(std::move(ptr)){};
    ~StarletInputStream() override = default;
    StarletInputStream(const StarletInputStream&) = delete;
    void operator=(const StarletInputStream&) = delete;
    StarletInputStream(StarletInputStream&&) = delete;
    void operator=(StarletInputStream&&) = delete;

    Status seek(int64_t position) override { return to_status(_ptr->seek(position, 0).status()); }
    StatusOr<int64_t> position() override {
        auto st = _ptr->position();
        if (st.ok()) {
            return *st;
        } else {
            return to_status(st.status());
        }
    }
    StatusOr<int64_t> get_size() override {
        auto st = _ptr->get_size();
        if (st.ok()) {
            return *st;
        } else {
            return to_status(st.status());
        }
    }
    StatusOr<int64_t> read(void* data, int64_t count) override {
        auto st = _ptr->read(data, count);
        if (st.ok()) {
            return *st;
        } else {
            return to_status(st.status());
        }
    }

private:
    staros::starlet::SeekableInputStreamPtr _ptr;
};

class StarletOutputStream : public starrocks::io::OutputStream {
public:
    explicit StarletOutputStream(staros::starlet::OutputStreamPtr ptr) : _ptr(std::move(ptr)){};
    ~StarletOutputStream() override = default;
    StarletOutputStream(const StarletOutputStream&) = delete;
    void operator=(const StarletOutputStream&) = delete;
    StarletOutputStream(StarletOutputStream&&) = delete;
    void operator=(StarletOutputStream&&) = delete;
    Status skip(int64_t count) override { return to_status(_ptr->skip(count)); }
    StatusOr<Buffer> get_direct_buffer() override { return Status::NotSupported("Not supported"); }
    StatusOr<Position> get_direct_buffer_and_advance(int64_t size) override {
        return Status::NotSupported("Not supported");
    }

    Status write(const void* data, int64_t size) override { return to_status(_ptr->write(data, size)); }
    bool allows_aliasing() const override { return false; }
    Status write_aliased(const void* data, int64_t size) override { return Status::NotSupported("Not supported"); }
    Status close() override { return to_status(_ptr->close()); }

private:
    staros::starlet::OutputStreamPtr _ptr;
};

class StarletFileSystem : public FileSystem {
public:
    StarletFileSystem() {}
    ~StarletFileSystem() override = default;

    StarletFileSystem(const StarletFileSystem&) = delete;
    void operator=(const StarletFileSystem&) = delete;
    StarletFileSystem(StarletFileSystem&&) = delete;
    void operator=(StarletFileSystem&&) = delete;

    Type type() const override { return STARLET; }

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const std::string& path) override {
        return new_random_access_file(RandomAccessFileOptions(), path);
    }

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& path) override {
        auto uri = parse_starlet_path(path);
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

    StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const std::string& path) override {
        auto uri = parse_starlet_path(path);
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

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const std::string& path) {
        return new_writable_file(WritableFileOptions(), path);
    }

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const WritableFileOptions& opts,
                                                              const std::string& path) override {
        if (!path.empty() && path.back() == '/') {
            return Status::NotSupported(fmt::format("Starlet: cannot create file with name ended with '/': {}", path));
        }
        auto uri = parse_starlet_path(path);
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

    Status delete_file(const std::string& path) override {
        auto uri = parse_starlet_path(path);
        if (uri->key().empty()) {
            return Status::InvalidArgument(fmt::format("root object can not be deleted", path));
        }
        if (uri->key().back() == '/') {
            return Status::InvalidArgument(fmt::format("object {} with slash not name a file", path));
        }
        auto format_str = format_starlet_path(uri);
        staros::starlet::ObjectStorePtr object_store = g_starlet->get_store(format_str);
        auto st = object_store->delete_object(format_str);
        return to_status(st);
    }

    Status iterate_dir(const std::string& dir, const std::function<bool(std::string_view)>& cb) override {
        auto uri = parse_starlet_path(dir);
        auto format_str = format_starlet_path(uri);

        if (!uri->key().empty() && uri->key().back() != '/') {
            format_str.push_back('/');
        }

        staros::starlet::ObjectStorePtr object_store = g_starlet->get_store(format_str);
        auto st = object_store->iterate_objects(format_str, cb);
        return to_status(st);
    }

    Status create_dir(const std::string& dirname) override {
        auto uri = parse_starlet_path(dirname);
        auto format_str = format_starlet_path(uri);
        auto st = is_directory(format_str);
        if (st.ok() && st.value()) {
            return Status::AlreadyExist(dirname);
        }
        if (uri->key().empty() || uri->key() == "/") {
            return Status::AlreadyExist(fmt::format("root directory already exist"));
        }

        if (uri->key().back() != '/') {
            format_str.push_back('/');
        }
        staros::starlet::ObjectStorePtr object_store = g_starlet->get_store(format_str);
        auto res = object_store->create_empty_object(format_str);
        return to_status(res);
    }

    Status create_dir_if_missing(const std::string& dirname, bool* created) override {
        auto st = create_dir(dirname);
        if (created != nullptr) {
            *created = st.ok();
        }
        if (st.is_already_exist()) {
            st = Status::OK();
        }
        return st;
    }

    Status create_dir_recursive(const std::string& dirname) override { return create_dir_if_missing(dirname, nullptr); }

    Status delete_dir(const std::string& dirname) override {
        auto uri = parse_starlet_path(dirname);
        auto format_str = format_starlet_path(uri);
        staros::starlet::ObjectStorePtr object_store = g_starlet->get_store(format_str);
        bool dir_empty = false;
        if (uri->key().empty() || uri->key() == "/") {
            return Status::NotSupported("Cannot delete root directory of StarletFS");
        }
        if (uri->key().back() != '/') {
            format_str.push_back('/');
        }
        auto cb = [&dir_empty](std::string_view file) {
            dir_empty = false;
            return true;
        };
        auto st = object_store->iterate_objects(format_str, cb);
        if (!st.ok()) {
            return to_status(st);
        }
        if (!dir_empty) {
            return Status::InternalError(fmt::format("dir {} is not empty", format_str));
        }
        auto res = object_store->delete_object(format_str);
        return to_status(res);
    }

    Status delete_dir_recursive(const std::string& dirname) override {
        auto uri = parse_starlet_path(dirname);
        auto format_str = format_starlet_path(uri);

        staros::starlet::ObjectStorePtr object_store = g_starlet->get_store(format_str);
        if (uri->key().empty() || uri->key() == "/") {
            return Status::NotSupported("Cannot delete root directory of StarletFS");
        }
        if (uri->key().back() != '/') {
            format_str.push_back('/');
        }
        auto st = object_store->delete_objects(format_str);
        return to_status(st);
    }

    // in starlet filesystem dir is an object with suffix '/' ;
    StatusOr<bool> is_directory(const std::string& path) override {
        auto uri = parse_starlet_path(path);
        auto format_str = format_starlet_path(uri);
        // root directory
        if (uri->key().empty() || uri->key() == "/") {
            return true;
        }

        staros::starlet::ObjectStorePtr object_store = g_starlet->get_store(format_str);
        auto st = object_store->object_exist(format_str);
        if (!st.ok()) {
            return to_status(st.status());
        }
        if (*st) {
            return false;
        }
        format_str.push_back('/');
        st = object_store->object_exist(format_str);
        if (!st.ok()) {
            return to_status(st.status());
        }
        if (*st) {
            return true;
        }
        return Status::NotFound(path);
    }

    Status sync_dir(const std::string& dirname) override {
        ASSIGN_OR_RETURN(const bool is_dir, is_directory(dirname));
        if (is_dir) return Status::OK();
        return Status::NotFound(fmt::format("{} not directory", dirname));
    }

    StatusOr<SpaceInfo> space(const std::string& path) override {
        const Status status = is_directory(path).status();
        if (!status.ok()) {
            return status;
        }
        return SpaceInfo{.capacity = std::numeric_limits<int64_t>::max(),
                         .free = std::numeric_limits<int64_t>::max(),
                         .available = std::numeric_limits<int64_t>::max()};
    }

    Status path_exists(const std::string& path) override { return Status::NotSupported("S3FileSystem::path_exists"); }

    Status get_children(const std::string& dir, std::vector<std::string>* file) override {
        return Status::NotSupported("S3FileSystem::get_children");
    }

    Status canonicalize(const std::string& path, std::string* file) override {
        return Status::NotSupported("S3FileSystem::canonicalize");
    }

    StatusOr<uint64_t> get_file_size(const std::string& path) override {
        return Status::NotSupported("S3FileSystem::get_file_size");
    }

    StatusOr<uint64_t> get_file_modified_time(const std::string& path) override {
        return Status::NotSupported("S3FileSystem::get_file_modified_time");
    }

    Status rename_file(const std::string& src, const std::string& target) override {
        return Status::NotSupported("S3FileSystem::rename_file");
    }

    Status link_file(const std::string& old_path, const std::string& new_path) override {
        return Status::NotSupported("S3FileSystem::link_file");
    }
};

std::unique_ptr<FileSystem> new_fs_starlet() {
    return std::make_unique<StarletFileSystem>();
}
} // namespace starrocks
