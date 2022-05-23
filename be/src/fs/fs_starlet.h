// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#pragma once

#include <fmt/core.h>
#include <fs/fs.h>
#include <io/input_stream.h>
#include <io/output_stream.h>
#include <io/output_stream_adapter.h>
#include <s3_uri.h>
#include <starlet.h>
#include <worker.h>

#include "input_stream.h"
#include "output_stream.h"
#include "seekable_input_stream.h"

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

struct StarletUri {
    static const std::string starlet_prefix;
    staros::starlet::S3URI uri;
};

std::shared_ptr<StarletUri> format_starlet_path(const std::string& path);
std::string format_starlet_path(std::shared_ptr<StarletUri> uri);

class StarletInputStream : public starrocks::io::SeekableInputStream {
public:
    explicit StarletInputStream(staros::starlet::SeekableInputStreamPtr ptr) : _ptr(ptr){};
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
        auto ptr = std::dynamic_pointer_cast<staros::starlet::SeekableInputStream>(_ptr);
        auto st = ptr->read(data, count);
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
    explicit StarletOutputStream(staros::starlet::OutputStreamPtr ptr) : _ptr(ptr){};
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

    StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const std::string& fname) override;
    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const std::string& fname) override;
    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& fname) override;
    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const std::string& fname) override;
    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const WritableFileOptions& opts,
                                                              const std::string& fname) override;
    Status iterate_dir(const std::string& dir, const std::function<bool(std::string_view)>& cb) override;
    Status delete_file(const std::string& fname) override;
    Status create_dir(const std::string& dirname) override;
    Status create_dir_if_missing(const std::string& dirname, bool* created) override;
    Status create_dir_recursive(const std::string& dirname) override;
    Status delete_dir(const std::string& dirname) override;
    Status delete_dir_recursive(const std::string& dirname) override;
    Status sync_dir(const std::string& dirname) override;
    StatusOr<bool> is_directory(const std::string& path) override;
    StatusOr<SpaceInfo> space(const std::string& path) override;

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

std::unique_ptr<FileSystem> new_fs_starlet();

} // namespace starrocks
