// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include <fmt/core.h>
#include <fslib/configuration.h>
#include <fslib/file.h>
#include <fslib/file_system.h>
#include <fslib/fslib_all_initializer.h>
#include <fslib/stream.h>
#include <s3_uri.h>
#include <starlet.h>
#include <worker.h>

#include "common/config.h"
#include "fs/output_stream_adapter.h"
#include "gutil/strings/util.h"
#include "io/input_stream.h"
#include "io/output_stream.h"
#include "io/seekable_input_stream.h"
#include "service/staros_worker.h"

namespace starrocks {

using FileSystemFactory = staros::starlet::fslib::FileSystemFactory;
using WriteOptions = staros::starlet::fslib::WriteOptions;
using ReadOptions = staros::starlet::fslib::ReadOptions;
using Configuration = staros::starlet::fslib::Configuration;
using FileSystemPtr = std::unique_ptr<staros::starlet::fslib::FileSystem>;
using ReadOnlyFilePtr = std::unique_ptr<staros::starlet::fslib::ReadOnlyFile>;
using WritableFilePtr = std::unique_ptr<staros::starlet::fslib::WritableFile>;
using Anchor = staros::starlet::fslib::Stream::Anchor;
using staros::starlet::fslib::kS3AccessKeyId;
using staros::starlet::fslib::kS3AccessKeySecret;
using staros::starlet::fslib::kS3OverrideEndpoint;
using staros::starlet::fslib::kS3Bucket;

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

StatusOr<std::pair<std::string, int64_t>> parse_starlet_path(const std::string& path) {
    int pos = path.find("?ShardId=");
    if (pos == std::string::npos) {
        return Status::InvalidArgument(fmt::format("Starlet Fs need a ShardId, {}", path));
    }
    int64_t shardid = std::stol(path.substr(pos + std::strlen("?ShardId=")));
    return std::make_pair(path.substr(0, pos), shardid);
};

class StarletInputStream : public starrocks::io::SeekableInputStream {
public:
    explicit StarletInputStream(ReadOnlyFilePtr file_ptr) : _file_ptr(std::move(file_ptr)){};
    ~StarletInputStream() = default;
    StarletInputStream(const StarletInputStream&) = delete;
    void operator=(const StarletInputStream&) = delete;
    StarletInputStream(StarletInputStream&&) = delete;
    void operator=(StarletInputStream&&) = delete;

    Status seek(int64_t position) override {
        auto stream_st = _file_ptr->stream();
        if (!stream_st.ok()) {
            return to_status(stream_st.status());
        }
        return to_status((*stream_st)->seek(position, Anchor::BEGIN).status());
    }

    StatusOr<int64_t> position() override {
        auto stream_st = _file_ptr->stream();
        if (!stream_st.ok()) {
            return to_status(stream_st.status());
        }
        if ((*stream_st)->support_tell()) {
            auto tell_st = (*stream_st)->tell();
            if (tell_st.ok()) {
                return *tell_st;
            } else {
                return to_status(tell_st.status());
            }
        } else {
            return Status::NotSupported("StarletInputStream::position");
        }
    }

    StatusOr<int64_t> get_size() override {
        //note: starlet s3 filesystem do not return not found err when file not exists;
        auto st = _file_ptr->size();
        if (st.ok()) {
            return *st;
        } else {
            return to_status(st.status());
        }
    }

    StatusOr<int64_t> read(void* data, int64_t count) override {
        auto stream_st = _file_ptr->stream();
        if (!stream_st.ok()) {
            return to_status(stream_st.status());
        }
        auto st = (*stream_st)->read(data, count);
        if (st.ok()) {
            return *st;
        } else {
            return to_status(st.status());
        }
    }

private:
    ReadOnlyFilePtr _file_ptr;
};

class StarletOutputStream : public starrocks::io::OutputStream {
public:
    explicit StarletOutputStream(WritableFilePtr file_ptr) : _file_ptr(std::move(file_ptr)){};
    ~StarletOutputStream() = default;
    StarletOutputStream(const StarletOutputStream&) = delete;
    void operator=(const StarletOutputStream&) = delete;
    StarletOutputStream(StarletOutputStream&&) = delete;
    void operator=(StarletOutputStream&&) = delete;
    Status skip(int64_t count) override { return Status::NotSupported("StarletOutputStream::skip"); }
    StatusOr<Buffer> get_direct_buffer() override {
        return Status::NotSupported("StarletOutputStream::get_direct_buffer");
    }
    StatusOr<Position> get_direct_buffer_and_advance(int64_t size) override {
        return Status::NotSupported("StarletOutputStream::get_direct_buffer_and_advance");
    }

    Status write(const void* data, int64_t size) override {
        auto stream_st = _file_ptr->stream();
        if (!stream_st.ok()) {
            return to_status(stream_st.status());
        }
        return to_status((*stream_st)->write(data, size).status());
    }

    bool allows_aliasing() const override { return false; }
    Status write_aliased(const void* data, int64_t size) override {
        return Status::NotSupported("StarletOutputStream::write_aliased");
    }
    Status close() override {
        auto stream_st = _file_ptr->stream();
        if (!stream_st.ok()) {
            return to_status(stream_st.status());
        }
        return to_status((*stream_st)->close());
    }

private:
    WritableFilePtr _file_ptr;
};

class StarletFileSystem : public FileSystem {
public:
    StarletFileSystem() { staros::starlet::fslib::register_builtin_filesystems(); }
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
        ASSIGN_OR_RETURN(auto pair, parse_starlet_path(path));
        ASSIGN_OR_RETURN(auto conf, get_shard_config(pair.second))

        auto fs_st = FileSystemFactory::new_filesystem(conf["scheme"], conf);
        if (!fs_st.ok()) {
            return to_status(fs_st.status());
        }

        auto file_st = (*fs_st)->open(pair.first, ReadOptions());

        if (!file_st.ok()) {
            return to_status(file_st.status());
        }
        auto istream = std::make_shared<StarletInputStream>(std::move(*file_st));
        return std::make_unique<RandomAccessFile>(std::move(istream), path);
    }

    StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const std::string& path) override {
        ASSIGN_OR_RETURN(auto pair, parse_starlet_path(path));
        ASSIGN_OR_RETURN(auto conf, get_shard_config(pair.second))

        auto fs_st = FileSystemFactory::new_filesystem(conf["scheme"], conf);
        if (!fs_st.ok()) {
            return to_status(fs_st.status());
        }
        auto file_st = (*fs_st)->open(pair.first, ReadOptions());

        if (!file_st.ok()) {
            return to_status(file_st.status());
        }
        auto istream = std::make_shared<StarletInputStream>(std::move(*file_st));
        return std::make_unique<SequentialFile>(std::move(istream), path);
    }

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const std::string& path) {
        return new_writable_file(WritableFileOptions(), path);
    }

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const WritableFileOptions& opts,
                                                              const std::string& path) override {
        ASSIGN_OR_RETURN(auto pair, parse_starlet_path(path));
        if (!pair.first.empty() && pair.first.back() == '/') {
            return Status::NotSupported(fmt::format("Starlet: cannot create file with name ended with '/': {}", path));
        }

        ASSIGN_OR_RETURN(auto conf, get_shard_config(pair.second))

        auto fs_st = FileSystemFactory::new_filesystem(conf["scheme"], conf);
        if (!fs_st.ok()) {
            return to_status(fs_st.status());
        }
        auto file_st = (*fs_st)->create(pair.first, WriteOptions());

        if (!file_st.ok()) {
            return to_status(file_st.status());
        }

        auto outputstream = std::make_unique<StarletOutputStream>(std::move(*file_st));
        return std::make_unique<starrocks::OutputStreamAdapter>(std::move(outputstream), path);
    }

    Status delete_file(const std::string& path) override {
        ASSIGN_OR_RETURN(auto pair, parse_starlet_path(path));
        ASSIGN_OR_RETURN(auto conf, get_shard_config(pair.second))
        auto fs_st = FileSystemFactory::new_filesystem(conf["scheme"], conf);
        if (!fs_st.ok()) {
            return to_status(fs_st.status());
        }
        auto st = (*fs_st)->delete_file(pair.first);
        return to_status(st);
    }

    Status iterate_dir(const std::string& dir, const std::function<bool(std::string_view)>& cb) override {
        ASSIGN_OR_RETURN(auto pair, parse_starlet_path(dir));
        if (!pair.first.empty() && pair.first.back() != '/') {
            pair.first.push_back('/');
        }

        ASSIGN_OR_RETURN(auto conf, get_shard_config(pair.second))
        auto fs_st = FileSystemFactory::new_filesystem(conf["scheme"], conf);
        if (!fs_st.ok()) {
            return to_status(fs_st.status());
        }
        auto st = (*fs_st)->list_dir(pair.first, false, cb);
        return to_status(st);
    }

    Status create_dir(const std::string& dirname) override {
        auto st = is_directory(dirname);
        if (st.ok() && st.value()) {
            return Status::AlreadyExist(dirname);
        }
        ASSIGN_OR_RETURN(auto pair, parse_starlet_path(dirname));
        if (pair.first.back() != '/') {
            pair.first.push_back('/');
        }

        ASSIGN_OR_RETURN(auto conf, get_shard_config(pair.second))

        auto fs_st = FileSystemFactory::new_filesystem(conf["scheme"], conf);
        if (!fs_st.ok()) {
            return to_status(fs_st.status());
        }

        auto res = (*fs_st)->mkdir(pair.first);
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
        ASSIGN_OR_RETURN(auto pair, parse_starlet_path(dirname));
        if (pair.first.back() != '/') {
            pair.first.push_back('/');
        }

        ASSIGN_OR_RETURN(auto conf, get_shard_config(pair.second))

        auto fs_st = FileSystemFactory::new_filesystem(conf["scheme"], conf);
        if (!fs_st.ok()) {
            return to_status(fs_st.status());
        }

        bool dir_empty = true;
        auto cb = [&dir_empty](std::string_view file) {
            dir_empty = false;
            return true;
        };
        auto st = (*fs_st)->list_dir(pair.first, false, cb);
        if (!st.ok()) {
            return to_status(st);
        }
        if (!dir_empty) {
            return Status::InternalError(fmt::format("dir {} is not empty", pair.first));
        }
        auto res = (*fs_st)->delete_dir(pair.first, false);
        return to_status(res);
    }

    Status delete_dir_recursive(const std::string& dirname) override {
        ASSIGN_OR_RETURN(auto pair, parse_starlet_path(dirname));
        ASSIGN_OR_RETURN(auto conf, get_shard_config(pair.second))

        auto fs_st = FileSystemFactory::new_filesystem(conf["scheme"], conf);
        if (!fs_st.ok()) {
            return to_status(fs_st.status());
        }

        if (pair.first.back() != '/') {
            pair.first.push_back('/');
        }
        auto st = (*fs_st)->delete_dir(pair.first);
        return to_status(st);
    }

    // in starlet filesystem dir is an object with suffix '/' ;
    StatusOr<bool> is_directory(const std::string& path) override {
        ASSIGN_OR_RETURN(auto pair, parse_starlet_path(path));
        ASSIGN_OR_RETURN(auto conf, get_shard_config(pair.second))
        bool dir_empty = true;

        auto fs_st = FileSystemFactory::new_filesystem(conf["scheme"], conf);
        if (!fs_st.ok()) {
            return to_status(fs_st.status());
        }

        auto st = (*fs_st)->exists(pair.first);
        if (!st.ok()) {
            return to_status(st.status());
        }

        if (*st) {
            return false;
        }

        pair.first.push_back('/');
        st = (*fs_st)->exists(pair.first);
        if (!st.ok()) {
            return to_status(st.status());
        }
        if (*st) {
            return true;
        }

        auto cb = [&dir_empty](std::string_view file) {
            dir_empty = false;
            return true;
        };
        auto res = (*fs_st)->list_dir(pair.first, false, cb);
        if (!res.ok()) {
            return to_status(res);
        }
        if (!dir_empty) {
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

    Status path_exists(const std::string& path) override {
        return Status::NotSupported("StarletFileSystem::path_exists");
    }

    Status get_children(const std::string& dir, std::vector<std::string>* file) override {
        return Status::NotSupported("StarletFileSystem::get_children");
    }

    Status canonicalize(const std::string& path, std::string* file) override {
        return Status::NotSupported("StarletFileSystem::canonicalize");
    }

    StatusOr<uint64_t> get_file_size(const std::string& path) override {
        return Status::NotSupported("StarletFileSystem::get_file_size");
    }

    StatusOr<uint64_t> get_file_modified_time(const std::string& path) override {
        return Status::NotSupported("StarletFileSystem::get_file_modified_time");
    }

    Status rename_file(const std::string& src, const std::string& target) override {
        return Status::NotSupported("StarletFileSystem::rename_file");
    }

    Status link_file(const std::string& old_path, const std::string& new_path) override {
        return Status::NotSupported("StarletFileSystem::link_file");
    }

private:
    StatusOr<Configuration> get_shard_config(int64_t shard_id) {
        Configuration conf;
        ASSIGN_OR_RETURN(auto shardinfo, g_worker->get_shard_info(shard_id));

        auto iter = shardinfo.properties.find("scheme");
        if (iter == shardinfo.properties.end()) {
            // default use starlet s3 filesystem
            conf.emplace(std::make_pair("scheme", "s3://"));
        } else {
            conf.emplace(std::make_pair("scheme", std::move(iter->second)));
        }

        iter = shardinfo.properties.find(kS3AccessKeyId);
        if (iter == shardinfo.properties.end()) {
            return Status::InvalidArgument(
                    fmt::format("shardinfo {} should contains {} conf", shard_id, kS3AccessKeyId));
        }
        conf.emplace(std::make_pair(kS3AccessKeyId, std::move(iter->second)));

        iter = shardinfo.properties.find(kS3AccessKeySecret);
        if (iter == shardinfo.properties.end()) {
            return Status::InvalidArgument(
                    fmt::format("shardinfo {} should contains {} conf", shard_id, kS3AccessKeySecret));
        }
        conf.emplace(std::make_pair(kS3AccessKeySecret, std::move(iter->second)));

        iter = shardinfo.properties.find(kS3OverrideEndpoint);
        if (iter == shardinfo.properties.end()) {
            return Status::InvalidArgument(
                    fmt::format("shardinfo {} should contains {} conf", shard_id, kS3OverrideEndpoint));
        }
        conf.emplace(std::make_pair(kS3OverrideEndpoint, std::move(iter->second)));

        iter = shardinfo.properties.find(kS3Bucket);
        if (iter == shardinfo.properties.end()) {
            return Status::InvalidArgument(fmt::format("shardinfo {} should contains {} conf", shard_id, kS3Bucket));
        }
        conf.emplace(std::make_pair(kS3Bucket, std::move(iter->second)));

        return conf;
    }
};

std::unique_ptr<FileSystem> new_fs_starlet() {
    return std::make_unique<StarletFileSystem>();
}
} // namespace starrocks
