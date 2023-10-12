//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// (https://developers.google.com/open-source/licenses/bsd)

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "common/statusor.h"
#include "fs/credential/cloud_configuration_factory.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/input_stream.h"
#include "io/seekable_input_stream.h"
#include "runtime/descriptors.h"
#include "util/slice.h"

namespace starrocks {

class RandomAccessFile;
class WritableFile;
class SequentialFile;
struct ResultFileOptions;
class TUploadReq;
class TDownloadReq;
struct WritableFileOptions;

struct SpaceInfo {
    // Total size of the filesystem, in bytes
    int64_t capacity = 0;
    // Free space on the filesystem, in bytes
    int64_t free = 0;
    // Free space available to a non-privileged process (may be equal or less than free)
    int64_t available = 0;
};

struct FSOptions {
private:
    FSOptions(const TBrokerScanRangeParams* scan_range_params, const TExportSink* export_sink,
              const ResultFileOptions* result_file_options, const TUploadReq* upload, const TDownloadReq* download,
              const TCloudConfiguration* cloud_configuration)
            : scan_range_params(scan_range_params),
              export_sink(export_sink),
              result_file_options(result_file_options),
              upload(upload),
              download(download),
              cloud_configuration(cloud_configuration) {}

public:
    FSOptions() : FSOptions(nullptr, nullptr, nullptr, nullptr, nullptr, nullptr) {}

    FSOptions(const TBrokerScanRangeParams* scan_range_params)
            : FSOptions(scan_range_params, nullptr, nullptr, nullptr, nullptr, nullptr) {}

    FSOptions(const TExportSink* export_sink) : FSOptions(nullptr, export_sink, nullptr, nullptr, nullptr, nullptr) {}

    FSOptions(const ResultFileOptions* result_file_options)
            : FSOptions(nullptr, nullptr, result_file_options, nullptr, nullptr, nullptr) {}

    FSOptions(const TUploadReq* upload) : FSOptions(nullptr, nullptr, nullptr, upload, nullptr, nullptr) {}

    FSOptions(const TDownloadReq* download) : FSOptions(nullptr, nullptr, nullptr, nullptr, download, nullptr) {}

    FSOptions(const TCloudConfiguration* cloud_configuration)
            : FSOptions(nullptr, nullptr, nullptr, nullptr, nullptr, cloud_configuration) {}

    const THdfsProperties* hdfs_properties() const;

    const TBrokerScanRangeParams* scan_range_params;
    const TExportSink* export_sink;
    const ResultFileOptions* result_file_options;
    const TUploadReq* upload;
    const TDownloadReq* download;
    const TCloudConfiguration* cloud_configuration;
};

struct SequentialFileOptions {
    // Don't cache remote file locally on read requests.
    // This options can be ignored if the underlying filesystem does not support local cache.
    bool skip_fill_local_cache = false;
};

struct RandomAccessFileOptions {
    // Don't cache remote file locally on read requests.
    // This options can be ignored if the underlying filesystem does not support local cache.
    bool skip_fill_local_cache = false;
};

struct DirEntry {
    std::string_view name;
    std::optional<int64_t> mtime;
    std::optional<int64_t> size; // Undefined if "is_dir" is true
    std::optional<bool> is_dir;
};

struct FileWriteStat {
    int64_t open_time{0};
    int64_t close_time{0};
    int64_t size{0};
    std::string path;
};

class FileSystem {
public:
    enum Type { POSIX, S3, HDFS, BROKER, MEMORY, STARLET };

    // Governs if/how the file is created.
    //
    // enum value                   | file exists       | file does not exist
    // -----------------------------+-------------------+--------------------
    // CREATE_OR_OPEN_WITH_TRUNCATE | opens + truncates | creates
    // CREATE_OR_OPEN               | opens             | creates
    // MUST_CREATE                  | fails             | creates
    // MUST_EXIST                   | opens             | fails
    enum OpenMode { CREATE_OR_OPEN_WITH_TRUNCATE, CREATE_OR_OPEN, MUST_CREATE, MUST_EXIST };

    FileSystem() = default;
    virtual ~FileSystem() = default;

    static StatusOr<std::unique_ptr<FileSystem>> CreateUniqueFromString(std::string_view uri,
                                                                        FSOptions options = FSOptions());

    static StatusOr<std::shared_ptr<FileSystem>> CreateSharedFromString(std::string_view uri);

    // Return a default environment suitable for the current operating
    // system.  Sophisticated users may wish to provide their own FileSystem
    // implementation instead of relying on this default environment.
    static FileSystem* Default();

    static void get_file_write_history(std::vector<FileWriteStat>* stats);
    static void on_file_write_open(WritableFile* file);
    static void on_file_write_close(WritableFile* file);

    virtual Type type() const = 0;

    // Create a brand new sequentially-readable file with the specified name.
    //  If the file does not exist, returns a non-OK status.
    //
    // The returned file will only be accessed by one thread at a time.
    StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const std::string& fname) {
        return new_sequential_file(SequentialFileOptions(), fname);
    }

    virtual StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const SequentialFileOptions& opts,
                                                                          const std::string& fname) = 0;

    // Create a brand new random access read-only file with the
    // specified name.
    //
    // The returned file will only be accessed by one thread at a time.
    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const std::string& fname) {
        return new_random_access_file(RandomAccessFileOptions(), fname);
    }

    virtual StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                               const std::string& fname) = 0;

    // Create an object that writes to a new file with the specified
    // name.  Deletes any existing file with the same name and creates a
    // new file.
    //
    // The returned file will only be accessed by one thread at a time.
    virtual StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const std::string& fname) = 0;

    // Like the previous new_writable_file, but allows options to be
    // specified.
    virtual StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const WritableFileOptions& opts,
                                                                      const std::string& fname) = 0;

    // Returns OK if the path exists.
    //         NotFound if the named file does not exist,
    //                  the calling process does not have permission to determine
    //                  whether this file exists, or if the path is invalid.
    //         IOError if an IO Error was encountered
    virtual Status path_exists(const std::string& fname) = 0;

    // Store in *result the names of the children of the specified directory.
    // The names are relative to "dir".
    // Original contents of *results are dropped.
    // Returns OK if "dir" exists and "*result" contains its children.
    //         NotFound if "dir" does not exist, the calling process does not have
    //                  permission to access "dir", or if "dir" is invalid.
    //         IOError if an IO Error was encountered
    virtual Status get_children(const std::string& dir, std::vector<std::string>* result) = 0;

    // Iterate the specified directory and call given callback function with child's
    // name. This function continues execution until all children have been iterated
    // or callback function return false.
    // The names are relative to "dir".
    //
    // The function call extra cost is acceptable. Compared with returning all children
    // into a given vector, the performance of this method is 5% worse. However this
    // approach is more flexiable and efficient in fulfilling other requirements.
    //
    // Returns OK if "dir" exists.
    //         NotFound if "dir" does not exist, the calling process does not have
    //                  permission to access "dir", or if "dir" is invalid.
    //         IOError if an IO Error was encountered
    virtual Status iterate_dir(const std::string& dir, const std::function<bool(std::string_view)>& cb) = 0;

    // `iterate_dir2` is similar to `iterate_dir` but in addition to returning the directory entry name, it
    // also returns some file statistics.
    virtual Status iterate_dir2(const std::string& dir, const std::function<bool(DirEntry)>& cb) = 0;

    // Delete the named file.
    // FIXME: If the named file does not exist, OK or NOT_FOUND is returned, depend on the implementation.
    virtual Status delete_file(const std::string& fname) = 0;

    // Create the specified directory.
    // NOTE: It will return error if the path already exist(not necessarily as a directory)
    virtual Status create_dir(const std::string& dirname) = 0;

    // Creates directory if missing.
    // Return OK if it exists, or successful in Creating.
    virtual Status create_dir_if_missing(const std::string& dirname, bool* created = nullptr) = 0;

    // Create directory for every element of 'dirname' that does not already exist.
    // If 'dirname' already exists, the function does nothing (this condition is not treated as an error).
    virtual Status create_dir_recursive(const std::string& dirname) = 0;

    // Delete the specified directory.
    // NOTE: The dir must be empty.
    virtual Status delete_dir(const std::string& dirname) = 0;

    // Deletes the contents of 'dirname' (if it is a directory) and the contents of all its subdirectories,
    // recursively, then deletes 'dirname' itself. Symlinks are not followed (symlink is removed, not its target).
    virtual Status delete_dir_recursive(const std::string& dirname) = 0;

    // Synchronize the entry for a specific directory.
    virtual Status sync_dir(const std::string& dirname) = 0;

    // Checks if the file is a directory. Returns an error if it doesn't
    // exist, otherwise return true or false.
    virtual StatusOr<bool> is_directory(const std::string& path) = 0;

    // Canonicalize 'path' by applying the following conversions:
    // - Converts a relative path into an absolute one using the cwd.
    // - Converts '.' and '..' references.
    // - Resolves all symbolic links.
    //
    // All directory entries in 'path' must exist on the filesystem.
    virtual Status canonicalize(const std::string& path, std::string* result) = 0;

    virtual StatusOr<uint64_t> get_file_size(const std::string& fname) = 0;

    // Get the last modification time by given 'fname'.
    virtual StatusOr<uint64_t> get_file_modified_time(const std::string& fname) = 0;

    // Rename file src to target.
    virtual Status rename_file(const std::string& src, const std::string& target) = 0;

    // create a hard-link
    virtual Status link_file(const std::string& /*old_path*/, const std::string& /*new_path*/) = 0;

    // Determines the information about the filesystem on which the pathname 'path' is located.
    virtual StatusOr<SpaceInfo> space(const std::string& path) { return Status::NotSupported("FileSystem::space()"); }

    // Given the path to a remote file, delete the file's cache on the local file system, if any.
    // On success, Status::OK is returned. If there is no cache, Status::NotFound is returned.
    virtual Status drop_local_cache(const std::string& path) { return Status::NotFound(path); }

    // Batch delete the given files.
    // return ok if all success (not found error ignored), error if any failed and the message indicates the fail message
    // possibly stop at the first error if is simulating batch deletes.
    virtual Status delete_files(const std::vector<std::string>& paths) {
        for (auto&& path : paths) {
            auto st = delete_file(path);
            if (!st.ok() && !st.is_not_found()) {
                return st;
            }
        }
        return Status::OK();
    }
};

// Creation-time options for WritableFile
struct WritableFileOptions {
    // Call Sync() during Close().
    bool sync_on_close = true;
    // For remote filesystem, skip filling local filesystem cache on write requests
    bool skip_fill_local_cache = false;
    // See OpenMode for details.
    FileSystem::OpenMode mode = FileSystem::MUST_CREATE;
};

// A `SequentialFile` is an `io::InputStream` with a name.
class SequentialFile final : public io::InputStreamWrapper {
public:
    explicit SequentialFile(std::shared_ptr<io::InputStream> stream, std::string name)
            : io::InputStreamWrapper(stream.get(), kDontTakeOwnership),
              _stream(std::move(stream)),
              _name(std::move(name)) {}

    const std::string& filename() const { return _name; }

    std::shared_ptr<io::InputStream> stream() { return _stream; }

private:
    std::shared_ptr<io::InputStream> _stream;
    std::string _name;
};

// A `RandomAccessFile` is an `io::SeekableInputStream` with a name.
class RandomAccessFile final : public io::SeekableInputStreamWrapper {
public:
    explicit RandomAccessFile(std::shared_ptr<io::SeekableInputStream> stream, std::string name)
            : io::SeekableInputStreamWrapper(stream.get(), kDontTakeOwnership),
              _stream(std::move(stream)),
              _name(std::move(name)) {}

    explicit RandomAccessFile(std::shared_ptr<io::SeekableInputStream> stream, std::string name, bool is_cache_hit)
            : io::SeekableInputStreamWrapper(stream.get(), kDontTakeOwnership),
              _stream(std::move(stream)),
              _name(std::move(name)),
              _is_cache_hit(is_cache_hit) {}

    std::shared_ptr<io::SeekableInputStream> stream() { return _stream; }

    const std::string& filename() const { return _name; }

    bool is_cache_hit() const { return _is_cache_hit; }

private:
    std::shared_ptr<io::SeekableInputStream> _stream;
    std::string _name;
    // for cachefs in fs_starlet
    bool _is_cache_hit{false};
};

// A file abstraction for sequential writing.  The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
// Note: To avoid user misuse, WritableFile's API should support only
// one of Append or PositionedAppend. We support only Append here.
class WritableFile {
public:
    enum FlushMode { FLUSH_SYNC, FLUSH_ASYNC };

    WritableFile() = default;
    virtual ~WritableFile() = default;

    // No copying allowed
    DISALLOW_COPY(WritableFile);

    // Append data to the end of the file
    virtual Status append(const Slice& data) = 0;

    // If possible, uses scatter-gather I/O to efficiently append
    // multiple buffers to a file. Otherwise, falls back to regular I/O.
    //
    // For implementation specific quirks and details, see comments in
    // implementation source code (e.g., env_posix.cc)
    virtual Status appendv(const Slice* data, size_t cnt) = 0;

    // Pre-allocates 'size' bytes for the file in the underlying filesystem.
    // size bytes are added to the current pre-allocated size or to the current
    // offset, whichever is bigger. In no case is the file truncated by this
    // operation.
    //
    // On some implementations, preallocation is done without initializing the
    // contents of the data blocks (as opposed to writing zeroes), requiring no
    // IO to the data blocks.
    //
    // In no case is the file truncated by this operation.
    virtual Status pre_allocate(uint64_t size) = 0;

    virtual Status close() = 0;

    // Flush all dirty data (not metadata) to disk.
    //
    // If the flush mode is synchronous, will wait for flush to finish and
    // return a meaningful status.
    virtual Status flush(FlushMode mode) = 0;

    virtual Status sync() = 0;

    virtual uint64_t size() const = 0;

    // Returns the filename provided when the WritableFile was constructed.
    virtual const std::string& filename() const = 0;
};

} // namespace starrocks
