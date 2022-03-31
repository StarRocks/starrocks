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
#include <string>
#include <string_view>

#include "common/statusor.h"
#include "util/slice.h"

namespace starrocks {

class NumericStatistics;
class RandomAccessFile;
class RandomRWFile;
class WritableFile;
class SequentialFile;
struct WritableFileOptions;
struct RandomAccessFileOptions;
struct RandomRWFileOptions;

struct SpaceInfo {
    // Total size of the filesystem, in bytes
    int64_t capacity = 0;
    // Free space on the filesystem, in bytes
    int64_t free = 0;
    // Free space available to a non-privileged process (may be equal or less than free)
    int64_t available = 0;
};

class Env {
public:
    // Governs if/how the file is created.
    //
    // enum value                   | file exists       | file does not exist
    // -----------------------------+-------------------+--------------------
    // CREATE_OR_OPEN_WITH_TRUNCATE | opens + truncates | creates
    // CREATE_OR_OPEN               | opens             | creates
    // MUST_CREATE                  | fails             | creates
    // MUST_EXIST                   | opens             | fails
    enum OpenMode { CREATE_OR_OPEN_WITH_TRUNCATE, CREATE_OR_OPEN, MUST_CREATE, MUST_EXIST };

    Env() = default;
    virtual ~Env() = default;

    static StatusOr<std::unique_ptr<Env>> CreateUniqueFromString(std::string_view uri);

    static StatusOr<std::shared_ptr<Env>> CreateSharedFromString(std::string_view uri);

    // Return a default environment suitable for the current operating
    // system.  Sophisticated users may wish to provide their own Env
    // implementation instead of relying on this default environment.
    static Env* Default();

    // Create a brand new sequentially-readable file with the specified name.
    //  If the file does not exist, returns a non-OK status.
    //
    // The returned file will only be accessed by one thread at a time.
    virtual StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const std::string& fname) = 0;

    // Create a brand new random access read-only file with the
    // specified name.
    //
    // The returned file will only be accessed by one thread at a time.
    virtual StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const std::string& fname) = 0;

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

    // Creates a new readable and writable file. If a file with the same name
    // already exists on disk, it is deleted.
    //
    // Some of the methods of the new file may be accessed concurrently,
    // while others are only safe for access by one thread at a time.
    virtual StatusOr<std::unique_ptr<RandomRWFile>> new_random_rw_file(const std::string& fname) = 0;

    // Like the previous new_random_rw_file, but allows options to be specified.
    virtual StatusOr<std::unique_ptr<RandomRWFile>> new_random_rw_file(const RandomRWFileOptions& opts,
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
    virtual StatusOr<SpaceInfo> space(const std::string& path) { return Status::NotSupported("Env::space()"); }
};

struct RandomAccessFileOptions {
    RandomAccessFileOptions() = default;
};

// Creation-time options for WritableFile
struct WritableFileOptions {
    // Call Sync() during Close().
    bool sync_on_close = false;
    // See OpenMode for details.
    Env::OpenMode mode = Env::CREATE_OR_OPEN_WITH_TRUNCATE;
};

// Creation-time options for RWFile
struct RandomRWFileOptions {
    // Call Sync() during Close().
    bool sync_on_close = false;
    // See OpenMode for details.
    Env::OpenMode mode = Env::CREATE_OR_OPEN_WITH_TRUNCATE;
};

// A file abstraction for reading sequentially through a file
class SequentialFile {
public:
    SequentialFile() = default;
    virtual ~SequentialFile() = default;

    // Read up to "size" bytes from the file.
    // Copies the resulting data into "data".
    //
    // On success, return the number of bytes read, otherwise return
    // an error and the contents of "result" are invalid.
    //
    // REQUIRES: External synchronization
    virtual StatusOr<int64_t> read(void* data, int64_t size) = 0;

    // Skip "n" bytes from the file. This is guaranteed to be no
    // slower that reading the same data, but may be faster.
    //
    // If end of file is reached, skipping will stop at the end of the
    // file, and Skip will return OK.
    //
    // REQUIRES: External synchronization
    virtual Status skip(uint64_t n) = 0;

    // Returns the filename provided when the SequentialFile was constructed.
    virtual const std::string& filename() const = 0;

    // Get statistics about the reads which this SequentialFile has done.
    // If the SequentialFile implementation doesn't support statistics, a null pointer or
    // an empty statistics is returned.
    virtual StatusOr<std::unique_ptr<NumericStatistics>> get_numeric_statistics() { return nullptr; }
};

class RandomAccessFile {
public:
    RandomAccessFile() = default;
    virtual ~RandomAccessFile() = default;

    // Read up to "size" bytes from the file.
    // Copies the resulting data into "data".
    //
    // Return the number of bytes read or error.
    virtual StatusOr<int64_t> read_at(int64_t offset, void* data, int64_t size) const = 0;

    // Read exactly the number of "size" bytes data from given position "offset".
    // This method does not return the number of bytes read because either
    // (1) the entire "size" bytes is read
    // (2) the end of the stream is reached
    // If the eof is reached, an IO error is returned.
    virtual Status read_at_fully(int64_t offset, void* data, int64_t size) const = 0;

    // Reads up to the "results" aggregate size, based on each Slice's "size",
    // from the file starting at 'offset'. The Slices must point to already-allocated
    // buffers for the data to be written to.
    //
    // If an error was encountered, returns a non-OK status.
    //
    // This method will internally retry on EINTR and "short reads" in order to
    // fully read the requested number of bytes. In the event that it is not
    // possible to read exactly 'length' bytes, an IOError is returned.
    //
    // Safe for concurrent use by multiple threads.
    virtual Status readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const = 0;

    // Return the size of this file.
    virtual StatusOr<uint64_t> get_size() const = 0;

    // Return name of this file
    virtual const std::string& filename() const = 0;

    // Get statistics about the reads which this RandomAccessFile has done.
    // If the RandomAccessFile implementation doesn't support statistics, a null pointer or
    // an empty statistics is returned.
    virtual StatusOr<std::unique_ptr<NumericStatistics>> get_numeric_statistics() { return nullptr; }
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

private:
    // No copying allowed
    WritableFile(const WritableFile&) = delete;
    void operator=(const WritableFile&) = delete;
};

// A file abstraction for random reading and writing.
class RandomRWFile {
public:
    enum FlushMode { FLUSH_SYNC, FLUSH_ASYNC };
    RandomRWFile() = default;
    virtual ~RandomRWFile() = default;

    virtual Status read_at(uint64_t offset, const Slice& result) const = 0;

    virtual Status readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const = 0;

    virtual Status write_at(uint64_t offset, const Slice& data) = 0;

    virtual Status writev_at(uint64_t offset, const Slice* data, size_t data_cnt) = 0;

    virtual Status flush(FlushMode mode, uint64_t offset, size_t length) = 0;

    virtual Status sync() = 0;

    virtual Status close() = 0;

    virtual StatusOr<uint64_t> get_size() const = 0;
    virtual const std::string& filename() const = 0;
};

class NumericStatistics {
public:
    NumericStatistics() = default;
    ~NumericStatistics() = default;

    NumericStatistics(const NumericStatistics&) = default;
    NumericStatistics& operator=(const NumericStatistics&) = default;
    NumericStatistics(NumericStatistics&&) = default;
    NumericStatistics& operator=(NumericStatistics&&) = default;

    void append(std::string_view name, int64_t value);

    int64_t size() const;

    const std::string& name(int64_t idx) const;

    int64_t value(int64_t idx) const;

    void reserve(int64_t size);

private:
    std::vector<std::string> _names;
    std::vector<int64_t> _values;
};

inline void NumericStatistics::append(std::string_view name, int64_t value) {
    _names.emplace_back(name);
    _values.emplace_back(value);
}

inline int64_t NumericStatistics::size() const {
    return static_cast<int64_t>(_names.size());
}

inline const std::string& NumericStatistics::name(int64_t idx) const {
    assert(idx >= 0 && idx < size());
    return _names[idx];
}

inline int64_t NumericStatistics::value(int64_t idx) const {
    assert(idx >= 0 && idx < size());
    return _values[idx];
}

inline void NumericStatistics::reserve(int64_t size) {
    _names.reserve(size);
    _values.reserve(size);
}

} // namespace starrocks
