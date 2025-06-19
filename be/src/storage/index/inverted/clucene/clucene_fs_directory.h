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

#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "CLucene.h" // IWYU pragma: keep
#include "CLucene/SharedHeader.h"
#include "CLucene/store/Directory.h"
#include "CLucene/store/IndexInput.h"
#include "CLucene/store/IndexOutput.h"
#include "CLucene/store/_RAMDirectory.h"
#include "common/statusor.h"

class CLuceneError;

namespace lucene::store {
class LockFactory;
} // namespace lucene::store

namespace starrocks {
struct WritableFileOptions;

class FileSystem;
class TabletIndex;

class RandomAccessFile;
class WritableFile;

class CLUCENE_EXPORT StarRocksFSDirectory : public lucene::store::Directory {
public:
    static const char* const WRITE_LOCK_FILE;
    static constexpr int64_t MAX_HEADER_DATA_SIZE = 1024 * 128; // 128k
    static const std::string CLASS_NAME;

protected:
    mutable std::mutex _this_lock;
    FileSystem* _fs = nullptr;
    std::string directory;

    /// Removes an existing file in the directory.
    bool doDeleteFile(const char* name) override;

public:
    StarRocksFSDirectory();
    ~StarRocksFSDirectory() override;

    FileSystem* getFileSystem() const { return _fs; }

    bool list(std::vector<std::string>* names) const override;
    bool fileExists(const char* name) const override;
    const std::string& getDirName() const;
    int64_t fileModified(const char* name) const override;
    int64_t fileLength(const char* name) const override;
    bool openInput(const char* name, lucene::store::IndexInput*& ret, CLuceneError& err,
                   int32_t bufferSize = -1) override;
    void renameFile(const char* from, const char* to) override;
    void touchFile(const char* name) override;
    lucene::store::IndexOutput* createOutput(const char* name) override;
    lucene::store::IndexOutput* createOutputV2(WritableFile* file_writer);
    void close() override;
    std::string toString() const override;
    static const char* getClassName();
    const char* getObjectName() const override;
    virtual bool deleteDirectory();

    virtual void init(FileSystem* fs, const char* path, lucene::store::LockFactory* lock_factory = nullptr);

    void set_file_writer_opts(std::shared_ptr<WritableFileOptions> opts);

private:
    int32_t filemode;
    std::shared_ptr<WritableFileOptions> _opts;
};

class CLUCENE_EXPORT StarRocksRAMFSDirectory final : public StarRocksFSDirectory {
protected:
    using FileMap = lucene::util::CLHashMap<char*, lucene::store::RAMFile*, lucene::util::Compare::Char,
                                            lucene::util::Equals::Char, lucene::util::Deletor::acArray,
                                            lucene::util::Deletor::Object<lucene::store::RAMFile>>;

    // unlike the java Hashtable, FileMap is not synchronized, and all access must be protected by a lock
    FileMap* filesMap;
    void init(FileSystem* fs, const char* path, lucene::store::LockFactory* lock_factory = nullptr) override;

public:
    int64_t sizeInBytes;

    /// Returns a null terminated array of strings, one for each file in the directory.
    bool list(std::vector<std::string>* names) const override;

    /** Constructs an empty {@link Directory}. */
    StarRocksRAMFSDirectory();

    ///Destructor - only call this if you are sure the directory
    ///is not being used anymore. Otherwise, use the ref-counting
    ///facilities of dir->close
    ~StarRocksRAMFSDirectory() override;

    bool doDeleteFile(const char* name) override;

    bool deleteDirectory() override;

    /// Returns true iff the named file exists in this directory.
    bool fileExists(const char* name) const override;

    /// Returns the time the named file was last modified.
    int64_t fileModified(const char* name) const override;

    /// Returns the length in bytes of a file in the directory.
    int64_t fileLength(const char* name) const override;

    /// Removes an existing file in the directory.
    void renameFile(const char* from, const char* to) override;

    /** Set the modified time of an existing file to now. */
    void touchFile(const char* name) override;

    /// Creates a new, empty file in the directory with the given name.
    ///	Returns a stream writing this file.
    lucene::store::IndexOutput* createOutput(const char* name) override;

    /// Returns a stream reading an existing file.
    bool openInput(const char* name, lucene::store::IndexInput*& ret, CLuceneError& error,
                   int32_t bufferSize = -1) override;

    void close() override;

    std::string toString() const override;

    static const char* getClassName();
    const char* getObjectName() const override;
};

class SharedHandle final : LUCENE_REFBASE {
public:
    std::unique_ptr<RandomAccessFile> _reader;
    uint64_t _length;
    int64_t _fpos;
    std::mutex _shared_lock;
    std::string _path;
    explicit SharedHandle(const std::string& path);
    ~SharedHandle() override;
};

class StarRocksIndexInput final : public lucene::store::BufferedIndexInput {
    std::shared_ptr<SharedHandle> _handle = nullptr;
    int64_t _pos;

    StarRocksIndexInput(std::shared_ptr<SharedHandle> handle, int32_t buffer_size) : BufferedIndexInput(buffer_size) {
        this->_pos = 0;
        this->_handle = std::move(handle);
    }

protected:
    StarRocksIndexInput(const StarRocksIndexInput& clone);

public:
    static StatusOr<IndexInput*> open(FileSystem* fs, const std::string& path, int32_t bufferSize = -1);
    ~StarRocksIndexInput() override;

    IndexInput* clone() const override;
    void close() override;
    int64_t length() const override { return _handle->_length; }

    const char* getDirectoryType() const override { return StarRocksFSDirectory::getClassName(); }
    const char* getObjectName() const override { return getClassName(); }
    static const char* getClassName() { return "StarRocksIndexInput"; }

    std::mutex _this_lock;

protected:
    // Random-access methods
    void seekInternal(const int64_t position) override;
    // IndexInput methods
    void readInternal(uint8_t* b, const int32_t len) override;
};

class StarRocksIndexOutput final : public lucene::store::BufferedIndexOutput {
protected:
    void flushBuffer(const uint8_t* b, int32_t size) override;

public:
    StarRocksIndexOutput() = default;
    ~StarRocksIndexOutput() override;

    int64_t length() const override;

    Status init(FileSystem* fs, const std::string& path);
    void set_file_writer_opts(std::shared_ptr<WritableFileOptions> opts);

private:
    std::unique_ptr<WritableFile> _writer;
    std::shared_ptr<WritableFileOptions> _opts;
};

class StarRocksIndexOutputV2 final : public lucene::store::BufferedIndexOutput {
    WritableFile* _index_v2_file_writer = nullptr;

protected:
    void flushBuffer(const uint8_t* b, int32_t size) override;

public:
    StarRocksIndexOutputV2() = default;
    ~StarRocksIndexOutputV2() override;

    void init(WritableFile* file_writer);

    int64_t length() const override;
};

/**
 * Factory function to create DorisFSDirectory
 */
class StarRocksFSDirectoryFactory {
public:
    static StatusOr<std::shared_ptr<StarRocksFSDirectory>> getDirectory(
            FileSystem* fs, const std::string& file, const bool& can_use_ram_dir = false,
            lucene::store::LockFactory* lock_factory = nullptr);
};

} // namespace starrocks