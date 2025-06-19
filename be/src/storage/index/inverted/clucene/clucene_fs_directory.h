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
#include <optional>
#include <string>
#include <vector>

#include "CLucene.h"
#include "CLucene/store/Directory.h"
#include "CLucene/store/IndexInput.h"
#include "CLucene/store/IndexOutput.h"
#include "common/statusor.h"

class CLuceneError;

namespace lucene::store {
class LockFactory;
} // namespace lucene::store

namespace starrocks {
struct WritableFileOptions;
struct RandomAccessFileOptions;

class FileSystem;
class TabletIndex;

class RandomAccessFile;
class WritableFile;

struct FileEntry {
    std::string file_name{};
    int64_t offset;
    int64_t length;
};

class StarRocksIndexInput final : public lucene::store::BufferedIndexInput {
public:
    static const std::string CLASS_NAME;

    explicit StarRocksIndexInput(FileSystem* fs, const std::string& path, int32_t buffer_size);
    StarRocksIndexInput(const StarRocksIndexInput& other);
    ~StarRocksIndexInput() override;

    IndexInput* clone() const override;
    void close() override;
    int64_t length() const override;

    const char* getDirectoryType() const override;
    const char* getObjectName() const override;
    static const char* getClassName();

    void set_length(int64_t length);
    void set_offset(int64_t offset);

private:
    void seekInternal(const int64_t position) override;
    void readInternal(uint8_t* b, const int32_t len) override;

    void init_if_necessary();

    FileSystem* _fs;
    const std::string _path;
    const int32_t _buffer_size;

    std::optional<int64_t> _offset = std::nullopt;
    std::optional<int64_t> _length = std::nullopt;

    int64_t _pos = 0;
    std::unique_ptr<RandomAccessFile> _reader = nullptr;
};

class StarRocksIndexOutput final : public lucene::store::BufferedIndexOutput {
public:
    explicit StarRocksIndexOutput(std::unique_ptr<WritableFile> writer);
    ~StarRocksIndexOutput() override;

    int64_t length() const override;

private:
    void flushBuffer(const uint8_t* b, int32_t size) override;

    std::unique_ptr<WritableFile> _writer;
};

class StarRocksMergingDirectory final : public lucene::store::Directory {
public:
    static const char* const WRITE_LOCK_FILE;
    static const std::string CLASS_NAME;

    explicit StarRocksMergingDirectory(FileSystem* fs, std::string directory,
                                       std::shared_ptr<WritableFileOptions> opts);
    ~StarRocksMergingDirectory() override;

    bool list(std::vector<std::string>* names) const override;
    bool fileExists(const char* name) const override;
    int64_t fileModified(const char* name) const override;
    int64_t fileLength(const char* name) const override;
    bool openInput(const char* name, lucene::store::IndexInput*& ret, CLuceneError& err,
                   int32_t bufferSize = -1) override;
    void renameFile(const char* from, const char* to) override;
    void touchFile(const char* name) override;
    lucene::store::IndexOutput* createOutput(const char* name) override;
    void close() override;
    std::string toString() const override;
    const char* getObjectName() const override;

    void deleteDirectory() const;

private:
    /// Removes an existing file in the directory.
    bool doDeleteFile(const char* name) override;

    FileSystem* _fs = nullptr;
    std::string _directory;
    std::shared_ptr<WritableFileOptions> _opts;
};

class StarRocksMergedDirectory final : public lucene::store::Directory {
public:
    static const std::string CLASS_NAME;

    explicit StarRocksMergedDirectory(const std::unique_ptr<StarRocksIndexInput>& stream, int64_t index_id,
                                      const std::map<std::string, FileEntry>& entries);
    ~StarRocksMergedDirectory() override;

    bool list(std::vector<std::string>* names) const override;
    bool fileExists(const char* name) const override;
    int64_t fileModified(const char* name) const override;
    int64_t fileLength(const char* name) const override;
    bool openInput(const char* name, lucene::store::IndexInput*& ret, CLuceneError& err,
                   int32_t bufferSize = -1) override;
    void renameFile(const char* from, const char* to) override;
    void touchFile(const char* name) override;
    lucene::store::IndexOutput* createOutput(const char* name) override;
    void close() override;
    std::string toString() const override;
    const char* getObjectName() const override;

private:
    /// Removes an existing file in the directory.
    bool doDeleteFile(const char* name) override;

    std::unique_ptr<StarRocksIndexInput> _stream = nullptr;
    const int64_t _index_id;
    const std::map<std::string, FileEntry>& _entries; // only hold the reference
};

class StarRocksFSDirectoryFactory {
public:
    static StatusOr<std::shared_ptr<StarRocksMergingDirectory>> getDirectory(
            FileSystem* fs, const std::string& file, const std::shared_ptr<WritableFileOptions>& opts);
};

} // namespace starrocks