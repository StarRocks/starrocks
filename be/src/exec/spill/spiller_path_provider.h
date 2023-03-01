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

#include <atomic>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exec/spill/common.h"
#include "fs/fs.h"
#include "io/input_stream.h"
#include "runtime/runtime_state.h"

namespace starrocks {
using SpillPaths = std::vector<std::string>;
// TODO: add metrics
class SpillFile {
public:
    SpillFile(std::string file_path, FileSystem* fs) : _file_path(std::move(file_path)), _fs(fs) {}
    ~SpillFile() noexcept {
        if (_del_file) {
            WARN_IF_ERROR(_fs->delete_file(_file_path), "delete spilled file error:");
        }
    }
    template <class T>
    StatusOr<std::unique_ptr<T>> as();

    StatusOr<int64_t> file_size() { return _fs->get_file_size(_file_path); }

private:
    const bool _del_file = AUTO_DEL_SPILL_FILE;
    std::string _file_path;
    FileSystem* _fs;
};

// a wrapper for io::InputStream
// RawInputStreamWrapper will return EOF if all data has been read
// io::InputStream::read_fully won't return EOF
class RawInputStreamWrapper {
public:
    RawInputStreamWrapper(std::unique_ptr<io::InputStream>&& input_stream, size_t file_size)
            : _file_size(file_size), _input_stream(std::move(input_stream)) {}

    Status read_fully(void* data, int64_t count) {
        if (_offset + count > _file_size) {
            return Status::EndOfFile("EOF");
        }
        _offset += count;
        return _input_stream->read_fully(data, count);
    }

private:
    size_t _offset{};
    size_t _file_size;
    std::unique_ptr<io::InputStream> _input_stream;
};

template <>
inline StatusOr<std::unique_ptr<WritableFile>> SpillFile::as<WritableFile>() {
    WritableFileOptions options;
    if (config::experimental_spill_skip_sync) {
        options.sync_on_close = false;
    }
    return _fs->new_writable_file(options, _file_path);
}

template <>
inline StatusOr<std::unique_ptr<RawInputStreamWrapper>> SpillFile::as<RawInputStreamWrapper>() {
    ASSIGN_OR_RETURN(auto f, _fs->new_sequential_file(_file_path));
    ASSIGN_OR_RETURN(auto sz, _fs->get_file_size(_file_path));
    return std::make_unique<RawInputStreamWrapper>(std::move(f), std::move(sz));
}
using SpillFilePtr = std::shared_ptr<SpillFile>;

// TODO: union with LocationProvider
class SpillerPathProvider {
public:
    virtual ~SpillerPathProvider() = default;
    virtual Status open(RuntimeState* state) = 0;
    virtual StatusOr<SpillFilePtr> get_file() = 0;
};
using SpillPathProviderFactory = std::function<StatusOr<std::shared_ptr<SpillerPathProvider>>()>;

// TODO: check path
class LocalPathProvider : public SpillerPathProvider {
public:
    LocalPathProvider(SpillPaths paths, std::string prefix, FileSystem* fs)
            : _paths(std::move(paths)), _prefix(std::move(prefix)), _fs(fs) {
        DCHECK_GT(_paths.size(), 0);
    };
    ~LocalPathProvider() noexcept override;
    Status open(RuntimeState* state) override;
    StatusOr<SpillFilePtr> get_file() override;

private:
    const bool _del_path = AUTO_DEL_SPILL_DIR;
    SpillPaths _paths;
    std::string _prefix;
    FileSystem* _fs;
    std::atomic_int _next_id = 0;
};
} // namespace starrocks