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

#include "fs/fs.h"
#include "gutil/macros.h"

namespace starrocks {

class FileStreamConverter {
public:
    explicit FileStreamConverter(std::string_view input_file_name, uint64_t input_file_size,
                                 std::unique_ptr<WritableFile> output_file)
            : _input_file_name(input_file_name),
              _input_file_size(input_file_size),
              _output_file(std::move(output_file)) {}

    virtual ~FileStreamConverter() = default;

    DISALLOW_COPY_AND_MOVE(FileStreamConverter);

    const std::string& input_file_name() const { return _input_file_name; }

    const std::string& output_file_name() const { return _output_file->filename(); }

    uint64_t input_file_size() const { return _input_file_size; }

    uint64_t output_file_size() const { return _output_file->size(); }

    virtual Status append(const void* data, size_t size) {
        return _output_file->append(Slice((const char*)data, size));
    }

    virtual Status close() {
        if (_input_file_size != _output_file->size()) {
            LOG(WARNING) << "File size not matched, input_file_size: " << _input_file_size
                         << ", output_file_size: " << _output_file->size();
            return Status::Corruption("File size not matched, input_file_size");
        }
        return _output_file->close();
    }

protected:
    const std::string _input_file_name;
    const uint64_t _input_file_size;
    std::unique_ptr<WritableFile> _output_file;
};

} // namespace starrocks
