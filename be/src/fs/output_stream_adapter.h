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
#include "io/output_stream.h"
namespace starrocks {
// // Wrap a `starrocks::io::OutputStream` into `starrocks::WritableFile`.
class OutputStreamAdapter : public WritableFile {
public:
    explicit OutputStreamAdapter(std::unique_ptr<io::OutputStream> os, std::string name)
            : _os(std::move(os)), _name(std::move(name)) {
        FileSystem::on_file_write_open(this);
    }

    Status append(const Slice& data) override {
        auto st = _os->write(data.data, data.size);
        _bytes_written += st.ok() ? data.size : 0;
        return st;
    }

    Status appendv(const Slice* data, size_t cnt) override {
        for (size_t i = 0; i < cnt; i++) {
            RETURN_IF_ERROR(append(data[i]));
        }
        return Status::OK();
    }

    Status pre_allocate(uint64_t size) override { return Status::NotSupported("OutputStreamAdapter::pre_allocate"); }

    Status close() override {
        FileSystem::on_file_write_close(this);
        return _os->close();
    }

    // NOTE: unlike posix file, the file cannot be writen anymore after `flush`ed.
    Status flush(FlushMode mode) override { return _os->close(); }

    // NOTE: unlike posix file, the file cannot be writen anymore after `sync`ed.
    Status sync() override { return _os->close(); }

    uint64_t size() const override { return _bytes_written; }

    const std::string& filename() const override { return _name; }

private:
    std::unique_ptr<io::OutputStream> _os;
    std::string _name;
    uint64_t _bytes_written{0};
};
} // namespace starrocks
