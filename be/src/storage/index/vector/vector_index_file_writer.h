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

#ifdef WITH_TENANN

#include <memory>
#include <string>

#include "base/string/slice.h"
#include "fs/fs.h"
#include "tenann/store/index_file_writer.h"

namespace starrocks {

// Bridge StarRocks WritableFile to TenANN IndexFileWriter interface,
// enabling TenANN to write vector index files to remote storage (S3/HDFS/OSS).
class VectorIndexFileWriter : public tenann::IndexFileWriter {
public:
    explicit VectorIndexFileWriter(std::unique_ptr<WritableFile> file)
            : _file(std::move(file)), _filename(_file->filename()) {}

    ~VectorIndexFileWriter() override = default;

    int64_t Write(const void* data, int64_t count) override {
        auto st = _file->append(Slice(static_cast<const char*>(data), count));
        return st.ok() ? count : -1;
    }

    void Flush() override { (void)_file->flush(WritableFile::FLUSH_SYNC); }

    void Close() override { (void)_file->close(); }

    const std::string& filename() const override { return _filename; }

private:
    std::unique_ptr<WritableFile> _file;
    std::string _filename;
};

} // namespace starrocks

#else // !WITH_TENANN

namespace starrocks {
// Stub for non-TenANN builds so that unique_ptr<VectorIndexFileWriter> compiles.
class VectorIndexFileWriter {};
} // namespace starrocks

#endif
