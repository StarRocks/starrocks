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
#include "common/logging.h"
#include "common/status.h"
#include "fs/fs.h"
#include "tenann/store/index_file_writer.h"

namespace starrocks {

// Bridge StarRocks WritableFile to TenANN IndexFileWriter interface,
// enabling TenANN to write vector index files to remote storage (S3/HDFS/OSS).
//
// TenANN's IndexFileWriter interface returns void for Flush/Close, so we retain the
// first error internally and surface it via status(). For object storage (S3), a
// silent Close() failure would leave the uploaded object missing or incomplete, so
// callers must check status() after Close() and fail the write if it is not OK.
class VectorIndexFileWriter : public tenann::IndexFileWriter {
public:
    explicit VectorIndexFileWriter(std::unique_ptr<WritableFile> file)
            : _file(std::move(file)), _filename(_file->filename()) {}

    ~VectorIndexFileWriter() override = default;

    int64_t Write(const void* data, int64_t count) override {
        if (!_status.ok()) {
            return -1;
        }
        auto st = _file->append(Slice(static_cast<const char*>(data), count));
        if (!st.ok()) {
            _record_error(st);
            return -1;
        }
        return count;
    }

    void Flush() override { _record_error(_file->flush(WritableFile::FLUSH_SYNC)); }

    // Idempotent: tenann's IndexBuilder::Close() and our own explicit close from
    // VectorIndexWriter::finish() can both route here, and WritableFile::close() on
    // an already-closed file (EBADF on posix, repeated CompleteMultipartUpload on S3)
    // would otherwise fabricate a spurious error in _status.
    void Close() override {
        if (_closed) {
            return;
        }
        _closed = true;
        _record_error(_file->close());
    }

    const std::string& filename() const override { return _filename; }

    const Status& status() const { return _status; }

private:
    void _record_error(const Status& st) {
        if (!st.ok() && _status.ok()) {
            LOG(WARNING) << "VectorIndexFileWriter failure on " << _filename << ": " << st;
            _status = st;
        }
    }

    std::unique_ptr<WritableFile> _file;
    std::string _filename;
    Status _status;
    bool _closed = false;
};

} // namespace starrocks

#else // !WITH_TENANN

namespace starrocks {
// Stub for non-TenANN builds so that unique_ptr<VectorIndexFileWriter> compiles.
class VectorIndexFileWriter {};
} // namespace starrocks

#endif
