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

namespace starrocks {

class BundleWritableFileContext {
public:
    BundleWritableFileContext() = default;
    ~BundleWritableFileContext() = default;

    Status try_create_bundle_file(const std::function<StatusOr<std::unique_ptr<WritableFile>>()>& create_file_fn);

    // Called when writer is prepared to write.
    void increase_active_writers();
    // last writer will close the shared file.
    Status decrease_active_writers();

    const std::string& filename() const { return _filename; }

    // Append slices to the shared file, and return the first offset of the slices.
    StatusOr<int64_t> appendv(const std::vector<Slice>& slices, const FileEncryptionInfo& info);

    WritableFile* get_writable_file() { return _bundle_file.get(); }

private:
    Status _close();

private:
    // shared file
    std::unique_ptr<WritableFile> _bundle_file;
    // mutex for shared file write
    std::mutex _bundle_file_mutex;
    // The number of active writers to this file.
    uint32_t _active_writers = 0;
    // filename, init when create shared file.
    std::string _filename;
};

class BundleWritableFile : public WritableFile {
public:
    explicit BundleWritableFile(BundleWritableFileContext* c, const FileEncryptionInfo& encryption_info)
            : _context(c), _encryption_info(encryption_info) {}

    Status append(const Slice& data) override;

    Status appendv(const Slice* data, size_t cnt) override;

    Status pre_allocate(uint64_t size) override { return Status::OK(); }

    Status close() override;

    Status flush(FlushMode mode) override { return Status::OK(); }

    Status sync() override { return Status::OK(); }

    uint64_t size() const override { return _local_buffer_file_size; }

    const std::string& filename() const override { return _context->filename(); }

    int64_t bundle_file_offset() const override { return _bundle_file_offset; }

    StatusOr<std::unique_ptr<io::NumericStatistics>> get_numeric_statistics() override {
        return _context->get_writable_file()->get_numeric_statistics();
    }

protected:
    // It will be shared with lots of threads.
    BundleWritableFileContext* _context = nullptr;
    // Each thread will have its own buffer.
    std::vector<std::unique_ptr<std::string>> _buffers;
    std::vector<Slice> _slices;
    // The size of the local buffer.
    uint64_t _local_buffer_file_size = 0;
    // The offset of current file in the shared file.
    int64_t _bundle_file_offset = 0;
    // The encryption info of the file.
    FileEncryptionInfo _encryption_info;
};

class BundleSeekableInputStream final : public io::SeekableInputStream {
public:
    explicit BundleSeekableInputStream(std::shared_ptr<SeekableInputStream> stream, int64_t offset, int64_t size)
            : _stream(std::move(stream)), _offset(offset), _size(size) {}

    Status init();
    Status seek(int64_t position) override;
    StatusOr<int64_t> position() override;
    StatusOr<int64_t> read_at(int64_t offset, void* out, int64_t count) override;
    Status read_at_fully(int64_t offset, void* out, int64_t count) override;
    StatusOr<int64_t> get_size() override;
    Status skip(int64_t count) override;
    StatusOr<std::string> read_all() override;
    const std::string& filename() const override;
    StatusOr<int64_t> read(void* data, int64_t count) override;
    Status touch_cache(int64_t offset, size_t length) override;
    StatusOr<std::unique_ptr<io::NumericStatistics>> get_numeric_statistics() override;

private:
    std::shared_ptr<io::SeekableInputStream> _stream;
    int64_t _offset = 0;
    int64_t _size = 0;
};

} // namespace starrocks
