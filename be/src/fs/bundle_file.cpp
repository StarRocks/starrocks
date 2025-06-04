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

#include "fs/bundle_file.h"

#include "fs/fs.h"
#include "fs/fs_util.h"

namespace starrocks {

Status BundleWritableFileContext::try_create_bundle_file(
        const std::function<StatusOr<std::unique_ptr<WritableFile>>()>& create_file_fn) {
    std::lock_guard<std::mutex> l(_bundle_file_mutex);
    if (_bundle_file == nullptr) {
        ASSIGN_OR_RETURN(_bundle_file, create_file_fn());
        _filename = _bundle_file->filename();
    } else {
        // If the shared file is already created, we don't need to create it again.
        // Just return OK.
    }
    return Status::OK();
}

Status BundleWritableFileContext::_close() {
    if (_bundle_file) {
        RETURN_IF_ERROR(_bundle_file->close());
    }
    return Status::OK();
}

void BundleWritableFileContext::increase_active_writers() {
    std::lock_guard<std::mutex> l(_bundle_file_mutex);
    _active_writers++;
}

Status BundleWritableFileContext::decrease_active_writers() {
    bool is_last_writer = false;
    {
        std::lock_guard<std::mutex> l(_bundle_file_mutex);
        _active_writers--;
        is_last_writer = (_active_writers == 0);
    }
    if (is_last_writer) {
        // If there are no active writers, we can close the shared file.
        RETURN_IF_ERROR(_close());
    }
    return Status::OK();
}

StatusOr<int64_t> BundleWritableFileContext::appendv(const std::vector<Slice>& slices, const FileEncryptionInfo& info) {
    // Use lock to make sure each thread will write to the shared file in order.
    std::lock_guard<std::mutex> l(_bundle_file_mutex);
    // Get the offset of current file in the shared file.
    int64_t bundle_file_offset = _bundle_file->size();
    // Append the slices to the shared file.
    _bundle_file->set_encryption_info(info);
    RETURN_IF_ERROR(_bundle_file->appendv(slices.data(), slices.size()));
    return bundle_file_offset;
}

Status BundleWritableFile::append(const Slice& data) {
    _buffers.emplace_back(std::make_unique<std::string>(data.data, data.size));
    _slices.emplace_back(*_buffers.back());
    _local_buffer_file_size += data.size;
    return Status::OK();
}

Status BundleWritableFile::appendv(const Slice* data, size_t cnt) {
    for (size_t i = 0; i < cnt; ++i) {
        RETURN_IF_ERROR(append(data[i]));
    }
    return Status::OK();
}

Status BundleWritableFile::close() {
    if (_local_buffer_file_size > 0) {
        ASSIGN_OR_RETURN(_bundle_file_offset, _context->appendv(_slices, _encryption_info));
        // Clear the local buffer.
        _slices.clear();
        _buffers.clear();
        _slices.shrink_to_fit();
        _buffers.shrink_to_fit();
        _local_buffer_file_size = 0;
    }
    return Status::OK();
}

Status BundleSeekableInputStream::init() {
    // Initialize the stream.
    RETURN_IF_ERROR(_stream->seek(_offset));
    return Status::OK();
}

Status BundleSeekableInputStream::seek(int64_t position) {
    return _stream->seek(_offset + position);
}

StatusOr<int64_t> BundleSeekableInputStream::position() {
    ASSIGN_OR_RETURN(auto pos, _stream->position());
    return pos - _offset;
}

StatusOr<int64_t> BundleSeekableInputStream::read_at(int64_t offset, void* out, int64_t count) {
    return _stream->read_at(_offset + offset, out, count);
}

Status BundleSeekableInputStream::read_at_fully(int64_t offset, void* out, int64_t count) {
    return _stream->read_at_fully(_offset + offset, out, count);
}

StatusOr<int64_t> BundleSeekableInputStream::get_size() {
    return _size;
}

Status BundleSeekableInputStream::skip(int64_t count) {
    return _stream->skip(count);
}

StatusOr<std::string> BundleSeekableInputStream::read_all() {
    std::string result;
    result.resize(_size);
    RETURN_IF_ERROR(_stream->read_at_fully(_offset, result.data(), _size));
    return std::move(result);
}

const std::string& BundleSeekableInputStream::filename() const {
    return _stream->filename();
}

StatusOr<int64_t> BundleSeekableInputStream::read(void* data, int64_t count) {
    return _stream->read(data, count);
}

} // namespace starrocks