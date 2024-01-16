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

#include "io/shared_buffered_input_stream.h"

#include "gutil/strings/fastmem.h"
#include "util/runtime_profile.h"
namespace starrocks::io {

SharedBufferedInputStream::SharedBufferedInputStream(std::shared_ptr<SeekableInputStream> stream,
                                                     const std::string& filename, size_t size)
        : _stream(stream), _size(size) {}

Status SharedBufferedInputStream::set_io_ranges(const std::vector<IORange>& ranges) {
    if (ranges.size() == 0) {
        return Status::OK();
    }

    // specify compare function is important. suppose we have zero range like [351,351],[351,356].
    // If we don't specify compare function, we may have [351,356],[351,351] which is bad order.
    std::vector<IORange> check(ranges);
    std::sort(check.begin(), check.end(), [](const IORange& a, const IORange& b) {
        if (a.offset != b.offset) {
            return a.offset < b.offset;
        }
        return a.size < b.size;
    });
    if (_align_size != 0) {
        const int64_t sz = _align_size;
        // do alignment and compaction
        for (int i = 0; i < check.size(); i++) {
            int64_t start = check[i].offset / sz * sz;
            int64_t end = std::min((check[i].offset + check[i].size + sz - 1) / sz * sz, _size);
            check[i].offset = start;
            check[i].size = end - start;
        }
        int j = 0;
        for (int i = 1; i < check.size(); i++) {
            if (check[i].offset <= (check[j].offset + check[j].size)) {
                check[j].size = (check[i].offset + check[i].size - check[j].offset);
            } else {
                j++;
                check[j] = check[i];
            }
        }
        check.resize(j + 1);
    }

    // check io range is not overlapped.
    for (size_t i = 1; i < check.size(); i++) {
        if (check[i].offset < (check[i - 1].offset + check[i - 1].size)) {
            return Status::RuntimeError("io ranges are overalpped");
        }
    }

    std::vector<IORange> small_ranges;
    for (const IORange& r : check) {
        if (r.size > _options.max_buffer_size) {
            SharedBuffer sb = SharedBuffer{.offset = r.offset, .size = r.size, .ref_count = 1};
            _map.insert(std::make_pair(r.offset + r.size, sb));
        } else {
            small_ranges.emplace_back(r);
        }
    }

    if (small_ranges.size() > 0) {
        auto update_map = [&](size_t from, size_t to) {
            // merge from [unmerge, i-1]
            int64_t ref_count = (to - from + 1);
            int64_t end = (small_ranges[to].offset + small_ranges[to].size);
            SharedBuffer sb = SharedBuffer{.offset = small_ranges[from].offset,
                                           .size = end - small_ranges[from].offset,
                                           .ref_count = ref_count};
            _map.insert(std::make_pair(sb.offset + sb.size, sb));
        };

        size_t unmerge = 0;
        for (size_t i = 1; i < small_ranges.size(); i++) {
            const auto& prev = small_ranges[i - 1];
            const auto& now = small_ranges[i];
            size_t now_end = now.offset + now.size;
            size_t prev_end = prev.offset + prev.size;
            if (((now_end - small_ranges[unmerge].offset) <= _options.max_buffer_size) &&
                (now.offset - prev_end) <= _options.max_dist_size) {
                continue;
            } else {
                update_map(unmerge, i - 1);
                unmerge = i;
            }
        }
        update_map(unmerge, small_ranges.size() - 1);
    }
    return Status::OK();
}

StatusOr<SharedBufferedInputStream::SharedBuffer*> SharedBufferedInputStream::_find_shared_buffer(size_t offset,
                                                                                                  size_t count) {
    auto iter = _map.upper_bound(offset);
    if (iter == _map.end()) {
        return Status::RuntimeError("failed to find shared buffer based on offset");
    }
    SharedBuffer& sb = iter->second;
    if ((sb.offset > offset) || (sb.offset + sb.size) < (offset + count)) {
        return Status::RuntimeError("bad construction of shared buffer");
    }
    return &sb;
}

Status SharedBufferedInputStream::get_bytes(const uint8_t** buffer, size_t offset, size_t* nbytes) {
    ASSIGN_OR_RETURN(auto ret, _find_shared_buffer(offset, *nbytes));
    SharedBuffer& sb = *ret;
    if (sb.buffer.capacity() == 0) {
        SCOPED_RAW_TIMER(&_shared_io_timer);
        _shared_io_count += 1;
        _shared_io_bytes += sb.size;
        sb.buffer.reserve(sb.size);
        RETURN_IF_ERROR(_stream->read_at_fully(sb.offset, sb.buffer.data(), sb.size));
    }

    *buffer = sb.buffer.data() + offset - sb.offset;
    return Status::OK();
}

void SharedBufferedInputStream::release() {
    _map.clear();
}

void SharedBufferedInputStream::release_to_offset(int64_t offset) {
    auto it = _map.upper_bound(offset);
    _map.erase(_map.begin(), it);
}

Status SharedBufferedInputStream::read_at_fully(int64_t offset, void* out, int64_t count) {
    if (_map.size() == 0) {
        SCOPED_RAW_TIMER(&_direct_io_timer);
        _direct_io_count += 1;
        _direct_io_bytes += count;
        RETURN_IF_ERROR(_stream->read_at_fully(offset, out, count));
        return Status::OK();
    }

    const uint8_t* buffer = nullptr;
    size_t nbytes = count;
    RETURN_IF_ERROR(get_bytes(&buffer, offset, &nbytes));
    strings::memcpy_inlined(out, buffer, count);
    return Status::OK();
}

StatusOr<int64_t> SharedBufferedInputStream::get_size() {
    return _size;
}

StatusOr<int64_t> SharedBufferedInputStream::read(void* data, int64_t count) {
    auto n = _stream->read_at(_offset, data, count);
    RETURN_IF_ERROR(n);
    _offset += n.value();
    return n;
}

StatusOr<std::string_view> SharedBufferedInputStream::peek(int64_t count) {
    ASSIGN_OR_RETURN(auto ret, _find_shared_buffer(_offset, count));
    if (ret->buffer.capacity() == 0) return Status::NotSupported("peek shared buffer empty");
    const uint8_t* buf = nullptr;
    size_t nbytes = count;
    RETURN_IF_ERROR(get_bytes(&buf, _offset, &nbytes));
    return std::string_view((const char*)buf, count);
}

} // namespace starrocks::io
