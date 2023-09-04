// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "util/buffered_stream.h"

#include "common/config.h"
#include "fs/fs.h"
#include "gutil/strings/fastmem.h"
#include "util/bit_util.h"

namespace starrocks {

// ===================================================================================

SharedBufferedInputStream::SharedBufferedInputStream(std::shared_ptr<SeekableInputStream> stream) : _stream(stream) {}

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

    // check io range is not overlapped.
    for (size_t i = 1; i < ranges.size(); i++) {
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

Status SharedBufferedInputStream::get_bytes(const uint8_t** buffer, size_t offset, size_t* nbytes) {
    auto iter = _map.upper_bound(offset);
    if (iter == _map.end()) {
        return Status::RuntimeError("failed to find shared buffer based on offset");
    }
    SharedBuffer& sb = iter->second;
    if ((sb.offset > offset) || (sb.offset + sb.size) < (offset + *nbytes)) {
        return Status::RuntimeError("bad construction of shared buffer");
    }

    if (sb.buffer.capacity() == 0) {
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

int64_t SharedBufferedInputStream::estimated_mem_usage() const {
    int64_t mem_usage = 0;
    for (const auto& [_, sb] : _map) {
        mem_usage += sb.size;
    }
    // in most cases, those data are compressed.
    // to read it, we need to decompress it, and let's say to add 50% overhead.
    mem_usage += mem_usage / 2;
    return mem_usage;
}

Status SharedBufferedInputStream::read_at_fully(int64_t offset, void* out, int64_t count) {
    const uint8_t* buffer = nullptr;
    size_t nbytes = count;
    RETURN_IF_ERROR(get_bytes(&buffer, offset, &nbytes));
    strings::memcpy_inlined(out, buffer, count);
    return Status::OK();
}

StatusOr<int64_t> SharedBufferedInputStream::get_size() {
    return _stream->get_size();
}

} // namespace starrocks
