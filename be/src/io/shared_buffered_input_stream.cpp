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

<<<<<<< HEAD
=======
#include <gutil/strings/substitute.h>

#include "common/config.h"
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
#include "gutil/strings/fastmem.h"
#include "runtime/current_thread.h"
#include "util/runtime_profile.h"

namespace starrocks::io {

<<<<<<< HEAD
SharedBufferedInputStream::SharedBufferedInputStream(std::shared_ptr<SeekableInputStream> stream,
                                                     const std::string& filename, size_t file_size)
        : _stream(std::move(stream)), _filename(filename), _file_size(file_size) {}
=======
SharedBufferedInputStream::SharedBufferedInputStream(std::shared_ptr<SeekableInputStream> stream, std::string filename,
                                                     size_t file_size)
        : _stream(std::move(stream)), _filename(std::move(filename)), _file_size(file_size) {}
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

void SharedBufferedInputStream::SharedBuffer::align(int64_t align_size, int64_t file_size) {
    if (align_size != 0) {
        offset = raw_offset / align_size * align_size;
        int64_t end = std::min((raw_offset + raw_size + align_size - 1) / align_size * align_size, file_size);
        size = end - offset;
    } else {
        offset = raw_offset;
        size = raw_size;
    }
}

<<<<<<< HEAD
Status SharedBufferedInputStream::set_io_ranges(const std::vector<IORange>& ranges) {
    if (ranges.size() == 0) {
        return Status::OK();
    }

    // specify compare function is important. suppose we have zero range like [351,351],[351,356].
    // If we don't specify compare function, we may have [351,356],[351,351] which is bad order.
    std::vector<IORange> check(ranges);
    std::sort(check.begin(), check.end(), [](const IORange& a, const IORange& b) {
=======
std::string SharedBufferedInputStream::SharedBuffer::debug_string() const {
    return strings::Substitute(
            "SharedBuffer raw_offset=$0, raw_size=$1, offset=$2, size=$3, ref_count=$4, buffer_capacity=$5", raw_offset,
            raw_size, offset, size, ref_count, buffer.capacity());
}

Status SharedBufferedInputStream::_sort_and_check_overlap(std::vector<IORange>& ranges) {
    // specify compare function is important. suppose we have zero range like [351,351],[351,356].
    // If we don't specify compare function, we may have [351,356],[351,351] which is bad order.
    std::sort(ranges.begin(), ranges.end(), [](const IORange& a, const IORange& b) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        if (a.offset != b.offset) {
            return a.offset < b.offset;
        }
        return a.size < b.size;
    });

    // check io range is not overlapped.
<<<<<<< HEAD
    for (size_t i = 1; i < check.size(); i++) {
        if (check[i].offset < (check[i - 1].offset + check[i - 1].size)) {
=======
    for (size_t i = 1; i < ranges.size(); i++) {
        if (ranges[i].offset < (ranges[i - 1].offset + ranges[i - 1].size)) {
            LOG(WARNING) << "io ranges are overalpped" << ranges[i].offset << " "
                         << ranges[i - 1].offset + ranges[i - 1].size;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            return Status::RuntimeError("io ranges are overalpped");
        }
    }

<<<<<<< HEAD
    std::vector<IORange> small_ranges;
    for (const IORange& r : check) {
        if (r.size > _options.max_buffer_size) {
            SharedBuffer sb = SharedBuffer{.raw_offset = r.offset, .raw_size = r.size, .ref_count = 1};
            sb.align(_align_size, _file_size);
            _map.insert(std::make_pair(sb.raw_offset + sb.raw_size, sb));
        } else {
            small_ranges.emplace_back(r);
        }
    }

=======
    return Status::OK();
}

void SharedBufferedInputStream::_merge_small_ranges(const std::vector<IORange>& small_ranges) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    if (small_ranges.size() > 0) {
        auto update_map = [&](size_t from, size_t to) {
            // merge from [unmerge, i-1]
            int64_t ref_count = (to - from + 1);
            int64_t end = (small_ranges[to].offset + small_ranges[to].size);
<<<<<<< HEAD
            SharedBuffer sb = SharedBuffer{.raw_offset = small_ranges[from].offset,
                                           .raw_size = end - small_ranges[from].offset,
                                           .ref_count = ref_count};
            sb.align(_align_size, _file_size);
            _map.insert(std::make_pair(sb.raw_offset + sb.raw_size, sb));
=======
            SharedBufferPtr sb(new SharedBuffer{.raw_offset = small_ranges[from].offset,
                                                .raw_size = end - small_ranges[from].offset,
                                                .ref_count = ref_count});
            sb->align(_align_size, _file_size);
            _map.insert(std::make_pair(sb->raw_offset + sb->raw_size, sb));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
=======
}

Status SharedBufferedInputStream::_set_io_ranges_all_columns(const std::vector<IORange>& ranges) {
    if (ranges.size() == 0) {
        return Status::OK();
    }

    std::vector<IORange> check(ranges);
    RETURN_IF_ERROR(_sort_and_check_overlap(check));

    std::vector<IORange> small_ranges;
    for (const IORange& r : check) {
        if (r.size > _options.max_buffer_size) {
            SharedBufferPtr sb(new SharedBuffer{.raw_offset = r.offset, .raw_size = r.size, .ref_count = 1});
            sb->align(_align_size, _file_size);
            _map.insert(std::make_pair(sb->raw_offset + sb->raw_size, sb));
        } else {
            small_ranges.emplace_back(r);
        }
    }

    _merge_small_ranges(small_ranges);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    _update_estimated_mem_usage();
    return Status::OK();
}

<<<<<<< HEAD
StatusOr<SharedBufferedInputStream::SharedBuffer*> SharedBufferedInputStream::find_shared_buffer(size_t offset,
                                                                                                 size_t count) {
=======
Status SharedBufferedInputStream::_set_io_ranges_active_and_lazy_columns(const std::vector<IORange>& ranges) {
    if (ranges.size() == 0) {
        return Status::OK();
    }

    // specify compare function is important. suppose we have zero range like [351,351],[351,356].
    // If we don't specify compare function, we may have [351,356],[351,351] which is bad order.
    std::vector<IORange> check(ranges);
    RETURN_IF_ERROR(_sort_and_check_overlap(check));

    std::vector<IORange> small_active_ranges;
    std::vector<bool> small_lazy_flag(ranges.size());
    small_lazy_flag.assign(ranges.size(), false);
    for (auto index = 0; index < check.size(); ++index) {
        const IORange& r = check[index];
        if (r.size > _options.max_buffer_size) {
            SharedBufferPtr sb(new SharedBuffer{.raw_offset = r.offset, .raw_size = r.size, .ref_count = 1});
            sb->align(_align_size, _file_size);
            _map.insert(std::make_pair(sb->raw_offset + sb->raw_size, sb));
        } else {
            if (r.is_active) {
                small_active_ranges.emplace_back(r);
            } else {
                small_lazy_flag[index] = true;
            }
        }
    }

    if (small_active_ranges.size() > 0) {
        _merge_small_ranges(small_active_ranges);
    }

    std::vector<IORange> small_lazy_batch_ranges;
    for (auto index = 0; index < small_lazy_flag.size(); ++index) {
        if (!small_lazy_flag[index]) {
            // active column or big column
            continue;
        } else {
            // 1. there may be lazy_column locate in the middle of two active_columns,
            // such as active_column, lazy_column, active_column,
            // that two active_columns have merged and the lazy_column had be contained.
            const IORange& r = check[index];
            auto iter = _map.upper_bound(r.offset);
            if (iter != _map.end()) {
                SharedBufferPtr& sb = iter->second;
                if (sb->offset <= r.offset && sb->offset + sb->size >= r.offset + r.size) {
                    sb->ref_count++;
                    continue;
                }
            }
            small_lazy_batch_ranges.emplace_back(r);
            // 2. there also may be active_column locate in the middle of two lazy_columns,
            // in this case active_column may be contained in two shared_buffer，
            // we should prevent that
            if (index + 1 >= small_lazy_flag.size() || !small_lazy_flag[index + 1]) {
                _merge_small_ranges(small_lazy_batch_ranges);
                small_lazy_batch_ranges.clear();
            }
        }
    }

    _update_estimated_mem_usage();
    return Status::OK();
}

Status SharedBufferedInputStream::set_io_ranges(const std::vector<IORange>& ranges, bool coalesce_lazy_column) {
    if (coalesce_lazy_column || !config::io_coalesce_adaptive_lazy_active) {
        return _set_io_ranges_all_columns(ranges);
    } else {
        return _set_io_ranges_active_and_lazy_columns(ranges);
    }
}

StatusOr<SharedBufferedInputStream::SharedBufferPtr> SharedBufferedInputStream::find_shared_buffer(size_t offset,
                                                                                                   size_t count) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    auto iter = _map.upper_bound(offset);
    if (iter == _map.end()) {
        return Status::RuntimeError("failed to find shared buffer based on offset");
    }
<<<<<<< HEAD
    SharedBuffer& sb = iter->second;
    if ((sb.offset > offset) || (sb.offset + sb.size) < (offset + count)) {
        return Status::RuntimeError("bad construction of shared buffer");
    }
    return &sb;
}

Status SharedBufferedInputStream::get_bytes(const uint8_t** buffer, size_t offset, size_t nbytes) {
    ASSIGN_OR_RETURN(auto ret, find_shared_buffer(offset, nbytes));
    SharedBuffer& sb = *ret;
=======
    const SharedBufferPtr& sb = iter->second;
    if ((sb->offset > offset) || (sb->offset + sb->size) < (offset + count)) {
        return Status::RuntimeError("bad construction of shared buffer");
    }
    return sb;
}

Status SharedBufferedInputStream::get_bytes(const uint8_t** buffer, size_t offset, size_t count,
                                            SharedBufferPtr shared_buffer) {
    if (!shared_buffer) {
        ASSIGN_OR_RETURN(auto ret, find_shared_buffer(offset, count));
        shared_buffer = ret;
    }

    SharedBuffer& sb = *shared_buffer;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    if (sb.buffer.capacity() == 0) {
        RETURN_IF_ERROR(CurrentThread::mem_tracker()->check_mem_limit("read into shared buffer"));
        SCOPED_RAW_TIMER(&_shared_io_timer);
        _shared_io_count += 1;
        _shared_io_bytes += sb.size;
        if (sb.size > sb.raw_size) {
<<<<<<< HEAD
            // after called _deduplicate_shared_buffer(), sb.size may smaller than sb.raw_size
            // we don't count this
=======
            // after called _deduplicate_shared_buffer(), sb.size maybe is larger than sb.raw_size
            // we will count how many extra bytes we read because of alignment.
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            _shared_align_io_bytes += sb.size - sb.raw_size;
        }
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
    auto st = find_shared_buffer(offset, count);
    if (!st.ok()) {
        SCOPED_RAW_TIMER(&_direct_io_timer);
        _direct_io_count += 1;
        _direct_io_bytes += count;
        RETURN_IF_ERROR(_stream->read_at_fully(offset, out, count));
        return Status::OK();
    }
    const uint8_t* buffer = nullptr;
<<<<<<< HEAD
    RETURN_IF_ERROR(get_bytes(&buffer, offset, count));
=======
    RETURN_IF_ERROR(get_bytes(&buffer, offset, count, st.value()));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    strings::memcpy_inlined(out, buffer, count);
    return Status::OK();
}

StatusOr<int64_t> SharedBufferedInputStream::get_size() {
    return _file_size;
}

StatusOr<int64_t> SharedBufferedInputStream::read(void* data, int64_t count) {
    auto n = _stream->read_at(_offset, data, count);
    RETURN_IF_ERROR(n);
    _offset += n.value();
    return n;
}

StatusOr<std::string_view> SharedBufferedInputStream::peek(int64_t count) {
    ASSIGN_OR_RETURN(auto ret, find_shared_buffer(_offset, count));
    if (ret->buffer.capacity() == 0) return Status::NotSupported("peek shared buffer empty");
    const uint8_t* buf = nullptr;
<<<<<<< HEAD
    RETURN_IF_ERROR(get_bytes(&buf, _offset, count));
=======
    RETURN_IF_ERROR(get_bytes(&buf, _offset, count, ret));
    return std::string_view((const char*)buf, count);
}

StatusOr<std::string_view> SharedBufferedInputStream::peek_shared_buffer(int64_t count,
                                                                         SharedBufferPtr* shared_buffer) {
    ASSIGN_OR_RETURN(auto ret, find_shared_buffer(_offset, count));
    if (ret->buffer.capacity() == 0) return Status::NotSupported("peek shared buffer empty");
    const uint8_t* buf = ret->buffer.data() + _offset - ret->offset;
    if (shared_buffer) {
        *shared_buffer = ret;
    }
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    return std::string_view((const char*)buf, count);
}

void SharedBufferedInputStream::_update_estimated_mem_usage() {
    int64_t mem_usage = 0;
    for (const auto& [_, sb] : _map) {
<<<<<<< HEAD
        mem_usage += sb.size;
=======
        mem_usage += sb->size;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }
    // in most cases, those data are compressed.
    // to read it, we need to decompress it, and let's say to add 50% overhead.
    mem_usage += mem_usage / 2;
    _estimated_mem_usage = std::max(mem_usage, _estimated_mem_usage);
}

<<<<<<< HEAD
=======
int64_t SharedBufferedInputStream::current_range_ref_sum() const {
    int64_t ref = 0;
    for (const auto& [_, sb] : _map) {
        ref += sb->ref_count;
    }
    return ref;
}

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
} // namespace starrocks::io
