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

#ifdef WITH_TENANN

#include "storage/index/vector/vector_index_file_reader.h"

#include <sys/mman.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <utility>
#include <vector>

#include "base/utility/defer_op.h"
#include "common/config_vector_index_fwd.h"
#include "common/logging.h"
#include "common/status.h"
#include "common/statusor.h"
#include "fs/fs.h"
#include "runtime/mem_tracker.h"
#include "storage/index/vector/vector_index_load_scheduler.h"

namespace starrocks {

namespace {

void merge_io_stats(io::IoStatsSnapshot* dst, const io::IoStatsSnapshot& src) {
    dst->bytes_read_local_disk += src.bytes_read_local_disk;
    dst->bytes_read_remote += src.bytes_read_remote;
    dst->io_count_local_disk += src.io_count_local_disk;
    dst->io_count_remote += src.io_count_remote;
    dst->io_ns_read_local_disk += src.io_ns_read_local_disk;
    dst->io_ns_read_remote += src.io_ns_read_remote;
    dst->prefetch_hit_count += src.prefetch_hit_count;
}

// Below this a range is dominated by its own request rather than by the bytes it moves.
constexpr int64_t kMinParallelChunkBytes = 1 << 20;

int64_t effective_chunk_bytes() {
    return std::max<int64_t>(kMinParallelChunkBytes, config::vector_index_load_parallel_chunk_bytes);
}

// Read [offset, offset+length) into `dst` through several range readers at once. The
// width is decided by VectorIndexLoadScheduler, not here.
Status parallel_read_range(const FileInfo& file_info, const RandomAccessFileOptions& opts, int64_t offset, char* dst,
                           int64_t length, int64_t chunk_bytes, io::IoStatsSnapshot* io_out) {
    // `chunk_bytes` already went through effective_chunk_bytes(); clamping again here
    // would put the floor in two places.
    return VectorIndexLoadScheduler::instance().read(file_info, opts, offset, dst, length, chunk_bytes, io_out);
}

} // namespace

StatusOr<std::unique_ptr<VectorIndexFileReader>> VectorIndexFileReader::open(const FileInfo& file_info,
                                                                             FetchMode mode) {
    if (file_info.fs == nullptr) {
        return Status::InvalidArgument("VectorIndexFileReader needs a FileSystem for " + file_info.path);
    }
    FileInfo resolved = file_info;

    RandomAccessFileOptions block_read_opts;
    block_read_opts.buffer_size = 0;

    ASSIGN_OR_RETURN(auto load_file, resolved.fs->new_random_access_file(resolved));
    if (!resolved.size.has_value()) {
        // Cold path only: the caller normally hands the size down from the factory's get_size().
        ASSIGN_OR_RETURN(auto size, load_file->get_size());
        resolved.size = size;
    }

    const int64_t file_size = resolved.size.value();
    // The size gate lives here because this is the first point where the size is certain;
    // below it the fan-out costs more than the one stream it replaces.
    if (mode != FetchMode::kStreamed && file_size < config::vector_index_load_parallel_min_bytes) {
        mode = FetchMode::kStreamed;
    }
    return std::make_unique<VectorIndexFileReader>(std::move(load_file), std::move(resolved),
                                                   std::move(block_read_opts), file_size, mode);
}

VectorIndexFileReader::TrackedMapping::~TrackedMapping() {
    munmap(_p, _bytes);
    if (_tracker != nullptr) {
        _tracker->release(static_cast<int64_t>(_bytes));
    }
}

void* VectorIndexFileReader::AllocateForRead(size_t bytes, std::shared_ptr<tenann::MemoryOwner>* owner) {
    // Below one huge page there is nothing to win: madvise cannot promote the mapping,
    // and a heap block is charged to the tracker by the allocator hook for free.
    if (bytes < static_cast<size_t>(config::vector_index_load_mmap_min_bytes)) {
        return nullptr;
    }
    // Accounting, not admission: declining sends faiss to the heap for the same bytes,
    // so this does not cap memory. It asks first because mmap bypasses the allocator
    // hook, and an unaccounted mapping would surface as an OOM kill rather than a limit.
    if (_tracker != nullptr && _tracker->try_consume(static_cast<int64_t>(bytes)) != nullptr) {
        return nullptr;
    }
    auto uncharge = [this, bytes] {
        if (_tracker != nullptr) {
            _tracker->release(static_cast<int64_t>(bytes));
        }
    };
    void* p = mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (p == MAP_FAILED) {
        uncharge();
        return nullptr;
    }
    // Until the owner exists, nothing would give the mapping or the charge back. The
    // allocation below can throw, and it is likeliest to throw exactly when memory is
    // tight -- when leaking a multi-gigabyte mapping costs the most.
    CancelableDefer rollback([&] {
        munmap(p, bytes);
        uncharge();
    });
    // The whole point: 4 KiB faults over a multi-gigabyte array cost more than the
    // transfer. Advisory -- without THP the mapping still works, just slower.
    madvise(p, bytes, MADV_HUGEPAGE);
    *owner = std::make_shared<TrackedMapping>(p, bytes, _tracker);
    rollback.cancel();
    return p;
}

int64_t VectorIndexFileReader::Read(void* data, int64_t count) {
    if (count < 0) {
        return -1;
    }
    if (count == 0) {
        return 0;
    }

    // Split anything that spans more than one chunk. The chunk is sized for what makes a
    // single request worthwhile, not for a reader count: how many readers turn up is the
    // scheduler's decision at that moment, and this code cannot know it.
    if (_mode == FetchMode::kParallel && count >= 2 * effective_chunk_bytes()) {
        auto st = parallel_read_range(_file_info, _block_read_options, _position, static_cast<char*>(data), count,
                                      effective_chunk_bytes(), &_parallel_io);
        if (st.ok()) {
            _position += count;
            return count;
        }
        LOG(WARNING) << "parallel vector index read failed, falling back to the streamed read, path=" << _file_info.path
                     << ", position=" << _position << ", count=" << count << ", status=" << st;
    }

    if (_load_file == nullptr) {
        LOG(WARNING) << "sequential read on a released vector index load stream, path=" << _file_info.path
                     << ", position=" << _position << ", count=" << count;
        return -1;
    }
    // read_at_fully() is seek()+read_fully(), and a remote filesystem's readahead treats
    // that seek as a jump, so it never engaged on a strictly forward load. Seek only when
    // the cursor has actually drifted.
    Status st;
    auto pos_or = _load_file->position();
    if (pos_or.ok() && *pos_or == _position) {
        st = _load_file->read_fully(data, count);
    } else {
        st = _load_file->read_at_fully(_position, data, count);
    }
    if (!st.ok()) {
        LOG(WARNING) << "sequential read of vector index file failed, path=" << _file_info.path
                     << ", position=" << _position << ", count=" << count << ", status=" << st;
        return -1;
    }
    _position += count;
    return count;
}

int64_t VectorIndexFileReader::ReadAt(int64_t offset, void* data, int64_t count) {
    // Each call needs an independent stream because RandomAccessFile is not thread-safe.
    auto file_or = _file_info.fs->new_random_access_file(_block_read_options, _file_info);
    if (!file_or.ok()) {
        LOG(WARNING) << "failed to open vector index file for block read, path=" << _file_info.path
                     << ", offset=" << offset << ", count=" << count << ", status=" << file_or.status();
        return -1;
    }
    auto st = (*file_or)->read_at_fully(offset, data, count);
    if (!st.ok()) {
        LOG(WARNING) << "block read of vector index file failed, path=" << _file_info.path << ", offset=" << offset
                     << ", count=" << count << ", status=" << st;
        return -1;
    }
    return count;
}

io::IoStatsSnapshot VectorIndexFileReader::load_file_io_stats() const {
    io::IoStatsSnapshot s;
    if (_load_file != nullptr) {
        if (auto stream = _load_file->stream(); stream) {
            s = stream->get_io_stats_snapshot();
        }
    }
    // The parallel readers use their own handles, so their bytes never reach the load
    // stream. Fold them in or the breakdown reports a load that moved no bytes at all.
    merge_io_stats(&s, _parallel_io);
    return s;
}

} // namespace starrocks

#endif
