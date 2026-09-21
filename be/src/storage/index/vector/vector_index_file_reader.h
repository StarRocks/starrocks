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

#include <cstdint>
#include <memory>
#include <string>

#include "common/statusor.h"
#include "fs/fs.h"
#include "storage/index/vector/vector_index_load_scheduler.h"
#include "tenann/store/index_file_reader.h"

namespace starrocks {

class MemTracker;

// How the `.vi` bytes reach memory. One connection is far slower than the node's
// bandwidth, so a cold multi-gigabyte load is bounded by the stream, not the link.
enum class VectorIndexFetchMode {
    // One sequential stream. Shared-nothing, indexes too small to pay for the fan-out,
    // and IVF-PQ, which reads per-list blocks on demand.
    kStreamed,
    // Each large read split across range readers working on one file at a time.
    kParallel,
};

// Bridge StarRocks RandomAccessFile to TenANN IndexFileReader interface,
// enabling TenANN to read vector index files from remote storage (S3/HDFS/OSS).
//
// TenANN may call ReadAt() concurrently for different IVF lists. Each call opens an
// independent RandomAccessFile because StarRocks streams are stateful and not thread-safe.
class VectorIndexFileReader : public tenann::IndexFileReader {
public:
    using FetchMode = VectorIndexFetchMode;

    // One block handed to faiss for an index array. faiss holds it through the cached
    // index, long after this reader is gone, so the block carries its own release and
    // its own accounting -- mmap bypasses the allocator hook that would otherwise
    // charge these bytes to the process tracker.
    class TrackedMapping : public tenann::MemoryOwner {
    public:
        TrackedMapping(void* p, size_t bytes, std::shared_ptr<MemTracker> tracker)
                : _p(p), _bytes(bytes), _tracker(std::move(tracker)) {}
        ~TrackedMapping() override;

    private:
        void* _p;
        size_t _bytes;
        std::shared_ptr<MemTracker> _tracker;
    };

    // `file_info.fs` must be non-null and is retained: this reader outlives the
    // SegmentIterator that opened it. `mode` is a request -- the size gate is applied
    // here, where the size is known, so the effective mode may come back as kStreamed.
    static StatusOr<std::unique_ptr<VectorIndexFileReader>> open(const FileInfo& file_info,
                                                                 FetchMode mode = FetchMode::kStreamed);

    VectorIndexFileReader(std::unique_ptr<RandomAccessFile> load_file, FileInfo file_info,
                          RandomAccessFileOptions block_read_options, int64_t file_size, FetchMode mode)
            : _load_file(std::move(load_file)),
              _file_info(std::move(file_info)),
              _block_read_options(std::move(block_read_options)),
              _file_size(file_size),
              _mode(mode),
              _filename(_file_info.path) {}

    ~VectorIndexFileReader() override = default;

    FetchMode fetch_mode() const { return _mode; }

    // Used only during the initial index load.
    int64_t Read(void* data, int64_t count) override;

    // Positioned reads for different inverted lists may run concurrently.
    int64_t ReadAt(int64_t offset, void* data, int64_t count) override;

    void Seek(int64_t pos) override { _position = pos; }

    int64_t GetSize() override { return _file_size; }

    const std::string& filename() const override { return _filename; }

    // Block reads open their own files, so the load stream can go once the index is
    // built. The mappings handed to faiss are not affected: they outlive this reader.
    void release_load_file() { _load_file.reset(); }

    // All-zero once the stream is released, and on plain POSIX, so snapshot before
    // release_load_file().
    io::IoStatsSnapshot load_file_io_stats() const;

    // Hand faiss a mapping for one index array instead of letting it resize a vector.
    // `resize()` zero-fills the whole array before the read overwrites it, and the heap
    // block it allocates is backed by 4 KiB pages; a mapping skips the zero pass and
    // takes huge pages, which is where nearly all of the load time goes.
    //
    // Returning nullptr is a supported outcome, not a failure: faiss allocates the array
    // itself and the load still succeeds. Used for small arrays and when the tracker has
    // no room.
    void* AllocateForRead(size_t bytes, std::shared_ptr<tenann::MemoryOwner>* owner) override;

    // The mappings outlive this reader, so the tracker they charge has to as well.
    void set_mem_tracker(std::shared_ptr<MemTracker> tracker) { _tracker = std::move(tracker); }

private:
    std::unique_ptr<RandomAccessFile> _load_file;
    // Range readers use their own handles, so their IO never reaches the load stream.
    io::IoStatsSnapshot _parallel_io;
    // Retains the FileSystem and resolved size for future block-cache misses.
    FileInfo _file_info;
    // Disables readahead for independent block reads.
    RandomAccessFileOptions _block_read_options;
    int64_t _file_size = 0;
    int64_t _position = 0;
    // Charged by every mapping handed to faiss, released by TrackedMapping. Null outside
    // the BE, where the reader is exercised without a tracker.
    std::shared_ptr<MemTracker> _tracker;
    FetchMode _mode = FetchMode::kStreamed;
    std::string _filename;
};

} // namespace starrocks

#endif
