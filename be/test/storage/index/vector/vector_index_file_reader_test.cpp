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

#include "storage/index/vector/vector_index_file_reader.h"

#include <gtest/gtest.h>

#ifdef WITH_TENANN

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "base/string/slice.h"
#include "base/testutil/assert.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "common/config_vector_index_fwd.h"
#include "common/status.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "io/seekable_input_stream.h"
#include "runtime/mem_tracker.h"
#include "storage/index/vector/tenann_index_reader.h"
#include "tenann/store/index_meta.h"

namespace starrocks {

namespace {

class ProbeFileSystem;

// A stream that is stateful in exactly the way every real StarRocks leaf stream is:
// seek() only records the offset and read() serves from the recorded offset. That is
// what makes the default SeekableInputStream::read_at_fully() (seek + read_fully) unsafe
// to share, so a reader that reuses one stream across concurrent ReadAt() calls reads
// another list's bytes here just like it would on S3 or posix.
class StatefulProbeStream final : public io::SeekableInputStream {
public:
    StatefulProbeStream(ProbeFileSystem* owner, std::string_view payload) : _owner(owner), _payload(payload) {}

    StatusOr<int64_t> read(void* data, int64_t count) override;

    Status seek(int64_t position) override {
        _offset = position;
        return Status::OK();
    }

    StatusOr<int64_t> position() override { return _offset; }

    StatusOr<int64_t> get_size() override { return static_cast<int64_t>(_payload.size()); }

    Status skip(int64_t count) override {
        _offset += count;
        return Status::OK();
    }

    // Real leaf streams report which layer served the bytes; the reader folds those
    // counters into the query profile, so the fake has to report something too.
    io::IoStatsSnapshot get_io_stats_snapshot() const override {
        io::IoStatsSnapshot s;
        s.bytes_read_remote = _bytes_read;
        s.io_count_remote = _reads;
        return s;
    }

private:
    ProbeFileSystem* _owner;
    std::string_view _payload;
    int64_t _offset = 0;
    int64_t _bytes_read = 0;
    int64_t _reads = 0;
};

// Records every RandomAccessFile it hands out, the options each open was given, and how
// many reads are in flight at once, so a test can tell "one file per ReadAt, run in
// parallel" apart from "one shared file" and from "serialized behind a mutex".
class ProbeFileSystem : public MemoryFileSystem {
public:
    explicit ProbeFileSystem(std::string payload) : _payload(std::move(payload)) {}

    using MemoryFileSystem::new_random_access_file;

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& url) override {
        {
            std::lock_guard<std::mutex> guard(_mu);
            _seen_options.push_back(opts);
        }
        if (fail_open.load()) {
            return Status::IOError("injected open failure");
        }
        auto stream = std::make_shared<StatefulProbeStream>(this, _payload);
        {
            std::lock_guard<std::mutex> guard(_mu);
            _handed_out.push_back(stream.get());
        }
        return std::make_unique<RandomAccessFile>(std::move(stream), url);
    }

    std::string_view payload() const { return _payload; }

    size_t open_count() {
        std::lock_guard<std::mutex> guard(_mu);
        return _seen_options.size();
    }

    // Distinct stream objects handed out. Equal to open_count() unless the reader reuses
    // a file, which is the bug this whole fix is about.
    size_t distinct_streams() {
        std::lock_guard<std::mutex> guard(_mu);
        std::vector<io::SeekableInputStream*> uniq(_handed_out);
        std::sort(uniq.begin(), uniq.end());
        uniq.erase(std::unique(uniq.begin(), uniq.end()), uniq.end());
        return uniq.size();
    }

    std::vector<RandomAccessFileOptions> seen_options() {
        std::lock_guard<std::mutex> guard(_mu);
        return _seen_options;
    }

    int max_active_reads() const { return _max_active_reads.load(); }
    int read_count() const { return _read_count.load(); }

    void enter_read() {
        _read_count.fetch_add(1);
        int active = _active_reads.fetch_add(1) + 1;
        int observed = _max_active_reads.load();
        while (active > observed && !_max_active_reads.compare_exchange_weak(observed, active)) {
        }
    }
    void leave_read() { _active_reads.fetch_sub(1); }

    std::atomic<bool> fail_open{false};
    std::atomic<bool> fail_read{false};
    std::atomic<int64_t> read_delay_ms{0};

private:
    std::string _payload;
    std::mutex _mu;
    std::vector<RandomAccessFileOptions> _seen_options;
    std::vector<io::SeekableInputStream*> _handed_out;
    std::atomic<int> _read_count{0};
    std::atomic<int> _active_reads{0};
    std::atomic<int> _max_active_reads{0};
};

StatusOr<int64_t> StatefulProbeStream::read(void* data, int64_t count) {
    _owner->enter_read();
    // Sleeping between the seek() that read_at_fully() already did and the copy below is
    // what a real remote read does; it is also what turns a shared stream into corruption.
    if (auto delay = _owner->read_delay_ms.load(); delay > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(delay));
    }
    if (_owner->fail_read.load()) {
        _owner->leave_read();
        return Status::IOError("injected read failure");
    }
    const auto size = static_cast<int64_t>(_payload.size());
    if (_offset < 0 || _offset > size) {
        _owner->leave_read();
        return Status::IOError("offset out of range");
    }
    const int64_t n = std::min(count, size - _offset);
    memcpy(data, _payload.data() + _offset, n);
    _offset += n;
    _bytes_read += n;
    _reads += 1;
    _owner->leave_read();
    return n;
}

// Each block gets its own byte so a read that lands on the wrong offset is visible.
std::string make_payload(int blocks, int block_size) {
    std::string payload;
    payload.reserve(blocks * block_size);
    for (int i = 0; i < blocks; ++i) {
        payload.append(block_size, static_cast<char>('a' + i));
    }
    return payload;
}

// Ranges are floored at 1 MiB, so a payload has to clear a few of those before anything
// is actually split. Distinct bytes per range keep a misplaced read visible.
constexpr int64_t kMiB = 1 << 20;

std::string make_ranged_payload(int ranges) {
    std::string payload;
    payload.reserve(ranges * kMiB);
    for (int i = 0; i < ranges; ++i) {
        payload.append(kMiB, static_cast<char>('a' + (i % 26)));
    }
    return payload;
}

// Restores every config this file moves, so one test cannot leak a setting into the next.
class ScopedLoadConfig {
public:
    ScopedLoadConfig(int32_t threads, int64_t min_bytes, int64_t chunk_bytes)
            : _threads(config::vector_index_load_parallel_threads),
              _min_bytes(config::vector_index_load_parallel_min_bytes),
              _chunk_bytes(config::vector_index_load_parallel_chunk_bytes) {
        config::vector_index_load_parallel_threads = threads;
        config::vector_index_load_parallel_min_bytes = min_bytes;
        config::vector_index_load_parallel_chunk_bytes = chunk_bytes;
    }
    ~ScopedLoadConfig() {
        config::vector_index_load_parallel_threads = _threads;
        config::vector_index_load_parallel_min_bytes = _min_bytes;
        config::vector_index_load_parallel_chunk_bytes = _chunk_bytes;
    }

private:
    int32_t _threads;
    int64_t _min_bytes;
    int64_t _chunk_bytes;
};

tenann::IndexMeta meta_of(tenann::IndexType type) {
    tenann::IndexMeta meta;
    meta.SetIndexFamily(tenann::IndexFamily::kVectorIndex);
    meta.SetIndexType(type);
    return meta;
}

} // namespace

// Concurrent ReadAt() on different offsets must return each caller's own bytes. With one
// shared RandomAccessFile the seek/read pair interleaves and callers get another block's
// data with an OK status, so this is the direct regression guard for the fix.
TEST(VectorIndexFileReaderTest, ConcurrentReadAtReturnsCorrectBytes) {
    constexpr int kBlocks = 4;
    constexpr int kBlockSize = 512;
    auto fs = std::make_shared<ProbeFileSystem>(make_payload(kBlocks, kBlockSize));
    fs->read_delay_ms.store(30);

    FileInfo file_info{.path = "/probe/index.vi", .fs = fs};
    ASSIGN_OR_ABORT(auto reader, VectorIndexFileReader::open(file_info));
    ASSERT_EQ(kBlocks * kBlockSize, reader->GetSize());
    const size_t opens_after_open = fs->open_count();

    std::vector<std::string> results(kBlocks);
    std::vector<std::thread> threads;
    for (int i = 0; i < kBlocks; ++i) {
        threads.emplace_back([&, i] {
            results[i].resize(kBlockSize);
            int64_t n = reader->ReadAt(i * kBlockSize, results[i].data(), kBlockSize);
            EXPECT_EQ(kBlockSize, n);
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    for (int i = 0; i < kBlocks; ++i) {
        EXPECT_EQ(std::string(kBlockSize, static_cast<char>('a' + i)), results[i]) << "block " << i;
    }
    // One fresh file per ReadAt, none reused.
    EXPECT_EQ(opens_after_open + kBlocks, fs->open_count());
    EXPECT_EQ(fs->open_count(), fs->distinct_streams());
}

// Guards against the fix regressing into a mutex around a shared file: that would still
// return the right bytes but would serialize remote reads, which is what the per-list
// concurrency in tenann's block cache exists to avoid.
TEST(VectorIndexFileReaderTest, ConcurrentReadAtStaysParallel) {
    constexpr int kBlocks = 4;
    constexpr int kBlockSize = 256;
    auto fs = std::make_shared<ProbeFileSystem>(make_payload(kBlocks, kBlockSize));
    fs->read_delay_ms.store(50);

    ASSIGN_OR_ABORT(auto reader, VectorIndexFileReader::open(FileInfo{.path = "/probe/index.vi", .fs = fs}));

    std::vector<std::thread> threads;
    for (int i = 0; i < kBlocks; ++i) {
        threads.emplace_back([&, i] {
            std::string buf(kBlockSize, '\0');
            EXPECT_EQ(kBlockSize, reader->ReadAt(i * kBlockSize, buf.data(), kBlockSize));
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_GT(fs->max_active_reads(), 1);
}

TEST(VectorIndexFileReaderTest, BlockReadsDisableReadAhead) {
    auto fs = std::make_shared<ProbeFileSystem>(make_payload(2, 64));
    ASSIGN_OR_ABORT(auto reader, VectorIndexFileReader::open(FileInfo{.path = "/probe/index.vi", .fs = fs}));

    std::string buf(64, '\0');
    ASSERT_EQ(64, reader->ReadAt(0, buf.data(), 64));
    ASSERT_EQ(64, reader->ReadAt(64, buf.data(), 64));

    auto options = fs->seen_options();
    ASSERT_GE(options.size(), 3u);
    EXPECT_EQ(-1, options[0].buffer_size);
    for (size_t i = 1; i < options.size(); ++i) {
        EXPECT_EQ(0, options[i].buffer_size);
    }
}

// The reader lives in the tenann index cache and outlives the Segment that opened it, so
// it must keep the FileSystem alive by itself. Dropping every other owner and then reading
// is exactly the production lifetime.
TEST(VectorIndexFileReaderTest, ReadsAfterCallerDropsFileSystem) {
    auto fs = std::make_shared<ProbeFileSystem>(make_payload(2, 32));
    auto* fs_raw = fs.get();
    ASSIGN_OR_ABORT(auto reader, VectorIndexFileReader::open(FileInfo{.path = "/probe/index.vi", .fs = fs}));

    fs.reset(); // the reader is now the only owner
    std::string buf(32, '\0');
    ASSERT_EQ(32, reader->ReadAt(32, buf.data(), 32));
    EXPECT_EQ(std::string(32, 'b'), buf);
    EXPECT_GT(fs_raw->read_count(), 0);
}

// Nothing reads the sequential stream after the initial index load, so dropping it must
// leave ReadAt() and GetSize() working while Read() fails cleanly instead of dereferencing.
TEST(VectorIndexFileReaderTest, ReleaseLoadFileKeepsBlockReadsWorking) {
    auto fs = std::make_shared<ProbeFileSystem>(make_payload(2, 16));
    ASSIGN_OR_ABORT(auto reader, VectorIndexFileReader::open(FileInfo{.path = "/probe/index.vi", .fs = fs}));

    std::string buf(16, '\0');
    ASSERT_EQ(16, reader->Read(buf.data(), 16));
    EXPECT_EQ(std::string(16, 'a'), buf);

    reader->release_load_file();

    EXPECT_EQ(-1, reader->Read(buf.data(), 16));
    EXPECT_EQ(32, reader->GetSize());
    ASSERT_EQ(16, reader->ReadAt(16, buf.data(), 16));
    EXPECT_EQ(std::string(16, 'b'), buf);
}

// Open failures and read failures both surface as -1 (tenann throws on that), but they must
// be distinguishable in the log, so keep both paths covered.
TEST(VectorIndexFileReaderTest, OpenAndReadFailuresBothReturnMinusOne) {
    auto fs = std::make_shared<ProbeFileSystem>(make_payload(1, 16));
    ASSIGN_OR_ABORT(auto reader, VectorIndexFileReader::open(FileInfo{.path = "/probe/index.vi", .fs = fs}));
    std::string buf(16, '\0');

    fs->fail_open.store(true);
    EXPECT_EQ(-1, reader->ReadAt(0, buf.data(), 16));
    fs->fail_open.store(false);

    fs->fail_read.store(true);
    EXPECT_EQ(-1, reader->ReadAt(0, buf.data(), 16));
    fs->fail_read.store(false);

    EXPECT_EQ(16, reader->ReadAt(0, buf.data(), 16));
}

// The factory normally hands the size down, but it skips opening the file on a cache hit;
// if the entry is evicted before init_searcher runs, open() has to resolve the size itself.
TEST(VectorIndexFileReaderTest, ResolvesSizeWhenCallerDidNotSupplyIt) {
    auto fs = std::make_shared<ProbeFileSystem>(make_payload(4, 8));
    ASSIGN_OR_ABORT(auto reader, VectorIndexFileReader::open(FileInfo{.path = "/probe/index.vi", .fs = fs}));
    EXPECT_EQ(32, reader->GetSize());
}

TEST(VectorIndexFileReaderTest, OpenWithoutFileSystemIsRejected) {
    auto res = VectorIndexFileReader::open(FileInfo{.path = "/probe/index.vi"});
    EXPECT_TRUE(res.status().is_invalid_argument()) << res.status();
}

// Same assertion as ConcurrentReadAtReturnsCorrectBytes, but over the real posix
// filesystem: FdInputStream::seek() stores an offset that read() then preads from, so a
// shared stream corrupts here for the same reason it does on S3. The fake-filesystem test
// above is the deterministic one; this covers the production stream stack end to end.
TEST(VectorIndexFileReaderTest, ConcurrentReadAtOverPosixFile) {
    constexpr int kBlocks = 8;
    constexpr int kBlockSize = 4096;
    constexpr int kRounds = 20;

    const std::string path = std::string(getenv("TMPDIR") != nullptr ? getenv("TMPDIR") : "/tmp") +
                             "/vector_index_file_reader_concurrent.bin";
    const std::string payload = make_payload(kBlocks, kBlockSize);
    {
        ASSIGN_OR_ABORT(auto wfile, FileSystem::Default()->new_writable_file(path));
        ASSERT_OK(wfile->append(Slice(payload)));
        ASSERT_OK(wfile->close());
    }

    // Default() is a process-lifetime singleton, so a non-owning alias is safe here.
    std::shared_ptr<FileSystem> fs(FileSystem::Default(), [](FileSystem*) {});
    ASSIGN_OR_ABORT(auto reader, VectorIndexFileReader::open(FileInfo{.path = path, .fs = fs}));
    ASSERT_EQ(payload.size(), static_cast<size_t>(reader->GetSize()));

    std::atomic<int> mismatches{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < kBlocks; ++i) {
        threads.emplace_back([&, i] {
            const std::string expected(kBlockSize, static_cast<char>('a' + i));
            std::string buf(kBlockSize, '\0');
            for (int round = 0; round < kRounds; ++round) {
                if (reader->ReadAt(i * kBlockSize, buf.data(), kBlockSize) != kBlockSize || buf != expected) {
                    mismatches.fetch_add(1);
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(0, mismatches.load());
    (void)FileSystem::Default()->delete_file(path);
}

// ==================== fetch mode selection ====================
// Everything below depends on this picking the right mode; a wrong answer here is silent.

TEST(VectorIndexFetchModeTest, SharedNothingNeverFansOut) {
    ScopedLoadConfig cfg(/*threads=*/8, /*min_bytes=*/1, /*chunk_bytes=*/kMiB);
    // No FileSystem means tenann reads a local path, where the kernel already does this.
    EXPECT_EQ(VectorIndexFetchMode::kStreamed,
              pick_vector_index_fetch_mode(meta_of(tenann::IndexType::kFaissHnsw), FileInfo{.path = "/local.vi"}));
}

TEST(VectorIndexFetchModeTest, IvfPqIsLeftOnTheStream) {
    ScopedLoadConfig cfg(8, 1, kMiB);
    auto fs = std::make_shared<ProbeFileSystem>(make_payload(1, 16));
    // IVF-PQ reads per-list blocks on demand. Reading ahead of it would pull the whole
    // file to serve a few lists -- the exact mis-trigger the old gate had.
    EXPECT_EQ(VectorIndexFetchMode::kStreamed,
              pick_vector_index_fetch_mode(meta_of(tenann::IndexType::kFaissIvfPq),
                                           FileInfo{.path = "/probe/index.vi", .fs = fs}));
}

TEST(VectorIndexFetchModeTest, OneThreadKeepsTheOldStreamedRead) {
    ScopedLoadConfig cfg(/*threads=*/1, 1, kMiB);
    auto fs = std::make_shared<ProbeFileSystem>(make_payload(1, 16));
    EXPECT_EQ(VectorIndexFetchMode::kStreamed,
              pick_vector_index_fetch_mode(meta_of(tenann::IndexType::kFaissHnsw),
                                           FileInfo{.path = "/probe/index.vi", .fs = fs}));
}

// The size gate lives in open(), which is the first point where the size is certain.
TEST(VectorIndexFileReaderTest, OpenDowngradesAModeBelowTheSizeGate) {
    ScopedLoadConfig cfg(8, /*min_bytes=*/1024, kMiB);
    auto fs = std::make_shared<ProbeFileSystem>(make_payload(1, 16));
    ASSIGN_OR_ABORT(auto small, VectorIndexFileReader::open(FileInfo{.path = "/probe/index.vi", .fs = fs},
                                                            VectorIndexFetchMode::kParallel));
    EXPECT_EQ(VectorIndexFetchMode::kStreamed, small->fetch_mode());

    auto big_fs = std::make_shared<ProbeFileSystem>(make_payload(64, 32));
    ASSIGN_OR_ABORT(auto big, VectorIndexFileReader::open(FileInfo{.path = "/probe/index.vi", .fs = big_fs},
                                                          VectorIndexFetchMode::kParallel));
    EXPECT_EQ(VectorIndexFetchMode::kParallel, big->fetch_mode());
}

// ==================== kParallel: parallel ranges into the caller's buffer ====================

TEST(VectorIndexFileReaderTest, ParallelSplitsALargeReadAndReturnsTheRightBytes) {
    constexpr int kRanges = 6;
    ScopedLoadConfig cfg(/*threads=*/4, /*min_bytes=*/1, /*chunk_bytes=*/kMiB);
    auto fs = std::make_shared<ProbeFileSystem>(make_ranged_payload(kRanges));
    fs->read_delay_ms.store(20);
    ASSIGN_OR_ABORT(auto reader, VectorIndexFileReader::open(FileInfo{.path = "/probe/index.vi", .fs = fs},
                                                             VectorIndexFetchMode::kParallel));

    std::string buf(kRanges * kMiB, '\0');
    ASSERT_EQ(kRanges * kMiB, reader->Read(buf.data(), kRanges * kMiB));
    EXPECT_EQ(fs->payload(), buf);
    // Reads really overlapped; a serialized implementation would return the same bytes.
    EXPECT_GT(fs->max_active_reads(), 1);
}

// One handle per worker, reused across every range it claims. At these sizes opening a
// file per range costs more than the read, which is why this is asserted and not assumed.
TEST(VectorIndexFileReaderTest, ParallelReusesOneHandlePerWorker) {
    constexpr int kRanges = 16;
    constexpr int32_t kThreads = 4;
    ScopedLoadConfig cfg(kThreads, 1, kMiB);
    auto fs = std::make_shared<ProbeFileSystem>(make_ranged_payload(kRanges));
    ASSIGN_OR_ABORT(auto reader, VectorIndexFileReader::open(FileInfo{.path = "/probe/index.vi", .fs = fs},
                                                             VectorIndexFetchMode::kParallel));
    const size_t opens_before = fs->open_count();

    std::string buf(kRanges * kMiB, '\0');
    ASSERT_EQ(kRanges * kMiB, reader->Read(buf.data(), kRanges * kMiB));

    const size_t opens = fs->open_count() - opens_before;
    EXPECT_LE(opens, static_cast<size_t>(kThreads)) << "a handle per range, not per worker";
    EXPECT_GE(opens, 1u);
}

// Only the bulk reads are worth splitting. The index load also makes a couple of dozen
// tiny ones, and fanning those out would cost more than it saves.
TEST(VectorIndexFileReaderTest, ParallelLeavesSmallReadsOnTheStream) {
    ScopedLoadConfig cfg(4, 1, kMiB);
    auto fs = std::make_shared<ProbeFileSystem>(make_ranged_payload(4));
    ASSIGN_OR_ABORT(auto reader, VectorIndexFileReader::open(FileInfo{.path = "/probe/index.vi", .fs = fs},
                                                             VectorIndexFetchMode::kParallel));
    const size_t opens_before = fs->open_count();

    std::string buf(1024, '\0');
    ASSERT_EQ(1024, reader->Read(buf.data(), 1024));
    EXPECT_EQ(std::string(1024, 'a'), buf);
    EXPECT_EQ(opens_before, fs->open_count()) << "a small read must not open anything";
}

// A range that cannot even be opened must not fail the load: the streamed read is still
// there, and it is correctness that matters, not speed.
TEST(VectorIndexFileReaderTest, ParallelFallsBackToTheStreamWhenRangesCannotOpen) {
    constexpr int kRanges = 4;
    ScopedLoadConfig cfg(4, 1, kMiB);
    auto fs = std::make_shared<ProbeFileSystem>(make_ranged_payload(kRanges));
    ASSIGN_OR_ABORT(auto reader, VectorIndexFileReader::open(FileInfo{.path = "/probe/index.vi", .fs = fs},
                                                             VectorIndexFetchMode::kParallel));

    // The load stream is already open, so only the range readers hit this.
    fs->fail_open.store(true);
    std::string buf(kRanges * kMiB, '\0');
    ASSERT_EQ(kRanges * kMiB, reader->Read(buf.data(), kRanges * kMiB));
    EXPECT_EQ(fs->payload(), buf);
}

// The bytes must not depend on how they were fetched.
TEST(VectorIndexFileReaderTest, ParallelAndStreamedReadsAgree) {
    constexpr int kRanges = 5;
    ScopedLoadConfig cfg(4, 1, kMiB);
    const std::string payload = make_ranged_payload(kRanges);

    auto read_all = [&](VectorIndexFetchMode mode) {
        auto fs = std::make_shared<ProbeFileSystem>(payload);
        auto r = VectorIndexFileReader::open(FileInfo{.path = "/probe/index.vi", .fs = fs}, mode);
        CHECK(r.ok());
        std::string buf(payload.size(), '\0');
        EXPECT_EQ(static_cast<int64_t>(payload.size()), (*r)->Read(buf.data(), payload.size()));
        return buf;
    };
    EXPECT_EQ(read_all(VectorIndexFetchMode::kStreamed), read_all(VectorIndexFetchMode::kParallel));
}

// ==================== The load scheduler ====================
// The width of a load is no longer decided by the load: it registers with a process-wide
// scheduler that hands out a bounded set of helpers across every concurrent load. Two
// properties of that arrangement are worth pinning down, because the rest of the design
// rests on them.

// With no helper budget at all the calling thread must still read the whole file on its
// own. This is the degenerate case the "never slower than before" guarantee rests on --
// if it ever stopped working, a busy node would silently return short reads.
TEST(VectorIndexFileReaderTest, NoHelperBudgetStillReadsEverything) {
    constexpr int kRanges = 8;
    ScopedLoadConfig cfg(/*threads=*/1, 1, kMiB); // 1 == the calling thread only
    const std::string payload = make_ranged_payload(kRanges);
    auto fs = std::make_shared<ProbeFileSystem>(payload);
    ASSIGN_OR_ABORT(auto reader, VectorIndexFileReader::open(FileInfo{.path = "/probe/index.vi", .fs = fs},
                                                             VectorIndexFetchMode::kParallel));
    const size_t opens_before = fs->open_count();

    std::string buf(payload.size(), '\0');
    ASSERT_EQ(static_cast<int64_t>(payload.size()), reader->Read(buf.data(), payload.size()));
    EXPECT_EQ(payload, buf);
    EXPECT_EQ(1u, fs->open_count() - opens_before) << "no helpers means exactly one reader";
}

// Several loads run through one scheduler and one worker budget. Each must come back with
// its own bytes: a request picking up another's chunk, or two sharing a stream, would
// corrupt an index rather than fail loudly.
// The window between a worker being handed a request and being counted as reading it.
//
// The request lives on the calling thread's stack and is kept alive only by the count of
// readers: the caller waits for that count to reach zero, unregisters, and returns, at
// which point the object is gone. A worker that is holding the request but has not been
// counted yet is therefore holding something that may be destroyed under it.
//
// Reproduced by parking a worker in exactly that gap and asking whether the caller can
// finish while it is parked. It must not be able to. The assertion is on that invariant
// rather than on a crash, so the test reports the defect instead of merely dying of it.
TEST(VectorIndexFileReaderTest, CallerCannotFinishWhileAWorkerHoldsTheRequest) {
    constexpr int kRanges = 8;
    ScopedLoadConfig cfg(/*threads=*/4, /*min_bytes=*/1, /*chunk_bytes=*/kMiB);

    std::atomic<bool> worker_parked{false};
    std::atomic<bool> read_returned{false};
    std::atomic<bool> release_worker{false};

    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp cleanup([] {
        SyncPoint::GetInstance()->ClearCallBack("VectorIndexLoadScheduler::worker_task:picked");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    SyncPoint::GetInstance()->SetCallBack("VectorIndexLoadScheduler::worker_task:picked", [&](void*) {
        // Only the first worker parks; the rest must stay free to finish the read.
        bool expected = false;
        if (!worker_parked.compare_exchange_strong(expected, true)) {
            return;
        }
        while (!release_worker.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });

    const std::string payload = make_ranged_payload(kRanges);
    std::string out(payload.size(), '\0');
    std::thread caller([&] {
        auto fs = std::make_shared<ProbeFileSystem>(payload);
        auto r = VectorIndexFileReader::open(FileInfo{.path = "/probe/index.vi", .fs = fs},
                                             VectorIndexFetchMode::kParallel);
        CHECK(r.ok());
        CHECK_EQ(static_cast<int64_t>(payload.size()), (*r)->Read(out.data(), payload.size()));
        read_returned.store(true, std::memory_order_release);
    });

    // Wait for a worker to reach the gap, then give the caller every chance to run away.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!worker_parked.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(worker_parked.load()) << "no worker ever reached the pick/read gap; the test proves nothing";
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    EXPECT_FALSE(read_returned.load(std::memory_order_acquire))
            << "the load finished while a worker still held its request: the request is on the caller's "
               "stack, so it has now been destroyed under a thread that is about to read through it";

    release_worker.store(true, std::memory_order_release);
    caller.join();
    EXPECT_EQ(payload, out);
}

TEST(VectorIndexFileReaderTest, ConcurrentLoadsEachGetTheirOwnBytes) {
    constexpr int kRanges = 6;
    constexpr int kLoads = 4;
    ScopedLoadConfig cfg(8, 1, kMiB);

    std::vector<std::string> payloads;
    payloads.reserve(kLoads);
    for (int i = 0; i < kLoads; ++i) {
        // Distinct content per load, so a cross-request mix-up cannot go unnoticed.
        std::string p = make_ranged_payload(kRanges);
        for (size_t j = 0; j < p.size(); j += kMiB) p[j] = static_cast<char>('A' + i);
        payloads.push_back(std::move(p));
    }

    std::vector<std::string> results(kLoads);
    std::vector<std::thread> callers;
    for (int i = 0; i < kLoads; ++i) {
        callers.emplace_back([&, i] {
            auto fs = std::make_shared<ProbeFileSystem>(payloads[i]);
            auto r = VectorIndexFileReader::open(FileInfo{.path = "/probe/index.vi", .fs = fs},
                                                 VectorIndexFetchMode::kParallel);
            CHECK(r.ok());
            results[i].assign(payloads[i].size(), '\0');
            CHECK_EQ(static_cast<int64_t>(payloads[i].size()), (*r)->Read(results[i].data(), payloads[i].size()));
        });
    }
    for (auto& t : callers) t.join();

    for (int i = 0; i < kLoads; ++i) {
        EXPECT_EQ(payloads[i], results[i]) << "load " << i << " got someone else's bytes";
    }
}

// One file must not soak up the whole reader budget. Past a handful of connections an
// object stays bounded by the same link, so the extra readers only queue requests at the
// same place -- readers beyond the cap are better left for the next file.
TEST(VectorIndexFileReaderTest, OneFileDoesNotTakeMoreThanItsShareOfReaders) {
    constexpr int kRanges = 64; // plenty of chunks, so the cap is what limits concurrency
    ScopedLoadConfig cfg(/*readers per file=*/8, 1, kMiB);
    auto fs = std::make_shared<ProbeFileSystem>(make_ranged_payload(kRanges));
    // Hold each read open long enough for the readers to overlap; without this the cap
    // would pass for the wrong reason -- nothing would ever be concurrent.
    fs->read_delay_ms.store(5);

    ASSIGN_OR_ABORT(auto reader, VectorIndexFileReader::open(FileInfo{.path = "/probe/index.vi", .fs = fs},
                                                             VectorIndexFetchMode::kParallel));
    std::string buf(kRanges * kMiB, '\0');
    ASSERT_EQ(kRanges * kMiB, reader->Read(buf.data(), kRanges * kMiB));
    EXPECT_EQ(fs->payload(), buf);

    // The calling thread is a reader too, so the ceiling is the cap plus it.
    const int cap = config::vector_index_load_parallel_threads;
    EXPECT_LE(fs->max_active_reads(), cap + 1) << "a single file pulled " << fs->max_active_reads() << " readers";
    EXPECT_GT(fs->max_active_reads(), 1) << "nothing overlapped, so the cap was not actually exercised";
}

// ==================== IO attribution ====================
// The range readers use their own handles, so their bytes never reach the load stream.
// Without folding them in, the profile reports a load that moved no bytes at all.

TEST(VectorIndexFileReaderTest, ParallelRangesAreCountedInTheIoBreakdown) {
    constexpr int kRanges = 4;
    ScopedLoadConfig cfg(4, 1, kMiB);
    auto fs = std::make_shared<ProbeFileSystem>(make_ranged_payload(kRanges));
    ASSIGN_OR_ABORT(auto reader, VectorIndexFileReader::open(FileInfo{.path = "/probe/index.vi", .fs = fs},
                                                             VectorIndexFetchMode::kParallel));

    std::string buf(kRanges * kMiB, '\0');
    ASSERT_EQ(kRanges * kMiB, reader->Read(buf.data(), kRanges * kMiB));
    EXPECT_EQ(static_cast<int64_t>(kRanges) * kMiB, reader->load_file_io_stats().bytes_read_remote);
}

// ---------------------------------------------------------------------------
// AllocateForRead: the memory faiss deserializes into
// ---------------------------------------------------------------------------

// Small arrays stay on the heap. Below one huge page a mapping cannot be promoted, so it
// would only add a syscall and page-granularity waste -- and the heap block is charged to
// the tracker by the allocator hook anyway.
TEST(VectorIndexFileReaderTest, SmallArraysAreLeftOnTheHeap) {
    ScopedLoadConfig cfg(4, 1, kMiB);
    auto fs = std::make_shared<ProbeFileSystem>(make_ranged_payload(3));
    auto tracker = std::make_shared<MemTracker>(-1, "vi_alloc_small");
    ASSIGN_OR_ABORT(auto reader, VectorIndexFileReader::open(FileInfo{.path = "/probe/index.vi", .fs = fs},
                                                             VectorIndexFetchMode::kParallel));
    reader->set_mem_tracker(tracker);

    std::shared_ptr<tenann::MemoryOwner> owner;
    EXPECT_EQ(nullptr, reader->AllocateForRead(config::vector_index_load_mmap_min_bytes - 1, &owner));
    EXPECT_EQ(nullptr, owner);
    EXPECT_EQ(0, tracker->consumption()) << "a declined request must not charge anything";
}

// A large array gets a mapping, and the mapping charges the tracker: mmap bypasses the
// allocator hook, so without this the bytes would be invisible until the OOM killer found
// them.
TEST(VectorIndexFileReaderTest, LargeArraysGetATrackedMapping) {
    ScopedLoadConfig cfg(4, 1, kMiB);
    auto fs = std::make_shared<ProbeFileSystem>(make_ranged_payload(3));
    auto tracker = std::make_shared<MemTracker>(-1, "vi_alloc_large");
    ASSIGN_OR_ABORT(auto reader, VectorIndexFileReader::open(FileInfo{.path = "/probe/index.vi", .fs = fs},
                                                             VectorIndexFetchMode::kParallel));
    reader->set_mem_tracker(tracker);

    const size_t bytes = 4 * static_cast<size_t>(kMiB);
    std::shared_ptr<tenann::MemoryOwner> owner;
    void* p = reader->AllocateForRead(bytes, &owner);
    ASSERT_NE(nullptr, p);
    ASSERT_NE(nullptr, owner);
    EXPECT_EQ(static_cast<int64_t>(bytes), tracker->consumption());

    // Writable, and ours: a mapping handed out twice would show up here.
    std::memset(p, 0x5A, bytes);
    EXPECT_EQ(0x5A, static_cast<unsigned char*>(p)[bytes - 1]);

    owner.reset();
    EXPECT_EQ(0, tracker->consumption()) << "the mapping must give the bytes back";
}

// The mapping outlives the reader that handed it out -- faiss keeps it through the cached
// index, long after the load is done.
TEST(VectorIndexFileReaderTest, AMappingOutlivesTheReader) {
    ScopedLoadConfig cfg(4, 1, kMiB);
    auto fs = std::make_shared<ProbeFileSystem>(make_ranged_payload(3));
    auto tracker = std::make_shared<MemTracker>(-1, "vi_alloc_outlive");
    const size_t bytes = 4 * static_cast<size_t>(kMiB);

    std::shared_ptr<tenann::MemoryOwner> owner;
    {
        ASSIGN_OR_ABORT(auto reader, VectorIndexFileReader::open(FileInfo{.path = "/probe/index.vi", .fs = fs},
                                                                 VectorIndexFetchMode::kParallel));
        reader->set_mem_tracker(tracker);
        ASSERT_NE(nullptr, reader->AllocateForRead(bytes, &owner));
    }
    EXPECT_EQ(static_cast<int64_t>(bytes), tracker->consumption()) << "the reader is gone, the bytes are not";

    owner.reset();
    EXPECT_EQ(0, tracker->consumption());
}

// No room means no mapping -- and faiss allocates the array itself, so the load still
// succeeds. Declining is a supported outcome, not a failure.
TEST(VectorIndexFileReaderTest, AFullTrackerDeclinesInsteadOfFailing) {
    ScopedLoadConfig cfg(4, 1, kMiB);
    auto fs = std::make_shared<ProbeFileSystem>(make_ranged_payload(3));
    const size_t bytes = 4 * static_cast<size_t>(kMiB);
    auto tracker = std::make_shared<MemTracker>(static_cast<int64_t>(bytes) / 2, "vi_alloc_full");
    ASSIGN_OR_ABORT(auto reader, VectorIndexFileReader::open(FileInfo{.path = "/probe/index.vi", .fs = fs},
                                                             VectorIndexFetchMode::kParallel));
    reader->set_mem_tracker(tracker);

    std::shared_ptr<tenann::MemoryOwner> owner;
    EXPECT_EQ(nullptr, reader->AllocateForRead(bytes, &owner));
    EXPECT_EQ(nullptr, owner);
    EXPECT_EQ(0, tracker->consumption()) << "a refused try_consume has to roll itself back";
}

// Without a tracker -- which is how this reader is exercised outside the BE -- the
// mapping still works. A null tracker must not be a crash or a refusal.
TEST(VectorIndexFileReaderTest, NoTrackerStillMaps) {
    ScopedLoadConfig cfg(4, 1, kMiB);
    auto fs = std::make_shared<ProbeFileSystem>(make_ranged_payload(3));
    ASSIGN_OR_ABORT(auto reader, VectorIndexFileReader::open(FileInfo{.path = "/probe/index.vi", .fs = fs},
                                                             VectorIndexFetchMode::kParallel));
    const size_t bytes = 4 * static_cast<size_t>(kMiB);
    std::shared_ptr<tenann::MemoryOwner> owner;
    void* p = reader->AllocateForRead(bytes, &owner);
    ASSERT_NE(nullptr, p);
    std::memset(p, 0x11, bytes);
    owner.reset();
}

// Every mapping is its own block. Handing the same address to two arrays would corrupt
// one of them, and the bug would look like a wrong search result rather than a crash.
TEST(VectorIndexFileReaderTest, EachRequestGetsItsOwnMapping) {
    ScopedLoadConfig cfg(4, 1, kMiB);
    auto fs = std::make_shared<ProbeFileSystem>(make_ranged_payload(3));
    auto tracker = std::make_shared<MemTracker>(-1, "vi_alloc_distinct");
    ASSIGN_OR_ABORT(auto reader, VectorIndexFileReader::open(FileInfo{.path = "/probe/index.vi", .fs = fs},
                                                             VectorIndexFetchMode::kParallel));
    reader->set_mem_tracker(tracker);

    const size_t bytes = 4 * static_cast<size_t>(kMiB);
    std::shared_ptr<tenann::MemoryOwner> a_owner;
    std::shared_ptr<tenann::MemoryOwner> b_owner;
    void* a = reader->AllocateForRead(bytes, &a_owner);
    void* b = reader->AllocateForRead(bytes, &b_owner);
    ASSERT_NE(nullptr, a);
    ASSERT_NE(nullptr, b);
    EXPECT_NE(a, b);
    EXPECT_EQ(2 * static_cast<int64_t>(bytes), tracker->consumption());

    std::memset(a, 0xA1, bytes);
    std::memset(b, 0xB2, bytes);
    EXPECT_EQ(0xA1, static_cast<unsigned char*>(a)[0]);
    EXPECT_EQ(0xB2, static_cast<unsigned char*>(b)[0]);

    a_owner.reset();
    EXPECT_EQ(static_cast<int64_t>(bytes), tracker->consumption()) << "releasing one must not release both";
    b_owner.reset();
    EXPECT_EQ(0, tracker->consumption());
}

} // namespace starrocks

#endif // WITH_TENANN
