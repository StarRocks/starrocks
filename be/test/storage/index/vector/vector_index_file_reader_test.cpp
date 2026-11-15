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
#include "common/status.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "io/seekable_input_stream.h"

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

private:
    ProbeFileSystem* _owner;
    std::string_view _payload;
    int64_t _offset = 0;
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

} // namespace starrocks

#endif // WITH_TENANN
