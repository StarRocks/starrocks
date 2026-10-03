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

#include "connector/hive/paimon/paimon_file_system.h"

#include <gtest/gtest.h>
#include <paimon/memory/memory_pool.h>
#include <paimon/table/source/data_split.h>
#include <paimon/table/source/split.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "base/testutil/assert.h"
#include "base/url_coding.h"
#include "cache/datacache.h"
#include "cache/disk_cache/test_cache_utils.h"
#include "connector/hive/paimon/paimon_query_allocator.h"
#include "formats/scan_context.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "io/seekable_input_stream.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"

namespace starrocks {
namespace {

struct StreamObservations {
    std::atomic<MemTracker*> last_operation{nullptr};
    std::atomic<MemTracker*> destroyed{nullptr};
    std::atomic<int> active_reads{0};
    std::atomic<int> max_active_reads{0};
};

struct AllocatorObservations {
    std::atomic<MemTracker*> destroyed{nullptr};
};

bool g_paimon_lifecycle_test_env_initialized = false;
MemTracker* g_paimon_lifecycle_test_process_tracker = nullptr;

bool paimon_lifecycle_test_env_initialized() {
    return g_paimon_lifecycle_test_env_initialized;
}

MemTracker* paimon_lifecycle_test_process_tracker() {
    return g_paimon_lifecycle_test_process_tracker;
}

class QueryOwnedObject {
public:
    explicit QueryOwnedObject(AllocatorObservations* observations) : _observations(observations) {}
    ~QueryOwnedObject() { _observations->destroyed = CurrentThread::mem_tracker(); }

private:
    AllocatorObservations* _observations;
};

class RecordingSeekableInputStream final : public io::SeekableInputStream {
public:
    RecordingSeekableInputStream(std::string data, StreamObservations* observations)
            : _data(std::move(data)), _observations(observations) {}

    ~RecordingSeekableInputStream() override { _observations->destroyed = CurrentThread::mem_tracker(); }

    StatusOr<int64_t> read(void* data, int64_t count) override {
        auto result = read_at(_position, data, count);
        if (result.ok()) {
            _position += result.value();
        }
        return result;
    }

    Status seek(int64_t position) override {
        _observations->last_operation = CurrentThread::mem_tracker();
        _position = position;
        return Status::OK();
    }

    StatusOr<int64_t> position() override {
        _observations->last_operation = CurrentThread::mem_tracker();
        return _position;
    }

    StatusOr<int64_t> read_at(int64_t offset, void* data, int64_t count) override {
        return _read_at(offset, data, count);
    }

    Status read_at_fully(int64_t offset, void* data, int64_t count) override {
        auto result = _read_at(offset, data, count);
        if (!result.ok()) {
            return result.status();
        }
        if (result.value() != count) {
            return Status::IOError("short injected read");
        }
        return Status::OK();
    }

    StatusOr<int64_t> get_size() override {
        _observations->last_operation = CurrentThread::mem_tracker();
        return static_cast<int64_t>(_data.size());
    }

private:
    StatusOr<int64_t> _read_at(int64_t offset, void* data, int64_t count) {
        _observations->last_operation = CurrentThread::mem_tracker();
        const int active = _observations->active_reads.fetch_add(1) + 1;
        int maximum = _observations->max_active_reads.load();
        while (active > maximum && !_observations->max_active_reads.compare_exchange_weak(maximum, active)) {
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        if (offset < 0 || count < 0 || offset > static_cast<int64_t>(_data.size()) ||
            count > static_cast<int64_t>(_data.size()) - offset) {
            _observations->active_reads.fetch_sub(1);
            return Status::IOError("injected read outside file");
        }
        std::memcpy(data, _data.data() + offset, count);
        _observations->active_reads.fetch_sub(1);
        return count;
    }

    std::string _data;
    StreamObservations* _observations;
    int64_t _position = 0;
};

class RecordingMemoryFileSystem final : public MemoryFileSystem {
public:
    RecordingMemoryFileSystem(std::string data, StreamObservations* observations)
            : _data(std::move(data)), _observations(observations) {}

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions&,
                                                                       const std::string& path) override {
        auto stream = std::make_shared<RecordingSeekableInputStream>(_data, _observations);
        return std::make_unique<RandomAccessFile>(std::move(stream), path);
    }

private:
    std::string _data;
    StreamObservations* _observations;
};

} // namespace

class PaimonFileSystemTest : public ::testing::Test {
protected:
    void SetUp() override {
#ifdef WITH_STARCACHE
        _block_cache = TestCacheUtils::create_cache(TestCacheUtils::create_simple_options(256 * KB, 4 * MB));
        DataCache::GetInstance()->set_block_cache(_block_cache);
#endif

        _local_fs = FileSystem::Default();
        (void)_local_fs->delete_dir_recursive(_root);
        ASSERT_OK(_local_fs->create_dir_recursive(_child_dir));

        ASSIGN_OR_ABORT(auto file, _local_fs->new_writable_file(_data_path));
        ASSERT_OK(file->append(_content));
        ASSERT_OK(file->close());
    }

    void TearDown() override {
        (void)_local_fs->delete_dir_recursive(_root);
        DataCache::GetInstance()->set_block_cache(nullptr);
        _block_cache.reset();
    }

    const std::string _root = "./ut_dir/paimon_file_system_test";
    const std::string _child_dir = _root + "/child";
    const std::string _data_path = _root + "/data.bin";
    const std::string _content = "abcdefghij";
    FileSystem* _local_fs = nullptr;
    std::shared_ptr<BlockCache> _block_cache;
};

class PaimonInputStreamLifecycleTest : public ::testing::Test {
protected:
    void SetUp() override {
        g_paimon_lifecycle_test_env_initialized = true;
        g_paimon_lifecycle_test_process_tracker = &_process_tracker;
        tls_mem_tracker = nullptr;
        CurrentThread::set_mem_tracker_source(paimon_lifecycle_test_env_initialized,
                                              paimon_lifecycle_test_process_tracker);
    }

    void TearDown() override {
        tls_thread_status.set_mem_tracker(nullptr);
        CurrentThread::set_mem_tracker_source(nullptr, nullptr);
        tls_mem_tracker = nullptr;
        g_paimon_lifecycle_test_env_initialized = false;
        g_paimon_lifecycle_test_process_tracker = nullptr;
    }

private:
    MemTracker _process_tracker;
};

TEST_F(PaimonFileSystemTest, OpenReadSeekAndReadAsync) {
    MemoryFileSystem memory_fs;
    ASSERT_OK(memory_fs.append_file("/data.bin", _content));

    PaimonFileSystem file_system(&memory_fs, DataCacheOptions{});

    auto open_result = file_system.Open("/data.bin");
    ASSERT_TRUE(open_result.ok()) << open_result.status().ToString();
    auto input = std::move(open_result).value();

    auto uri_result = input->GetUri();
    ASSERT_TRUE(uri_result.ok()) << uri_result.status().ToString();
    EXPECT_EQ("/data.bin", uri_result.value());

    auto length_result = input->Length();
    ASSERT_TRUE(length_result.ok()) << length_result.status().ToString();
    EXPECT_EQ(_content.size(), length_result.value());

    std::string sequential_buffer(4, '\0');
    auto read_result = input->Read(sequential_buffer.data(), sequential_buffer.size());
    ASSERT_TRUE(read_result.ok()) << read_result.status().ToString();
    EXPECT_EQ(4, read_result.value());
    EXPECT_EQ("abcd", sequential_buffer);

    auto position_result = input->GetPos();
    ASSERT_TRUE(position_result.ok()) << position_result.status().ToString();
    EXPECT_EQ(4, position_result.value());

    std::string positional_buffer(3, '\0');
    read_result = input->Read(positional_buffer.data(), positional_buffer.size(), 5);
    ASSERT_TRUE(read_result.ok()) << read_result.status().ToString();
    EXPECT_EQ(3, read_result.value());
    EXPECT_EQ("fgh", positional_buffer);

    EXPECT_TRUE(input->Seek(2, paimon::FS_SEEK_SET).ok());
    EXPECT_TRUE(input->Seek(2, paimon::FS_SEEK_CUR).ok());
    position_result = input->GetPos();
    ASSERT_TRUE(position_result.ok()) << position_result.status().ToString();
    EXPECT_EQ(4, position_result.value());

    EXPECT_TRUE(input->Seek(-2, paimon::FS_SEEK_END).ok());
    position_result = input->GetPos();
    ASSERT_TRUE(position_result.ok()) << position_result.status().ToString();
    EXPECT_EQ(8, position_result.value());
    EXPECT_TRUE(input->Seek(-11, paimon::FS_SEEK_END).IsInvalid());

    EXPECT_TRUE(input->Read(nullptr, -1).status().IsInvalid());
    EXPECT_TRUE(input->Read(nullptr, 1, -1).status().IsInvalid());

    std::string async_buffer(4, '\0');
    bool callback_invoked = false;
    paimon::Status async_status;
    input->ReadAsync(async_buffer.data(), async_buffer.size(), 1, [&](paimon::Status status) {
        callback_invoked = true;
        async_status = std::move(status);
    });
    EXPECT_TRUE(callback_invoked);
    EXPECT_TRUE(async_status.ok()) << async_status.ToString();
    EXPECT_EQ("bcde", async_buffer);

    std::string short_buffer(3, '\0');
    callback_invoked = false;
    input->ReadAsync(short_buffer.data(), short_buffer.size(), 9, [&](paimon::Status status) {
        callback_invoked = true;
        async_status = std::move(status);
    });
    EXPECT_TRUE(callback_invoked);
    EXPECT_TRUE(async_status.IsIOError()) << async_status.ToString();
    EXPECT_EQ('j', short_buffer[0]);

    callback_invoked = false;
    input->ReadAsync(nullptr, -1, 0, [&](paimon::Status status) {
        callback_invoked = true;
        async_status = std::move(status);
    });
    EXPECT_TRUE(callback_invoked);
    EXPECT_TRUE(async_status.IsInvalid()) << async_status.ToString();

    const auto io_stats = file_system.get_stats();
    EXPECT_EQ(1, io_stats.sequential_read_count);
    EXPECT_EQ(4, io_stats.sequential_read_bytes);
    EXPECT_EQ(1, io_stats.positional_read_count);
    EXPECT_EQ(3, io_stats.positional_read_bytes);
    EXPECT_EQ(2, io_stats.async_read_count);
    EXPECT_EQ(4, io_stats.async_read_bytes);
    EXPECT_EQ(4, io_stats.app_io_count());
    EXPECT_EQ(11, io_stats.app_io_bytes());
    EXPECT_EQ(4, io_stats.fs_io_count);
    EXPECT_EQ(11, io_stats.fs_io_bytes);
    EXPECT_TRUE(input->Close().ok());

    auto missing_result = file_system.Open("/missing.bin");
    EXPECT_FALSE(missing_result.ok());
    EXPECT_TRUE(missing_result.status().IsIOError()) << missing_result.status().ToString();
}

TEST_F(PaimonInputStreamLifecycleTest, RestoresCallerTrackerAndReleasesOnWorker) {
    auto owner = std::make_shared<MemTracker>();
    auto caller = std::make_shared<MemTracker>();
    std::weak_ptr<MemTracker> weak_owner = owner;
    MemTracker* owner_address = owner.get();
    StreamObservations observations;
    RecordingMemoryFileSystem recording_fs("abcdefghij", &observations);
    auto file_system = std::make_unique<PaimonFileSystem>(&recording_fs, DataCacheOptions{}, owner);

    std::unique_ptr<paimon::InputStream> input;
    {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(caller.get());
        auto open_result = file_system->Open("/recording.bin");
        ASSERT_TRUE(open_result.ok()) << open_result.status().ToString();
        input = std::move(open_result).value();
        EXPECT_EQ(caller.get(), CurrentThread::mem_tracker());
    }
    file_system.reset();
    owner.reset();
    ASSERT_FALSE(weak_owner.expired());

    std::thread worker([&] {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(caller.get());
        char sequential[4];
        auto read_result = input->Read(sequential, sizeof(sequential));
        ASSERT_TRUE(read_result.ok()) << read_result.status().ToString();
        EXPECT_EQ("abcd", std::string(sequential, sizeof(sequential)));
        EXPECT_EQ(owner_address, observations.last_operation.load());
        EXPECT_EQ(caller.get(), CurrentThread::mem_tracker());

        int error_callbacks = 0;
        input->ReadAsync(sequential, sizeof(sequential), 20, [&](paimon::Status status) {
            ++error_callbacks;
            EXPECT_FALSE(status.ok());
            EXPECT_EQ(caller.get(), CurrentThread::mem_tracker());
        });
        EXPECT_EQ(1, error_callbacks);

        char async_data[4];
        int callbacks = 0;
        input->ReadAsync(async_data, sizeof(async_data), 4, [&](paimon::Status status) {
            ++callbacks;
            EXPECT_TRUE(status.ok()) << status.ToString();
            EXPECT_EQ(caller.get(), CurrentThread::mem_tracker());
            char nested[4];
            auto nested_result = input->Read(nested, sizeof(nested), 0);
            ASSERT_TRUE(nested_result.ok()) << nested_result.status().ToString();
            EXPECT_EQ("abcd", std::string(nested, sizeof(nested)));
            EXPECT_TRUE(input->Close().ok());
            EXPECT_EQ(owner_address, observations.destroyed.load());
            EXPECT_EQ(caller.get(), CurrentThread::mem_tracker());
        });
        EXPECT_EQ(1, callbacks);
        EXPECT_EQ("efgh", std::string(async_data, sizeof(async_data)));
        EXPECT_TRUE(input->Close().ok());
        input.reset();
        EXPECT_EQ(caller.get(), CurrentThread::mem_tracker());
    });
    worker.join();

    EXPECT_EQ(owner_address, observations.last_operation.load());
    EXPECT_EQ(owner_address, observations.destroyed.load());
    EXPECT_TRUE(weak_owner.expired());
}

TEST_F(PaimonInputStreamLifecycleTest, QueryAllocatorOwnsCrossThreadFinalRelease) {
    auto owner = std::make_shared<MemTracker>();
    auto caller = std::make_shared<MemTracker>();
    std::weak_ptr<MemTracker> weak_owner = owner;
    MemTracker* owner_address = owner.get();
    AllocatorObservations observations;
    auto object = std::allocate_shared<QueryOwnedObject>(PaimonQueryAllocator<QueryOwnedObject>(owner), &observations);
    owner.reset();
    ASSERT_FALSE(weak_owner.expired());

    std::thread worker([&] {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(caller.get());
        object.reset();
        EXPECT_EQ(caller.get(), CurrentThread::mem_tracker());
    });
    worker.join();

    EXPECT_EQ(owner_address, observations.destroyed.load());
    EXPECT_TRUE(weak_owner.expired());
}

TEST_F(PaimonInputStreamLifecycleTest, SerializesPositionalAndAsyncReads) {
    auto owner = std::make_shared<MemTracker>();
    auto caller = std::make_shared<MemTracker>();
    StreamObservations observations;
    RecordingMemoryFileSystem recording_fs("0123456789abcdef", &observations);
    PaimonFileSystem file_system(&recording_fs, DataCacheOptions{}, owner);
    auto open_result = file_system.Open("/recording.bin");
    ASSERT_TRUE(open_result.ok()) << open_result.status().ToString();
    auto input = std::move(open_result).value();

    constexpr int kThreads = 8;
    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::atomic<int> failures{0};
    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (int i = 0; i < kThreads; ++i) {
        workers.emplace_back([&, i] {
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(caller.get());
            ready.fetch_add(1);
            while (!start.load()) {
                std::this_thread::yield();
            }
            char data[2];
            if ((i & 1) == 0) {
                auto result = input->Read(data, sizeof(data), i);
                if (!result.ok()) {
                    failures.fetch_add(1);
                }
            } else {
                input->ReadAsync(data, sizeof(data), i, [&](paimon::Status status) {
                    if (!status.ok() || CurrentThread::mem_tracker() != caller.get()) {
                        failures.fetch_add(1);
                    }
                });
            }
        });
    }
    while (ready.load() != kThreads) {
        std::this_thread::yield();
    }
    start = true;
    for (auto& worker : workers) {
        worker.join();
    }

    EXPECT_EQ(0, failures.load());
    EXPECT_EQ(0, observations.active_reads.load());
    EXPECT_EQ(1, observations.max_active_reads.load());
    EXPECT_EQ(owner.get(), observations.last_operation.load());
    const auto stats = file_system.get_stats();
    EXPECT_EQ(kThreads / 2, stats.positional_read_count);
    EXPECT_EQ(kThreads / 2, stats.async_read_count);
    EXPECT_TRUE(input->Close().ok());
}

TEST_F(PaimonFileSystemTest, ReadsThroughDataCache) {
#ifndef WITH_STARCACHE
    GTEST_SKIP() << "DataCache test requires StarCache";
#endif
    MemoryFileSystem memory_fs;
    ASSERT_OK(memory_fs.append_file("/cached.bin", _content));

    DataCacheOptions cache_options{.enable_datacache = true, .enable_populate_datacache = true};
    PaimonFileSystem file_system(&memory_fs, cache_options);

    auto open_result = file_system.Open("/cached.bin");
    ASSERT_TRUE(open_result.ok()) << open_result.status().ToString();
    auto input = std::move(open_result).value();

    std::string sequential_buffer(_content.size(), '\0');
    auto read_result = input->Read(sequential_buffer.data(), sequential_buffer.size());
    ASSERT_TRUE(read_result.ok()) << read_result.status().ToString();
    EXPECT_EQ(_content, sequential_buffer);
    std::string same_stream_positional_buffer(4, '\0');
    read_result = input->Read(same_stream_positional_buffer.data(), same_stream_positional_buffer.size(), 0);
    ASSERT_TRUE(read_result.ok()) << read_result.status().ToString();
    EXPECT_EQ("abcd", same_stream_positional_buffer);
    auto position_result = input->GetPos();
    ASSERT_TRUE(position_result.ok()) << position_result.status().ToString();
    EXPECT_EQ(_content.size(), position_result.value());
    input.reset();

    open_result = file_system.Open("/cached.bin");
    ASSERT_TRUE(open_result.ok()) << open_result.status().ToString();
    input = std::move(open_result).value();
    std::string positional_buffer(4, '\0');
    read_result = input->Read(positional_buffer.data(), positional_buffer.size(), 0);
    ASSERT_TRUE(read_result.ok()) << read_result.status().ToString();
    EXPECT_EQ("abcd", positional_buffer);
    input.reset();

    open_result = file_system.Open("/cached.bin");
    ASSERT_TRUE(open_result.ok()) << open_result.status().ToString();
    input = std::move(open_result).value();
    const auto finalized_cache_reads_before_async = file_system.get_stats().datacache.read_block_cache_count;
    std::string async_buffer(4, '\0');
    bool callback_invoked = false;
    paimon::Status async_status;
    input->ReadAsync(async_buffer.data(), async_buffer.size(), 0, [&](paimon::Status status) {
        callback_invoked = true;
        async_status = std::move(status);
    });
    EXPECT_TRUE(callback_invoked);
    EXPECT_TRUE(async_status.ok()) << async_status.ToString();
    EXPECT_EQ("abcd", async_buffer);
    EXPECT_EQ(finalized_cache_reads_before_async, file_system.get_stats().datacache.read_block_cache_count);
    EXPECT_TRUE(input->Close().ok());
    const auto stats_after_close = file_system.get_stats();
    EXPECT_TRUE(input->Close().ok());
    const auto stats_after_second_close = file_system.get_stats();
    EXPECT_EQ(stats_after_close.datacache.read_block_cache_count,
              stats_after_second_close.datacache.read_block_cache_count);
    EXPECT_EQ(stats_after_close.datacache.read_block_cache_bytes,
              stats_after_second_close.datacache.read_block_cache_bytes);
    input.reset();

    const auto io_stats = file_system.get_stats();
    const auto& cache_stats = io_stats.datacache;
    EXPECT_GT(cache_stats.write_block_cache_count, 0);
    EXPECT_GE(cache_stats.read_block_cache_count, 2);
    EXPECT_EQ(4, io_stats.app_io_count());
    EXPECT_EQ(22, io_stats.app_io_bytes());
    EXPECT_GT(io_stats.fs_io_count, 0);
    EXPECT_GT(io_stats.fs_io_bytes, 0);
}

TEST_F(PaimonFileSystemTest, GetStatusListAndExists) {
    PaimonFileSystem file_system(_local_fs, DataCacheOptions{});

    auto exists_result = file_system.Exists(_data_path);
    ASSERT_TRUE(exists_result.ok()) << exists_result.status().ToString();
    EXPECT_TRUE(exists_result.value());

    exists_result = file_system.Exists(_root + "/missing");
    ASSERT_TRUE(exists_result.ok()) << exists_result.status().ToString();
    EXPECT_FALSE(exists_result.value());

    auto file_status_result = file_system.GetFileStatus(_data_path);
    ASSERT_TRUE(file_status_result.ok()) << file_status_result.status().ToString();
    auto file_status = std::move(file_status_result).value();
    EXPECT_EQ(_data_path, file_status->GetPath());
    EXPECT_FALSE(file_status->IsDir());
    EXPECT_EQ(_content.size(), file_status->GetLen());
    ASSIGN_OR_ABORT(auto starrocks_modification_time, _local_fs->get_file_modified_time(_data_path));
    // StarRocks reports seconds, paimon::FileStatus carries milliseconds.
    EXPECT_EQ(starrocks_modification_time * 1000, file_status->GetModificationTime());

    auto directory_status_result = file_system.GetFileStatus(_child_dir);
    ASSERT_TRUE(directory_status_result.ok()) << directory_status_result.status().ToString();
    auto directory_status = std::move(directory_status_result).value();
    EXPECT_EQ(_child_dir, directory_status->GetPath());
    EXPECT_TRUE(directory_status->IsDir());

    auto missing_status_result = file_system.GetFileStatus(_root + "/missing");
    EXPECT_FALSE(missing_status_result.ok());
    EXPECT_TRUE(missing_status_result.status().IsNotExist()) << missing_status_result.status().ToString();

    std::vector<std::unique_ptr<paimon::BasicFileStatus>> basic_statuses;
    auto list_status = file_system.ListDir(_root, &basic_statuses);
    ASSERT_TRUE(list_status.ok()) << list_status.ToString();
    std::sort(basic_statuses.begin(), basic_statuses.end(),
              [](const auto& lhs, const auto& rhs) { return lhs->GetPath() < rhs->GetPath(); });
    ASSERT_EQ(2, basic_statuses.size());
    EXPECT_EQ(_child_dir, basic_statuses[0]->GetPath());
    EXPECT_TRUE(basic_statuses[0]->IsDir());
    EXPECT_EQ(_data_path, basic_statuses[1]->GetPath());
    EXPECT_FALSE(basic_statuses[1]->IsDir());

    std::vector<std::unique_ptr<paimon::FileStatus>> detailed_statuses;
    list_status = file_system.ListFileStatus(_root, &detailed_statuses);
    ASSERT_TRUE(list_status.ok()) << list_status.ToString();
    std::sort(detailed_statuses.begin(), detailed_statuses.end(),
              [](const auto& lhs, const auto& rhs) { return lhs->GetPath() < rhs->GetPath(); });
    ASSERT_EQ(2, detailed_statuses.size());
    EXPECT_EQ(_child_dir, detailed_statuses[0]->GetPath());
    EXPECT_TRUE(detailed_statuses[0]->IsDir());
    EXPECT_EQ(_data_path, detailed_statuses[1]->GetPath());
    EXPECT_FALSE(detailed_statuses[1]->IsDir());
    EXPECT_EQ(_content.size(), detailed_statuses[1]->GetLen());

    detailed_statuses.clear();
    list_status = file_system.ListFileStatus(_data_path, &detailed_statuses);
    ASSERT_TRUE(list_status.ok()) << list_status.ToString();
    ASSERT_EQ(1, detailed_statuses.size());
    EXPECT_EQ(_data_path, detailed_statuses[0]->GetPath());
    EXPECT_EQ(_content.size(), detailed_statuses[0]->GetLen());

    EXPECT_TRUE(file_system.ListDir(_root, nullptr).IsInvalid());
    EXPECT_TRUE(file_system.ListFileStatus(_root, nullptr).IsInvalid());
}

TEST_F(PaimonFileSystemTest, RejectsWrites) {
    PaimonFileSystem file_system(_local_fs, DataCacheOptions{});
    auto create_result = file_system.Create(_root + "/new-file", false);
    ASSERT_FALSE(create_result.ok());
    EXPECT_TRUE(create_result.status().IsNotImplemented());
    EXPECT_TRUE(file_system.Mkdirs(_root + "/new-directory").IsNotImplemented());
    EXPECT_TRUE(file_system.Rename(_data_path, _root + "/renamed-file").IsNotImplemented());
    EXPECT_TRUE(file_system.Delete(_data_path, false).IsNotImplemented());
}

TEST(PaimonFileSystemStatsTest, ConcurrentUpdates) {
    PaimonFileSystemStats stats;
    constexpr int kThreadCount = 8;
    constexpr int kUpdatesPerThread = 1000;
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);
    for (int i = 0; i < kThreadCount; ++i) {
        threads.emplace_back([&]() {
            for (int j = 0; j < kUpdatesPerThread; ++j) {
                stats.record_app_read(PaimonFileSystemStats::ReadType::ASYNC, 4096, 10);
                stats.record_fs_read(4096, 5);
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    const auto snapshot = stats.snapshot();
    constexpr int64_t kUpdates = kThreadCount * kUpdatesPerThread;
    EXPECT_EQ(kUpdates, snapshot.async_read_count);
    EXPECT_EQ(kUpdates * 4096, snapshot.async_read_bytes);
    EXPECT_EQ(kUpdates * 10, snapshot.async_read_ns);
    EXPECT_EQ(kUpdates, snapshot.app_io_count());
    EXPECT_EQ(kUpdates * 4096, snapshot.app_io_bytes());
    EXPECT_EQ(kUpdates * 10, snapshot.app_io_ns());
    EXPECT_EQ(kUpdates, snapshot.fs_io_count);
    EXPECT_EQ(kUpdates * 4096, snapshot.fs_io_bytes);
    EXPECT_EQ(kUpdates * 5, snapshot.fs_io_ns);
}

TEST(PaimonFileSystemStatsTest, AccumulatesFinalizedStreamStats) {
    PaimonFileSystemStats stats;
    CacheInputStream::Stats cache_stats;
    cache_stats.read_block_cache_count = 2;
    cache_stats.read_block_cache_bytes = 20;
    cache_stats.read_peer_cache_count = 3;
    cache_stats.read_peer_cache_bytes = 30;
    cache_stats.write_block_cache_count = 3;
    cache_stats.write_block_cache_bytes = 30;
    PaimonFileSystemStats::SharedBufferedStats shared_stats{
            .shared_io_count = 4,
            .shared_io_bytes = 40,
            .direct_io_count = 5,
            .direct_io_bytes = 50,
    };

    stats.record_stream_stats(cache_stats, shared_stats);
    stats.record_stream_stats(cache_stats, shared_stats);
    const auto snapshot = stats.snapshot();
    EXPECT_EQ(4, snapshot.datacache.read_block_cache_count);
    EXPECT_EQ(40, snapshot.datacache.read_block_cache_bytes);
    EXPECT_EQ(6, snapshot.datacache.read_peer_cache_count);
    EXPECT_EQ(60, snapshot.datacache.read_peer_cache_bytes);
    EXPECT_EQ(6, snapshot.datacache.write_block_cache_count);
    EXPECT_EQ(60, snapshot.datacache.write_block_cache_bytes);
    EXPECT_EQ(8, snapshot.shared_buffered.shared_io_count);
    EXPECT_EQ(80, snapshot.shared_buffered.shared_io_bytes);
    EXPECT_EQ(10, snapshot.shared_buffered.direct_io_count);
    EXPECT_EQ(100, snapshot.shared_buffered.direct_io_bytes);
}

TEST(PaimonSplitCompatibilityTest, DeserializeJavaVersion8DataSplit) {
    // Generated by paimon-java and kept in paimon-cpp's version-8 DataSplit
    // compatibility corpus. This guards the FE DataSplit.serialize() -> BE
    // Split::Deserialize() wire contract independently of either serializer.
    const std::string encoded_split =
            "3sPSMCwZ7GYAAAAIAAAAAAAAAAQAAAAUAAAAAQAAAAAAAAAACgAAAAAAAAAAAAABAF5kYXRhL29yYy9wa19kdl9pbmRleF9pbl9k"
            "YXRhX3dpdGhfZXh0ZXJuYWwuZGIvcGtfZHZfaW5kZXhfaW5fZGF0YV93aXRoX2V4dGVybmFsL2YxPTEwL2J1Y2tldC0xAQAAAAIA"
            "AAAAAAAAAAEAAAKYAABADQAAAAAvAAAAqAAAAMEDAAAAAAAABQAAAAAAAAAcAAAA2AAAABwAAAD4AAAAeAAAABgBAACoAAAAkAEA"
            "AAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAUAAAAAAAAACAAAADgCAABfVnwqmQEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            "AAAAAAAAUQAAAEACAAAAAAAAAAAAAAAAAAAAAAAAZGF0YS03MmI2MmE1Zi1kNjk4LTRkYjUtYjUxYS0wNGMwZGMwMjc3MDItMC5v"
            "cmMAAAAAAgAAAAAAAAAAQWxleAAAAIQAAAAAAAAAAAAAAAAAAAACAAAAAAAAAABUb255AAAAhAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            "HAAAACAAAAAcAAAAQAAAABgAAABgAAAAAAAAAgAAAAAAAAAAQWxleAAAAIQAAAAAAAAAAAAAAAAAAAACAAAAAAAAAABUb255AAAA"
            "hAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAALAAAACAAAAAsAAAAUAAAACgAAACAAAAAAAAABAAA"
            "AAAAAAAAQWxleAAAAIQKAAAAAAAAAAAAAAAAAAAAMzMzMzMzKEAAAAAAAAAABAAAAAAAAAAAVG9ueQAAAIQKAAAAAAAAAAAAAAAA"
            "AAAAmpmZmZkZMUAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAARklMRTovdG1wL2V4"
            "dGVybmFsL2YxPTEwL2J1Y2tldC0xL2RhdGEtNzJiNjJhNWYtZDY5OC00ZGI1LWI1MWEtMDRjMGRjMDI3NzAyLTAub3JjAAAAAAAA"
            "AAEAAAABAQBORklMRTovdG1wL2V4dGVybmFsL2YxPTEwL2J1Y2tldC0xL2luZGV4LTQxOWU3YzZiLTljYWQtNDllOC05Y2QyLTYx"
            "ODc0NzFkZjk1NC0xAAAAAAAAAAEAAAAAAAAAFgAAAAAAAAABAAE=";

    std::string serialized_split;
    ASSERT_TRUE(base64_decode(encoded_split, &serialized_split));
    auto memory_pool = paimon::GetDefaultPool();
    auto split_result = paimon::Split::Deserialize(serialized_split.data(), serialized_split.size(), memory_pool);
    ASSERT_TRUE(split_result.ok()) << split_result.status().ToString();

    auto data_split = std::dynamic_pointer_cast<paimon::DataSplit>(split_result.value());
    ASSERT_NE(nullptr, data_split);
    EXPECT_EQ(1, data_split->Bucket());
    const auto files = data_split->GetFileList();
    ASSERT_EQ(1, files.size());
    EXPECT_EQ(961, files[0].file_size);

    auto serialize_result = paimon::Split::Serialize(split_result.value(), memory_pool);
    ASSERT_TRUE(serialize_result.ok()) << serialize_result.status().ToString();
    EXPECT_EQ(serialized_split, serialize_result.value());
}

TEST(PaimonSplitCompatibilityTest, DeserializePaimonJava200DataSplit) {
    // Generated by writing a bucketed primary-key table with paimon-java 2.0.0,
    // then serializing its planned DataSplit with DataSplit.serialize().
    const std::string encoded_split =
            "3sPSMCwZ7GYAAAAIAAAAAAAAAAEAAAAMAAAAAAAAAAAAAAAAAAAAAABUZmlsZTovdG1wL3BhaW1vbi0yLXNwbGl0LTE0MzA2ODQx"
            "MjQ4MDI4MjkxOTgzL2NvbXBhdGliaWxpdHkuZGIvbmF0aXZlX3NwbGl0L2J1Y2tldC0wAQAAAAEAAAAAAAAAAAEAAAIQAABADwAA"
            "AAAzAAAAqAAAALsEAAAAAAAAAgAAAAAAAAAUAAAA4AAAABQAAAD4AAAAYAAAABABAACYAAAAcAEAAAAAAAAAAAAAAQAAAAAAAAAA"
            "AAAAAAAAAAAAAAAAAAAACAAAAAgCAAAkA6HGoAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            "AAAAAAAAAAAAAAAAZGF0YS05NjkzMjllYy1lZDFhLTRlODAtYmE2Ni0yMGZlZDQwZDZkZDQtMC5wYXJxdWV0AAAAAAAAAAABAAAA"
            "AAAAAAABAAAAAAAAAAAAAAAAAAABAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAABQAAAAgAAAAFAAAADgAAAAQAAAAUAAAAAAA"
            "AAEAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAIAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACwAAAAg"
            "AAAALAAAAFAAAAAYAAAAgAAAAAAAAAIAAAAAAAAAAAEAAAAAAAAAEAAAABgAAABwYWltb24tY3BwLTAuMy4wAAAAAAAAAAIAAAAA"
            "AAAAAAIAAAAAAAAAEAAAABgAAABwYWltb24tamF2YS0yLjAvAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB";

    std::string serialized_split;
    ASSERT_TRUE(base64_decode(encoded_split, &serialized_split));
    auto memory_pool = paimon::GetDefaultPool();
    auto split_result = paimon::Split::Deserialize(serialized_split.data(), serialized_split.size(), memory_pool);
    ASSERT_TRUE(split_result.ok()) << split_result.status().ToString();

    auto data_split = std::dynamic_pointer_cast<paimon::DataSplit>(split_result.value());
    ASSERT_NE(nullptr, data_split);
    EXPECT_EQ(0, data_split->Bucket());
    const auto files = data_split->GetFileList();
    ASSERT_EQ(1, files.size());
    EXPECT_EQ(1211, files[0].file_size);

    auto serialize_result = paimon::Split::Serialize(split_result.value(), memory_pool);
    ASSERT_TRUE(serialize_result.ok()) << serialize_result.status().ToString();
    EXPECT_EQ(serialized_split, serialize_result.value());
}

} // namespace starrocks
