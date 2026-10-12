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

#include "fs/hdfs/fs_hdfs.h"

#include <gtest/gtest.h>

#include <filesystem>

#include "base/bthreads/util.h"
#include "fs/fs_util.h"
#include "fs/hdfs/hdfs_fs_cache.h"
#include "testutil/sync_point.h"
#include "udf/java/utils.h"
#include "util/defer_op.h"

namespace starrocks {

class HdfsFileSystemTest : public ::testing::Test {
public:
    HdfsFileSystemTest() = default;
    ~HdfsFileSystemTest() override = default;

    void SetUp() override {
        const testing::TestInfo* const test_info = testing::UnitTest::GetInstance()->current_test_info();
        std::string root = std::string("hdfs_filesystem_test_") + test_info->name();
        std::filesystem::path rootpath(root);
        _root_path = std::filesystem::absolute(rootpath).string();
        ASSERT_TRUE(fs::create_directories(_root_path).ok());
    }
    void TearDown() override { ASSERT_TRUE(fs::remove_all(_root_path).ok()); }

    void create_file_and_destroy();
    enum class WriterRelease { kClose, kCloseFromBthread, kDestroy };
    void writable_file_survives_cache_eviction(WriterRelease release);

public:
    std::string _root_path;
};

void HdfsFileSystemTest::writable_file_survives_cache_eviction(WriterRelease release) {
    if (release == WriterRelease::kCloseFromBthread && JavaEnv::GetInstance()->jvm_call_pool() == nullptr) {
        auto st = JavaEnv::GetInstance()->init();
        ASSERT_TRUE(st.ok()) << st;
    }
    const auto old_capacity = config::hdfs_client_max_cache_size;
    DeferOp restore_capacity([&] { config::hdfs_client_max_cache_size = old_capacity; });
    config::hdfs_client_max_cache_size = 1;

    // Distinct configurations force eviction even if earlier tests populated the singleton cache.
    TCloudConfiguration writer_cloud;
    writer_cloud.__set_cloud_properties({{"starrocks.test.cache.identity", _root_path + "/writer"}});
    FSOptions writer_options(&writer_cloud);
    auto fs = new_fs_hdfs(writer_options);
    const std::string filepath = "file://" + _root_path + "/writer";
    const std::string seedpath = "file://" + _root_path + "/seed";
    const std::string payload = "data written across filesystem cache eviction";
    {
        auto seed = fs->new_writable_file(seedpath);
        ASSERT_TRUE(seed.ok()) << seed.status();
        ASSERT_TRUE((*seed)->append(Slice(payload)).ok());
        ASSERT_TRUE((*seed)->close().ok());
    }

    // Keep a guard until the ownership assertion so a regression fails without using a disconnected handle.
    // Declare it before the writer so assertion failure also closes the writer before releasing the guard.
    std::shared_ptr<HdfsFsClient> client;
    ASSERT_TRUE(HdfsFsCache::instance()->get_connection("file:///", client, writer_options).ok());
    std::weak_ptr<HdfsFsClient> weak_client = client;
    auto writer = fs->new_writable_file(filepath);
    ASSERT_TRUE(writer.ok()) << writer.status();
    ASSERT_TRUE((*writer)->append(Slice(payload)).ok());

    TCloudConfiguration reader_cloud;
    reader_cloud.__set_cloud_properties({{"starrocks.test.cache.identity", _root_path + "/reader"}});
    auto reader_fs = new_fs_hdfs(FSOptions(&reader_cloud));
    auto reader = reader_fs->new_random_access_file(seedpath);
    ASSERT_TRUE(reader.ok()) << reader.status();
    auto seed_contents = (*reader)->read_all(); // Force the lazy reader to acquire a different cached client.
    ASSERT_TRUE(seed_contents.ok()) << seed_contents.status();
    ASSERT_EQ(payload, *seed_contents);

    // Only the guard and the writer own the evicted client; the reader uses a different client.
    ASSERT_EQ(2, client.use_count());
    client.reset();
    ASSERT_FALSE(weak_client.expired());
    ASSERT_TRUE((*writer)->append(Slice(payload)).ok());
    ASSERT_TRUE((*writer)->flush(WritableFile::FlushMode::FLUSH_SYNC).ok());
    ASSERT_TRUE((*writer)->sync().ok());
    switch (release) {
    case WriterRelease::kClose:
        ASSERT_TRUE((*writer)->close().ok());
        break;
    case WriterRelease::kCloseFromBthread: {
        // close() hops to a pthread and must drop the last client reference there, so hdfsDisconnect()
        // never runs on the bthread. The client is therefore already gone when close() returns.
        Status close_status;
        bool expired_after_close = false;
        auto bthread_status = bthreads::start_bthread_and_join([&] {
            close_status = (*writer)->close();
            expired_after_close = weak_client.expired();
        });
        ASSERT_TRUE(bthread_status.ok()) << bthread_status;
        ASSERT_TRUE(close_status.ok()) << close_status;
        ASSERT_TRUE(expired_after_close);
        break;
    }
    case WriterRelease::kDestroy:
        break;
    }
    if (release != WriterRelease::kDestroy) {
        // close() itself releases the evicted client instead of deferring disconnect to destruction.
        EXPECT_TRUE(weak_client.expired());
        ASSERT_TRUE((*writer)->close().ok()); // Closing twice must remain harmless after eviction.
    }
    (*writer).reset(); // For kDestroy this exercises the implicit close in the destructor.
    EXPECT_TRUE(weak_client.expired());

    auto result = reader_fs->new_random_access_file(filepath);
    ASSERT_TRUE(result.ok()) << result.status();
    auto contents = (*result)->read_all();
    ASSERT_TRUE(contents.ok()) << contents.status();
    EXPECT_EQ(payload + payload, *contents);
}

void HdfsFileSystemTest::create_file_and_destroy() {
    auto fs = new_fs_hdfs(FSOptions());
    // use file:// as the fs scheme, which will leverage Hadoop LocalFileSystem for testing
    std::string filepath = "file://" + _root_path + "/create_file_and_destroy_file";
    auto st = fs->path_exists(filepath);
    EXPECT_TRUE(st.is_not_found());
    auto wfile = fs->new_writable_file(filepath);
    EXPECT_TRUE(wfile.ok());
    (*wfile)->close();
    EXPECT_TRUE(fs->new_sequential_file(filepath).ok());
    EXPECT_TRUE(fs->new_random_access_file(filepath).ok());

    // error injection during HDFSWritableFile::close, it is hard to simulate a sync failure with a POSIX filesystem.
    // HDFS FileSystem sync operation will fail if the file is removed from remote name node.
    SyncPoint::GetInstance()->SetCallBack("HDFSWritableFile::close", [](void* arg) { *(int*)arg = -1; });
    SyncPoint::GetInstance()->EnableProcessing();

    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("HDFSWritableFile::close");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    // done the file, check if there is any memory leak
    (*wfile).reset();
}

TEST_F(HdfsFileSystemTest, create_file_and_destroy) {
    // NOTE: use separate thread to run the test case to avoid some weird tls memory issue introduced by JVM
    auto thread = std::thread([this] { create_file_and_destroy(); });
    thread.join();
}

TEST_F(HdfsFileSystemTest, create_file_with_open_truncate) {
    auto fs = new_fs_hdfs(FSOptions());
    const std::string filepath = "file://" + _root_path + "/create_file_with_open_truncate";
    auto st = fs->path_exists(filepath);
    EXPECT_TRUE(st.is_not_found());
    std::string str1 = "123";
    std::string str2 = "456";
    Slice data1(str1);
    Slice data2(str2);

    WritableFileOptions opts{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};

    auto wfile_1 = fs->new_writable_file(opts, filepath);
    EXPECT_TRUE(wfile_1.ok());
    EXPECT_TRUE((*wfile_1)->append(data1).ok());
    EXPECT_TRUE((*wfile_1)->flush(WritableFile::FlushMode::FLUSH_SYNC).ok());
    EXPECT_TRUE((*wfile_1)->sync().ok());
    (*wfile_1)->close();

    st = fs->path_exists(filepath);
    EXPECT_TRUE(st.ok());

    auto open_res1 = fs->new_random_access_file(filepath);
    EXPECT_TRUE(open_res1.ok());
    auto rfile1 = std::move(open_res1.value());
    auto read_res_1 = rfile1->read_all();
    EXPECT_TRUE(read_res_1.ok());
    EXPECT_TRUE(read_res_1.value() == str1);

    auto wfile_2 = fs->new_writable_file(opts, filepath);
    EXPECT_TRUE(wfile_2.ok());
    EXPECT_TRUE((*wfile_2)->append(data2).ok());
    EXPECT_TRUE((*wfile_2)->flush(WritableFile::FlushMode::FLUSH_SYNC).ok());
    EXPECT_TRUE((*wfile_2)->sync().ok());
    (*wfile_2)->close();

    st = fs->path_exists(filepath);
    EXPECT_TRUE(st.ok());

    auto open_res2 = fs->new_random_access_file(filepath);
    EXPECT_TRUE(open_res2.ok());
    auto rfile2 = std::move(open_res2.value());
    auto read_res_2 = rfile2->read_all();
    EXPECT_TRUE(read_res_2.ok());
    EXPECT_TRUE(read_res_2.value() == str2);

    (*wfile_1).reset();
    (*wfile_2).reset();
}

TEST_F(HdfsFileSystemTest, writable_file_close_after_cache_eviction) {
    writable_file_survives_cache_eviction(WriterRelease::kClose);
}

TEST_F(HdfsFileSystemTest, writable_file_close_from_bthread_after_cache_eviction) {
    writable_file_survives_cache_eviction(WriterRelease::kCloseFromBthread);
}

TEST_F(HdfsFileSystemTest, writable_file_destructor_after_cache_eviction) {
    writable_file_survives_cache_eviction(WriterRelease::kDestroy);
}

} // namespace starrocks
