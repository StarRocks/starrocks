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

#include "common/config.h"
#include "fs/fs_util.h"
#include "fs/hdfs/hdfs_fs_cache.h"
#include "testutil/sync_point.h"
#include "util/bthreads/util.h"
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
        // make sure the directory is clean before test run
        (void)fs::remove_all(_root_path);
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

TEST_F(HdfsFileSystemTest, writable_file_close_after_cache_eviction) {
    writable_file_survives_cache_eviction(WriterRelease::kClose);
}

TEST_F(HdfsFileSystemTest, writable_file_close_from_bthread_after_cache_eviction) {
    writable_file_survives_cache_eviction(WriterRelease::kCloseFromBthread);
}

TEST_F(HdfsFileSystemTest, writable_file_destructor_after_cache_eviction) {
    writable_file_survives_cache_eviction(WriterRelease::kDestroy);
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

    // done the file, check if there is any memory leak
    (*wfile).reset();

    // error injection during HDFSWritableFile::close, it is hard to simulate a close failure with a POSIX filesystem.
    // HDFS FileSystem close operation will fail if the file is removed from remote name node.
    std::string filepath2 = "file://" + _root_path + "/create_file_and_destroy_close_failure_file";
    auto wfile2 = fs->new_writable_file(filepath2);
    ASSERT_TRUE(wfile2.ok());
    SyncPoint::GetInstance()->SetCallBack("HDFSWritableFile::close", [](void* arg) { *(int*)arg = -1; });
    SyncPoint::GetInstance()->EnableProcessing();

    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("HDFSWritableFile::close");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    auto close_st = (*wfile2)->close();
    EXPECT_TRUE(close_st.is_io_error()) << close_st;
    // the file is marked closed even if hdfsCloseFile() failed, so a second close() is a no-op
    EXPECT_TRUE((*wfile2)->close().ok());
    (*wfile2).reset();
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

TEST_F(HdfsFileSystemTest, directory_operations) {
    auto fs = new_fs_hdfs(FSOptions());
    const std::string dirpath = "file://" + _root_path + "/test_directory";
    const std::string nested_dirpath = "file://" + _root_path + "/test_directory/nested";

    // Test directory doesn't exist initially
    auto st = fs->path_exists(dirpath);
    EXPECT_TRUE(st.is_not_found());

    // Test is_directory on non-existent path
    auto is_dir_res = fs->is_directory(dirpath);
    EXPECT_FALSE(is_dir_res.ok());

    // Test create_dir
    st = fs->create_dir(dirpath);
    EXPECT_TRUE(st.ok());

    // Test directory now exists
    st = fs->path_exists(dirpath);
    EXPECT_TRUE(st.ok());

    // Test is_directory on directory
    is_dir_res = fs->is_directory(dirpath);
    EXPECT_TRUE(is_dir_res.ok());
    EXPECT_TRUE(is_dir_res.value());

    // Test create_dir on existing directory
    // Don't check the existence of directory, so won't fail
    st = fs->create_dir(dirpath);
    EXPECT_TRUE(st.ok()) << st;

    // Test create_dir_if_missing on existing directory
    bool created = false;
    st = fs->create_dir_if_missing(dirpath, &created);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(created);

    // Test create_dir_recursive for nested path
    st = fs->create_dir_recursive(nested_dirpath);
    EXPECT_TRUE(st.ok());

    // Test nested directory exists and is a directory
    st = fs->path_exists(nested_dirpath);
    EXPECT_TRUE(st.ok());
    is_dir_res = fs->is_directory(nested_dirpath);
    EXPECT_TRUE(is_dir_res.ok());
    EXPECT_TRUE(is_dir_res.value());

    // Test delete_dir_recursive
    st = fs->delete_dir_recursive(dirpath);
    EXPECT_TRUE(st.ok());

    // Test directory no longer exists
    st = fs->path_exists(dirpath);
    EXPECT_TRUE(st.is_not_found());
    st = fs->path_exists(nested_dirpath);
    EXPECT_TRUE(st.is_not_found());
}

TEST_F(HdfsFileSystemTest, create_dir_if_missing_new_directory) {
    auto fs = new_fs_hdfs(FSOptions());
    const std::string dirpath = "file://" + _root_path + "/new_test_directory";

    // Test directory doesn't exist initially
    auto st = fs->path_exists(dirpath);
    EXPECT_TRUE(st.is_not_found());

    // Test create_dir_if_missing on non-existent directory
    bool created = false;
    st = fs->create_dir_if_missing(dirpath, &created);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(created);

    // Test directory now exists and is a directory
    st = fs->path_exists(dirpath);
    EXPECT_TRUE(st.ok());
    auto is_dir_res = fs->is_directory(dirpath);
    EXPECT_TRUE(is_dir_res.ok());
    EXPECT_TRUE(is_dir_res.value());

    // Cleanup
    st = fs->delete_dir_recursive(dirpath);
    EXPECT_TRUE(st.ok());
}

TEST_F(HdfsFileSystemTest, create_dir_if_missing_on_file) {
    auto fs = new_fs_hdfs(FSOptions());
    const std::string filepath = "file://" + _root_path + "/test_file_not_dir";

    // Create a file first
    auto wfile = fs->new_writable_file(filepath);
    EXPECT_TRUE(wfile.ok());
    std::string data = "test data";
    EXPECT_TRUE((*wfile)->append(Slice(data)).ok());
    (*wfile)->close();

    // Test create_dir_if_missing on existing file (should fail)
    bool created = false;
    auto st = fs->create_dir_if_missing(filepath, &created);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.is_invalid_argument());

    // Cleanup
    st = fs->delete_file(filepath);
    EXPECT_TRUE(st.ok());
}

TEST_F(HdfsFileSystemTest, is_directory_on_file) {
    auto fs = new_fs_hdfs(FSOptions());
    const std::string filepath = "file://" + _root_path + "/test_file_for_is_directory";

    // Create a file
    auto wfile = fs->new_writable_file(filepath);
    EXPECT_TRUE(wfile.ok());
    std::string data = "test data";
    EXPECT_TRUE((*wfile)->append(Slice(data)).ok());
    (*wfile)->close();

    // Test is_directory on file (should return false)
    auto is_dir_res = fs->is_directory(filepath);
    EXPECT_TRUE(is_dir_res.ok());
    EXPECT_FALSE(is_dir_res.value());

    // Cleanup
    auto st = fs->delete_file(filepath);
    EXPECT_TRUE(st.ok());
}

TEST_F(HdfsFileSystemTest, delete_empty_directory) {
    auto fs = new_fs_hdfs(FSOptions());
    const std::string dirpath = "file://" + _root_path + "/empty_directory";

    // Create directory
    auto st = fs->create_dir(dirpath);
    EXPECT_TRUE(st.ok());

    // Test delete_dir on empty directory
    st = fs->delete_dir(dirpath);
    EXPECT_TRUE(st.ok());

    // Test directory no longer exists
    st = fs->path_exists(dirpath);
    EXPECT_TRUE(st.is_not_found());
}

TEST_F(HdfsFileSystemTest, delete_directory_with_files) {
    auto fs = new_fs_hdfs(FSOptions());
    const std::string dirpath = "file://" + _root_path + "/directory_with_files";
    const std::string filepath = dirpath + "/test_file.txt";

    // Create directory and file
    auto st = fs->create_dir(dirpath);
    EXPECT_TRUE(st.ok());

    auto wfile = fs->new_writable_file(filepath);
    EXPECT_TRUE(wfile.ok());
    std::string data = "test data";
    EXPECT_TRUE((*wfile)->append(Slice(data)).ok());
    (*wfile)->close();

    // Test delete_dir on non-empty directory (should fail)
    st = fs->delete_dir(dirpath);
    EXPECT_FALSE(st.ok());

    // Test delete_dir_recursive on non-empty directory (should succeed)
    st = fs->delete_dir_recursive(dirpath);
    EXPECT_TRUE(st.ok());

    // Test directory and file no longer exist
    st = fs->path_exists(dirpath);
    EXPECT_TRUE(st.is_not_found());
    st = fs->path_exists(filepath);
    EXPECT_TRUE(st.is_not_found());
}

TEST_F(HdfsFileSystemTest, delete_file_on_directory) {
    auto fs = new_fs_hdfs(FSOptions());
    const std::string dirpath = "file://" + _root_path + "/dir_for_delete_file_test";

    // Create directory
    auto st = fs->create_dir(dirpath);
    EXPECT_TRUE(st.ok());

    const std::string filepath = "file://" + _root_path + "/dir_for_delete_file_test/file_for_delete_dir_test";

    // Create file
    auto wfile = fs->new_writable_file(filepath);
    EXPECT_TRUE(wfile.ok());
    EXPECT_TRUE((*wfile)->append(Slice("test")).ok());
    (*wfile)->close();

    // Try to delete a non-empty directory with delete_file, should fail
    st = fs->delete_file(dirpath);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.is_invalid_argument());

    // Try to delete the file first
    st = fs->delete_file(filepath);
    EXPECT_TRUE(st.ok()) << st;

    // Try to delete the directory with delete_file interface, now it is allowed in hdfs filesystem
    st = fs->delete_file(dirpath);
    EXPECT_TRUE(st.ok());
}

TEST_F(HdfsFileSystemTest, delete_dir_on_file) {
    auto fs = new_fs_hdfs(FSOptions());
    const std::string filepath = "file://" + _root_path + "/file_for_delete_dir_test";

    // Create file
    auto wfile = fs->new_writable_file(filepath);
    EXPECT_TRUE(wfile.ok());
    EXPECT_TRUE((*wfile)->append(Slice("test")).ok());
    (*wfile)->close();

    // Try to delete file with delete_dir, should succeed
    auto st = fs->delete_dir(filepath);
    EXPECT_TRUE(st.ok()) << st;
}

TEST_F(HdfsFileSystemTest, delete_dir_recursive_on_file) {
    auto fs = new_fs_hdfs(FSOptions());
    const std::string filepath = "file://" + _root_path + "/file_for_delete_dir_recursive_test";

    // Create file
    auto wfile = fs->new_writable_file(filepath);
    EXPECT_TRUE(wfile.ok());
    EXPECT_TRUE((*wfile)->append(Slice("test")).ok());
    (*wfile)->close();

    // Try to delete file with delete_dir_recursive, should succeed in HDFS filesystem
    auto st = fs->delete_dir_recursive(filepath);
    EXPECT_TRUE(st.ok()) << st;
}

TEST_F(HdfsFileSystemTest, delete_dir_recursive_on_non_existent_path) {
    auto fs = new_fs_hdfs(FSOptions());
    const std::string dirpath = "file://" + _root_path + "/non_existent_dir";

    // delete_dir_recursive on non-existent path should succeed
    auto st = fs->delete_dir_recursive(dirpath);
    EXPECT_TRUE(st.ok());
}

TEST_F(HdfsFileSystemTest, spilling_scenario_simulation) {
    // This test simulates the spilling workflow that was failing
    auto fs = new_fs_hdfs(FSOptions());
    const std::string base_spill_dir = "file://" + _root_path + "/spill_base";
    const std::string query_spill_dir = base_spill_dir + "/query_123";
    const std::string container_dir = query_spill_dir + "/container_456";

    // Simulate spilling directory creation workflow

    // 1. Create base spill directory (create_dir_if_missing in dir_manager.cpp)
    bool created = false;
    auto st = fs->create_dir_if_missing(base_spill_dir, &created);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(created);

    // 2. Create query-specific directory (create_dir_if_missing in query_spill_manager.cpp)
    created = false;
    st = fs->create_dir_if_missing(query_spill_dir, &created);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(created);

    // 3. Create container directory (create_dir_if_missing in file_block_manager.cpp)
    created = false;
    st = fs->create_dir_if_missing(container_dir, &created);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(created);

    // 4. Verify all directories exist and are directories
    st = fs->path_exists(base_spill_dir);
    EXPECT_TRUE(st.ok());
    auto is_dir_res = fs->is_directory(base_spill_dir);
    EXPECT_TRUE(is_dir_res.ok());
    EXPECT_TRUE(is_dir_res.value());

    st = fs->path_exists(query_spill_dir);
    EXPECT_TRUE(st.ok());
    is_dir_res = fs->is_directory(query_spill_dir);
    EXPECT_TRUE(is_dir_res.ok());
    EXPECT_TRUE(is_dir_res.value());

    st = fs->path_exists(container_dir);
    EXPECT_TRUE(st.ok());
    is_dir_res = fs->is_directory(container_dir);
    EXPECT_TRUE(is_dir_res.ok());
    EXPECT_TRUE(is_dir_res.value());

    // 5. Create some spill files to simulate actual spilling
    const std::string spill_file1 = container_dir + "/spill_block_001.dat";
    const std::string spill_file2 = container_dir + "/spill_block_002.dat";

    auto wfile1 = fs->new_writable_file(spill_file1);
    EXPECT_TRUE(wfile1.ok());
    std::string data1 = "spilled data block 1";
    EXPECT_TRUE((*wfile1)->append(Slice(data1)).ok());
    (*wfile1)->close();

    auto wfile2 = fs->new_writable_file(spill_file2);
    EXPECT_TRUE(wfile2.ok());
    std::string data2 = "spilled data block 2";
    EXPECT_TRUE((*wfile2)->append(Slice(data2)).ok());
    (*wfile2)->close();

    // 6. Verify files exist and are not directories
    st = fs->path_exists(spill_file1);
    EXPECT_TRUE(st.ok());
    is_dir_res = fs->is_directory(spill_file1);
    EXPECT_TRUE(is_dir_res.ok());
    EXPECT_FALSE(is_dir_res.value());

    st = fs->path_exists(spill_file2);
    EXPECT_TRUE(st.ok());
    is_dir_res = fs->is_directory(spill_file2);
    EXPECT_TRUE(is_dir_res.ok());
    EXPECT_FALSE(is_dir_res.value());

    // 7. Cleanup using recursive delete (simulate cleanup in dir_manager.cpp)
    st = fs->delete_dir_recursive(base_spill_dir);
    EXPECT_TRUE(st.ok());

    // 8. Verify everything is cleaned up
    st = fs->path_exists(base_spill_dir);
    EXPECT_TRUE(st.is_not_found());
    st = fs->path_exists(query_spill_dir);
    EXPECT_TRUE(st.is_not_found());
    st = fs->path_exists(container_dir);
    EXPECT_TRUE(st.is_not_found());
    st = fs->path_exists(spill_file1);
    EXPECT_TRUE(st.is_not_found());
    st = fs->path_exists(spill_file2);
    EXPECT_TRUE(st.is_not_found());
}

TEST_F(HdfsFileSystemTest, verify_hdfs_create_directory_behavior) {
    auto fs = new_fs_hdfs(FSOptions());
    const std::string deep_nested = "file://" + _root_path + "/level1/level2/level3";

    // Test if hdfsCreateDirectory (via create_dir) creates parent directories automatically
    auto st = fs->create_dir(deep_nested);
    if (st.ok()) {
        // hdfsCreateDirectory creates parents automatically
        LOG(INFO) << "hdfsCreateDirectory creates parent directories automatically";

        // Verify all levels exist
        EXPECT_TRUE(fs->path_exists("file://" + _root_path + "/level1").ok());
        EXPECT_TRUE(fs->path_exists("file://" + _root_path + "/level1/level2").ok());
        EXPECT_TRUE(fs->path_exists(deep_nested).ok());

        // Cleanup
        fs->delete_dir_recursive("file://" + _root_path + "/level1");
    } else {
        // hdfsCreateDirectory does NOT create parents automatically
        LOG(INFO) << "hdfsCreateDirectory does NOT create parent directories automatically";
        EXPECT_FALSE(st.ok());
    }
}

} // namespace starrocks
