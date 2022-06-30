// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "fs/fs_starlet.h"

#include <fmt/format.h>
#include <fslib/configuration.h>
#include <fslib/fslib_all_initializer.h>
#include <gtest/gtest.h>

#include <fstream>

#include "common/config.h"
#include "gutil/strings/join.h"
#include "service/staros_worker.h"
#include "testutil/assert.h"

namespace starrocks {

class StarletFileSystemTest : public testing::Test {
public:
    StarletFileSystemTest() = default;
    ~StarletFileSystemTest() override = default;
    void SetUp() override {
        staros::starlet::fslib::register_builtin_filesystems();
        staros::starlet::ShardInfo shard_info;
        shard_info.id = 10086;
        shard_info.obj_store_info.uri = "";
        shard_info.properties[staros::starlet::fslib::kS3AccessKeyId] = "5LXNPOQY3KB1LH4X4UQ6";
        shard_info.properties[staros::starlet::fslib::kS3AccessKeySecret] = "EhniJDQcMAFQwpulH1jLomfu1b+VaJboCJO+Cytb";
        shard_info.properties[staros::starlet::fslib::kS3OverrideEndpoint] = "172.26.92.205:39000";
        shard_info.properties[staros::starlet::fslib::kS3Bucket] = "starrocks-test-bucket";
        shard_info.properties["storageGroup"] = "10010";
        g_worker = std::make_shared<starrocks::StarOSWorker>();
        (void)g_worker->add_shard(shard_info);
    }
    void TearDown() override {
        (void)g_worker->remove_shard(10086);
        g_worker.reset();
    }

    std::string StarletPath(std::string_view path) {
        if (path.front() == '/') {
            return fmt::format("{}?ShardId={}", path.substr(1), 10086);
        } else {
            return fmt::format("{}?ShardId={}", path, 10086);
        }
    }

    void CheckIsDirectory(FileSystem* fs, const std::string& dir_name, bool expected_success,
                          bool expected_is_dir = true) {
        const StatusOr<bool> status_or = fs->is_directory(dir_name);
        EXPECT_EQ(expected_success, status_or.ok());
        if (status_or.ok()) {
            EXPECT_EQ(expected_is_dir, status_or.value());
        }
    }
};

TEST_F(StarletFileSystemTest, test_write_and_read) {
    auto uri = StarletPath("test1");
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateUniqueFromString("staros_s3://"));
    ASSIGN_OR_ABORT(auto wf, fs->new_writable_file(uri));
    EXPECT_OK(wf->append("hello"));
    EXPECT_OK(wf->append(" world!"));
    EXPECT_OK(wf->sync());
    EXPECT_OK(wf->close());
    EXPECT_EQ(sizeof("hello world!"), wf->size() + 1);

    char buf[1024];
    ASSIGN_OR_ABORT(auto rf, fs->new_random_access_file(uri));
    ASSIGN_OR_ABORT(auto nr, rf->read_at(0, buf, sizeof(buf)));
    EXPECT_EQ("hello world!", std::string_view(buf, nr));

    ASSIGN_OR_ABORT(nr, rf->read_at(3, buf, sizeof(buf)));
    EXPECT_EQ("lo world!", std::string_view(buf, nr));

    EXPECT_OK(fs->delete_file(uri));
    EXPECT_TRUE(fs->new_random_access_file(uri).status().is_not_found());
}

TEST_F(StarletFileSystemTest, test_directory) {
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateUniqueFromString("staros_s3://"));
    bool created = false;

    //
    //  /dirname0/
    //
    EXPECT_OK(fs->create_dir(StarletPath("/dirname0")));
    CheckIsDirectory(fs.get(), StarletPath("/dirname"), false);
    CheckIsDirectory(fs.get(), StarletPath("/dirname0"), true, true);
    EXPECT_TRUE(fs->create_dir(StarletPath("/dirname0")).is_already_exist());

    EXPECT_OK(fs->create_dir_if_missing(StarletPath("/dirname0"), &created));
    EXPECT_FALSE(created);
    CheckIsDirectory(fs.get(), StarletPath("/dirname0"), true, true);

    //
    //  /dirname0/
    //  /dirname1/
    //
    EXPECT_OK(fs->create_dir_if_missing(StarletPath("/dirname1"), &created));
    EXPECT_TRUE(created);
    CheckIsDirectory(fs.get(), StarletPath("/dirname1"), true, true);

    CheckIsDirectory(fs.get(), StarletPath("/noexistdir"), false);
    EXPECT_ERROR(fs->new_writable_file(StarletPath("/filename/")));

    //
    //  /dirname0/
    //  /dirname1/
    //  /file0
    //
    {
        ASSIGN_OR_ABORT(auto of, fs->new_writable_file(StarletPath("/file0")));
        EXPECT_OK(of->append("hello"));
        EXPECT_OK(of->close());
    }
    CheckIsDirectory(fs.get(), StarletPath("/file0"), true, false);

    //
    //  /dirname0/
    //  /dirname1/
    //  /dirname2/0.dat
    //  /file0
    //
    {
        // NOTE: Although directory "/dirname2" does not exist, we can still create file under "/dirname2" successfully
        ASSIGN_OR_ABORT(auto of, fs->new_writable_file(StarletPath("/dirname2/0.dat")));
        EXPECT_OK(of->append("hello"));
        EXPECT_OK(of->close());
        CheckIsDirectory(fs.get(), StarletPath("/dirname2/0.dat"), true, false);
        CheckIsDirectory(fs.get(), StarletPath("/dirname2/0"), false);
        CheckIsDirectory(fs.get(), StarletPath("/dirname2/0.da"), false);
    }
    CheckIsDirectory(fs.get(), StarletPath("/dirname2"), true, true);

    //
    //  /dirname0/
    //  /dirname1/
    //  /dirname2/0.dat
    //  /dirname2/1.dat
    //  /file0
    //
    {
        ASSIGN_OR_ABORT(auto of, fs->new_writable_file(StarletPath("/dirname2/1.dat")));
        EXPECT_OK(of->append("hello"));
        EXPECT_OK(of->close());
        CheckIsDirectory(fs.get(), StarletPath("/dirname2/1.dat"), true, false);
    }
    CheckIsDirectory(fs.get(), StarletPath("/dirname2"), true, true);

    //
    //  /dirname0/
    //  /dirname1/
    //  /dirname2/0.dat
    //  /dirname2/1.dat
    //  /dirname2/subdir0/
    //  /file0
    //
    EXPECT_OK(fs->create_dir(StarletPath("/dirname2/subdir0")));
    CheckIsDirectory(fs.get(), StarletPath("/dirname2/subdir0"), true, true);

    std::vector<std::string> entries;
    auto cb = [&](std::string_view name) -> bool {
        entries.emplace_back(name);
        return true;
    };

    EXPECT_ERROR(fs->iterate_dir(StarletPath("/nonexistdir"), cb));
    EXPECT_ERROR(fs->delete_dir(StarletPath("/nonexistdir")));

    entries.clear();
    EXPECT_OK(fs->iterate_dir(StarletPath("/"), cb));
    EXPECT_EQ("dirname0/,dirname1/,dirname2/,file0", JoinStrings(entries, ","));

    entries.clear();
    EXPECT_OK(fs->iterate_dir(StarletPath("/dirname0"), cb));
    EXPECT_EQ("", JoinStrings(entries, ","));

    entries.clear();
    EXPECT_OK(fs->iterate_dir(StarletPath("/dirname1"), cb));
    EXPECT_EQ("", JoinStrings(entries, ","));

    entries.clear();
    EXPECT_OK(fs->iterate_dir(StarletPath("/dirname2"), cb));
    EXPECT_EQ("subdir0/,0.dat,1.dat", JoinStrings(entries, ","));

    entries.clear();
    EXPECT_OK(fs->iterate_dir(StarletPath("/dirname2/subdir0"), cb));
    EXPECT_EQ("", JoinStrings(entries, ","));

    EXPECT_ERROR(fs->delete_dir(StarletPath("/dirname2"))); // dirname2 is not empty

    EXPECT_OK(fs->delete_dir(StarletPath("/dirname0")));
    EXPECT_OK(fs->delete_dir(StarletPath("/dirname1")));
    EXPECT_OK(fs->delete_file(StarletPath("/dirname2/0.dat")));
    EXPECT_OK(fs->delete_file(StarletPath("/dirname2/1.dat")));
    EXPECT_OK(fs->delete_dir(StarletPath("/dirname2/subdir0")));
    EXPECT_ERROR(fs->delete_dir(StarletPath("/dirname2"))); // "/dirname2/" is a non-exist object
    EXPECT_OK(fs->delete_file(StarletPath("/file0")));
}

TEST_F(StarletFileSystemTest, test_delete_dir_recursive) {
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateUniqueFromString("staros_s3://"));

    std::vector<std::string> entries;
    auto cb = [&](std::string_view name) -> bool {
        entries.emplace_back(name);
        return true;
    };

    bool created;
    fs->delete_dir_recursive(StarletPath("/dirname0"));
    EXPECT_OK(fs->create_dir_if_missing(StarletPath("/dirname0"), &created));
    ASSERT_OK(fs->delete_dir_recursive(StarletPath("/dirname0")));
    EXPECT_OK(fs->iterate_dir(StarletPath("/"), cb));
    ASSERT_EQ(0, entries.size());

    EXPECT_OK(fs->create_dir_if_missing(StarletPath("/dirname0"), &created));
    EXPECT_OK(fs->create_dir_if_missing(StarletPath("/dirname0/a"), &created));
    EXPECT_OK(fs->create_dir_if_missing(StarletPath("/dirname0/b"), &created));
    EXPECT_OK(fs->create_dir_if_missing(StarletPath("/dirname0/a/a"), &created));
    EXPECT_OK(fs->create_dir_if_missing(StarletPath("/dirname0/a/b"), &created));
    EXPECT_OK(fs->create_dir_if_missing(StarletPath("/dirname0/a/c"), &created));
    {
        ASSIGN_OR_ABORT(auto of, fs->new_writable_file(StarletPath("/dirname0/1.dat")));
        EXPECT_OK(of->append("hello"));
        EXPECT_OK(of->close());
    }
    {
        ASSIGN_OR_ABORT(auto of, fs->new_writable_file(StarletPath("/dirname0/a/1.dat")));
        EXPECT_OK(of->append("hello"));
        EXPECT_OK(of->close());
    }

    EXPECT_OK(fs->create_dir_if_missing(StarletPath("/dirname0x"), &created));
    ASSERT_OK(fs->delete_dir_recursive(StarletPath("/dirname0")));
    EXPECT_OK(fs->iterate_dir(StarletPath("/"), cb));
    ASSERT_EQ(1, entries.size());
    ASSERT_EQ("dirname0x/", entries[0]);
    ASSERT_OK(fs->delete_dir(StarletPath("/dirname0x")));
    ASSERT_ERROR(fs->delete_dir_recursive(StarletPath("/")));
}

TEST_F(StarletFileSystemTest, test_delete_nonexist_file) {
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateUniqueFromString("staros_s3://"));
    ASSERT_OK(fs->delete_file(StarletPath("/nonexist.dat")));
}

} // namespace starrocks
