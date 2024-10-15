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

#include "fs/fs_s3.h"

#include <aws/core/Aws.h>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include <fstream>

#include "common/config.h"
#include "fs/fs_s3.cpp"
#include "gutil/strings/join.h"
#include "testutil/assert.h"
#include "util/uid_util.h"

namespace starrocks {

// NOTE: The bucket must be created before running this test.
constexpr static const char* kBucketName = "starrocks-fs-s3-ut";

class S3FileSystemTest : public testing::Test {
public:
    S3FileSystemTest() : _root_path(generate_uuid_string()) {}
    ~S3FileSystemTest() override = default;

    static void SetUpTestCase() {
        CHECK(!config::object_storage_access_key_id.empty()) << "Need set object_storage_access_key_id in be_test.conf";
        CHECK(!config::object_storage_secret_access_key.empty())
                << "Need set object_storage_secret_access_key in be_test.conf";
        CHECK(!config::object_storage_endpoint.empty()) << "Need set object_storage_endpoint in be_test.conf";

        Aws::InitAPI(_s_options);
    }

    virtual void SetUp() override {
        ASSIGN_OR_ABORT(auto fs, FileSystem::CreateUniqueFromString("s3://"));
        (void)fs->delete_dir_recursive(S3Path("/"));
    }

    virtual void TearDown() override {
        ASSIGN_OR_ABORT(auto fs, FileSystem::CreateUniqueFromString("s3://"));
        (void)fs->delete_dir_recursive(S3Path("/"));
    }

    static void TearDownTestCase() {
        close_s3_clients();
        Aws::ShutdownAPI(_s_options);
    }

    std::string S3Path(std::string_view path) { return fmt::format("s3://{}/{}{}", kBucketName, _root_path, path); }

    static std::string S3Root() { return fmt::format("s3://{}", kBucketName); }

    void CheckIsDirectory(FileSystem* fs, const std::string& dir_name, bool expected_success,
                          bool expected_is_dir = true) {
        const StatusOr<bool> status_or = fs->is_directory(dir_name);
        EXPECT_EQ(expected_success, status_or.ok());
        if (status_or.ok()) {
            EXPECT_EQ(expected_is_dir, status_or.value());
        }
    }

private:
    std::string _root_path;
    static inline Aws::SDKOptions _s_options;
};

TEST_F(S3FileSystemTest, test_write_and_read) {
    auto uri = S3Path("/dir/test-object.png");
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateUniqueFromString(uri));
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
    ASSIGN_OR_ABORT(rf, fs->new_random_access_file(uri));
    EXPECT_ERROR(rf->read_at(0, buf, sizeof(buf)));
}

TEST_F(S3FileSystemTest, test_write_and_read_with_options) {
    auto uri = S3Path("/dir/test-object.png");
    auto fs_opts = FSOptions(
            {{FSOptions::FS_S3_ENDPOINT, config::object_storage_endpoint},
             {FSOptions::FS_S3_ENDPOINT_REGION, config::object_storage_region},
             {FSOptions::FS_S3_PATH_STYLE_ACCESS, std::to_string(config::object_storage_endpoint_path_style_access)},
             {FSOptions::FS_S3_ACCESS_KEY, config::object_storage_access_key_id},
             {FSOptions::FS_S3_SECRET_KEY, config::object_storage_secret_access_key},
             {FSOptions::FS_S3_CONNECTION_SSL_ENABLED, std::to_string(config::object_storage_endpoint_use_https)},
             {FSOptions::FS_S3_READ_AHEAD_RANGE, std::to_string(64 * 1024)},
             {FSOptions::FS_S3_RETRY_LIMIT, std::to_string(config::object_storage_max_retries)},
             {FSOptions::FS_S3_RETRY_INTERVAL, std::to_string(config::object_storage_retry_scale_factor)}});
    ASSERT_TRUE(nullptr == fs_opts.hdfs_properties());
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateUniqueFromString(uri, fs_opts));
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
    ASSIGN_OR_ABORT(rf, fs->new_random_access_file(uri));
    EXPECT_ERROR(rf->read_at(0, buf, sizeof(buf)));
}

TEST_F(S3FileSystemTest, test_root_directory) {
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateUniqueFromString("s3://"));
    bool created = false;
    auto bucket_root = S3Root();

    // no need to create the bucket_root
    ASSERT_TRUE(fs->create_dir(bucket_root).is_already_exist());
    ASSERT_OK(fs->create_dir_if_missing(bucket_root, &created));
    ASSERT_FALSE(created);
    CheckIsDirectory(fs.get(), bucket_root, true, true);
    // can't directly delete from bucket root
    ASSERT_ERROR(fs->delete_dir(bucket_root));
}

TEST_F(S3FileSystemTest, test_directory) {
    auto now = ::time(nullptr);
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateUniqueFromString("s3://"));
    bool created = false;

    //
    //  /dirname0/
    //
    EXPECT_OK(fs->create_dir(S3Path("/dirname0")));
    CheckIsDirectory(fs.get(), S3Path("/dirname"), false);
    CheckIsDirectory(fs.get(), S3Path("/dirname0"), true, true);
    EXPECT_TRUE(fs->create_dir(S3Path("/dirname0")).is_already_exist());

    EXPECT_OK(fs->create_dir_if_missing(S3Path("/dirname0"), &created));
    EXPECT_FALSE(created);
    CheckIsDirectory(fs.get(), S3Path("/dirname0"), true, true);

    //
    //  /dirname0/
    //  /dirname1/
    //
    EXPECT_OK(fs->create_dir_if_missing(S3Path("/dirname1"), &created));
    EXPECT_TRUE(created);
    CheckIsDirectory(fs.get(), S3Path("/dirname1"), true, true);

    CheckIsDirectory(fs.get(), S3Path("/noexistdir"), false);
    EXPECT_ERROR(fs->new_writable_file(S3Path("/filename/")));

    //
    //  /dirname0/
    //  /dirname1/
    //  /file0
    //
    {
        ASSIGN_OR_ABORT(auto of, fs->new_writable_file(S3Path("/file0")));
        EXPECT_OK(of->append("hello"));
        EXPECT_OK(of->close());
    }
    CheckIsDirectory(fs.get(), S3Path("/file0"), true, false);

    //
    //  /dirname0/
    //  /dirname1/
    //  /dirname2/0.dat
    //  /file0
    //
    {
        // NOTE: Although directory "/dirname2" does not exist, we can still create file under "/dirname2" successfully
        ASSIGN_OR_ABORT(auto of, fs->new_writable_file(S3Path("/dirname2/0.dat")));
        EXPECT_OK(of->append("hello"));
        EXPECT_OK(of->close());
        CheckIsDirectory(fs.get(), S3Path("/dirname2/0.dat"), true, false);
        CheckIsDirectory(fs.get(), S3Path("/dirname2/0"), false);
        CheckIsDirectory(fs.get(), S3Path("/dirname2/0.da"), false);
    }
    CheckIsDirectory(fs.get(), S3Path("/dirname2"), true, true);

    //
    //  /dirname0/
    //  /dirname1/
    //  /dirname2/0.dat
    //  /dirname2/1.dat
    //  /file0
    //
    {
        ASSIGN_OR_ABORT(auto of, fs->new_writable_file(S3Path("/dirname2/1.dat")));
        EXPECT_OK(of->append("starrocks"));
        EXPECT_OK(of->close());
        CheckIsDirectory(fs.get(), S3Path("/dirname2/1.dat"), true, false);
    }
    CheckIsDirectory(fs.get(), S3Path("/dirname2"), true, true);

    //
    //  /dirname0/
    //  /dirname1/
    //  /dirname2/0.dat
    //  /dirname2/1.dat
    //  /dirname2/subdir0/
    //  /file0
    //
    EXPECT_OK(fs->create_dir(S3Path("/dirname2/subdir0")));
    CheckIsDirectory(fs.get(), S3Path("/dirname2/subdir0"), true, true);

    EXPECT_OK(fs->iterate_dir2(S3Path("/dirname2/"), [&](DirEntry entry) {
        auto name = entry.name;
        if (name == "0.dat") {
            CHECK(entry.is_dir.has_value());
            CHECK(!entry.is_dir.value());
            CHECK(entry.size.has_value());
            CHECK_EQ(/* length of "hello" = */ 5, entry.size.value());
            CHECK(entry.mtime.has_value());
            CHECK_GE(entry.mtime.value(), now);
        } else if (name == "1.dat") {
            CHECK(entry.is_dir.has_value());
            CHECK(!entry.is_dir.value());
            CHECK(entry.size.has_value());
            CHECK_EQ(/* length of "starrocks" = */ 9, entry.size.value());
            CHECK(entry.mtime.has_value());
            CHECK_GE(entry.mtime.value(), now);
        } else if (name == "subdir0") {
            CHECK(entry.is_dir.has_value());
            CHECK(entry.is_dir.value());
            CHECK(!entry.size.has_value());
            CHECK(!entry.mtime.has_value());
        } else {
            CHECK(false) << "Unexpected file " << name;
        }
        return true;
    }));

    std::vector<std::string> entries;
    auto cb = [&](std::string_view name) -> bool {
        entries.emplace_back(name);
        return true;
    };

    EXPECT_ERROR(fs->iterate_dir(S3Path("/nonexistdir"), cb));
    EXPECT_ERROR(fs->delete_dir(S3Path("/nonexistdir")));

    entries.clear();
    EXPECT_OK(fs->iterate_dir(S3Path("/"), cb));
    EXPECT_EQ("dirname0,dirname1,dirname2,file0", JoinStrings(entries, ","));

    entries.clear();
    EXPECT_OK(fs->iterate_dir(S3Path("/dirname0"), cb));
    EXPECT_EQ("", JoinStrings(entries, ","));

    entries.clear();
    EXPECT_OK(fs->iterate_dir(S3Path("/dirname1"), cb));
    EXPECT_EQ("", JoinStrings(entries, ","));

    entries.clear();
    EXPECT_OK(fs->iterate_dir(S3Path("/dirname2"), cb));
    EXPECT_EQ("0.dat,1.dat,subdir0", JoinStrings(entries, ","));

    entries.clear();
    EXPECT_OK(fs->iterate_dir(S3Path("/dirname2/subdir0"), cb));
    EXPECT_EQ("", JoinStrings(entries, ","));

    EXPECT_ERROR(fs->delete_dir(S3Path("/dirname2"))); // dirname2 is not empty

    EXPECT_OK(fs->delete_dir(S3Path("/dirname0")));
    EXPECT_OK(fs->delete_dir(S3Path("/dirname1")));
    EXPECT_OK(fs->delete_file(S3Path("/dirname2/0.dat")));
    EXPECT_OK(fs->delete_file(S3Path("/dirname2/1.dat")));
    EXPECT_OK(fs->delete_dir(S3Path("/dirname2/subdir0")));
    EXPECT_ERROR(fs->delete_dir(S3Path("/dirname2"))); // "/dirname2/" is a non-exist object
    EXPECT_OK(fs->delete_file(S3Path("/file0")));
}

TEST_F(S3FileSystemTest, test_directory_v1) {
    bool s3_use_list_objects_v1 = config::s3_use_list_objects_v1;
    config::s3_use_list_objects_v1 = true;

    auto now = ::time(nullptr);
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateUniqueFromString("s3://"));
    bool created = false;

    //
    //  /dirname0/
    //
    EXPECT_OK(fs->create_dir(S3Path("/dirname0")));
    CheckIsDirectory(fs.get(), S3Path("/dirname"), false);
    CheckIsDirectory(fs.get(), S3Path("/dirname0"), true, true);
    EXPECT_TRUE(fs->create_dir(S3Path("/dirname0")).is_already_exist());

    EXPECT_OK(fs->create_dir_if_missing(S3Path("/dirname0"), &created));
    EXPECT_FALSE(created);
    CheckIsDirectory(fs.get(), S3Path("/dirname0"), true, true);

    //
    //  /dirname0/
    //  /dirname1/
    //
    EXPECT_OK(fs->create_dir_if_missing(S3Path("/dirname1"), &created));
    EXPECT_TRUE(created);
    CheckIsDirectory(fs.get(), S3Path("/dirname1"), true, true);

    CheckIsDirectory(fs.get(), S3Path("/noexistdir"), false);
    EXPECT_ERROR(fs->new_writable_file(S3Path("/filename/")));

    //
    //  /dirname0/
    //  /dirname1/
    //  /file0
    //
    {
        ASSIGN_OR_ABORT(auto of, fs->new_writable_file(S3Path("/file0")));
        EXPECT_OK(of->append("hello"));
        EXPECT_OK(of->close());
    }
    CheckIsDirectory(fs.get(), S3Path("/file0"), true, false);

    //
    //  /dirname0/
    //  /dirname1/
    //  /dirname2/0.dat
    //  /file0
    //
    {
        // NOTE: Although directory "/dirname2" does not exist, we can still create file under "/dirname2" successfully
        ASSIGN_OR_ABORT(auto of, fs->new_writable_file(S3Path("/dirname2/0.dat")));
        EXPECT_OK(of->append("hello"));
        EXPECT_OK(of->close());
        CheckIsDirectory(fs.get(), S3Path("/dirname2/0.dat"), true, false);
        CheckIsDirectory(fs.get(), S3Path("/dirname2/0"), false);
        CheckIsDirectory(fs.get(), S3Path("/dirname2/0.da"), false);
    }
    CheckIsDirectory(fs.get(), S3Path("/dirname2"), true, true);

    //
    //  /dirname0/
    //  /dirname1/
    //  /dirname2/0.dat
    //  /dirname2/1.dat
    //  /file0
    //
    {
        ASSIGN_OR_ABORT(auto of, fs->new_writable_file(S3Path("/dirname2/1.dat")));
        EXPECT_OK(of->append("starrocks"));
        EXPECT_OK(of->close());
        CheckIsDirectory(fs.get(), S3Path("/dirname2/1.dat"), true, false);
    }
    CheckIsDirectory(fs.get(), S3Path("/dirname2"), true, true);

    //
    //  /dirname0/
    //  /dirname1/
    //  /dirname2/0.dat
    //  /dirname2/1.dat
    //  /dirname2/subdir0/
    //  /file0
    //
    EXPECT_OK(fs->create_dir(S3Path("/dirname2/subdir0")));
    CheckIsDirectory(fs.get(), S3Path("/dirname2/subdir0"), true, true);

    EXPECT_OK(fs->iterate_dir2(S3Path("/dirname2/"), [&](DirEntry entry) {
        auto name = entry.name;
        if (name == "0.dat") {
            CHECK(entry.is_dir.has_value());
            CHECK(!entry.is_dir.value());
            CHECK(entry.size.has_value());
            CHECK_EQ(/* length of "hello" = */ 5, entry.size.value());
            CHECK(entry.mtime.has_value());
            CHECK_GE(entry.mtime.value(), now);
        } else if (name == "1.dat") {
            CHECK(entry.is_dir.has_value());
            CHECK(!entry.is_dir.value());
            CHECK(entry.size.has_value());
            CHECK_EQ(/* length of "starrocks" = */ 9, entry.size.value());
            CHECK(entry.mtime.has_value());
            CHECK_GE(entry.mtime.value(), now);
        } else if (name == "subdir0") {
            CHECK(entry.is_dir.has_value());
            CHECK(entry.is_dir.value());
            CHECK(!entry.size.has_value());
            CHECK(!entry.mtime.has_value());
        } else {
            CHECK(false) << "Unexpected file " << name;
        }
        return true;
    }));

    std::vector<std::string> entries;
    auto cb = [&](std::string_view name) -> bool {
        entries.emplace_back(name);
        return true;
    };

    EXPECT_ERROR(fs->iterate_dir(S3Path("/nonexistdir"), cb));
    EXPECT_ERROR(fs->delete_dir(S3Path("/nonexistdir")));

    entries.clear();
    EXPECT_OK(fs->iterate_dir(S3Path("/"), cb));
    EXPECT_EQ("dirname0,dirname1,dirname2,file0", JoinStrings(entries, ","));

    entries.clear();
    EXPECT_OK(fs->iterate_dir(S3Path("/dirname0"), cb));
    EXPECT_EQ("", JoinStrings(entries, ","));

    entries.clear();
    EXPECT_OK(fs->iterate_dir(S3Path("/dirname1"), cb));
    EXPECT_EQ("", JoinStrings(entries, ","));

    entries.clear();
    EXPECT_OK(fs->iterate_dir(S3Path("/dirname2"), cb));
    EXPECT_EQ("0.dat,1.dat,subdir0", JoinStrings(entries, ","));

    entries.clear();
    EXPECT_OK(fs->iterate_dir(S3Path("/dirname2/subdir0"), cb));
    EXPECT_EQ("", JoinStrings(entries, ","));

    EXPECT_ERROR(fs->delete_dir(S3Path("/dirname2"))); // dirname2 is not empty

    EXPECT_OK(fs->delete_dir(S3Path("/dirname0")));
    EXPECT_OK(fs->delete_dir(S3Path("/dirname1")));
    EXPECT_OK(fs->delete_file(S3Path("/dirname2/0.dat")));
    EXPECT_OK(fs->delete_file(S3Path("/dirname2/1.dat")));
    EXPECT_OK(fs->delete_dir(S3Path("/dirname2/subdir0")));
    EXPECT_ERROR(fs->delete_dir(S3Path("/dirname2"))); // "/dirname2/" is a non-exist object
    EXPECT_OK(fs->delete_file(S3Path("/file0")));

    config::s3_use_list_objects_v1 = s3_use_list_objects_v1;
}

TEST_F(S3FileSystemTest, test_delete_dir_recursive) {
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateUniqueFromString("s3://"));

    std::vector<std::string> entries;
    auto cb = [&](std::string_view name) -> bool {
        entries.emplace_back(name);
        return true;
    };

    bool created;
    EXPECT_OK(fs->create_dir_if_missing(S3Path("/dirname0"), &created));
    ASSERT_OK(fs->delete_dir_recursive(S3Path("/dirname0")));
    EXPECT_TRUE(fs->iterate_dir(S3Path("/"), cb).is_not_found());
    ASSERT_EQ(0, entries.size());

    EXPECT_OK(fs->create_dir_if_missing(S3Path("/dirname0"), &created));
    EXPECT_OK(fs->create_dir_if_missing(S3Path("/dirname0/a"), &created));
    EXPECT_OK(fs->create_dir_if_missing(S3Path("/dirname0/b"), &created));
    EXPECT_OK(fs->create_dir_if_missing(S3Path("/dirname0/a/a"), &created));
    EXPECT_OK(fs->create_dir_if_missing(S3Path("/dirname0/a/b"), &created));
    EXPECT_OK(fs->create_dir_if_missing(S3Path("/dirname0/a/c"), &created));
    {
        ASSIGN_OR_ABORT(auto of, fs->new_writable_file(S3Path("/dirname0/1.dat")));
        EXPECT_OK(of->append("hello"));
        EXPECT_OK(of->close());
    }
    {
        ASSIGN_OR_ABORT(auto of, fs->new_writable_file(S3Path("/dirname0/a/1.dat")));
        EXPECT_OK(of->append("hello"));
        EXPECT_OK(of->close());
    }

    EXPECT_OK(fs->create_dir_if_missing(S3Path("/dirname0x"), &created));
    ASSERT_OK(fs->delete_dir_recursive(S3Path("/dirname0")));
    EXPECT_OK(fs->iterate_dir(S3Path("/"), cb));
    ASSERT_EQ(1, entries.size());
    ASSERT_EQ("dirname0x", entries[0]);
    ASSERT_OK(fs->delete_dir(S3Path("/dirname0x")));
    ASSERT_TRUE(fs->delete_dir_recursive(S3Path("/")).is_not_found());
}

TEST_F(S3FileSystemTest, test_delete_dir_recursive_v1) {
    bool s3_use_list_objects_v1 = config::s3_use_list_objects_v1;
    config::s3_use_list_objects_v1 = true;

    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateUniqueFromString("s3://"));

    std::vector<std::string> entries;
    auto cb = [&](std::string_view name) -> bool {
        entries.emplace_back(name);
        return true;
    };

    bool created;
    EXPECT_OK(fs->create_dir_if_missing(S3Path("/dirname0"), &created));
    ASSERT_OK(fs->delete_dir_recursive(S3Path("/dirname0")));
    EXPECT_TRUE(fs->iterate_dir(S3Path("/"), cb).is_not_found());
    ASSERT_EQ(0, entries.size());

    EXPECT_OK(fs->create_dir_if_missing(S3Path("/dirname0"), &created));
    EXPECT_OK(fs->create_dir_if_missing(S3Path("/dirname0/a"), &created));
    EXPECT_OK(fs->create_dir_if_missing(S3Path("/dirname0/b"), &created));
    EXPECT_OK(fs->create_dir_if_missing(S3Path("/dirname0/a/a"), &created));
    EXPECT_OK(fs->create_dir_if_missing(S3Path("/dirname0/a/b"), &created));
    EXPECT_OK(fs->create_dir_if_missing(S3Path("/dirname0/a/c"), &created));
    {
        ASSIGN_OR_ABORT(auto of, fs->new_writable_file(S3Path("/dirname0/1.dat")));
        EXPECT_OK(of->append("hello"));
        EXPECT_OK(of->close());
    }
    {
        ASSIGN_OR_ABORT(auto of, fs->new_writable_file(S3Path("/dirname0/a/1.dat")));
        EXPECT_OK(of->append("hello"));
        EXPECT_OK(of->close());
    }

    EXPECT_OK(fs->create_dir_if_missing(S3Path("/dirname0x"), &created));
    ASSERT_OK(fs->delete_dir_recursive(S3Path("/dirname0")));
    EXPECT_OK(fs->iterate_dir(S3Path("/"), cb));
    ASSERT_EQ(1, entries.size());
    ASSERT_EQ("dirname0x", entries[0]);
    ASSERT_OK(fs->delete_dir(S3Path("/dirname0x")));
    ASSERT_TRUE(fs->delete_dir_recursive(S3Path("/")).is_not_found());

    config::s3_use_list_objects_v1 = s3_use_list_objects_v1;
}

TEST_F(S3FileSystemTest, test_delete_nonexist_file) {
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateUniqueFromString("s3://"));
    ASSERT_OK(fs->delete_file(S3Path("/nonexist.dat")));
}

TEST_F(S3FileSystemTest, test_new_S3_client_with_rename_operation) {
    int default_value = config::object_storage_rename_file_request_timeout_ms;
    config::object_storage_rename_file_request_timeout_ms = 2000;
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateUniqueFromString("s3://"));
    // only used for generate a new S3 client into global cache
    (void)fs->rename_file(S3Path("/dir/source_name"), S3Path("/dir/target_name"));

    // basic config
    Aws::Client::ClientConfiguration config = S3ClientFactory::getClientConfig();
    S3URI src_uri;
    ASSERT_TRUE(src_uri.parse(S3Path("/dir/source_name")));
    if (!src_uri.endpoint().empty()) {
        config.endpointOverride = src_uri.endpoint();
    } else if (!config::object_storage_endpoint.empty()) {
        config.endpointOverride = config::object_storage_endpoint;
    } else if (config::object_storage_endpoint_use_https) {
        config.scheme = Aws::Http::Scheme::HTTPS;
    } else {
        config.scheme = Aws::Http::Scheme::HTTP;
    }
    if (!config::object_storage_region.empty()) {
        config.region = config::object_storage_region;
    }
    config.maxConnections = config::object_storage_max_connection;
    if (config::object_storage_connect_timeout_ms > 0) {
        config.connectTimeoutMs = config::object_storage_connect_timeout_ms;
    }

    // reset requestTimeoutMs as config::object_storage_rename_file_request_timeout_ms
    // to check hit the cache or not.
    config.requestTimeoutMs = config::object_storage_rename_file_request_timeout_ms;
    ASSERT_TRUE(S3ClientFactory::instance().find_client_cache_keys_by_config_TEST(config));

    // use config::object_storage_request_timeout_ms instead
    int old_object_storage_rename_file_request_timeout_ms = config::object_storage_rename_file_request_timeout_ms;
    int old_object_storage_request_timeout_ms = config::object_storage_request_timeout_ms;
    config::object_storage_rename_file_request_timeout_ms = -1;
    config::object_storage_request_timeout_ms = 1000;
    // only used for generate a new S3 client into global cache
    (void)fs->rename_file(S3Path("/dir/source_name"), S3Path("/dir/target_name"));
    config.requestTimeoutMs = config::object_storage_request_timeout_ms;
    ASSERT_TRUE(S3ClientFactory::instance().find_client_cache_keys_by_config_TEST(config));
    config::object_storage_rename_file_request_timeout_ms = old_object_storage_rename_file_request_timeout_ms;
    config::object_storage_request_timeout_ms = old_object_storage_request_timeout_ms;

    std::map<std::string, std::string> test_properties;
    test_properties[AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR] = "true";
    TCloudConfiguration tCloudConfiguration;
    tCloudConfiguration.__set_cloud_type(TCloudType::AWS);
    tCloudConfiguration.__set_cloud_properties(test_properties);
    auto cloud_config = CloudConfigurationFactory::create_aws(tCloudConfiguration);

    config.requestTimeoutMs = config::object_storage_rename_file_request_timeout_ms;
    (void)S3ClientFactory::instance().new_client(tCloudConfiguration, S3ClientFactory::OperationType::RENAME_FILE);
    ASSERT_TRUE(S3ClientFactory::instance().find_client_cache_keys_by_config_TEST(config, &cloud_config));

    old_object_storage_rename_file_request_timeout_ms = config::object_storage_rename_file_request_timeout_ms;
    old_object_storage_request_timeout_ms = config::object_storage_request_timeout_ms;
    config::object_storage_rename_file_request_timeout_ms = -1;
    config::object_storage_request_timeout_ms = 1000;
    (void)S3ClientFactory::instance().new_client(tCloudConfiguration, S3ClientFactory::OperationType::RENAME_FILE);
    config.requestTimeoutMs = config::object_storage_request_timeout_ms;
    ASSERT_TRUE(S3ClientFactory::instance().find_client_cache_keys_by_config_TEST(config, &cloud_config));
    config::object_storage_rename_file_request_timeout_ms = default_value;
    config::object_storage_request_timeout_ms = old_object_storage_request_timeout_ms;
}

} // namespace starrocks
