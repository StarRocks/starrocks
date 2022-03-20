// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "env/env_s3.h"

#include <aws/core/Aws.h>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include <fstream>

#include "common/config.h"
#include "gutil/strings/join.h"
#include "testutil/assert.h"

namespace starrocks {

// NOTE: The bucket must be created before running this test.
constexpr static const char* kBucketName = "starrocks-env-s3-unit-test";

class EnvS3Test : public testing::Test {
public:
    EnvS3Test() = default;
    ~EnvS3Test() override = default;
    void SetUp() override { Aws::InitAPI(_options); }
    void TearDown() override { Aws::ShutdownAPI(_options); }

    std::string S3Path(std::string_view path) {
        return fmt::format("s3://{}.{}{}", kBucketName, config::object_storage_endpoint, path);
    }

    void CheckIsDirectory(Env* env, const std::string& dir_name, bool expected_success, bool expected_is_dir = true) {
        const StatusOr<bool> status_or = env->is_directory(dir_name);
        EXPECT_EQ(expected_success, status_or.ok());
        if (status_or.ok()) {
            EXPECT_EQ(expected_is_dir, status_or.value());
        }
    }

private:
    Aws::SDKOptions _options;
};

TEST_F(EnvS3Test, test_write_and_read) {
    auto uri = fmt::format("s3://{}.{}/dir/test-object.png", kBucketName, config::object_storage_endpoint);
    ASSIGN_OR_ABORT(auto env, Env::CreateUniqueFromString(uri));
    ASSIGN_OR_ABORT(auto wf, env->new_writable_file(uri));
    EXPECT_OK(wf->append("hello"));
    EXPECT_OK(wf->append(" world!"));
    EXPECT_OK(wf->sync());
    EXPECT_OK(wf->close());
    EXPECT_EQ(sizeof("hello world!"), wf->size() + 1);

    char buf[1024];
    ASSIGN_OR_ABORT(auto rf, env->new_random_access_file(uri));
    ASSIGN_OR_ABORT(auto nr, rf->read_at(0, buf, sizeof(buf)));
    EXPECT_EQ("hello world!", std::string_view(buf, nr));

    ASSIGN_OR_ABORT(nr, rf->read_at(3, buf, sizeof(buf)));
    EXPECT_EQ("lo world!", std::string_view(buf, nr));

    EXPECT_OK(env->delete_file(uri));
    ASSIGN_OR_ABORT(rf, env->new_random_access_file(uri));
    EXPECT_ERROR(rf->read_at(0, buf, sizeof(buf)));
}

TEST_F(EnvS3Test, test_directory) {
    ASSIGN_OR_ABORT(auto env, Env::CreateUniqueFromString("s3://"));
    bool created = false;

    ASSERT_TRUE(env->create_dir(S3Path("/")).is_already_exist());
    ASSERT_OK(env->create_dir_if_missing(S3Path("/"), &created));
    ASSERT_FALSE(created);
    CheckIsDirectory(env.get(), S3Path("/"), true, true);
    ASSERT_OK(env->iterate_dir(S3Path("/"), [&](std::string_view /*name*/) -> bool {
        CHECK(false) << "root directory should be empty";
        return true;
    }));
    ASSERT_ERROR(env->delete_dir(S3Path(("/"))));

    //
    //  /dirname0/
    //
    EXPECT_OK(env->create_dir(S3Path("/dirname0")));
    CheckIsDirectory(env.get(), S3Path("/dirname"), false);
    CheckIsDirectory(env.get(), S3Path("/dirname0"), true, true);
    EXPECT_TRUE(env->create_dir(S3Path("/dirname0")).is_already_exist());

    EXPECT_OK(env->create_dir_if_missing(S3Path("/dirname0"), &created));
    EXPECT_FALSE(created);
    CheckIsDirectory(env.get(), S3Path("/dirname0"), true, true);

    //
    //  /dirname0/
    //  /dirname1/
    //
    EXPECT_OK(env->create_dir_if_missing(S3Path("/dirname1"), &created));
    EXPECT_TRUE(created);
    CheckIsDirectory(env.get(), S3Path("/dirname1"), true, true);

    CheckIsDirectory(env.get(), S3Path("/noexistdir"), false);
    EXPECT_ERROR(env->new_writable_file(S3Path("/filename/")));

    //
    //  /dirname0/
    //  /dirname1/
    //  /file0
    //
    {
        ASSIGN_OR_ABORT(auto of, env->new_writable_file(S3Path("/file0")));
        EXPECT_OK(of->append("hello"));
        EXPECT_OK(of->close());
    }
    CheckIsDirectory(env.get(), S3Path("/file0"), true, false);

    //
    //  /dirname0/
    //  /dirname1/
    //  /dirname2/0.dat
    //  /file0
    //
    {
        // NOTE: Although directory "/dirname2" does not exist, we can still create file under "/dirname2" successfully
        ASSIGN_OR_ABORT(auto of, env->new_writable_file(S3Path("/dirname2/0.dat")));
        EXPECT_OK(of->append("hello"));
        EXPECT_OK(of->close());
        CheckIsDirectory(env.get(), S3Path("/dirname2/0.dat"), true, false);
        CheckIsDirectory(env.get(), S3Path("/dirname2/0"), false);
        CheckIsDirectory(env.get(), S3Path("/dirname2/0.da"), false);
    }
    CheckIsDirectory(env.get(), S3Path("/dirname2"), true, true);

    //
    //  /dirname0/
    //  /dirname1/
    //  /dirname2/0.dat
    //  /dirname2/1.dat
    //  /file0
    //
    {
        ASSIGN_OR_ABORT(auto of, env->new_writable_file(S3Path("/dirname2/1.dat")));
        EXPECT_OK(of->append("hello"));
        EXPECT_OK(of->close());
        CheckIsDirectory(env.get(), S3Path("/dirname2/1.dat"), true, false);
    }
    CheckIsDirectory(env.get(), S3Path("/dirname2"), true, true);

    //
    //  /dirname0/
    //  /dirname1/
    //  /dirname2/0.dat
    //  /dirname2/1.dat
    //  /dirname2/subdir0/
    //  /file0
    //
    EXPECT_OK(env->create_dir(S3Path("/dirname2/subdir0")));
    CheckIsDirectory(env.get(), S3Path("/dirname2/subdir0"), true, true);

    std::vector<std::string> entries;
    auto cb = [&](std::string_view name) -> bool {
        entries.emplace_back(name);
        return true;
    };

    EXPECT_ERROR(env->iterate_dir(S3Path("/nonexistdir"), cb));
    EXPECT_ERROR(env->delete_dir(S3Path("/nonexistdir")));

    entries.clear();
    EXPECT_OK(env->iterate_dir(S3Path("/"), cb));
    EXPECT_EQ("dirname0,dirname1,dirname2,file0", JoinStrings(entries, ","));

    entries.clear();
    EXPECT_OK(env->iterate_dir(S3Path("/dirname0"), cb));
    EXPECT_EQ("", JoinStrings(entries, ","));

    entries.clear();
    EXPECT_OK(env->iterate_dir(S3Path("/dirname1"), cb));
    EXPECT_EQ("", JoinStrings(entries, ","));

    entries.clear();
    EXPECT_OK(env->iterate_dir(S3Path("/dirname2"), cb));
    EXPECT_EQ("0.dat,1.dat,subdir0", JoinStrings(entries, ","));

    entries.clear();
    EXPECT_OK(env->iterate_dir(S3Path("/dirname2/subdir0"), cb));
    EXPECT_EQ("", JoinStrings(entries, ","));

    EXPECT_ERROR(env->delete_dir(S3Path("/dirname2"))); // dirname2 is not empty

    EXPECT_OK(env->delete_dir(S3Path("/dirname0")));
    EXPECT_OK(env->delete_dir(S3Path("/dirname1")));
    EXPECT_OK(env->delete_file(S3Path("/dirname2/0.dat")));
    EXPECT_OK(env->delete_file(S3Path("/dirname2/1.dat")));
    EXPECT_OK(env->delete_dir(S3Path("/dirname2/subdir0")));
    EXPECT_ERROR(env->delete_dir(S3Path("/dirname2"))); // "/dirname2/" is a non-exist object
    EXPECT_OK(env->delete_file(S3Path("/file0")));
}

TEST_F(EnvS3Test, test_delete_dir_recursive) {
    ASSIGN_OR_ABORT(auto env, Env::CreateUniqueFromString("s3://"));

    std::vector<std::string> entries;
    auto cb = [&](std::string_view name) -> bool {
        entries.emplace_back(name);
        return true;
    };

    bool created;
    EXPECT_OK(env->create_dir_if_missing(S3Path("/dirname0"), &created));
    ASSERT_OK(env->delete_dir_recursive(S3Path("/dirname0")));
    EXPECT_OK(env->iterate_dir(S3Path("/"), cb));
    ASSERT_EQ(0, entries.size());

    EXPECT_OK(env->create_dir_if_missing(S3Path("/dirname0"), &created));
    EXPECT_OK(env->create_dir_if_missing(S3Path("/dirname0/a"), &created));
    EXPECT_OK(env->create_dir_if_missing(S3Path("/dirname0/b"), &created));
    EXPECT_OK(env->create_dir_if_missing(S3Path("/dirname0/a/a"), &created));
    EXPECT_OK(env->create_dir_if_missing(S3Path("/dirname0/a/b"), &created));
    EXPECT_OK(env->create_dir_if_missing(S3Path("/dirname0/a/c"), &created));
    {
        ASSIGN_OR_ABORT(auto of, env->new_writable_file(S3Path("/dirname0/1.dat")));
        EXPECT_OK(of->append("hello"));
        EXPECT_OK(of->close());
    }
    {
        ASSIGN_OR_ABORT(auto of, env->new_writable_file(S3Path("/dirname0/a/1.dat")));
        EXPECT_OK(of->append("hello"));
        EXPECT_OK(of->close());
    }

    EXPECT_OK(env->create_dir_if_missing(S3Path("/dirname0x"), &created));
    ASSERT_OK(env->delete_dir_recursive(S3Path("/dirname0")));
    EXPECT_OK(env->iterate_dir(S3Path("/"), cb));
    ASSERT_EQ(1, entries.size());
    ASSERT_EQ("dirname0x", entries[0]);
    ASSERT_OK(env->delete_dir(S3Path("/dirname0x")));
    ASSERT_ERROR(env->delete_dir_recursive(S3Path("/")));
}

} // namespace starrocks
