// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#ifdef STARROCKS_WITH_AWS

#include "env/env_s3.h"

#include <aws/core/Aws.h>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include <fstream>

#include "common/config.h"
#include "testutil/assert.h"
#include "util/file_utils.h"

namespace starrocks {

// NOTE: The bucket must be created before running this test.
constexpr static const char* kBucketName = "starrocks-env-s3-unit-test";

class EnvS3Test : public testing::Test {
public:
    EnvS3Test() {}
    virtual ~EnvS3Test() {}
    void SetUp() override { Aws::InitAPI(_options); }
    void TearDown() override { Aws::ShutdownAPI(_options); }

private:
    Aws::SDKOptions _options;
};

TEST_F(EnvS3Test, write_and_read) {
    auto uri = fmt::format("http://{}.{}/dir/test-object.png", kBucketName, config::object_storage_endpoint);
    EnvS3 env;
    ASSIGN_OR_ABORT(auto wf, env.new_writable_file(uri));
    ASSERT_OK(wf->append("hello"));
    ASSERT_OK(wf->append(" world!"));
    ASSERT_OK(wf->sync());
    ASSERT_OK(wf->close());
    ASSERT_EQ(sizeof("hello world!"), wf->size() + 1);

    char buf[1024];
    Slice s(buf, 1024);
    ASSIGN_OR_ABORT(auto rf, env.new_random_access_file(uri));
    ASSERT_OK(rf->read(0, &s));
    ASSERT_EQ("hello world!", s);

    s.size = 5;
    ASSERT_OK(rf->read_at(3, s));
    ASSERT_EQ("lo wo", s);
}

} // namespace starrocks

#endif
