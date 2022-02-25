// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#ifdef STARROCKS_WITH_AWS

#include "object_store/s3_object_store.h"

#include <aws/core/Aws.h>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include <fstream>

#include "testutil/assert.h"
#include "util/file_utils.h"

namespace starrocks {

static constexpr const char* kBucketName = "starrocks-cloud-test-2022";

class S3ObjectStoreTest : public testing::Test {
public:
    S3ObjectStoreTest() {}
    virtual ~S3ObjectStoreTest() {}
    void SetUp() override { Aws::InitAPI(_options); }
    void TearDown() override { Aws::ShutdownAPI(_options); }

private:
    Aws::SDKOptions _options;
};

TEST_F(S3ObjectStoreTest, bucket_operation) {
    ASSIGN_OR_ABORT(auto client, ObjectStore::create_unique_from_uri(fmt::format("s3://{}", kBucketName)));

    (void)client->delete_bucket(kBucketName);

    // create bucket
    ASSERT_OK(client->create_bucket(kBucketName));

    // delete bucket
    ASSERT_OK(client->delete_bucket(kBucketName));
}

TEST_F(S3ObjectStoreTest, object_operation) {
    ASSIGN_OR_ABORT(auto client, ObjectStore::create_unique_from_uri(fmt::format("s3://{}", kBucketName)));

    // create bucket
    ASSERT_OK(client->create_bucket(kBucketName));

    // put object
    const std::string uri = fmt::format("s3://{}/hello", kBucketName);
    const std::string object_value = "world";
    const std::string object_path = "./S3Test_hello";

    ASSIGN_OR_ABORT(auto os, client->put_object(uri));
    ASSERT_OK(os->write(object_value.data(), object_value.size()));
    ASSERT_OK(os->close());

    // test object existence
    ASSERT_OK(client->exist_object(uri));

    // get object size
    ASSIGN_OR_ABORT(auto size, client->get_object_size(uri));
    ASSERT_EQ(size, object_value.size());

    // download object to local file
    ASSERT_FALSE(client->download_object(uri, "").ok());
    ASSERT_OK(client->download_object(uri, object_path));
    std::ifstream file(object_path.c_str(), std::ios::in);
    ASSERT_TRUE(file.good());
    std::string object_value_tmp;
    std::getline(file, object_value_tmp);
    ASSERT_EQ(object_value_tmp, object_value);
    FileUtils::remove(object_path);

    // read object
    char buf[2];
    ASSIGN_OR_ABORT(auto input_stream, client->get_object(uri));
    ASSIGN_OR_ABORT(auto bytes_read, input_stream->read_at(1, buf, sizeof(buf)));
    ASSERT_EQ("or", std::string_view(buf, bytes_read));

    // list object
    const std::string uri2 = fmt::format("s3://{}/test/hello", kBucketName);
    ASSIGN_OR_ABORT(auto os2, client->put_object(uri2));
    ASSERT_OK(os2->write(object_value.data(), object_value.size()));
    ASSERT_OK(os2->close());
    std::vector<std::string> vector;
    ASSERT_OK(client->list_objects(kBucketName, "/" /* object_prefix */, &vector));
    ASSERT_EQ(vector.size(), 2);
    ASSERT_EQ(vector[0], "hello");
    ASSERT_EQ(vector[1], "test/hello");

    // delete object
    ASSERT_OK(client->delete_object(uri));
    ASSERT_OK(client->delete_object(uri2));

    // test object not exist
    ASSERT_TRUE(client->exist_object(uri).is_not_found());
    ASSERT_TRUE(client->exist_object(uri2).is_not_found());

    // delete bucket
    ASSERT_OK(client->delete_bucket(kBucketName));
}

} // namespace starrocks

#endif
