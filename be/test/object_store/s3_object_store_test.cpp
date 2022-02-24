// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#ifdef STARROCKS_WITH_AWS

#include "object_store/s3_object_store.h"

#include <aws/core/Aws.h>
#include <gtest/gtest.h>

#include <fstream>

#include "testutil/assert.h"
#include "util/file_utils.h"

namespace starrocks {

static const std::string bucket_name = "starrocks-cloud-test-2022";

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
    Aws::Client::ClientConfiguration config;
    config.region = Aws::Region::AP_SOUTHEAST_1;

    S3ObjectStore client(config);
    ASSERT_OK(client.init(nullptr, false));

    (void)client.delete_bucket(bucket_name);

    // create bucket
    ASSERT_OK(client.create_bucket(bucket_name));

    // delete bucket
    ASSERT_OK(client.delete_bucket(bucket_name));
}

TEST_F(S3ObjectStoreTest, object_operation) {
    Aws::Client::ClientConfiguration config;
    config.region = Aws::Region::AP_SOUTHEAST_1;

    S3ObjectStore client(config);
    ASSERT_OK(client.init(nullptr, false));

    // create bucket
    ASSERT_OK(client.create_bucket(bucket_name));

    // put object
    const std::string object_key = "hello";
    const std::string object_value = "world";
    const std::string object_path = "./S3Test_hello";
    ASSERT_OK(client.put_string_object(bucket_name, object_key, object_value));

    // test object exist
    ASSERT_OK(client.exist_object(bucket_name, object_key));

    // get object size
    size_t size = 0;
    ASSERT_OK(client.get_object_size(bucket_name, object_key, &size));
    ASSERT_EQ(size, object_value.size());

    // get object
    ASSERT_FALSE(client.get_object(bucket_name, object_key, "").ok());
    ASSERT_OK(client.get_object(bucket_name, object_key, object_path));
    std::ifstream file(object_path.c_str(), std::ios::in);
    ASSERT_TRUE(file.good());
    std::string object_value_tmp;
    std::getline(file, object_value_tmp);
    ASSERT_EQ(object_value_tmp, object_value);
    FileUtils::remove(object_path);

    // get object range
    char buf[2];
    ASSIGN_OR_ABORT(auto input_stream, client.get_object(bucket_name, object_key));
    ASSIGN_OR_ABORT(auto bytes_read, input_stream->read_at(1, buf, sizeof(buf)));
    ASSERT_EQ("or", std::string_view(buf, bytes_read));

    // list object
    const std::string object_key2 = "test/hello";
    ASSERT_OK(client.put_string_object(bucket_name, object_key2, object_value));
    std::vector<std::string> vector;
    ASSERT_OK(client.list_objects(bucket_name, "/" /* object_prefix */, &vector));
    ASSERT_EQ(vector.size(), 2);
    ASSERT_EQ(vector[0], "hello");
    ASSERT_EQ(vector[1], "test/hello");

    // delete object
    ASSERT_OK(client.delete_object(bucket_name, object_key));
    ASSERT_OK(client.delete_object(bucket_name, object_key2));

    // test object not exist
    ASSERT_TRUE(client.exist_object(bucket_name, object_key).is_not_found());
    ASSERT_TRUE(client.exist_object(bucket_name, object_key2).is_not_found());

    // delete bucket
    ASSERT_OK(client.delete_bucket(bucket_name));
}

} // namespace starrocks

#endif
