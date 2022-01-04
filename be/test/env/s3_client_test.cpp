// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "env/s3_client.h"

#include <aws/core/Aws.h>
#include <gtest/gtest.h>

#include <fstream>

#include "util/file_utils.h"

namespace starrocks {

static const std::string bucket_name = "starrocks-cloud-test-2022";

class S3Test : public testing::Test {
public:
    S3Test() {}
    virtual ~S3Test() {}
    void SetUp() override { Aws::InitAPI(_options); }
    void TearDown() override { Aws::ShutdownAPI(_options); }

private:
    Aws::SDKOptions _options;
};

TEST_F(S3Test, bucket_operation) {
    S3Client client;

    // 1. create bucket
    ASSERT_TRUE(client.create_bucket(bucket_name).ok());

    // 2. delete bucket
    ASSERT_TRUE(client.delete_bucket(bucket_name).ok());
}

TEST_F(S3Test, object_operation) {
    S3Client client;

    // create bucket
    ASSERT_TRUE(client.create_bucket(bucket_name).ok());

    // put object
    const std::string object_key = "hello";
    const std::string object_value = "world";
    const std::string object_path = "./S3Test_hello";
    ASSERT_TRUE(client.put_string_object(bucket_name, object_key, object_value).ok());

    // test object exist
    ASSERT_TRUE(client.exist_object(bucket_name, object_key).ok());

    // get object size
    size_t size = 0;
    ASSERT_TRUE(client.get_object_size(bucket_name, object_key, &size).ok());
    ASSERT_EQ(size, object_value.size());

    // get object
    ASSERT_FALSE(client.get_object(bucket_name, object_key, "").ok());
    ASSERT_TRUE(client.get_object(bucket_name, object_key, object_path).ok());
    std::ifstream file(object_path.c_str(), std::ios::in);
    ASSERT_TRUE(file.good());
    std::string object_value_tmp;
    std::getline(file, object_value_tmp);
    ASSERT_EQ(object_value_tmp, object_value);
    FileUtils::remove(object_path);

    // get object range
    std::string object_value_range;
    size_t read_bytes;
    ASSERT_TRUE(client.get_object_range(bucket_name, object_key, &object_value_range, 1 /* offset */, 2 /* length */,
                                        &read_bytes)
                        .ok());
    ASSERT_EQ(read_bytes, 2);
    ASSERT_EQ(object_value_range, "or");

    // list object
    const std::string object_key2 = "test/hello";
    ASSERT_TRUE(client.put_string_object(bucket_name, object_key2, object_value).ok());
    std::vector<std::string> vector;
    ASSERT_TRUE(client.list_objects(bucket_name, "/" /* object_prefix */, &vector).ok());
    ASSERT_EQ(vector.size(), 2);
    ASSERT_EQ(vector[0], "hello");
    ASSERT_EQ(vector[1], "test/hello");

    // delete object
    ASSERT_TRUE(client.delete_object(bucket_name, object_key).ok());
    ASSERT_TRUE(client.delete_object(bucket_name, object_key2).ok());

    // test object not exist
    ASSERT_TRUE(client.exist_object(bucket_name, object_key).is_not_found());
    ASSERT_TRUE(client.exist_object(bucket_name, object_key2).is_not_found());

    // delete bucket
    ASSERT_TRUE(client.delete_bucket(bucket_name).ok());
}

TEST_F(S3Test, transfer_manager) {
    // TODO
}

} // namespace starrocks
