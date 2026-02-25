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

#include "io/s3_output_stream.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/BucketLocationConstraint.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "common/config.h"
#include "common/logging.h"
#include "fs/fs_s3.h"
#include "io/direct_s3_output_stream.h"
#include "io/s3_input_stream.h"

namespace starrocks::io {

static const char* kBucketName = "s3-outputstream-test";
static std::shared_ptr<Aws::S3::S3Client> g_s3client;

static void init_s3client();
static void init_bucket();
static void destroy_bucket();
static void destroy_s3client();

class S3OutputStreamTest : public testing::Test {
public:
    S3OutputStreamTest() = default;

    ~S3OutputStreamTest() override = default;

    static void SetUpTestCase();

    static void TearDownTestCase();

    void SetUp() override {}

    void TearDown() override {}
};

void S3OutputStreamTest::SetUpTestCase() {
    Aws::InitAPI(Aws::SDKOptions());
    init_s3client();
    init_bucket();
}

void S3OutputStreamTest::TearDownTestCase() {
    destroy_bucket();
    destroy_s3client();
    Aws::ShutdownAPI(Aws::SDKOptions());
}

void init_s3client() {
    Aws::Client::ClientConfiguration config = S3ClientFactory::getClientConfig();
    config.endpointOverride = config::object_storage_endpoint;
    const char* ak = config::object_storage_access_key_id.c_str();
    const char* sk = config::object_storage_secret_access_key.c_str();
    auto credentials = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(ak, sk);
    g_s3client = std::make_shared<Aws::S3::S3Client>(std::move(credentials), std::move(config),
                                                     Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);
}

void init_bucket() {
    Aws::S3::Model::CreateBucketRequest request;
    request.SetBucket(kBucketName);
    Aws::S3::Model::CreateBucketOutcome outcome = g_s3client->CreateBucket(request);
    LOG_IF(ERROR, !outcome.IsSuccess()) << outcome.GetError().GetMessage();
}

void destroy_bucket() {
    Aws::S3::Model::DeleteBucketRequest request;
    request.SetBucket(kBucketName);
    Aws::S3::Model::DeleteBucketOutcome outcome = g_s3client->DeleteBucket(request);
    if (outcome.IsSuccess()) {
        std::cout << "Deleted bucket " << kBucketName << '\n';
    } else {
        std::cerr << "Fail to delete bucket " << kBucketName << ": " << outcome.GetError().GetMessage() << '\n';
    }
}

void destroy_s3client() {
    g_s3client.reset();
}

void delete_object(const std::string& object) {
    Aws::S3::Model::DeleteObjectRequest request;
    request.SetBucket(kBucketName);
    request.SetKey(object);
    Aws::S3::Model::DeleteObjectOutcome outcome = g_s3client->DeleteObject(request);
    if (!outcome.IsSuccess()) {
        std::cerr << "Fail to delete s3://" << kBucketName << "/" << object << ": " << outcome.GetError().GetMessage()
                  << '\n';
    } else {
        std::cout << "Deleted s3://" << kBucketName << "/" << object << '\n';
    }
}

std::string get_object_content_type(const std::string& object) {
    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(kBucketName);
    request.SetKey(object);
    Aws::S3::Model::HeadObjectOutcome outcome = g_s3client->HeadObject(request);
    if (outcome.IsSuccess()) {
        return outcome.GetResult().GetContentType();
    }
    return "";
}

TEST_F(S3OutputStreamTest, test_singlepart_upload) {
    char buff[128];
    const char* kObjectName = "test_singlepart_upload";
    delete_object(kObjectName);
    S3OutputStream os(g_s3client, kBucketName, kObjectName, 1024, 1024);
    S3InputStream is(g_s3client, kBucketName, kObjectName);

    std::string s1("first line of singlepart upload\n");
    std::string s2("second line of singlepart upload\n");
    ASSERT_OK(os.write(s1.data(), s1.size()));
    ASSERT_OK(os.write(s2.data(), s2.size()));

    ASSERT_FALSE(is.read(buff, sizeof(buff)).ok());

    ASSERT_OK(os.close());

    ASSIGN_OR_ABORT(auto length, is.read(buff, sizeof(buff)));
    ASSERT_EQ("first line of singlepart upload\nsecond line of singlepart upload\n", std::string_view(buff, length));

    delete_object(kObjectName);
}

TEST_F(S3OutputStreamTest, test_multipart_upload) {
    char buff[128];
    const char* kObjectName = "test_multipart_upload";
    delete_object(kObjectName);
    S3OutputStream os(g_s3client, kBucketName, kObjectName, 12, /*5MB=*/5 * 1024 * 1024);
    S3InputStream is(g_s3client, kBucketName, kObjectName, /*5MB=*/5 * 1024 * 1024);

    std::string s1("first line of multipart upload\n");
    std::string s2("second line of multipart upload\n");
    ASSERT_OK(os.write(s1.data(), s1.size()));
    ASSERT_OK(os.write(s2.data(), s2.size()));

    ASSERT_FALSE(is.read(buff, sizeof(buff)).ok());

    ASSERT_OK(os.close());

    ASSIGN_OR_ABORT(auto length, is.read(buff, sizeof(buff)));
    ASSERT_EQ("first line of multipart upload\nsecond line of multipart upload\n", std::string_view(buff, length));

    delete_object(kObjectName);
}

TEST_F(S3OutputStreamTest, test_skip) {
    char buff[32];
    const char* kObjectName = "test_multipart_upload";
    delete_object(kObjectName);
    S3OutputStream os(g_s3client, kBucketName, kObjectName, 1024, 1024);
    S3InputStream is(g_s3client, kBucketName, kObjectName);

    ASSERT_OK(os.write("abc", 3));
    ASSERT_OK(os.skip(2));
    ASSERT_OK(os.write("xyz", 3));
    ASSERT_OK(os.close());

    ASSIGN_OR_ABORT(auto length, is.read(buff, sizeof(buff)));
    ASSERT_EQ(std::string_view("abc\x00\x00xyz", 8), std::string_view(buff, length));

    delete_object(kObjectName);
}

TEST_F(S3OutputStreamTest, test_get_direct_buffer_and_advance) {
    char buff[32];
    const char* kObjectName = "test_get_direct_buffer_and_advance";
    delete_object(kObjectName);
    S3OutputStream os(g_s3client, kBucketName, kObjectName, 1024, 1024);
    S3InputStream is(g_s3client, kBucketName, kObjectName);

    ASSERT_OK(os.write("abc", 3));
    ASSIGN_OR_ABORT(uint8_t * buf, os.get_direct_buffer_and_advance(3));
    memcpy(buf, "xyz", 3);
    ASSERT_OK(os.close());

    ASSIGN_OR_ABORT(auto length, is.read(buff, sizeof(buff)));
    ASSERT_EQ("abcxyz", std::string_view(buff, length));

    delete_object(kObjectName);
}

TEST_F(S3OutputStreamTest, test_singlepart_upload_with_content_type) {
    const char* kObjectName = "test_singlepart_upload_with_content_type";
    delete_object(kObjectName);

    // Test with custom content type for CSV
    S3OutputStream os(g_s3client, kBucketName, kObjectName, 1024, 1024, "text/csv");
    std::string content("col1,col2\nval1,val2\n");
    ASSERT_OK(os.write(content.data(), content.size()));
    ASSERT_OK(os.close());

    // Verify the content type was set correctly
    std::string actual_content_type = get_object_content_type(kObjectName);
    ASSERT_EQ("text/csv", actual_content_type);

    delete_object(kObjectName);
}

TEST_F(S3OutputStreamTest, test_singlepart_upload_with_parquet_content_type) {
    const char* kObjectName = "test_singlepart_upload_with_parquet_content_type";
    delete_object(kObjectName);

    // Test with custom content type for Parquet
    S3OutputStream os(g_s3client, kBucketName, kObjectName, 1024, 1024, "application/parquet");
    std::string content("dummy parquet content");
    ASSERT_OK(os.write(content.data(), content.size()));
    ASSERT_OK(os.close());

    // Verify the content type was set correctly
    std::string actual_content_type = get_object_content_type(kObjectName);
    ASSERT_EQ("application/parquet", actual_content_type);

    delete_object(kObjectName);
}

TEST_F(S3OutputStreamTest, test_singlepart_upload_with_default_content_type) {
    const char* kObjectName = "test_singlepart_upload_with_default_content_type";
    delete_object(kObjectName);

    // Test with default content type (application/octet-stream)
    S3OutputStream os(g_s3client, kBucketName, kObjectName, 1024, 1024);
    std::string content("binary content");
    ASSERT_OK(os.write(content.data(), content.size()));
    ASSERT_OK(os.close());

    // Verify the default content type was set
    std::string actual_content_type = get_object_content_type(kObjectName);
    ASSERT_EQ("application/octet-stream", actual_content_type);

    delete_object(kObjectName);
}

TEST_F(S3OutputStreamTest, test_multipart_upload_with_content_type) {
    const char* kObjectName = "test_multipart_upload_with_content_type";
    delete_object(kObjectName);

    // Use small max_single_part_size to trigger multipart upload
    S3OutputStream os(g_s3client, kBucketName, kObjectName, 12, /*5MB=*/5 * 1024 * 1024, "text/csv");

    std::string s1("first line of multipart upload\n");
    std::string s2("second line of multipart upload\n");
    ASSERT_OK(os.write(s1.data(), s1.size()));
    ASSERT_OK(os.write(s2.data(), s2.size()));
    ASSERT_OK(os.close());

    // Verify the content type was set correctly for multipart upload
    std::string actual_content_type = get_object_content_type(kObjectName);
    ASSERT_EQ("text/csv", actual_content_type);

    delete_object(kObjectName);
}

TEST_F(S3OutputStreamTest, test_multipart_upload_with_orc_content_type) {
    const char* kObjectName = "test_multipart_upload_with_orc_content_type";
    delete_object(kObjectName);

    // Use small max_single_part_size to trigger multipart upload
    S3OutputStream os(g_s3client, kBucketName, kObjectName, 12, /*5MB=*/5 * 1024 * 1024, "application/x-orc");

    std::string s1("first line of multipart upload\n");
    std::string s2("second line of multipart upload\n");
    ASSERT_OK(os.write(s1.data(), s1.size()));
    ASSERT_OK(os.write(s2.data(), s2.size()));
    ASSERT_OK(os.close());

    // Verify the content type was set correctly for multipart upload
    std::string actual_content_type = get_object_content_type(kObjectName);
    ASSERT_EQ("application/x-orc", actual_content_type);

    delete_object(kObjectName);
}

// Tests for DirectS3OutputStream
TEST_F(S3OutputStreamTest, test_direct_s3_output_stream_with_content_type) {
    const char* kObjectName = "test_direct_s3_output_stream_with_content_type";
    delete_object(kObjectName);

    // DirectS3OutputStream always uses multipart upload
    DirectS3OutputStream os(g_s3client, kBucketName, kObjectName, "text/csv");

    // Write enough data to complete the upload (minimum 5MB for each part in real S3)
    std::string content("col1,col2\nval1,val2\n");
    ASSERT_OK(os.write(content.data(), content.size()));
    ASSERT_OK(os.close());

    // Verify the content type was set correctly
    std::string actual_content_type = get_object_content_type(kObjectName);
    ASSERT_EQ("text/csv", actual_content_type);

    delete_object(kObjectName);
}

TEST_F(S3OutputStreamTest, test_direct_s3_output_stream_with_parquet_content_type) {
    const char* kObjectName = "test_direct_s3_output_stream_with_parquet_content_type";
    delete_object(kObjectName);

    DirectS3OutputStream os(g_s3client, kBucketName, kObjectName, "application/parquet");

    std::string content("dummy parquet content");
    ASSERT_OK(os.write(content.data(), content.size()));
    ASSERT_OK(os.close());

    // Verify the content type was set correctly
    std::string actual_content_type = get_object_content_type(kObjectName);
    ASSERT_EQ("application/parquet", actual_content_type);

    delete_object(kObjectName);
}

TEST_F(S3OutputStreamTest, test_direct_s3_output_stream_with_default_content_type) {
    const char* kObjectName = "test_direct_s3_output_stream_with_default_content_type";
    delete_object(kObjectName);

    // Test with default content type
    DirectS3OutputStream os(g_s3client, kBucketName, kObjectName);

    std::string content("binary content");
    ASSERT_OK(os.write(content.data(), content.size()));
    ASSERT_OK(os.close());

    // Verify the default content type was set
    std::string actual_content_type = get_object_content_type(kObjectName);
    ASSERT_EQ("application/octet-stream", actual_content_type);

    delete_object(kObjectName);
}

} // namespace starrocks::io
