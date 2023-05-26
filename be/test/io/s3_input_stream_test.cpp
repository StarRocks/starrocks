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

#include "io/s3_input_stream.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/BucketLocationConstraint.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <gtest/gtest.h>

#include "common/config.h"
#include "common/logging.h"
#include "testutil/assert.h"

namespace starrocks::io {

static const char* kBucketName = "s3randomaccessfiletest";
static const char* kObjectName = "test.txt";
static std::shared_ptr<Aws::S3::S3Client> g_s3client;

static void init_s3client();
static void init_bucket();
static void destroy_bucket();
static void destroy_s3client();

class S3InputStreamTest : public testing::Test {
public:
    S3InputStreamTest() = default;

    ~S3InputStreamTest() override = default;

    static void SetUpTestCase();

    static void TearDownTestCase();

    static void put_object(const std::string& object_content);

    void SetUp() override {}

    void TearDown() override {}

    std::unique_ptr<S3InputStream> new_random_access_file();
};

void S3InputStreamTest::SetUpTestCase() {
    Aws::InitAPI(Aws::SDKOptions());
    init_s3client();
    init_bucket();
    put_object("0123456789");
}

void S3InputStreamTest::TearDownTestCase() {
    destroy_bucket();
    destroy_s3client();
    Aws::ShutdownAPI(Aws::SDKOptions());
}

void init_s3client() {
    Aws::Client::ClientConfiguration config;
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
    // delete object first
    {
        Aws::S3::Model::DeleteObjectRequest request;
        request.SetBucket(kBucketName);
        request.SetKey(kObjectName);
        Aws::S3::Model::DeleteObjectOutcome outcome = g_s3client->DeleteObject(request);
        if (!outcome.IsSuccess()) {
            std::cerr << "Fail to delete s3://" << kBucketName << "/" << kObjectName << ": "
                      << outcome.GetError().GetMessage() << '\n';
        } else {
            std::cout << "Deleted object s3://" << kBucketName << "/" << kObjectName << '\n';
        }
    }
    // delete bucket
    {
        Aws::S3::Model::DeleteBucketRequest request;
        request.SetBucket(kBucketName);
        Aws::S3::Model::DeleteBucketOutcome outcome = g_s3client->DeleteBucket(request);
        if (outcome.IsSuccess()) {
            std::cout << "Deleted bucket " << kBucketName << '\n';
        } else {
            std::cerr << "Fail to delete bucket " << kBucketName << ": " << outcome.GetError().GetMessage() << '\n';
        }
    }
}

void destroy_s3client() {
    g_s3client.reset();
}

std::unique_ptr<S3InputStream> S3InputStreamTest::new_random_access_file() {
    return std::make_unique<S3InputStream>(g_s3client, kBucketName, kObjectName);
}

void S3InputStreamTest::put_object(const std::string& object_content) {
    std::shared_ptr<Aws::IOStream> stream = Aws::MakeShared<Aws::StringStream>("", object_content);

    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(kBucketName);
    request.SetKey(kObjectName);
    request.SetBody(stream);

    Aws::S3::Model::PutObjectOutcome outcome = g_s3client->PutObject(request);
    CHECK(outcome.IsSuccess()) << outcome.GetError().GetMessage();
}

TEST_F(S3InputStreamTest, test_read) {
    auto f = new_random_access_file();
    char buf[6];
    ASSIGN_OR_ABORT(auto r, f->read(buf, sizeof(buf)));
    ASSERT_EQ("012345", std::string_view(buf, r));
    ASSERT_EQ(6, *f->position());

    ASSIGN_OR_ABORT(r, f->read(buf, sizeof(buf)));
    ASSERT_EQ("6789", std::string_view(buf, r));
    ASSERT_EQ(10, *f->position());

    ASSIGN_OR_ABORT(r, f->read(buf, sizeof(buf)));
    ASSERT_EQ(0, r);
    ASSERT_EQ(10, *f->position());
}

TEST_F(S3InputStreamTest, test_skip) {
    auto f = new_random_access_file();
    char buf[6];
    ASSERT_OK(f->skip(2));
    ASSIGN_OR_ABORT(auto r, f->read(buf, sizeof(buf)));
    ASSERT_EQ("234567", std::string_view(buf, r));
    ASSERT_OK(f->skip(3));
    ASSIGN_OR_ABORT(r, f->read(buf, sizeof(buf)));
    ASSERT_EQ(0, r);
}

TEST_F(S3InputStreamTest, test_seek) {
    auto f = new_random_access_file();
    char buf[6];
    ASSERT_OK(f->seek(2));
    ASSERT_EQ(2, *f->position());

    ASSIGN_OR_ABORT(auto r, f->read(buf, sizeof(buf)));
    ASSERT_EQ("234567", std::string_view(buf, r));

    ASSIGN_OR_ABORT(r, f->read(buf, sizeof(buf)));
    ASSERT_EQ("89", std::string_view(buf, r));
    ASSIGN_OR_ABORT(r, f->read(buf, sizeof(buf)));
    ASSERT_EQ(0, r);

    ASSERT_OK(f->seek(5));
    ASSERT_EQ(5, *f->position());
    ASSIGN_OR_ABORT(r, f->read(buf, sizeof(buf)));
    ASSERT_EQ("56789", std::string_view(buf, r));

    ASSERT_OK(f->seek(100));
    ASSIGN_OR_ABORT(r, f->read(buf, sizeof(buf)));
    ASSERT_EQ("", std::string_view(buf, r));
}

TEST_F(S3InputStreamTest, test_read_at) {
    auto f = new_random_access_file();
    char buf[6];
    ASSIGN_OR_ABORT(auto r, f->read_at(3, buf, sizeof(buf)));
    ASSERT_EQ("345678", std::string_view(buf, r));
    ASSERT_EQ(9, *f->position());

    ASSIGN_OR_ABORT(r, f->read(buf, sizeof(buf)));
    ASSERT_EQ("9", std::string_view(buf, r));
    ASSERT_EQ(10, *f->position());

    ASSIGN_OR_ABORT(r, f->read_at(7, buf, sizeof(buf)));
    ASSERT_EQ("789", std::string_view(buf, r));
    ASSERT_EQ(10, *f->position());

    ASSIGN_OR_ABORT(r, f->read_at(11, buf, sizeof(buf)));
    ASSERT_EQ("", std::string_view(buf, r));
    ASSERT_EQ(11, *f->position());

    ASSERT_FALSE(f->read_at(-1, buf, sizeof(buf)).ok());
}

} // namespace starrocks::io
