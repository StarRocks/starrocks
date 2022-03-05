// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#ifdef STARROCKS_WITH_AWS

#include "io/s3_seekable_input_stream.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/BucketLocationConstraint.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <gtest/gtest.h>

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

class S3RandomAccessFileTest : public testing::Test {
public:
    S3RandomAccessFileTest() {}

    ~S3RandomAccessFileTest() override = default;

    static void SetUpTestCase();

    static void TearDownTestCase();

    static void put_object(const std::string& object_content);

    void SetUp() override {}

    void TearDown() override {}

    std::unique_ptr<S3SeekableInputStream> new_random_access_file();
};

void S3RandomAccessFileTest::SetUpTestCase() {
    Aws::InitAPI(Aws::SDKOptions());
    init_s3client();
    init_bucket();
    put_object("0123456789");
}

void S3RandomAccessFileTest::TearDownTestCase() {
    destroy_bucket();
    destroy_s3client();
    Aws::ShutdownAPI(Aws::SDKOptions());
}

void init_s3client() {
    Aws::Client::ClientConfiguration config;
    config.region = Aws::Region::AP_SOUTHEAST_1;
    const char* ak = getenv("AWS_ACCESS_KEY_ID");
    const char* sk = getenv("AWS_SECRET_ACCESS_KEY");
    CHECK(ak != nullptr) << "Env AWS_ACCESS_KEY_ID not set";
    CHECK(sk != nullptr) << "Env AWS_SECRET_ACCESS_KEY not set";
    auto credentials = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(ak, sk);
    g_s3client = std::make_shared<Aws::S3::S3Client>(std::move(credentials), std::move(config));
}

void init_bucket() {
    Aws::Client::ClientConfiguration config;
    config.region = Aws::Region::AP_SOUTHEAST_1;
    auto constraint = Aws::S3::Model::BucketLocationConstraintMapper::GetBucketLocationConstraintForName(
            Aws::Region::AP_SOUTHEAST_1);
    Aws::S3::Model::CreateBucketConfiguration bucket_config;
    bucket_config.SetLocationConstraint(constraint);

    Aws::S3::Model::CreateBucketRequest request;
    request.SetCreateBucketConfiguration(bucket_config);
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

std::unique_ptr<S3SeekableInputStream> S3RandomAccessFileTest::new_random_access_file() {
    return std::make_unique<S3SeekableInputStream>(g_s3client, kBucketName, kObjectName);
}

void S3RandomAccessFileTest::put_object(const std::string& object_content) {
    std::shared_ptr<Aws::IOStream> stream = Aws::MakeShared<Aws::StringStream>("", object_content);

    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(kBucketName);
    request.SetKey(kObjectName);
    request.SetBody(stream);

    Aws::S3::Model::PutObjectOutcome outcome = g_s3client->PutObject(request);
    CHECK(outcome.IsSuccess()) << outcome.GetError().GetMessage();
}

TEST_F(S3RandomAccessFileTest, test_read) {
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

TEST_F(S3RandomAccessFileTest, test_skip) {
    auto f = new_random_access_file();
    char buf[6];
    ASSERT_OK(f->skip(2));
    ASSIGN_OR_ABORT(auto r, f->read(buf, sizeof(buf)));
    ASSERT_EQ("234567", std::string_view(buf, r));
    ASSERT_OK(f->skip(3));
    ASSIGN_OR_ABORT(r, f->read(buf, sizeof(buf)));
    ASSERT_EQ(0, r);
}

TEST_F(S3RandomAccessFileTest, test_seek) {
    auto f = new_random_access_file();
    char buf[6];
    ASSIGN_OR_ABORT(auto p, f->seek(2, SEEK_CUR));
    ASSERT_EQ(2, p);

    ASSIGN_OR_ABORT(auto r, f->read(buf, sizeof(buf)));
    ASSERT_EQ("234567", std::string_view(buf, r));

    ASSIGN_OR_ABORT(r, f->read(buf, sizeof(buf)));
    ASSERT_EQ("89", std::string_view(buf, r));
    ASSIGN_OR_ABORT(r, f->read(buf, sizeof(buf)));
    ASSERT_EQ(0, r);

    ASSIGN_OR_ABORT(p, f->seek(5, SEEK_SET));
    ASSERT_EQ(5, p);
    ASSIGN_OR_ABORT(r, f->read(buf, sizeof(buf)));
    ASSERT_EQ("56789", std::string_view(buf, r));

    ASSIGN_OR_ABORT(p, f->seek(-7, SEEK_END));
    ASSERT_EQ(3, p);
    ASSIGN_OR_ABORT(r, f->read(buf, sizeof(buf)));
    ASSERT_EQ("345678", std::string_view(buf, r));

    ASSIGN_OR_ABORT(p, f->seek(0, SEEK_END));
    ASSERT_EQ(10, p);
    ASSIGN_OR_ABORT(r, f->read(buf, sizeof(buf)));
    ASSERT_EQ("", std::string_view(buf, r));

    ASSIGN_OR_ABORT(p, f->seek(1, SEEK_END));
    ASSERT_EQ(11, p);
    ASSIGN_OR_ABORT(r, f->read(buf, sizeof(buf)));
    ASSERT_EQ("", std::string_view(buf, r));
}

TEST_F(S3RandomAccessFileTest, test_read_at) {
    auto f = new_random_access_file();
    char buf[6];
    ASSIGN_OR_ABORT(auto r, f->read_at(3, buf, sizeof(buf)));
    ASSERT_EQ("345678", std::string_view(buf, r));
    ASSIGN_OR_ABORT(r, f->read(buf, sizeof(buf)));
    ASSERT_EQ("012345", std::string_view(buf, r));

    ASSIGN_OR_ABORT(r, f->read_at(7, buf, sizeof(buf)));
    ASSERT_EQ("789", std::string_view(buf, r));

    ASSIGN_OR_ABORT(r, f->read_at(11, buf, sizeof(buf)));
    ASSERT_EQ("", std::string_view(buf, r));

    ASSERT_FALSE(f->read_at(-1, buf, sizeof(buf)).ok());
}

} // namespace starrocks::io

#endif
