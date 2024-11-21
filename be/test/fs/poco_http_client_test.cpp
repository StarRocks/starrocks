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

#include "fs/s3/poco_http_client.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <gtest/gtest.h>

#include <memory>

#include "common/config.h"
#include "common/logging.h"
#include "fs/fs_s3.h"
#include "fs/s3/poco_http_client_factory.h"
#include "io/s3_input_stream.h"
#include "testutil/assert.h"

namespace starrocks::poco {

static const char* kObjectName = "starrocks_ut_poco_test.txt";
static const char* kObjectContent = "0123456789";

class PocoHttpClientTest : public testing::Test {
public:
    PocoHttpClientTest() = default;
    ~PocoHttpClientTest() override = default;

    static void SetUpTestCase();
    static void TearDownTestCase();

    static void put_object(const std::string& object_content);

protected:
    inline static const char* s_bucket_name = nullptr;
    inline static const char* ak = nullptr;
    inline static const char* sk = nullptr;
};

void PocoHttpClientTest::SetUpTestCase() {
    Aws::InitAPI(Aws::SDKOptions());
    Aws::Http::SetHttpClientFactory(std::make_shared<starrocks::poco::PocoHttpClientFactory>());

    s_bucket_name = config::object_storage_bucket.empty() ? getenv("STARROCKS_UT_S3_BUCKET")
                                                          : config::object_storage_bucket.c_str();
    if (s_bucket_name == nullptr) {
        FAIL() << "s3 bucket name not set";
    }

    ak = config::object_storage_access_key_id.empty() ? getenv("STARROCKS_UT_S3_AK")
                                                      : config::object_storage_access_key_id.c_str();
    sk = config::object_storage_secret_access_key.empty() ? getenv("STARROCKS_UT_S3_SK")
                                                          : config::object_storage_secret_access_key.c_str();
    if (ak == nullptr) {
        FAIL() << "s3 access key id not set";
    }
    if (sk == nullptr) {
        FAIL() << "s3 secret access key not set";
    }
    put_object(kObjectContent);
}

void PocoHttpClientTest::TearDownTestCase() {
    Aws::ShutdownAPI(Aws::SDKOptions());
}

void PocoHttpClientTest::put_object(const std::string& object_content) {
    Aws::Client::ClientConfiguration config = S3ClientFactory::getClientConfig();
    config.endpointOverride = config::object_storage_endpoint.empty() ? getenv("STARROCKS_UT_S3_ENDPOINT")
                                                                      : config::object_storage_endpoint;

    auto credentials = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(ak, sk);
    auto client = std::make_shared<Aws::S3::S3Client>(std::move(credentials), std::move(config),
                                                      Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, true);

    std::shared_ptr<Aws::IOStream> stream = Aws::MakeShared<Aws::StringStream>("", object_content);

    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(s_bucket_name);
    request.SetKey(kObjectName);
    request.SetBody(stream);

    Aws::S3::Model::PutObjectOutcome outcome = client->PutObject(request);
    CHECK(outcome.IsSuccess()) << outcome.GetError().GetMessage();
}

TEST_F(PocoHttpClientTest, TestNormalAccess) {
    Aws::Client::ClientConfiguration config = S3ClientFactory::getClientConfig();
    config.endpointOverride = config::object_storage_endpoint.empty() ? getenv("STARROCKS_UT_S3_ENDPOINT")
                                                                      : config::object_storage_endpoint;
    // Create a custom retry strategy
    int maxRetries = 2;
    long scaleFactor = 25;
    std::shared_ptr<Aws::Client::RetryStrategy> retryStrategy =
            std::make_shared<Aws::Client::DefaultRetryStrategy>(maxRetries, scaleFactor);

    // Create a client configuration object and set the custom retry strategy
    config.retryStrategy = retryStrategy;

    auto credentials = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(ak, sk);

    auto client = std::make_shared<Aws::S3::S3Client>(std::move(credentials), std::move(config),
                                                      Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, true);

    auto stream = std::make_unique<starrocks::io::S3InputStream>(client, s_bucket_name, kObjectName);
    char buf[6];
    ASSIGN_OR_ABORT(auto r, stream->read(buf, sizeof(buf)));
    ASSERT_EQ("012345", std::string_view(buf, r));
}

TEST_F(PocoHttpClientTest, TestErrorEndpoint) {
    Aws::Client::ClientConfiguration config = S3ClientFactory::getClientConfig();
    config.endpointOverride = "http://127.0.0.1";

    // Create a custom retry strategy
    int maxRetries = 2;
    long scaleFactor = 25;
    std::shared_ptr<Aws::Client::RetryStrategy> retryStrategy =
            std::make_shared<Aws::Client::DefaultRetryStrategy>(maxRetries, scaleFactor);

    // Create a client configuration object and set the custom retry strategy
    config.retryStrategy = retryStrategy;

    auto credentials = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(ak, sk);
    auto client = std::make_shared<Aws::S3::S3Client>(std::move(credentials), std::move(config),
                                                      Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, true);

    auto stream = std::make_unique<starrocks::io::S3InputStream>(client, s_bucket_name, kObjectName);
    char buf[6];
    auto r = stream->read(buf, sizeof(buf));
    EXPECT_TRUE(r.status().message().find("Poco::Exception") != std::string::npos);
}

TEST_F(PocoHttpClientTest, TestErrorAkSk) {
    Aws::Client::ClientConfiguration config = S3ClientFactory::getClientConfig();
    config.endpointOverride = config::object_storage_endpoint.empty() ? getenv("STARROCKS_UT_S3_ENDPOINT")
                                                                      : config::object_storage_endpoint;

    // Create a custom retry strategy
    int maxRetries = 2;
    long scaleFactor = 25;
    std::shared_ptr<Aws::Client::RetryStrategy> retryStrategy =
            std::make_shared<Aws::Client::DefaultRetryStrategy>(maxRetries, scaleFactor);

    // Create a client configuration object and set the custom retry strategy
    config.retryStrategy = retryStrategy;

    std::string error_sk = "12345";
    auto credentials = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(ak, error_sk.data());
    auto client = std::make_shared<Aws::S3::S3Client>(std::move(credentials), std::move(config),
                                                      Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, true);

    auto stream = std::make_unique<starrocks::io::S3InputStream>(client, s_bucket_name, kObjectName);
    char buf[6];
    auto r = stream->read(buf, sizeof(buf));
    EXPECT_TRUE(r.status().message().find("SdkResponseCode=403") != std::string::npos);
}

TEST_F(PocoHttpClientTest, TestNotFoundKey) {
    Aws::Client::ClientConfiguration config = S3ClientFactory::getClientConfig();
    config.endpointOverride = config::object_storage_endpoint.empty() ? getenv("STARROCKS_UT_S3_ENDPOINT")
                                                                      : config::object_storage_endpoint;
    // Create a custom retry strategy
    int maxRetries = 2;
    long scaleFactor = 25;
    std::shared_ptr<Aws::Client::RetryStrategy> retryStrategy =
            std::make_shared<Aws::Client::DefaultRetryStrategy>(maxRetries, scaleFactor);

    // Create a client configuration object and set the custom retry strategy
    config.retryStrategy = retryStrategy;

    auto credentials = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(ak, sk);

    auto client = std::make_shared<Aws::S3::S3Client>(std::move(credentials), std::move(config),
                                                      Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, true);

    auto stream = std::make_unique<starrocks::io::S3InputStream>(client, s_bucket_name, "not_found_key");
    char buf[6];
    auto r = stream->read(buf, sizeof(buf));
    EXPECT_TRUE(r.status().message().find("SdkResponseCode=404") != std::string::npos);
    // ErrorCode 16 means RESOURCE_NOT_FOUND
    EXPECT_TRUE(r.status().message().find("SdkErrorType=16") != std::string::npos);
}

} // namespace starrocks::poco
