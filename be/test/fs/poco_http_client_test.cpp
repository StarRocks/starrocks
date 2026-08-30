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

#include "platform/aws/poco_http_client.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <memory>
#include <string>

#include "base/testutil/assert.h"
#include "common/config_object_storage_fwd.h"
#include "common/logging.h"
#include "fs/fs_s3.h"
#include "io/s3_input_stream.h"
#include "platform/aws/poco_common.h"
#include "platform/aws/poco_http_client_factory.h"

namespace starrocks::poco {

static const char* kObjectName = "starrocks_ut_poco_test.txt";
static const char* kObjectContent = "0123456789";

class S3PocoHttpClientTest : public testing::Test {
public:
    S3PocoHttpClientTest() = default;
    ~S3PocoHttpClientTest() override = default;

    static void SetUpTestCase();
    static void TearDownTestCase();

    static void put_object(const std::string& object_content);
    static const char* get_config_or_env(const std::string& config_value, const char* env_name);
    static void apply_endpoint_override(Aws::Client::ClientConfiguration* config);

protected:
    void SetUp() override;

    inline static const char* s_bucket_name = nullptr;
    inline static const char* ak = nullptr;
    inline static const char* sk = nullptr;
    inline static bool s_skip = false;
    inline static std::string s_skip_reason;
};

void S3PocoHttpClientTest::SetUpTestCase() {
    s_skip = false;
    s_skip_reason.clear();
    Aws::InitAPI(Aws::SDKOptions());
    Aws::Http::SetHttpClientFactory(std::make_shared<starrocks::poco::PocoHttpClientFactory>());

    s_bucket_name = get_config_or_env(config::object_storage_bucket, "STARROCKS_UT_S3_BUCKET");
    if (s_bucket_name == nullptr) {
        s_skip = true;
        s_skip_reason = "s3 bucket name not set";
        return;
    }

    ak = get_config_or_env(config::object_storage_access_key_id, "STARROCKS_UT_S3_AK");
    sk = get_config_or_env(config::object_storage_secret_access_key, "STARROCKS_UT_S3_SK");
    if (ak == nullptr) {
        s_skip = true;
        s_skip_reason = "s3 access key id not set";
        return;
    }
    if (sk == nullptr) {
        s_skip = true;
        s_skip_reason = "s3 secret access key not set";
        return;
    }
    put_object(kObjectContent);
}

void S3PocoHttpClientTest::TearDownTestCase() {
    HTTPSessionPools::instance().shutdown();
    Aws::ShutdownAPI(Aws::SDKOptions());
}

void S3PocoHttpClientTest::SetUp() {
    if (s_skip) {
        GTEST_SKIP() << s_skip_reason;
    }
}

const char* S3PocoHttpClientTest::get_config_or_env(const std::string& config_value, const char* env_name) {
    return config_value.empty() ? std::getenv(env_name) : config_value.c_str();
}

void S3PocoHttpClientTest::apply_endpoint_override(Aws::Client::ClientConfiguration* config) {
    const char* endpoint = get_config_or_env(config::object_storage_endpoint, "STARROCKS_UT_S3_ENDPOINT");
    if (endpoint != nullptr) {
        config->endpointOverride = endpoint;
    }
}

void S3PocoHttpClientTest::put_object(const std::string& object_content) {
    Aws::Client::ClientConfiguration config = S3ClientFactory::getClientConfig();
    apply_endpoint_override(&config);

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

TEST_F(S3PocoHttpClientTest, TestNormalAccess) {
    Aws::Client::ClientConfiguration config = S3ClientFactory::getClientConfig();
    apply_endpoint_override(&config);
    // Set timeout for faster test execution
    config.connectTimeoutMs = 500;
    config.requestTimeoutMs = 2000;
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

TEST_F(S3PocoHttpClientTest, TestErrorEndpoint) {
    Aws::Client::ClientConfiguration config = S3ClientFactory::getClientConfig();
    config.endpointOverride = "http://127.0.0.1";
    // Set very short timeout for faster failure detection
    config.connectTimeoutMs = 100;
    config.requestTimeoutMs = 200;
    // Disable retry for error test to speed up
    int maxRetries = 0;
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

TEST_F(S3PocoHttpClientTest, TestErrorAkSk) {
    Aws::Client::ClientConfiguration config = S3ClientFactory::getClientConfig();
    apply_endpoint_override(&config);
    // Set timeout for faster test execution
    config.connectTimeoutMs = 500;
    config.requestTimeoutMs = 2000;
    // Reduce retry for error test to speed up
    int maxRetries = 1;
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

TEST_F(S3PocoHttpClientTest, TestNotFoundKey) {
    Aws::Client::ClientConfiguration config = S3ClientFactory::getClientConfig();
    apply_endpoint_override(&config);
    // Set timeout for faster test execution
    config.connectTimeoutMs = 500;
    config.requestTimeoutMs = 2000;
    // Reduce retry for error test to speed up
    int maxRetries = 1;
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

// The timeouts a caller hands to makeHTTPSession() have to reach the session. They used to be
// dropped: getSession() consumed only connection_timeout, to bound the pool wait, and returned
// without touching send/receive, so object_storage_request_timeout_ms had no effect on this path
// and a read that stopped receiving waited out Poco's own default instead.
//
// No network is involved -- Poco connects lazily on sendRequest(), so constructing a session for
// an address nothing listens on is enough to inspect what was applied to it.
TEST(PocoSessionTimeoutTest, SendAndReceiveTimeoutsReachTheSession) {
    const Poco::Timespan connect(1 * 1000000);
    const Poco::Timespan request(5 * 1000000);
    ConnectionTimeouts timeouts(connect, request, request);

    Poco::URI uri("http://127.0.0.1:1/");
    auto session = makeHTTPSession(uri, timeouts, false);

    EXPECT_EQ(request.totalMicroseconds(), session->getSendTimeout().totalMicroseconds());
    EXPECT_EQ(request.totalMicroseconds(), session->getReceiveTimeout().totalMicroseconds());
}

// object_storage_request_timeout_ms defaults to -1, which arrives here as a negative Timespan,
// and Poco gives no defined meaning to that. Treat non-positive as "unset" and leave the session
// on its own default rather than passing the value through.
TEST(PocoSessionTimeoutTest, NonPositiveTimeoutLeavesTheSessionDefault) {
    Poco::Net::HTTPClientSession session("127.0.0.1", 1);
    const Poco::Timespan before_send = session.getSendTimeout();
    const Poco::Timespan before_recv = session.getReceiveTimeout();

    ConnectionTimeouts negative(Poco::Timespan(1 * 1000000), Poco::Timespan(-1 * 1000),
                                Poco::Timespan(-1 * 1000));
    apply_request_timeouts(session, negative);

    EXPECT_EQ(before_send.totalMicroseconds(), session.getSendTimeout().totalMicroseconds());
    EXPECT_EQ(before_recv.totalMicroseconds(), session.getReceiveTimeout().totalMicroseconds());
}

// Keep-alive belongs to the pool, not to a single request: ConnectionTimeouts default-initializes
// http_keep_alive_timeout to zero, and pushing that onto a pooled session would work against the
// pool, which exists to reuse connections.
TEST(PocoSessionTimeoutTest, KeepAliveTimeoutIsLeftAlone) {
    Poco::Net::HTTPClientSession session("127.0.0.1", 1);
    const Poco::Timespan before = session.getKeepAliveTimeout();

    ConnectionTimeouts timeouts(Poco::Timespan(1 * 1000000), Poco::Timespan(5 * 1000000),
                                Poco::Timespan(5 * 1000000));
    ASSERT_EQ(0, timeouts.http_keep_alive_timeout.totalMicroseconds());
    apply_request_timeouts(session, timeouts);

    EXPECT_EQ(before.totalMicroseconds(), session.getKeepAliveTimeout().totalMicroseconds());
}

} // namespace starrocks::poco
