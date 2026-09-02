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

class S3PocoHttpClientTest : public testing::Test {
public:
    S3PocoHttpClientTest() = default;
    ~S3PocoHttpClientTest() override = default;

    static void SetUpTestCase();
    static void TearDownTestCase();

    static void put_object(const std::string& object_content);

protected:
    inline static const char* s_bucket_name = nullptr;
    inline static const char* ak = nullptr;
    inline static const char* sk = nullptr;
};

void S3PocoHttpClientTest::SetUpTestCase() {
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

void S3PocoHttpClientTest::TearDownTestCase() {
    Aws::ShutdownAPI(Aws::SDKOptions());
}

void S3PocoHttpClientTest::put_object(const std::string& object_content) {
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

TEST_F(S3PocoHttpClientTest, TestNormalAccess) {
    Aws::Client::ClientConfiguration config = S3ClientFactory::getClientConfig();
    config.endpointOverride = config::object_storage_endpoint.empty() ? getenv("STARROCKS_UT_S3_ENDPOINT")
                                                                      : config::object_storage_endpoint;
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
    config.endpointOverride = config::object_storage_endpoint.empty() ? getenv("STARROCKS_UT_S3_ENDPOINT")
                                                                      : config::object_storage_endpoint;
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
    config.endpointOverride = config::object_storage_endpoint.empty() ? getenv("STARROCKS_UT_S3_ENDPOINT")
                                                                      : config::object_storage_endpoint;
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

// A request with no timeout must not inherit the previous borrower's. Sessions are pooled per
// endpoint rather than per client, and clients sharing an endpoint do not share a timeout: a
// RENAME_FILE client carries object_storage_rename_file_request_timeout_ms while an ordinary read
// carries the runtime-mutable object_storage_request_timeout_ms. Checking out a session must
// therefore put it in the state this request asked for, not leave it in the state the last one did.
TEST(PocoSessionTimeoutTest, UnsetTimeoutDoesNotInheritThePreviousRequests) {
    Poco::Net::HTTPClientSession session("127.0.0.1", 1);
    const Poco::Timespan pristine = session.getReceiveTimeout();

    // A client with a timeout borrows it first.
    const Poco::Timespan thirty(30 * 1000000);
    apply_request_timeouts(session, ConnectionTimeouts(Poco::Timespan(1 * 1000000), thirty, thirty));
    ASSERT_EQ(thirty.totalMicroseconds(), session.getReceiveTimeout().totalMicroseconds());

    // A client without one borrows it next, and must not be left on the 30 s above.
    ConnectionTimeouts unset(Poco::Timespan(1 * 1000000), Poco::Timespan(-1 * 1000), Poco::Timespan(-1 * 1000));
    apply_request_timeouts(session, unset);

    EXPECT_EQ(pristine.totalMicroseconds(), session.getReceiveTimeout().totalMicroseconds());
    EXPECT_EQ(pristine.totalMicroseconds(), session.getSendTimeout().totalMicroseconds());
}

// Zero explicitly disables the request timeout. It must not be confused with an unset (negative)
// timeout and restored to Poco's default, especially after a pooled session carried a positive
// timeout for its previous borrower.
TEST(PocoSessionTimeoutTest, ZeroTimeoutDisablesThePreviousRequestsTimeout) {
    Poco::Net::HTTPClientSession session("127.0.0.1", 1);
    const Poco::Timespan thirty(30 * 1000000);
    apply_request_timeouts(session, ConnectionTimeouts(Poco::Timespan(1 * 1000000), thirty, thirty));
    ASSERT_EQ(thirty.totalMicroseconds(), session.getReceiveTimeout().totalMicroseconds());

    const Poco::Timespan zero(0);
    apply_request_timeouts(session, ConnectionTimeouts(Poco::Timespan(1 * 1000000), zero, zero));

    EXPECT_EQ(0, session.getReceiveTimeout().totalMicroseconds());
    EXPECT_EQ(0, session.getSendTimeout().totalMicroseconds());
}

// Keep-alive belongs to the pool, not to a single request: ConnectionTimeouts default-initializes
// http_keep_alive_timeout to zero, and pushing that onto a pooled session would work against the
// pool, which exists to reuse connections.
TEST(PocoSessionTimeoutTest, KeepAliveTimeoutIsLeftAlone) {
    Poco::Net::HTTPClientSession session("127.0.0.1", 1);
    const Poco::Timespan before = session.getKeepAliveTimeout();

    ConnectionTimeouts timeouts(Poco::Timespan(1 * 1000000), Poco::Timespan(5 * 1000000), Poco::Timespan(5 * 1000000));
    ASSERT_EQ(0, timeouts.http_keep_alive_timeout.totalMicroseconds());
    apply_request_timeouts(session, timeouts);

    EXPECT_EQ(before.totalMicroseconds(), session.getKeepAliveTimeout().totalMicroseconds());
}

} // namespace starrocks::poco
