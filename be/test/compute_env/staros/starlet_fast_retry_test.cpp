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

#include <aws/core/Aws.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/internal/AWSHttpResourceClient.h>
#include <fslib/fslib_all_initializer.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "base/testutil/assert.h"
#include "base/testutil/delaying_http_server.h"
#include "common/config_object_storage_fwd.h"
#include "common/shutdown_hook.h"
#include "compute_env/staros/starlet_filesystem.h"
#include "compute_env/staros/staros_worker.h"
#include "compute_env/staros/staros_worker_runtime.h"
#include "platform/aws/object_storage_fast_retry.h"

namespace starrocks {

namespace {

using testutil::DelayingHttpServer;
using testutil::ReceivedRequest;

constexpr int64_t kShardId = 10087;
// How long the server holds the first send of a request. Much longer than the short timeout, so that only a resend
// can finish in time.
constexpr int64_t kHoldMs = 5000;
constexpr char kImdsDisabledEnv[] = "AWS_EC2_METADATA_DISABLED";

int64_t ms_since(std::chrono::steady_clock::time_point start) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
}

// The requests with `method` whose target ends with `suffix`.
std::vector<ReceivedRequest> requests_to(const DelayingHttpServer& server, std::string_view method,
                                         std::string_view suffix) {
    std::vector<ReceivedRequest> result;
    for (ReceivedRequest& request : server.requests()) {
        if (request.method == method && std::string_view(request.target).ends_with(suffix)) {
            result.push_back(std::move(request));
        }
    }
    return result;
}

int64_t fast_retry_count(FastRetryRequestClass request_class, FastRetryResult result) {
    return ObjectStorageFastRetryMetrics::instance()->value(request_class, result);
}

int64_t resend_count(FastRetryRequestClass request_class) {
    return fast_retry_count(request_class, FastRetryResult::RESEND_FAST) +
           fast_retry_count(request_class, FastRetryResult::RESEND_SLOW);
}

} // namespace

// Fast retry acts on the requests that starlet sends to shared-data storage. These cases send them through
// starlet's S3 file system of a shard, with the HTTP client factory installed into the SDK as the BE process
// installs it, to a local server that holds the first send of every request.
class StarletFastRetryTest : public ::testing::Test {
public:
    static void SetUpTestSuite() {
        // The region of starlet's clients is given; do not probe the instance metadata service for it.
        _set_imds_env = ::getenv(kImdsDisabledEnv) == nullptr;
        if (_set_imds_env) {
            ::setenv(kImdsDisabledEnv, "true", /*overwrite=*/0);
        }
        // As in the BE process, the test main sets libcurl up and the fast retry factory is installed right after
        // the SDK is initialized. skip_aws_init_api is left as it is: starlet keeps it in a process-wide object the
        // first time it builds an S3 file system, and the other suites in this process rely on it.
        Aws::InitAPI(sdk_options());
        Aws::Http::SetHttpClientFactory(std::make_shared<FastRetryHttpClientFactory>());
    }

    static void TearDownTestSuite() {
        // As at BE shutdown, starlet drops the S3 clients it caches. starlet may also have initialized the SDK, which
        // then outlives this suite, so the SDK's own HTTP client factory is put back for the suites that follow.
        staros::starlet::common::ShutdownHook::shutdown();
        restore_sdk_http_client_factory();
        Aws::ShutdownAPI(sdk_options());
        if (_set_imds_env) {
            ::unsetenv(kImdsDisabledEnv);
        }
    }

    // What Aws::Http::SetHttpClientFactory() does, with the SDK's default factory.
    static void restore_sdk_http_client_factory() {
        const bool had_ec2_metadata_client = Aws::Internal::GetEC2MetadataClient() != nullptr;
        Aws::Http::CleanupHttp();
        Aws::Http::InitHttp();
        if (had_ec2_metadata_client) {
            Aws::Internal::InitEC2MetadataClient();
        }
    }

protected:
    void SetUp() override {
        _saved_enable = config::enable_object_storage_fast_retry;
        _saved_put_timeout_ms = config::object_storage_fast_retry_put_timeout_ms;
        _saved_put_short_attempts = config::object_storage_fast_retry_put_short_attempts;
        _saved_put_max_bytes = config::object_storage_fast_retry_put_max_bytes;
        _saved_head_timeout_ms = config::object_storage_fast_retry_head_timeout_ms;
        _saved_head_short_attempts = config::object_storage_fast_retry_head_short_attempts;
        _saved_get_idle_ms = config::object_storage_fast_retry_get_idle_ms;
        _saved_budget_percent = config::object_storage_fast_retry_budget_percent;

        config::enable_object_storage_fast_retry = true;
        config::object_storage_fast_retry_put_timeout_ms = 300;
        config::object_storage_fast_retry_put_short_attempts = 1;
        config::object_storage_fast_retry_put_max_bytes = 1048576;
        config::object_storage_fast_retry_head_timeout_ms = 300;
        config::object_storage_fast_retry_head_short_attempts = 1;
        config::object_storage_fast_retry_get_idle_ms = 300;
        config::object_storage_fast_retry_budget_percent = 2;

        // The first send of every request is held; resends, and the warm-up below, are answered at once.
        _server = std::make_unique<DelayingHttpServer>([this](size_t, const ReceivedRequest& request) -> int64_t {
            if (std::string_view(request.target).ends_with("/warm-up")) {
                return 0;
            }
            std::lock_guard lock(_seen_mutex);
            return _seen.insert(request.method + " " + request.target).second ? _hold_ms.load() : 0;
        });

        staros::starlet::fslib::register_builtin_filesystems();
        staros::starlet::ShardInfo shard_info;
        shard_info.id = kShardId;
        auto* fs_info = shard_info.path_info.mutable_fs_info();
        fs_info->set_fs_type(staros::FileStoreType::S3);
        auto* s3_fs_info = fs_info->mutable_s3_fs_info();
        s3_fs_info->set_bucket("bucket");
        s3_fs_info->set_endpoint("127.0.0.1:" + std::to_string(_server->port()));
        s3_fs_info->set_region("us-east-1");
        // Path-style addressing: the endpoint is an IP address.
        s3_fs_info->set_path_style_access(1);
        auto* credential = s3_fs_info->mutable_credential()->mutable_simple_credential();
        credential->set_access_key("ak");
        credential->set_access_key_secret("sk");
        shard_info.path_info.set_full_path("s3://bucket/fast-retry/");
        shard_info.cache_info.set_enable_cache(false);
        shard_info.cache_info.set_async_write_back(false);

        _worker = std::make_shared<StarOSWorker>();
        set_staros_worker_for_test(_worker);
        ASSERT_TRUE(_worker->add_shard(shard_info).ok());

        // Let starlet build the shard's file system and its S3 client before the cases time their requests.
        ASSERT_OK(new_fs_starlet()->path_exists(build_starlet_uri(kShardId, "warm-up")));
    }

    void TearDown() override {
        if (_worker != nullptr) {
            (void)_worker->remove_shard(kShardId);
        }
        set_staros_worker_for_test(nullptr);
        _worker.reset();
        _server.reset();

        config::enable_object_storage_fast_retry = _saved_enable;
        config::object_storage_fast_retry_put_timeout_ms = _saved_put_timeout_ms;
        config::object_storage_fast_retry_put_short_attempts = _saved_put_short_attempts;
        config::object_storage_fast_retry_put_max_bytes = _saved_put_max_bytes;
        config::object_storage_fast_retry_head_timeout_ms = _saved_head_timeout_ms;
        config::object_storage_fast_retry_head_short_attempts = _saved_head_short_attempts;
        config::object_storage_fast_retry_get_idle_ms = _saved_get_idle_ms;
        config::object_storage_fast_retry_budget_percent = _saved_budget_percent;
    }

    static Aws::SDKOptions sdk_options() {
        Aws::SDKOptions options;
        options.httpOptions.initAndCleanupCurl = false;
        return options;
    }

    // Declared before the server, whose threads use them, so that they outlive it.
    std::atomic<int64_t> _hold_ms{kHoldMs};
    std::mutex _seen_mutex;
    std::set<std::string> _seen;
    std::unique_ptr<DelayingHttpServer> _server;
    std::shared_ptr<StarOSWorker> _worker;

private:
    static inline bool _set_imds_env = false;
    bool _saved_enable = false;
    int64_t _saved_put_timeout_ms = 0;
    int32_t _saved_put_short_attempts = 0;
    int64_t _saved_put_max_bytes = 0;
    int64_t _saved_head_timeout_ms = 0;
    int32_t _saved_head_short_attempts = 0;
    int64_t _saved_get_idle_ms = 0;
    int32_t _saved_budget_percent = 0;
};

TEST_F(StarletFastRetryTest, starlet_put_object_is_resent_at_once) {
    std::unique_ptr<FileSystem> fs = new_fs_starlet();
    ASSIGN_OR_ABORT(auto file, fs->new_writable_file(build_starlet_uri(kShardId, "put-key")));
    ASSERT_OK(file->append("payload"));
    const int64_t expired = fast_retry_count(FastRetryRequestClass::PUT_OBJECT, FastRetryResult::EXPIRED);
    const int64_t resent = resend_count(FastRetryRequestClass::PUT_OBJECT);

    // starlet sends a small file in one PutObject when it is closed.
    auto start = std::chrono::steady_clock::now();
    ASSERT_OK(file->close());
    const int64_t elapsed_ms = ms_since(start);

    // It waited for the short timeout, not for the held first send.
    EXPECT_LT(elapsed_ms, kHoldMs / 2);
    std::vector<ReceivedRequest> puts = requests_to(*_server, "PUT", "/put-key");
    ASSERT_EQ(2u, puts.size());
    for (const ReceivedRequest& request : puts) {
        EXPECT_EQ(puts[0].target, request.target);
        EXPECT_EQ("payload", request.body);
        // The resend is the SDK's first attempt sent again, not a retry of the SDK.
        EXPECT_EQ("attempt=1", request.header("amz-sdk-request"));
    }
    EXPECT_EQ(puts[0].header("authorization"), puts[1].header("authorization"));
    EXPECT_EQ(1, fast_retry_count(FastRetryRequestClass::PUT_OBJECT, FastRetryResult::EXPIRED) - expired);
    EXPECT_EQ(1, resend_count(FastRetryRequestClass::PUT_OBJECT) - resent);
}

TEST_F(StarletFastRetryTest, starlet_head_object_is_resent_at_once) {
    std::unique_ptr<FileSystem> fs = new_fs_starlet();
    const int64_t expired = fast_retry_count(FastRetryRequestClass::HEAD_OBJECT, FastRetryResult::EXPIRED);
    const int64_t resent = resend_count(FastRetryRequestClass::HEAD_OBJECT);

    // starlet takes the size of a file from a HeadObject.
    auto start = std::chrono::steady_clock::now();
    ASSERT_TRUE(fs->get_file_size(build_starlet_uri(kShardId, "head-key")).ok());
    const int64_t elapsed_ms = ms_since(start);

    EXPECT_LT(elapsed_ms, kHoldMs / 2);
    std::vector<ReceivedRequest> heads = requests_to(*_server, "HEAD", "/head-key");
    ASSERT_EQ(2u, heads.size());
    EXPECT_EQ(heads[0].target, heads[1].target);
    EXPECT_EQ(1, fast_retry_count(FastRetryRequestClass::HEAD_OBJECT, FastRetryResult::EXPIRED) - expired);
    EXPECT_EQ(1, resend_count(FastRetryRequestClass::HEAD_OBJECT) - resent);
}

TEST_F(StarletFastRetryTest, starlet_range_get_is_resent_at_once) {
    auto fs = new_fs_starlet();
    const int64_t expired = fast_retry_count(FastRetryRequestClass::GET_OBJECT, FastRetryResult::EXPIRED);
    const int64_t resent = resend_count(FastRetryRequestClass::GET_OBJECT);

    // With the size known, starlet reads the range with one GetObject and no HeadObject.
    FileInfo info;
    info.path = build_starlet_uri(kShardId, "get-key");
    info.size = 1024 * 1024;
    ASSIGN_OR_ABORT(auto file, fs->new_random_access_file(info));
    std::string data(64 * 1024, '\0');
    auto start = std::chrono::steady_clock::now();
    ASSERT_OK(file->read_at_fully(4096, data.data(), data.size()));
    EXPECT_LT(ms_since(start), kHoldMs / 2);
    for (size_t i = 0; i < data.size(); ++i) {
        ASSERT_EQ(DelayingHttpServer::object_byte(4096 + static_cast<int64_t>(i)), data[i]) << i;
    }

    std::vector<ReceivedRequest> gets = requests_to(*_server, "GET", "/get-key");
    ASSERT_GE(gets.size(), 2u);
    EXPECT_FALSE(gets[0].header("range").empty());
    EXPECT_EQ(gets[0].header("range"), gets[1].header("range"));
    EXPECT_EQ("attempt=1", gets[1].header("amz-sdk-request"));
    EXPECT_EQ(1, fast_retry_count(FastRetryRequestClass::GET_OBJECT, FastRetryResult::EXPIRED) - expired);
    EXPECT_EQ(1, resend_count(FastRetryRequestClass::GET_OBJECT) - resent);
}

TEST_F(StarletFastRetryTest, switch_applies_to_the_clients_starlet_keeps) {
    // A shorter hold, so that the request sent while the switch is off does not take long.
    _hold_ms = 800;
    std::unique_ptr<FileSystem> fs = new_fs_starlet();

    // The client that starlet created in SetUp sends the request once while the switch is off.
    config::enable_object_storage_fast_retry = false;
    auto start = std::chrono::steady_clock::now();
    ASSERT_TRUE(fs->get_file_size(build_starlet_uri(kShardId, "switch-off")).ok());
    EXPECT_GE(ms_since(start), 800 - 50);
    EXPECT_EQ(1u, requests_to(*_server, "HEAD", "/switch-off").size());

    // Turned on at runtime, the same client resends the next slow request.
    config::enable_object_storage_fast_retry = true;
    const int64_t expired = fast_retry_count(FastRetryRequestClass::HEAD_OBJECT, FastRetryResult::EXPIRED);
    ASSERT_TRUE(fs->get_file_size(build_starlet_uri(kShardId, "switch-on")).ok());
    EXPECT_EQ(2u, requests_to(*_server, "HEAD", "/switch-on").size());
    EXPECT_EQ(1, fast_retry_count(FastRetryRequestClass::HEAD_OBJECT, FastRetryResult::EXPIRED) - expired);
}

} // namespace starrocks
