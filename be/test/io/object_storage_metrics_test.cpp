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

#include "io/object_storage_metrics.h"

#include <gtest/gtest.h>

#include "base/metrics.h"
#include "base/utility/defer_op.h"
#include "common/config_object_storage_fwd.h"

namespace starrocks::io {

namespace {

// The metrics are a process singleton that binds to one registry, so every test shares this one.
MetricRegistry* test_registry() {
    static auto* registry = new MetricRegistry("object_storage_metrics_test");
    return registry;
}

MetricLabels labels_of(ObjectStorageBackend backend, ObjectStorageClient client, ObjectStorageOp op) {
    return MetricLabels()
            .add("backend", std::string(to_string(backend)))
            .add("client", std::string(to_string(client)))
            .add("op", std::string(to_string(op)));
}

std::string metric_value(const std::string& name, const MetricLabels& labels) {
    auto* metric = test_registry()->get_metric(name, labels);
    return metric == nullptr ? "<missing>" : metric->to_string();
}

} // namespace

TEST(ObjectStorageMetricsTest, backend_of_uri) {
    // The S3 schemes come from a config that this test binary does not initialize.
    const auto old_list = config::s3_compatible_fs_list;
    config::s3_compatible_fs_list = {"s3://", "oss://"};
    DeferOp restore([&]() { config::s3_compatible_fs_list = old_list; });
    EXPECT_EQ(ObjectStorageBackend::kS3, object_storage_backend_of("s3://bucket/key"));
    EXPECT_EQ(ObjectStorageBackend::kS3, object_storage_backend_of("oss://bucket/key"));
    EXPECT_EQ(ObjectStorageBackend::kAzblob, object_storage_backend_of("wasbs://c@a.blob.core.windows.net/k"));
    EXPECT_EQ(ObjectStorageBackend::kAdls2, object_storage_backend_of("abfss://c@a.dfs.core.windows.net/k"));
    EXPECT_EQ(ObjectStorageBackend::kGcs, object_storage_backend_of("gs://bucket/key"));
    EXPECT_EQ(ObjectStorageBackend::kHdfs, object_storage_backend_of("hdfs://nn:8020/path"));
    EXPECT_EQ(ObjectStorageBackend::kHdfs, object_storage_backend_of("viewfs://ns/path"));
}

TEST(ObjectStorageMetricsTest, timeout_message) {
    EXPECT_TRUE(ObjectStorageRequestScope::is_timeout_message("curlCode: 28, Timeout was reached"));
    EXPECT_TRUE(ObjectStorageRequestScope::is_timeout_message("Poco::TimeoutException"));
    EXPECT_TRUE(ObjectStorageRequestScope::is_timeout_message("Connection timed out"));
    EXPECT_FALSE(ObjectStorageRequestScope::is_timeout_message("Access Denied"));
}

TEST(ObjectStorageMetricsTest, scope_records_request_and_attempts) {
    // A series registered before install shows up once the registry is installed, one registered
    // after install shows up right away.
    const auto before = labels_of(ObjectStorageBackend::kS3, ObjectStorageClient::kCurl, ObjectStorageOp::kGet);
    {
        ObjectStorageRequestScope scope(ObjectStorageBackend::kS3, ObjectStorageClient::kCurl, ObjectStorageOp::kGet);
        // Two failed attempts the SDK retried, the first a timeout, then the request still failed.
        ObjectStorageRequestScope::on_attempt_failed(/*timed_out=*/true, /*will_retry=*/true);
        ObjectStorageRequestScope::on_attempt_failed(/*timed_out=*/false, /*will_retry=*/true);
        ObjectStorageRequestScope::on_attempt_failed(/*timed_out=*/false, /*will_retry=*/false);
        scope.set_failed();
    }
    auto snap = ObjectStorageMetrics::instance()->snapshot(ObjectStorageBackend::kS3, ObjectStorageClient::kCurl,
                                                           ObjectStorageOp::kGet);
    EXPECT_EQ(1, snap.requests);
    EXPECT_EQ(1, snap.failed);
    EXPECT_EQ(2, snap.retries);
    EXPECT_EQ(1, snap.timeouts);
    EXPECT_GE(snap.latency_us, 0);

    ObjectStorageMetrics::instance()->install(test_registry());
    EXPECT_EQ("1", metric_value("object_storage_requests_total", before));
    EXPECT_EQ("1", metric_value("object_storage_request_failed_total", before));
    EXPECT_EQ("2", metric_value("object_storage_request_retry_total", before));
    EXPECT_EQ("1", metric_value("object_storage_request_timeout_total", before));

    // Azure style: the hook sees every attempt but no retry decision, so the second attempt is a retry.
    const auto after = labels_of(ObjectStorageBackend::kAzblob, ObjectStorageClient::kAzureSdk, ObjectStorageOp::kPut);
    {
        ObjectStorageRequestScope scope(ObjectStorageBackend::kAzblob, ObjectStorageClient::kAzureSdk,
                                        ObjectStorageOp::kPut);
        ObjectStorageRequestScope::on_attempt_started();
        ObjectStorageRequestScope::on_attempt_failed(/*timed_out=*/true, /*will_retry=*/false);
        ObjectStorageRequestScope::on_attempt_started();
    }
    EXPECT_EQ("1", metric_value("object_storage_requests_total", after));
    EXPECT_EQ("0", metric_value("object_storage_request_failed_total", after));
    EXPECT_EQ("1", metric_value("object_storage_request_retry_total", after));
    EXPECT_EQ("1", metric_value("object_storage_request_timeout_total", after));

    // A store this process never talked to exports nothing.
    EXPECT_EQ("<missing>", metric_value("object_storage_requests_total",
                                        labels_of(ObjectStorageBackend::kGcs, ObjectStorageClient::kLibhdfs,
                                                  ObjectStorageOp::kGet)));
}

TEST(ObjectStorageMetricsTest, attempts_go_to_innermost_scope) {
    const auto outer_labels =
            labels_of(ObjectStorageBackend::kHdfs, ObjectStorageClient::kLibhdfs, ObjectStorageOp::kPut);
    const auto inner_labels =
            labels_of(ObjectStorageBackend::kHdfs, ObjectStorageClient::kLibhdfs, ObjectStorageOp::kGet);
    {
        ObjectStorageRequestScope outer(ObjectStorageBackend::kHdfs, ObjectStorageClient::kLibhdfs,
                                        ObjectStorageOp::kPut);
        {
            ObjectStorageRequestScope inner(ObjectStorageBackend::kHdfs, ObjectStorageClient::kLibhdfs,
                                            ObjectStorageOp::kGet);
            ObjectStorageRequestScope::on_attempt_failed(/*timed_out=*/false, /*will_retry=*/true);
        }
        // Back in the outer scope.
        ObjectStorageRequestScope::on_attempt_failed(/*timed_out=*/true, /*will_retry=*/false);
    }
    // Outside any scope: dropped.
    ObjectStorageRequestScope::on_attempt_failed(/*timed_out=*/true, /*will_retry=*/true);

    auto inner = ObjectStorageMetrics::instance()->snapshot(ObjectStorageBackend::kHdfs, ObjectStorageClient::kLibhdfs,
                                                            ObjectStorageOp::kGet);
    auto outer = ObjectStorageMetrics::instance()->snapshot(ObjectStorageBackend::kHdfs, ObjectStorageClient::kLibhdfs,
                                                            ObjectStorageOp::kPut);
    EXPECT_EQ(1, inner.retries);
    EXPECT_EQ(0, inner.timeouts);
    EXPECT_EQ(0, outer.retries);
    EXPECT_EQ(1, outer.timeouts);
}

TEST(ObjectStorageMetricsTest, aws_client_follows_config) {
    const bool old = config::enable_poco_client_for_aws_sdk;
    config::enable_poco_client_for_aws_sdk = false;
    EXPECT_EQ(ObjectStorageClient::kCurl, aws_sdk_http_client());
    config::enable_poco_client_for_aws_sdk = true;
    EXPECT_EQ(ObjectStorageClient::kPoco, aws_sdk_http_client());
    config::enable_poco_client_for_aws_sdk = old;
}

} // namespace starrocks::io
