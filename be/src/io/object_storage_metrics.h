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

#pragma once

#include <chrono>
#include <cstdint>
#include <string_view>

namespace starrocks {

class MetricRegistry;

namespace io {

// Where a request goes, which client library sends it, and whether it reads or writes. The three
// form the labels of the object_storage_request_* metrics, so one query covers every store.
enum class ObjectStorageBackend : uint8_t { kS3, kAzblob, kAdls2, kGcs, kHdfs, kCount };
enum class ObjectStorageClient : uint8_t { kCurl, kPoco, kAzureSdk, kLibhdfs, kCount };
enum class ObjectStorageOp : uint8_t { kGet, kPut, kCount };

std::string_view to_string(ObjectStorageBackend backend);
std::string_view to_string(ObjectStorageClient client);
std::string_view to_string(ObjectStorageOp op);

// The backend a path is served by when it goes through the Hadoop client (libhdfs), by scheme.
ObjectStorageBackend object_storage_backend_of(std::string_view uri);
// The HTTP client the AWS SDK sends with in this process.
ObjectStorageClient aws_sdk_http_client();

// Requests BE's own storage clients send: fs_s3, the native Azure Blob client and libhdfs (which
// also serves ADLS2, GCS and every other Hadoop scheme). Starlet's clients are not covered, they
// have their own metrics.
//
// Exported per (backend, client, op): object_storage_requests_total, _request_failed_total,
// _request_latency_us_total, _request_retry_total and _request_timeout_total. The BE Prometheus
// export drops histograms, so the latency distribution per (backend, op) is a bvar
// LatencyRecorder, object_storage_<backend>_<op>_latency{,_99,...}.
class ObjectStorageMetrics {
public:
    static ObjectStorageMetrics* instance();

    void install(MetricRegistry* registry);

    void record(ObjectStorageBackend backend, ObjectStorageClient client, ObjectStorageOp op, int64_t latency_us,
                bool failed, int64_t retries, int64_t timeouts);

    // For tests: the counters of one label set, all zero when nothing was recorded for it.
    struct Snapshot {
        int64_t requests = 0;
        int64_t failed = 0;
        int64_t latency_us = 0;
        int64_t retries = 0;
        int64_t timeouts = 0;
    };
    Snapshot snapshot(ObjectStorageBackend backend, ObjectStorageClient client, ObjectStorageOp op) const;

    // One label set's counters, defined in the .cpp.
    struct Series;

private:
    ObjectStorageMetrics() = default;
    Series* _series(ObjectStorageBackend backend, ObjectStorageClient client, ObjectStorageOp op);
};

// Measures one request on the calling thread, from construction to destruction. The SDK hooks that
// see each failed attempt (the AWS retry strategy, the Azure per-retry policy) run on the same
// thread and report into the innermost live scope through on_attempt_failed. Mark a request that
// finally failed with set_failed.
class ObjectStorageRequestScope {
public:
    ObjectStorageRequestScope(ObjectStorageBackend backend, ObjectStorageClient client, ObjectStorageOp op);
    ~ObjectStorageRequestScope();

    ObjectStorageRequestScope(const ObjectStorageRequestScope&) = delete;
    ObjectStorageRequestScope& operator=(const ObjectStorageRequestScope&) = delete;

    void set_failed(bool failed = true) { _failed = failed; }

    // A failed attempt of the request running on this thread, for SDKs that decide on retries in a
    // hook that sees the error (AWS). No-op outside a scope, so requests this module does not measure
    // (list, delete, head) are not counted.
    static void on_attempt_failed(bool timed_out, bool will_retry);
    // An attempt of the request running on this thread is about to be sent, for SDKs whose hook sees
    // every attempt but not the retry decision (Azure): every attempt after the first is a retry.
    static void on_attempt_started();

    // Whether a client error message describes a timeout. curl reports "Timeout was reached", Poco a
    // TimeoutException, Azure "timed out"; none has a dedicated error type.
    static bool is_timeout_message(std::string_view message);

private:
    ObjectStorageBackend _backend;
    ObjectStorageClient _client;
    ObjectStorageOp _op;
    std::chrono::steady_clock::time_point _start;
    ObjectStorageRequestScope* _outer;
    bool _failed = false;
    int64_t _retries = 0;
    int64_t _attempts = 0;
    int64_t _timeouts = 0;
};

} // namespace io
} // namespace starrocks
