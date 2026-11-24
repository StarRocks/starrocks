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

#include <bvar/bvar.h>
#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <mutex>
#include <string>

#include "base/metrics.h"
#include "common/config_object_storage_fwd.h"

namespace starrocks::io {

namespace {

constexpr size_t kNumBackends = static_cast<size_t>(ObjectStorageBackend::kCount);
constexpr size_t kNumClients = static_cast<size_t>(ObjectStorageClient::kCount);
constexpr size_t kNumOps = static_cast<size_t>(ObjectStorageOp::kCount);

bool has_prefix(std::string_view s, std::string_view prefix) {
    return s.size() >= prefix.size() && s.compare(0, prefix.size(), prefix) == 0;
}

thread_local ObjectStorageRequestScope* tls_current_scope = nullptr;

} // namespace

std::string_view to_string(ObjectStorageBackend backend) {
    switch (backend) {
    case ObjectStorageBackend::kS3:
        return "s3";
    case ObjectStorageBackend::kAzblob:
        return "azblob";
    case ObjectStorageBackend::kAdls2:
        return "adls2";
    case ObjectStorageBackend::kGcs:
        return "gs";
    case ObjectStorageBackend::kHdfs:
    case ObjectStorageBackend::kCount:
        break;
    }
    return "hdfs";
}

std::string_view to_string(ObjectStorageClient client) {
    switch (client) {
    case ObjectStorageClient::kCurl:
        return "curl";
    case ObjectStorageClient::kPoco:
        return "poco";
    case ObjectStorageClient::kAzureSdk:
        return "azure_sdk";
    case ObjectStorageClient::kLibhdfs:
    case ObjectStorageClient::kCount:
        break;
    }
    return "libhdfs";
}

std::string_view to_string(ObjectStorageOp op) {
    return op == ObjectStorageOp::kPut ? "put" : "get";
}

ObjectStorageBackend object_storage_backend_of(std::string_view uri) {
    if (has_prefix(uri, "wasb://") || has_prefix(uri, "wasbs://")) {
        return ObjectStorageBackend::kAzblob;
    }
    if (has_prefix(uri, "abfs://") || has_prefix(uri, "abfss://") || has_prefix(uri, "adl://")) {
        return ObjectStorageBackend::kAdls2;
    }
    if (has_prefix(uri, "gs://")) {
        return ObjectStorageBackend::kGcs;
    }
    for (const auto& prefix : config::s3_compatible_fs_list) {
        if (has_prefix(uri, prefix)) {
            return ObjectStorageBackend::kS3;
        }
    }
    return ObjectStorageBackend::kHdfs;
}

ObjectStorageClient aws_sdk_http_client() {
    return config::enable_poco_client_for_aws_sdk ? ObjectStorageClient::kPoco : ObjectStorageClient::kCurl;
}

struct ObjectStorageMetrics::Series {
    METRIC_DEFINE_INT_COUNTER(requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(failed_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(latency_us_total, MetricUnit::MICROSECONDS);
    METRIC_DEFINE_INT_COUNTER(retry_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(timeout_total, MetricUnit::REQUESTS);
    MetricLabels labels;
    bvar::LatencyRecorder* latency = nullptr;
};

namespace {

// Series are created on first use, so a BE that never talks to a store exports nothing for it, and
// live for the life of the process: registered metrics keep back-pointers into the registry.
std::mutex g_mutex;
MetricRegistry* g_registry = nullptr;
std::array<std::atomic<ObjectStorageMetrics::Series*>, kNumBackends * kNumClients * kNumOps> g_series{};
std::array<bvar::LatencyRecorder*, kNumBackends * kNumOps> g_latency{};

void register_series(MetricRegistry* registry, ObjectStorageMetrics::Series* s) {
    registry->register_metric("object_storage_requests_total", s->labels, &s->requests_total);
    registry->register_metric("object_storage_request_failed_total", s->labels, &s->failed_total);
    registry->register_metric("object_storage_request_latency_us_total", s->labels, &s->latency_us_total);
    registry->register_metric("object_storage_request_retry_total", s->labels, &s->retry_total);
    registry->register_metric("object_storage_request_timeout_total", s->labels, &s->timeout_total);
}

size_t series_index(ObjectStorageBackend backend, ObjectStorageClient client, ObjectStorageOp op) {
    return (static_cast<size_t>(backend) * kNumClients + static_cast<size_t>(client)) * kNumOps +
           static_cast<size_t>(op);
}

} // namespace

ObjectStorageMetrics* ObjectStorageMetrics::instance() {
    static auto* instance = new ObjectStorageMetrics();
    return instance;
}

void ObjectStorageMetrics::install(MetricRegistry* registry) {
    std::lock_guard l(g_mutex);
    if (g_registry != nullptr) {
        DCHECK_EQ(g_registry, registry);
        return;
    }
    g_registry = registry;
    for (auto& slot : g_series) {
        if (auto* s = slot.load(std::memory_order_acquire); s != nullptr) {
            register_series(registry, s);
        }
    }
}

ObjectStorageMetrics::Series* ObjectStorageMetrics::_series(ObjectStorageBackend backend, ObjectStorageClient client,
                                                            ObjectStorageOp op) {
    auto& slot = g_series[series_index(backend, client, op)];
    if (auto* s = slot.load(std::memory_order_acquire); s != nullptr) {
        return s;
    }
    std::lock_guard l(g_mutex);
    if (auto* s = slot.load(std::memory_order_acquire); s != nullptr) {
        return s;
    }
    auto* s = new Series();
    s->labels.add("backend", std::string(to_string(backend)))
            .add("client", std::string(to_string(client)))
            .add("op", std::string(to_string(op)));
    auto& latency = g_latency[static_cast<size_t>(backend) * kNumOps + static_cast<size_t>(op)];
    if (latency == nullptr) {
        latency = new bvar::LatencyRecorder(fmt::format("object_storage_{}_{}", to_string(backend), to_string(op)));
    }
    s->latency = latency;
    if (g_registry != nullptr) {
        register_series(g_registry, s);
    }
    slot.store(s, std::memory_order_release);
    return s;
}

void ObjectStorageMetrics::record(ObjectStorageBackend backend, ObjectStorageClient client, ObjectStorageOp op,
                                  int64_t latency_us, bool failed, int64_t retries, int64_t timeouts) {
    auto* s = _series(backend, client, op);
    s->requests_total.increment(1);
    s->latency_us_total.increment(latency_us);
    if (failed) {
        s->failed_total.increment(1);
    }
    if (retries > 0) {
        s->retry_total.increment(retries);
    }
    if (timeouts > 0) {
        s->timeout_total.increment(timeouts);
    }
    *s->latency << latency_us;
}

ObjectStorageMetrics::Snapshot ObjectStorageMetrics::snapshot(ObjectStorageBackend backend, ObjectStorageClient client,
                                                              ObjectStorageOp op) const {
    Snapshot snap;
    auto* s = g_series[series_index(backend, client, op)].load(std::memory_order_acquire);
    if (s != nullptr) {
        snap.requests = s->requests_total.value();
        snap.failed = s->failed_total.value();
        snap.latency_us = s->latency_us_total.value();
        snap.retries = s->retry_total.value();
        snap.timeouts = s->timeout_total.value();
    }
    return snap;
}

ObjectStorageRequestScope::ObjectStorageRequestScope(ObjectStorageBackend backend, ObjectStorageClient client,
                                                     ObjectStorageOp op)
        : _backend(backend),
          _client(client),
          _op(op),
          _start(std::chrono::steady_clock::now()),
          _outer(tls_current_scope) {
    tls_current_scope = this;
}

ObjectStorageRequestScope::~ObjectStorageRequestScope() {
    tls_current_scope = _outer;
    const auto latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - _start).count();
    const int64_t retries = _retries + std::max<int64_t>(0, _attempts - 1);
    ObjectStorageMetrics::instance()->record(_backend, _client, _op, latency_us, _failed, retries, _timeouts);
}

void ObjectStorageRequestScope::on_attempt_failed(bool timed_out, bool will_retry) {
    auto* scope = tls_current_scope;
    if (scope == nullptr) {
        return;
    }
    if (timed_out) {
        scope->_timeouts++;
    }
    if (will_retry) {
        scope->_retries++;
    }
}

void ObjectStorageRequestScope::on_attempt_started() {
    if (auto* scope = tls_current_scope; scope != nullptr) {
        scope->_attempts++;
    }
}

bool ObjectStorageRequestScope::is_timeout_message(std::string_view message) {
    std::string lower(message);
    std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) { return std::tolower(c); });
    return lower.find("timeout") != std::string::npos || lower.find("timed out") != std::string::npos;
}

} // namespace starrocks::io
