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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/util/doris_metrics_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/starrocks_metrics.h"

#include <gtest/gtest.h>

#include "common/config.h"
#include "storage/page_cache.h"
#include "testutil/assert.h"
#include "util/logging.h"

namespace starrocks {

class StarRocksMetricsTest : public testing::Test {
public:
    StarRocksMetricsTest() = default;
    ~StarRocksMetricsTest() override = default;

protected:
    void SetUp() override { _page_cache = CacheEnv::GetInstance()->page_cache(); }

    void TearDown() override {}

    StoragePageCache* _page_cache;
};

class TestMetricsVisitor : public MetricsVisitor {
public:
    ~TestMetricsVisitor() override = default;
    void visit(const std::string& prefix, const std::string& name, MetricCollector* collector) override {
        for (auto& it : collector->metrics()) {
            Metric* metric = it.second;
            auto& labels = it.first;
            switch (metric->type()) {
            case MetricType::COUNTER: {
                bool has_prev = false;
                if (!prefix.empty()) {
                    _ss << prefix;
                    has_prev = true;
                }
                if (!name.empty()) {
                    if (has_prev) {
                        _ss << "_";
                    }
                    _ss << name;
                }
                if (!labels.empty()) {
                    if (has_prev) {
                        _ss << "{";
                    }
                    _ss << labels.to_string();
                    if (has_prev) {
                        _ss << "}";
                    }
                }
                _ss << " " << metric->to_string() << std::endl;
                break;
            }
            default:
                break;
            }
        }
    }
    std::string to_string() { return _ss.str(); }

private:
    std::stringstream _ss;
};

TEST_F(StarRocksMetricsTest, Normal) {
    TestMetricsVisitor visitor;
    auto instance = StarRocksMetrics::instance();
    auto metrics = instance->metrics();
    metrics->collect(&visitor);
    LOG(INFO) << "\n" << visitor.to_string();
    // check metric
    {
        instance->fragment_requests_total.increment(12);
        auto metric = metrics->get_metric("fragment_requests_total");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("12", metric->to_string().c_str());
    }
    {
        instance->fragment_request_duration_us.increment(101);
        auto metric = metrics->get_metric("fragment_request_duration_us");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("101", metric->to_string().c_str());
    }
    {
        instance->http_requests_total.increment(102);
        auto metric = metrics->get_metric("http_requests_total");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("102", metric->to_string().c_str());
    }
    {
        instance->http_request_send_bytes.increment(104);
        auto metric = metrics->get_metric("http_request_send_bytes");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("104", metric->to_string().c_str());
    }
    {
        instance->query_scan_bytes.increment(104);
        auto metric = metrics->get_metric("query_scan_bytes");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("104", metric->to_string().c_str());
    }
    {
        instance->query_scan_rows.increment(105);
        auto metric = metrics->get_metric("query_scan_rows");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("105", metric->to_string().c_str());
    }
    {
        instance->push_requests_success_total.increment(106);
        auto metric = metrics->get_metric("push_requests_total", MetricLabels().add("status", "SUCCESS"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("106", metric->to_string().c_str());
    }
    {
        instance->push_requests_fail_total.increment(107);
        auto metric = metrics->get_metric("push_requests_total", MetricLabels().add("status", "FAIL"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("107", metric->to_string().c_str());
    }
    {
        instance->push_request_duration_us.increment(108);
        auto metric = metrics->get_metric("push_request_duration_us");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("108", metric->to_string().c_str());
    }
    {
        instance->push_request_write_bytes.increment(109);
        auto metric = metrics->get_metric("push_request_write_bytes");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("109", metric->to_string().c_str());
    }
    {
        instance->push_request_write_rows.increment(110);
        auto metric = metrics->get_metric("push_request_write_rows");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("110", metric->to_string().c_str());
    }
    // engine request
    {
        instance->create_tablet_requests_total.increment(15);
        auto metric = metrics->get_metric("engine_requests_total",
                                          MetricLabels().add("type", "create_tablet").add("status", "total"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("15", metric->to_string().c_str());
    }
    {
        instance->drop_tablet_requests_total.increment(16);
        auto metric = metrics->get_metric("engine_requests_total",
                                          MetricLabels().add("type", "drop_tablet").add("status", "total"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("16", metric->to_string().c_str());
    }
    {
        instance->report_all_tablets_requests_total.increment(17);
        auto metric = metrics->get_metric("engine_requests_total",
                                          MetricLabels().add("type", "report_all_tablets").add("status", "total"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("17", metric->to_string().c_str());
    }
    {
        instance->report_tablet_requests_total.increment(18);
        auto metric = metrics->get_metric("engine_requests_total",
                                          MetricLabels().add("type", "report_tablet").add("status", "total"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("18", metric->to_string().c_str());
    }
    {
        instance->schema_change_requests_total.increment(19);
        auto metric = metrics->get_metric("engine_requests_total",
                                          MetricLabels().add("type", "schema_change").add("status", "total"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("19", metric->to_string().c_str());
    }
    {
        instance->create_rollup_requests_total.increment(20);
        auto metric = metrics->get_metric("engine_requests_total",
                                          MetricLabels().add("type", "create_rollup").add("status", "total"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("20", metric->to_string().c_str());
    }
    {
        instance->storage_migrate_requests_total.increment(21);
        auto metric = metrics->get_metric("engine_requests_total",
                                          MetricLabels().add("type", "storage_migrate").add("status", "total"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("21", metric->to_string().c_str());
    }
    {
        instance->delete_requests_total.increment(22);
        auto metric = metrics->get_metric("engine_requests_total",
                                          MetricLabels().add("type", "delete").add("status", "total"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("22", metric->to_string().c_str());
    }
    //  comapction
    {
        instance->base_compaction_deltas_total.increment(30);
        auto metric = metrics->get_metric("compaction_deltas_total", MetricLabels().add("type", "base"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("30", metric->to_string().c_str());
    }
    {
        instance->cumulative_compaction_deltas_total.increment(31);
        auto metric = metrics->get_metric("compaction_deltas_total", MetricLabels().add("type", "cumulative"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("31", metric->to_string().c_str());
    }
    {
        instance->base_compaction_bytes_total.increment(32);
        auto metric = metrics->get_metric("compaction_bytes_total", MetricLabels().add("type", "base"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("32", metric->to_string().c_str());
    }
    {
        instance->cumulative_compaction_bytes_total.increment(33);
        auto metric = metrics->get_metric("compaction_bytes_total", MetricLabels().add("type", "cumulative"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("33", metric->to_string().c_str());
    }
    // Gauge
    {
        instance->memory_pool_bytes_total.increment(40);
        auto metric = metrics->get_metric("memory_pool_bytes_total");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("40", metric->to_string().c_str());
    }
}

TEST_F(StarRocksMetricsTest, PageCacheMetrics) {
    TestMetricsVisitor visitor;
    auto instance = StarRocksMetrics::instance();
    auto metrics = instance->metrics();
    metrics->collect(&visitor);
    LOG(INFO) << "\n" << visitor.to_string();

    auto lookup_metric = metrics->get_metric("page_cache_lookup_count");
    ASSERT_TRUE(lookup_metric != nullptr);
    auto hit_metric = metrics->get_metric("page_cache_hit_count");
    ASSERT_TRUE(hit_metric != nullptr);
    auto capacity_metric = metrics->get_metric("page_cache_capacity");
    ASSERT_TRUE(capacity_metric != nullptr);
    {
        {
            StoragePageCache::CacheKey key("abc", 0);
            PageCacheHandle handle;
            Slice data(new char[1024], 1024);
            ASSERT_OK(_page_cache->insert(key, data, &handle, false));
            ASSERT_TRUE(_page_cache->lookup(key, &handle));
        }
        for (int i = 0; i < 1024; i++) {
            PageCacheHandle handle;
            StoragePageCache::CacheKey key(std::to_string(i), 0);
            _page_cache->lookup(key, &handle);
        }
    }
    config::enable_metric_calculator = false;
    metrics->collect(&visitor);
    ASSERT_STREQ("1025", lookup_metric->to_string().c_str());
    ASSERT_STREQ("1", hit_metric->to_string().c_str());
    ASSERT_STREQ(std::to_string(_page_cache->get_capacity()).c_str(), capacity_metric->to_string().c_str());
}

void assert_threadpool_metrics_register(const std::string& pool_name, MetricRegistry* instance) {
    ASSERT_TRUE(instance->get_metric(pool_name + "_threadpool_size") != nullptr);
    ASSERT_TRUE(instance->get_metric(pool_name + "_executed_tasks_total") != nullptr);
    ASSERT_TRUE(instance->get_metric(pool_name + "_pending_time_ns_total") != nullptr);
    ASSERT_TRUE(instance->get_metric(pool_name + "_execute_time_ns_total") != nullptr);
    ASSERT_TRUE(instance->get_metric(pool_name + "_queue_count") != nullptr);
}

TEST_F(StarRocksMetricsTest, test_metrics_register) {
    auto instance = StarRocksMetrics::instance()->metrics();
    ASSERT_NE(nullptr, instance->get_metric("memtable_flush_total"));
    ASSERT_NE(nullptr, instance->get_metric("memtable_flush_duration_us"));
    ASSERT_NE(nullptr, instance->get_metric("memtable_flush_io_time_us"));
    ASSERT_NE(nullptr, instance->get_metric("memtable_flush_memory_bytes_total"));
    ASSERT_NE(nullptr, instance->get_metric("memtable_flush_disk_bytes_total"));
    ASSERT_NE(nullptr, instance->get_metric("segment_flush_total"));
    ASSERT_NE(nullptr, instance->get_metric("segment_flush_duration_us"));
    ASSERT_NE(nullptr, instance->get_metric("segment_flush_io_time_us"));
    ASSERT_NE(nullptr, instance->get_metric("segment_flush_bytes_total"));
    assert_threadpool_metrics_register("async_delta_writer", instance);
    assert_threadpool_metrics_register("memtable_flush", instance);
    assert_threadpool_metrics_register("lake_memtable_flush", instance);
    assert_threadpool_metrics_register("segment_replicate", instance);
    assert_threadpool_metrics_register("segment_flush", instance);
    assert_threadpool_metrics_register("update_apply", instance);
    assert_threadpool_metrics_register("pk_index_compaction", instance);
    assert_threadpool_metrics_register("drop", instance);
    assert_threadpool_metrics_register("create_tablet", instance);
    assert_threadpool_metrics_register("alter_tablet", instance);
    assert_threadpool_metrics_register("clear_transaction", instance);
    assert_threadpool_metrics_register("storage_medium_migrate", instance);
    assert_threadpool_metrics_register("check_consistency", instance);
    assert_threadpool_metrics_register("manual_compaction", instance);
    assert_threadpool_metrics_register("compaction_control", instance);
    assert_threadpool_metrics_register("update_schema", instance);
    assert_threadpool_metrics_register("upload", instance);
    assert_threadpool_metrics_register("download", instance);
    assert_threadpool_metrics_register("make_snapshot", instance);
    assert_threadpool_metrics_register("release_snapshot", instance);
    assert_threadpool_metrics_register("move_dir", instance);
    assert_threadpool_metrics_register("update_tablet_meta_info", instance);
    assert_threadpool_metrics_register("drop_auto_increment_map_dir", instance);
    assert_threadpool_metrics_register("clone", instance);
    assert_threadpool_metrics_register("remote_snapshot", instance);
    assert_threadpool_metrics_register("replicate_snapshot", instance);
    assert_threadpool_metrics_register("load_channel", instance);
    ASSERT_NE(nullptr, instance->get_metric("load_channel_add_chunks_total"));
    ASSERT_NE(nullptr, instance->get_metric("load_channel_add_chunks_eos_total"));
    ASSERT_NE(nullptr, instance->get_metric("load_channel_add_chunks_duration_us"));
    ASSERT_NE(nullptr, instance->get_metric("load_channel_add_chunks_wait_memtable_duration_us"));
    ASSERT_NE(nullptr, instance->get_metric("load_channel_add_chunks_wait_writer_duration_us"));
    ASSERT_NE(nullptr, instance->get_metric("load_channel_add_chunks_wait_replica_duration_us"));
    ASSERT_NE(nullptr, instance->get_metric("async_delta_writer_execute_total"));
    ASSERT_NE(nullptr, instance->get_metric("async_delta_writer_task_total"));
    ASSERT_NE(nullptr, instance->get_metric("async_delta_writer_task_execute_duration_us"));
    ASSERT_NE(nullptr, instance->get_metric("async_delta_writer_task_pending_duration_us"));
    ASSERT_NE(nullptr, instance->get_metric("delta_writer_commit_task_total"));
    ASSERT_NE(nullptr, instance->get_metric("delta_writer_wait_flush_task_total"));
    ASSERT_NE(nullptr, instance->get_metric("delta_writer_wait_flush_duration_us"));
    ASSERT_NE(nullptr, instance->get_metric("delta_writer_pk_preload_duration_us"));
    ASSERT_NE(nullptr, instance->get_metric("delta_writer_wait_replica_duration_us"));
    ASSERT_NE(nullptr, instance->get_metric("delta_writer_txn_commit_duration_us"));
    ASSERT_NE(nullptr, instance->get_metric("memtable_finalize_duration_us"));
}

} // namespace starrocks
