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

#include <gtest/gtest.h>

#include <mutex>

#include "agent/agent_metrics.h"
#include "base/testutil/assert.h"
#include "cache/mem_cache/lrucache_engine.h"
#include "cache/mem_cache/page_cache.h"
#include "common/config_metrics_fwd.h"
#include "common/metrics/process_metrics_registry.h"
#include "exec/pipeline/pipeline_metrics.h"
#include "http/http_metrics.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_metrics.h"
#include "runtime/stream_load/stream_load_metrics.h"
#include "service/backend_metrics_initializer.h"
#include "service/service_metrics.h"
#include "storage/storage_metrics.h"
#include "util/metrics/catalog_scan_metrics.h"
#include "util/metrics/query_scan_metrics.h"

namespace starrocks {

namespace {

const char* const kTestDiskPath = "test_metric_path";

ProcessMetricsRegistry* backend_process_metrics_registry_for_test() {
    if (auto* registry = ExecEnv::GetInstance()->process_metrics_registry(); registry != nullptr) {
        return registry;
    }
    static auto* registry = new ProcessMetricsRegistry("starrocks_be");
    return registry;
}

MetricRegistry* backend_metrics_registry_for_test() {
    return backend_process_metrics_registry_for_test()->root_registry();
}

void init_backend_metrics_for_test() {
    static std::once_flag once;
    std::call_once(once, [] {
        BackendMetricsInitOptions options;
        options.storage_paths.emplace_back(kTestDiskPath);
        options.collect_hook_enabled = true;
        options.init_system_metrics = false;
        options.init_jvm_metrics = false;
        BackendMetricsInitializer::initialize(backend_process_metrics_registry_for_test(), options);
    });
}

} // namespace

class BackendMetricsTest : public testing::Test {
public:
    BackendMetricsTest() = default;
    ~BackendMetricsTest() override = default;

protected:
    void SetUp() override {
        init_backend_metrics_for_test();

        MemCacheOptions opts{.mem_space_size = 10 * 1024 * 1024};
        _lru_cache = std::make_shared<LRUCacheEngine>();
        ASSERT_OK(_lru_cache->init(opts));

        _page_cache = std::make_shared<StoragePageCache>();
        _page_cache->init(_lru_cache.get());
        _page_cache->init_metrics(backend_metrics_registry_for_test());
    }

    void TearDown() override {}

    std::shared_ptr<LRUCacheEngine> _lru_cache = nullptr;
    std::shared_ptr<StoragePageCache> _page_cache = nullptr;
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

TEST_F(BackendMetricsTest, Normal) {
    TestMetricsVisitor visitor;
    auto agent_metrics = AgentMetrics::instance();
    auto http_metrics = HttpMetrics::instance();
    auto query_scan_metrics = QueryScanMetrics::instance();
    auto runtime_metrics = RuntimeMetrics::instance();
    auto service_metrics = ServiceMetrics::instance();
    auto storage_metrics = StorageMetrics::instance();
    auto metrics = backend_metrics_registry_for_test();
    metrics->collect(&visitor);
    // check metric
    {
        runtime_metrics->fragment_requests_total.increment(12);
        auto metric = metrics->get_metric("fragment_requests_total");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("12", metric->to_string().c_str());
    }
    {
        runtime_metrics->fragment_request_duration_us.increment(101);
        auto metric = metrics->get_metric("fragment_request_duration_us");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("101", metric->to_string().c_str());
    }
    {
        runtime_metrics->lake_txn_log_collect_legacy_total.increment(1);
        auto metric = metrics->get_metric("lake_txn_log_collect_legacy_total");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("1", metric->to_string().c_str());
    }
    {
        http_metrics->http_requests_total.increment(102);
        auto metric = metrics->get_metric("http_requests_total");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("102", metric->to_string().c_str());
    }
    {
        http_metrics->http_request_send_bytes.increment(104);
        auto metric = metrics->get_metric("http_request_send_bytes");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("104", metric->to_string().c_str());
    }
    {
        service_metrics->short_circuit_request_total.increment(1);
        auto metric = metrics->get_metric("short_circuit_request_total");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("1", metric->to_string().c_str());
    }
    {
        query_scan_metrics->query_scan_bytes.increment(104);
        auto metric = metrics->get_metric("query_scan_bytes");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("104", metric->to_string().c_str());
    }
    {
        query_scan_metrics->query_scan_rows.increment(105);
        auto metric = metrics->get_metric("query_scan_rows");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("105", metric->to_string().c_str());
    }
    {
        storage_metrics->push_requests_success_total.increment(106);
        auto metric = metrics->get_metric("push_requests_total", MetricLabels().add("status", "SUCCESS"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("106", metric->to_string().c_str());
    }
    {
        storage_metrics->push_requests_fail_total.increment(107);
        auto metric = metrics->get_metric("push_requests_total", MetricLabels().add("status", "FAIL"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("107", metric->to_string().c_str());
    }
    {
        storage_metrics->push_request_duration_us.increment(108);
        auto metric = metrics->get_metric("push_request_duration_us");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("108", metric->to_string().c_str());
    }
    {
        storage_metrics->push_request_write_bytes.increment(109);
        auto metric = metrics->get_metric("push_request_write_bytes");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("109", metric->to_string().c_str());
    }
    {
        storage_metrics->push_request_write_rows.increment(110);
        auto metric = metrics->get_metric("push_request_write_rows");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("110", metric->to_string().c_str());
    }
    // engine request
    {
        agent_metrics->create_tablet_requests_total.increment(15);
        auto metric = metrics->get_metric("engine_requests_total",
                                          MetricLabels().add("type", "create_tablet").add("status", "total"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("15", metric->to_string().c_str());
    }
    {
        agent_metrics->drop_tablet_requests_total.increment(16);
        auto metric = metrics->get_metric("engine_requests_total",
                                          MetricLabels().add("type", "drop_tablet").add("status", "total"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("16", metric->to_string().c_str());
    }
    {
        agent_metrics->report_all_tablets_requests_total.increment(17);
        auto metric = metrics->get_metric("engine_requests_total",
                                          MetricLabels().add("type", "report_all_tablets").add("status", "total"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("17", metric->to_string().c_str());
    }
    {
        agent_metrics->report_tablet_requests_total.increment(18);
        auto metric = metrics->get_metric("engine_requests_total",
                                          MetricLabels().add("type", "report_tablet").add("status", "total"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("18", metric->to_string().c_str());
    }
    {
        agent_metrics->schema_change_requests_total.increment(19);
        auto metric = metrics->get_metric("engine_requests_total",
                                          MetricLabels().add("type", "schema_change").add("status", "total"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("19", metric->to_string().c_str());
    }
    {
        agent_metrics->create_rollup_requests_total.increment(20);
        auto metric = metrics->get_metric("engine_requests_total",
                                          MetricLabels().add("type", "create_rollup").add("status", "total"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("20", metric->to_string().c_str());
    }
    {
        storage_metrics->storage_migrate_requests_total.increment(21);
        auto metric = metrics->get_metric("engine_requests_total",
                                          MetricLabels().add("type", "storage_migrate").add("status", "total"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("21", metric->to_string().c_str());
    }
    {
        storage_metrics->delete_requests_total.increment(22);
        auto metric = metrics->get_metric("engine_requests_total",
                                          MetricLabels().add("type", "delete").add("status", "total"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("22", metric->to_string().c_str());
    }
    {
        agent_metrics->clone_requests_total.increment(23);
        auto metric = metrics->get_metric("engine_requests_total",
                                          MetricLabels().add("type", "clone").add("status", "total"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("23", metric->to_string().c_str());
    }
    //  comapction
    {
        storage_metrics->base_compaction_deltas_total.increment(30);
        auto metric = metrics->get_metric("compaction_deltas_total", MetricLabels().add("type", "base"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("30", metric->to_string().c_str());
    }
    {
        storage_metrics->cumulative_compaction_deltas_total.increment(31);
        auto metric = metrics->get_metric("compaction_deltas_total", MetricLabels().add("type", "cumulative"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("31", metric->to_string().c_str());
    }
    {
        storage_metrics->base_compaction_bytes_total.increment(32);
        auto metric = metrics->get_metric("compaction_bytes_total", MetricLabels().add("type", "base"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("32", metric->to_string().c_str());
    }
    {
        storage_metrics->cumulative_compaction_bytes_total.increment(33);
        auto metric = metrics->get_metric("compaction_bytes_total", MetricLabels().add("type", "cumulative"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("33", metric->to_string().c_str());
    }
    // Gauge
    {
        runtime_metrics->memory_pool_bytes_total.increment(40);
        auto metric = metrics->get_metric("memory_pool_bytes_total");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("40", metric->to_string().c_str());
    }
}

TEST_F(BackendMetricsTest, PageCacheMetrics) {
    TestMetricsVisitor visitor;
    auto metrics = backend_metrics_registry_for_test();
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
            std::string key("abc0");
            PageCacheHandle handle;
            auto data = std::make_unique<std::vector<uint8_t>>(1024);
            MemCacheWriteOptions opts;
            ASSERT_OK(_page_cache->insert(key, data.get(), opts, &handle));
            ASSERT_TRUE(_page_cache->lookup(key, &handle));
            data.release();
        }
        for (int i = 0; i < 1024; i++) {
            PageCacheHandle handle;
            std::string key(std::to_string(i));
            key.append("0");
            _page_cache->lookup(key, &handle);
        }
    }
    config::enable_metric_calculator = false;
    metrics->set_collect_hook_enabled(true);
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

TEST_F(BackendMetricsTest, test_metrics_register) {
    auto instance = backend_metrics_registry_for_test();
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
    assert_threadpool_metrics_register("merge_commit", instance);
    assert_threadpool_metrics_register("exec_state_report", instance);
    assert_threadpool_metrics_register("priority_exec_state_report", instance);
    assert_threadpool_metrics_register("automatic_partition", instance);
    assert_threadpool_metrics_register("lake_metadata_fetch", instance);
    ASSERT_NE(nullptr, instance->get_metric("disks_total_capacity", MetricLabels().add("path", kTestDiskPath)));
    ASSERT_NE(nullptr, instance->get_metric("http_requests_total"));
    ASSERT_NE(nullptr, instance->get_metric("http_request_send_bytes"));
    ASSERT_NE(nullptr, instance->get_metric("txn_request", MetricLabels().add("type", "begin")));
    ASSERT_NE(nullptr, instance->get_metric("txn_request", MetricLabels().add("type", "commit")));
    ASSERT_NE(nullptr, instance->get_metric("txn_request", MetricLabels().add("type", "rollback")));
    ASSERT_NE(nullptr, instance->get_metric("txn_request", MetricLabels().add("type", "exec")));
    ASSERT_NE(nullptr, instance->get_metric("stream_load", MetricLabels().add("type", "receive_bytes")));
    ASSERT_NE(nullptr, instance->get_metric("stream_load", MetricLabels().add("type", "load_rows")));
    ASSERT_NE(nullptr, instance->get_metric("load_rows"));
    ASSERT_NE(nullptr, instance->get_metric("load_bytes"));
    ASSERT_NE(nullptr, instance->get_metric("streaming_load_requests_total"));
    ASSERT_NE(nullptr, instance->get_metric("streaming_load_bytes"));
    ASSERT_NE(nullptr, instance->get_metric("streaming_load_duration_ms"));
    ASSERT_NE(nullptr, instance->get_metric("streaming_load_current_processing"));
    ASSERT_NE(nullptr, instance->get_metric("transaction_streaming_load_requests_total"));
    ASSERT_NE(nullptr, instance->get_metric("transaction_streaming_load_bytes"));
    ASSERT_NE(nullptr, instance->get_metric("transaction_streaming_load_duration_ms"));
    ASSERT_NE(nullptr, instance->get_metric("transaction_streaming_load_current_processing"));
    ASSERT_NE(nullptr, instance->get_metric("pipe_driver_schedule_count"));
    ASSERT_NE(nullptr, instance->get_metric("pipe_driver_overloaded"));
    ASSERT_NE(nullptr, instance->get_metric("files_scan_num_files_read",
                                            MetricLabels().add("file_format", "avro").add("scan_type", "insert")));
    ASSERT_NE(nullptr, instance->get_metric("query_spill_trigger_total", MetricLabels().add("storage_type", "local")));
    CatalogScanMetrics::instance()->update_scan_bytes("hive", 7);
    ASSERT_NE(nullptr, instance->get_metric("catalog_query_scan_bytes", MetricLabels().add("catalog_type", "hive")));
    ASSERT_NE(nullptr, instance->get_metric("query_scan_bytes"));
    ASSERT_NE(nullptr, instance->get_metric("query_scan_rows"));
    ASSERT_NE(nullptr, instance->get_metric("query_scan_bytes_per_second"));
    ASSERT_NE(nullptr, instance->get_metric("flat_json_segment_write_total"));
    ASSERT_NE(nullptr, instance->get_metric("flat_json_write_rows_total"));
    ASSERT_NE(nullptr, instance->get_metric("flat_json_paths_discovered_total"));
    ASSERT_NE(nullptr, instance->get_metric("flat_json_paths_extracted_total"));
    ASSERT_NE(nullptr, instance->get_metric("flat_json_access_hit_total"));
    ASSERT_NE(nullptr, instance->get_metric("flat_json_access_miss_total"));
    ASSERT_NE(nullptr, instance->get_metric("flat_json_cast_duration_ns_total"));
    ASSERT_NE(nullptr, instance->get_metric("flat_json_merge_duration_ns_total"));
    ASSERT_NE(nullptr, instance->get_metric("flat_json_flatten_duration_ns_total"));
    ASSERT_NE(nullptr, instance->get_metric("flat_json_compaction_total"));
    ASSERT_NE(nullptr, instance->get_metric("flat_json_compaction_schema_change_total"));
    ASSERT_NE(nullptr, instance->get_metric("load_channel_add_chunks_total"));
    ASSERT_NE(nullptr, instance->get_metric("load_channel_add_chunks_eos_total"));
    ASSERT_NE(nullptr, instance->get_metric("load_channel_add_chunks_duration_us"));
    ASSERT_NE(nullptr, instance->get_metric("load_channel_add_chunks_wait_memtable_duration_us"));
    ASSERT_NE(nullptr, instance->get_metric("load_channel_add_chunks_wait_writer_duration_us"));
    ASSERT_NE(nullptr, instance->get_metric("load_channel_add_chunks_wait_replica_duration_us"));
    ASSERT_NE(nullptr, instance->get_metric("lake_txn_log_collect_legacy_total"));
    ASSERT_NE(nullptr, instance->get_metric("lake_txn_log_collect_per_partition_total"));
    ASSERT_NE(nullptr, instance->get_metric("lake_txn_log_collect_orphan_partition_total"));
    ASSERT_NE(nullptr, instance->get_metric("short_circuit_request_total"));
    ASSERT_NE(nullptr, instance->get_metric("short_circuit_request_duration_us"));
    ASSERT_NE(nullptr, instance->get_metric("staros_shard_info_fallback_total"));
    ASSERT_NE(nullptr, instance->get_metric("staros_shard_info_fallback_failed_total"));
    ASSERT_NE(nullptr, instance->get_metric("meta_request_total", MetricLabels().add("type", "write")));
    ASSERT_NE(nullptr, instance->get_metric("meta_request_duration", MetricLabels().add("type", "read")));
    ASSERT_NE(nullptr, instance->get_metric("segment_read", MetricLabels().add("type", "segment_total_read_times")));
    ASSERT_NE(nullptr, instance->get_metric("segment_file_not_found_total"));
    ASSERT_NE(nullptr, instance->get_metric("primary_key_table_error_state_total"));
    ASSERT_NE(nullptr, instance->get_metric("pk_index_sst_read_error_total"));
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
    ASSERT_NE(nullptr, instance->get_metric("clone_task_copy_bytes", MetricLabels().add("type", "INTER_NODE")));
    ASSERT_NE(nullptr, instance->get_metric("clone_task_copy_bytes", MetricLabels().add("type", "INTRA_NODE")));
    ASSERT_NE(nullptr, instance->get_metric("clone_task_copy_duration_ms", MetricLabels().add("type", "INTER_NODE")));
    ASSERT_NE(nullptr, instance->get_metric("clone_task_copy_duration_ms", MetricLabels().add("type", "INTRA_NODE")));
}

} // namespace starrocks
