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

#include "storage/storage_metrics.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "common/thread/threadpool.h"

namespace starrocks {

namespace {

void assert_metric_value(MetricRegistry* registry, const std::string& name, const std::string& value) {
    auto* metric = registry->get_metric(name);
    ASSERT_NE(nullptr, metric);
    ASSERT_EQ(value, metric->to_string());
}

void assert_metric_value(MetricRegistry* registry, const std::string& name, const MetricLabels& labels,
                         const std::string& value) {
    auto* metric = registry->get_metric(name, labels);
    ASSERT_NE(nullptr, metric);
    ASSERT_EQ(value, metric->to_string());
}

} // namespace

TEST(StorageMetricsTest, InstallRegistersLoadMetrics) {
    MetricRegistry registry("test_registry");
    StorageMetrics metrics(&registry);
    metrics.install(&registry);

    metrics.push_requests_success_total.increment(3);
    assert_metric_value(&registry, "push_requests_total", MetricLabels().add("status", "SUCCESS"), "3");

    metrics.push_requests_fail_total.increment(4);
    assert_metric_value(&registry, "push_requests_total", MetricLabels().add("status", "FAIL"), "4");

    metrics.push_request_duration_us.increment(5);
    assert_metric_value(&registry, "push_request_duration_us", "5");

    metrics.push_request_write_bytes.increment(6);
    assert_metric_value(&registry, "push_request_write_bytes", "6");

    metrics.push_request_write_rows.increment(7);
    assert_metric_value(&registry, "push_request_write_rows", "7");

    metrics.storage_migrate_requests_total.increment(8);
    assert_metric_value(&registry, "engine_requests_total",
                        MetricLabels().add("type", "storage_migrate").add("status", "total"), "8");

    metrics.delete_requests_failed.increment(9);
    assert_metric_value(&registry, "engine_requests_total",
                        MetricLabels().add("type", "delete").add("status", "failed"), "9");
}

TEST(StorageMetricsTest, InstallRegistersCompactionMetrics) {
    MetricRegistry registry("test_registry");
    StorageMetrics metrics(&registry);

    metrics.base_compaction_request_total.increment(10);
    assert_metric_value(&registry, "engine_requests_total",
                        MetricLabels().add("type", "base_compaction").add("status", "total"), "10");

    metrics.cumulative_compaction_request_failed.increment(11);
    assert_metric_value(&registry, "engine_requests_total",
                        MetricLabels().add("type", "cumulative_compaction").add("status", "failed"), "11");

    metrics.update_compaction_request_total.increment(12);
    assert_metric_value(&registry, "engine_requests_total",
                        MetricLabels().add("type", "update_compaction").add("status", "total"), "12");

    metrics.base_compaction_deltas_total.increment(13);
    assert_metric_value(&registry, "compaction_deltas_total", MetricLabels().add("type", "base"), "13");

    metrics.cumulative_compaction_bytes_total.increment(14);
    assert_metric_value(&registry, "compaction_bytes_total", MetricLabels().add("type", "cumulative"), "14");

    metrics.update_compaction_outputs_bytes_total.increment(15);
    assert_metric_value(&registry, "update_compaction_outputs_bytes_total", MetricLabels().add("type", "update"), "15");

    metrics.update_compaction_duration_us.increment(16);
    assert_metric_value(&registry, "update_compaction_duration_us", MetricLabels().add("type", "update"), "16");

    metrics.running_update_compaction_task_num.set_value(17);
    assert_metric_value(&registry, "running_update_compaction_task_num", "17");
}

TEST(StorageMetricsTest, InstallRegistersFlushAndSpillMetrics) {
    MetricRegistry registry("test_registry");
    StorageMetrics metrics(&registry);

    metrics.async_delta_writer_execute_total.increment(18);
    assert_metric_value(&registry, "async_delta_writer_execute_total", "18");

    metrics.load_spill_remote_bytes_read_total.increment(19);
    assert_metric_value(&registry, "load_spill_remote_bytes_read_total", "19");

    metrics.delta_writer_wait_flush_duration_us.increment(20);
    assert_metric_value(&registry, "delta_writer_wait_flush_duration_us", "20");

    metrics.memtable_flush_memory_bytes_total.increment(21);
    assert_metric_value(&registry, "memtable_flush_memory_bytes_total", "21");

    metrics.segment_flush_bytes_total.increment(22);
    assert_metric_value(&registry, "segment_flush_bytes_total", "22");

    metrics.update_rowset_commit_apply_duration_us.increment(23);
    assert_metric_value(&registry, "update_rowset_commit_apply_duration_us", "23");
}

TEST(StorageMetricsTest, InstallRegistersMetaSegmentTxnAndUpdateMetrics) {
    MetricRegistry registry("test_registry");
    StorageMetrics metrics(&registry);

    metrics.meta_write_request_total.increment(24);
    assert_metric_value(&registry, "meta_request_total", MetricLabels().add("type", "write"), "24");

    metrics.meta_read_request_duration_us.increment(25);
    assert_metric_value(&registry, "meta_request_duration", MetricLabels().add("type", "read"), "25");

    metrics.segment_read_total.increment(26);
    assert_metric_value(&registry, "segment_read", MetricLabels().add("type", "segment_total_read_times"), "26");

    metrics.segment_rows_read_by_zone_map.increment(27);
    assert_metric_value(&registry, "segment_read", MetricLabels().add("type", "segment_rows_read_by_zone_map"), "27");

    metrics.txn_persist_total.increment(28);
    assert_metric_value(&registry, "txn_persist_total", "28");

    metrics.primary_key_table_error_state_total.increment(29);
    assert_metric_value(&registry, "primary_key_table_error_state_total", "29");

    metrics.pk_index_sst_read_error_total.increment(30);
    assert_metric_value(&registry, "pk_index_sst_read_error_total", "30");
}

TEST(StorageMetricsTest, InstallRegistersBlockManagerMetrics) {
    MetricRegistry registry("test_registry");
    StorageMetrics metrics(&registry);

    metrics.readable_blocks_total.increment(31);
    assert_metric_value(&registry, "readable_blocks_total", "31");

    metrics.bytes_written_total.increment(32);
    assert_metric_value(&registry, "bytes_written_total", "32");

    metrics.blocks_open_writing.set_value(33);
    assert_metric_value(&registry, "blocks_open_writing", "33");
}

TEST(StorageMetricsTest, RegisterGaugeHooksBeforeInstall) {
    StorageMetrics metrics;
    metrics.register_metadata_cache_bytes_total_hook([] { return 34; });
    metrics.register_unused_rowsets_count_hook([] { return 35; });
    metrics.register_rowset_count_generated_and_in_use_hook([] { return 36; });

    MetricRegistry registry("test_registry");
    metrics.install(&registry);
    registry.trigger_hook();

    assert_metric_value(&registry, "metadata_cache_bytes_total", "34");
    assert_metric_value(&registry, "unused_rowsets_count", "35");
    assert_metric_value(&registry, "rowset_count_generated_and_in_use", "36");
}

TEST(StorageMetricsTest, RegisterThreadPoolMetricsBeforeInstall) {
    std::unique_ptr<ThreadPool> threadpool;
    auto status = ThreadPoolBuilder("storage_metrics_test")
                          .set_min_threads(0)
                          .set_max_threads(3)
                          .set_max_queue_size(5)
                          .build(&threadpool);
    ASSERT_TRUE(status.ok()) << status;

    StorageMetrics metrics;
    metrics.register_thread_pool_metrics("pindex_load", threadpool.get());

    MetricRegistry registry("test_registry");
    metrics.install(&registry);
    registry.trigger_hook();

    assert_metric_value(&registry, "pindex_load_threadpool_size", "3");
    assert_metric_value(&registry, "pindex_load_queue_count", "0");
}

} // namespace starrocks
