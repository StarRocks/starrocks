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

#pragma once

#include <map>
#include <memory>

#include "util/metrics.h"

namespace starrocks {

class CpuMetrics;
class DiskMetrics;
class NetMetrics;
class FileDescriptorMetrics;
class SnmpMetrics;
class QueryCacheMetrics;
class VectorIndexCacheMetrics;
class RuntimeFilterMetrics;
class VectorIndexCacheMetrics;

class IOMetrics {
public:
    METRIC_DEFINE_INT_GAUGE(read_ops, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(read_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(write_ops, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(write_bytes, MetricUnit::BYTES);
};

class MemoryMetrics {
public:
    METRIC_DEFINE_INT_GAUGE(jemalloc_allocated_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jemalloc_active_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jemalloc_metadata_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jemalloc_metadata_thp, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(jemalloc_resident_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jemalloc_mapped_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jemalloc_retained_bytes, MetricUnit::BYTES);

    // MemPool metrics
    METRIC_DEFINE_INT_GAUGE(process_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(query_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(load_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(metadata_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(tablet_metadata_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(rowset_metadata_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(segment_metadata_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(column_metadata_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(tablet_schema_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(column_zonemap_index_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(ordinal_index_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(bitmap_index_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(bloom_filter_index_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(segment_zonemap_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(short_key_index_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(compaction_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(schema_change_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(storage_page_cache_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(jit_cache_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(update_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(chunk_allocator_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(passthrough_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(clone_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(consistency_mem_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(datacache_mem_bytes, MetricUnit::BYTES);
};

class SystemMetrics {
public:
    SystemMetrics();
    ~SystemMetrics();

    // install system metrics to registry
    void install(MetricRegistry* registry, const std::set<std::string>& disk_devices,
                 const std::vector<std::string>& network_interfaces);

    // update metrics
    void update();

    void get_disks_io_time(std::map<std::string, int64_t>* map);
    int64_t get_max_io_util(const std::map<std::string, int64_t>& lst_value, int64_t interval_sec);

    void get_network_traffic(std::map<std::string, int64_t>* send_map, std::map<std::string, int64_t>* rcv_map);
    void get_max_net_traffic(const std::map<std::string, int64_t>& lst_send_map,
                             const std::map<std::string, int64_t>& lst_rcv_map, int64_t interval_sec,
                             int64_t* send_rate, int64_t* rcv_rate);
    const MemoryMetrics* memory_metrics() const { return _memory_metrics.get(); }
    IOMetrics* get_io_metrics_by_tag(uint32_t tag) const { return !_io_metrics.empty() ? _io_metrics[tag] : nullptr; }

private:
    void _install_cpu_metrics(MetricRegistry*);
    // On Intel(R) Xeon(R) CPU E5-2450 0 @ 2.10GHz;
    // read /proc/stat would cost about 170us
    void _update_cpu_metrics();

    void _install_memory_metrics(MetricRegistry* registry);
    void _update_memory_metrics();

    void _install_disk_metrics(MetricRegistry* registry, const std::set<std::string>& devices);
    void _update_disk_metrics();

    void _install_net_metrics(MetricRegistry* registry, const std::vector<std::string>& interfaces);
    void _update_net_metrics();

    void _install_fd_metrics(MetricRegistry* registry);

    void _update_fd_metrics();

    void _install_snmp_metrics(MetricRegistry* registry);

    void _update_snmp_metrics();

    void _install_query_cache_metrics(MetricRegistry* registry);

    void _update_query_cache_metrics();

    void _install_runtime_filter_metrics(MetricRegistry* registry);

    void _update_runtime_filter_metrics();

    void _install_vector_index_cache_metrics(MetricRegistry* registry);

    void _update_vector_index_cache_metrics();

    void _install_io_metrics(MetricRegistry* registry);

private:
    static const char* const _s_hook_name;

    std::unique_ptr<CpuMetrics> _cpu_metrics;
    std::unique_ptr<MemoryMetrics> _memory_metrics;
    std::map<std::string, DiskMetrics*> _disk_metrics;
    std::map<std::string, NetMetrics*> _net_metrics;
    std::unique_ptr<FileDescriptorMetrics> _fd_metrics;
    std::unique_ptr<QueryCacheMetrics> _query_cache_metrics;
    std::unique_ptr<VectorIndexCacheMetrics> _vector_index_cache_metrics;
    std::map<std::string, RuntimeFilterMetrics*> _runtime_filter_metrics;
    int _proc_net_dev_version = 0;
    std::unique_ptr<SnmpMetrics> _snmp_metrics;
    std::vector<IOMetrics*> _io_metrics;

    char* _line_ptr = nullptr;
    size_t _line_buf_size = 0;
    MetricRegistry* _registry = nullptr;
};

} // namespace starrocks
