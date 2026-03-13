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

#include "runtime/descriptors.h"

#include "connector/cache_stats_connector.h"

#include "connector/connector.h"
#include "exec/cache_stats_scanner.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace starrocks::connector {

DataSourceProviderPtr CacheStatsConnector::create_data_source_provider(ConnectorScanNode* scan_node,
                                                                       const TPlanNode& plan_node) const {
    (void)scan_node;
    return std::make_unique<CacheStatsDataSourceProvider>(plan_node);
}

CacheStatsDataSourceProvider::CacheStatsDataSourceProvider(const TPlanNode& plan_node)
        : _cache_stats_scan_node(plan_node.cache_stats_scan_node) {}

DataSourcePtr CacheStatsDataSourceProvider::create_data_source(const TScanRange& scan_range) {
    return std::make_unique<CacheStatsDataSource>(this, scan_range);
}

const TupleDescriptor* CacheStatsDataSourceProvider::tuple_descriptor(RuntimeState* state) const {
    return state->desc_tbl().get_tuple_descriptor(_cache_stats_scan_node.tuple_id);
}

CacheStatsDataSource::CacheStatsDataSource(const CacheStatsDataSourceProvider* provider, const TScanRange& scan_range)
        : _provider(provider), _scan_range(scan_range.internal_scan_range) {}

std::string CacheStatsDataSource::name() const {
    return "CacheStatsDataSource";
}

Status CacheStatsDataSource::open(RuntimeState* state) {
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_provider->_cache_stats_scan_node.tuple_id);
    if (_tuple_desc == nullptr) {
        return Status::InternalError("Failed to get tuple descriptor for cache stats scan");
    }

    _scanner = std::make_unique<starrocks::CacheStatsScanner>(_tuple_desc);
    RETURN_IF_ERROR(_scanner->init(state, _scan_range));
    RETURN_IF_ERROR(_scanner->open(state));
    return Status::OK();
}

void CacheStatsDataSource::close(RuntimeState* state) {
    if (_scanner != nullptr) {
        _scanner->close(state);
    }
}

Status CacheStatsDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    bool eos = false;
    do {
        RETURN_IF_ERROR(_scanner->get_chunk(state, chunk, &eos));
    } while (!eos && (*chunk)->num_rows() == 0);
    if (eos) {
        return Status::EndOfFile("");
    }
    _rows_read += (*chunk)->num_rows();
    _bytes_read += (*chunk)->bytes_usage();
    return Status::OK();
}

int64_t CacheStatsDataSource::raw_rows_read() const {
    return _rows_read;
}

int64_t CacheStatsDataSource::num_rows_read() const {
    return _rows_read;
}

int64_t CacheStatsDataSource::num_bytes_read() const {
    return _bytes_read;
}

int64_t CacheStatsDataSource::cpu_time_spent() const {
    return 0;
}

} // namespace starrocks::connector
