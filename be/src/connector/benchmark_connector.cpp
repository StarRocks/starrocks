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

#include "connector/benchmark_connector.h"

#include <algorithm>

#include "runtime/runtime_state.h"

namespace starrocks::connector {

DataSourceProviderPtr BenchmarkConnector::create_data_source_provider(ConnectorScanNode* scan_node,
                                                                      const TPlanNode& plan_node) const {
    return std::make_unique<BenchmarkDataSourceProvider>(scan_node, plan_node);
}

BenchmarkDataSourceProvider::BenchmarkDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node)
        : _scan_node(scan_node), _benchmark_scan_node(plan_node.benchmark_scan_node) {}

DataSourcePtr BenchmarkDataSourceProvider::create_data_source(const TScanRange& scan_range) {
    return std::make_unique<BenchmarkDataSource>(this, scan_range);
}

const TupleDescriptor* BenchmarkDataSourceProvider::tuple_descriptor(RuntimeState* state) const {
    return state->desc_tbl().get_tuple_descriptor(_benchmark_scan_node.tuple_id);
}

BenchmarkDataSource::BenchmarkDataSource(const BenchmarkDataSourceProvider* provider, const TScanRange& scan_range)
        : _provider(provider) {
    if (scan_range.__isset.benchmark_scan_range) {
        _benchmark_scan_range = scan_range.benchmark_scan_range;
        _has_scan_range = true;
    }
}

std::string BenchmarkDataSource::name() const {
    return "BenchmarkDataSource";
}

Status BenchmarkDataSource::open(RuntimeState* state) {
    RETURN_IF_ERROR(_init_params(state));
    RETURN_IF_ERROR(_scanner->open(state));
    return Status::OK();
}

void BenchmarkDataSource::close(RuntimeState* state) {
    if (_scanner != nullptr) {
        _scanner->close(state);
    }
}

Status BenchmarkDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    bool eos = false;
    do {
        RETURN_IF_ERROR(_scanner->get_next(state, chunk, &eos));
    } while (!eos && (*chunk)->num_rows() == 0);
    if (eos) {
        return Status::EndOfFile("");
    }
    _rows_read += (*chunk)->num_rows();
    _bytes_read += (*chunk)->bytes_usage();
    return Status::OK();
}

int64_t BenchmarkDataSource::raw_rows_read() const {
    return _rows_read;
}

int64_t BenchmarkDataSource::num_rows_read() const {
    return _rows_read;
}

int64_t BenchmarkDataSource::num_bytes_read() const {
    return _bytes_read;
}

int64_t BenchmarkDataSource::cpu_time_spent() const {
    return 0;
}

Status BenchmarkDataSource::_init_params(RuntimeState* state) {
    const TBenchmarkScanNode& scan_node = _provider->_benchmark_scan_node;
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(scan_node.tuple_id);
    if (_tuple_desc == nullptr) {
        return Status::InternalError("Failed to get tuple descriptor for benchmark scan");
    }

    BenchmarkScannerParam param;
    param.db_name = scan_node.db_name;
    param.table_name = scan_node.table_name;
    param.options.scale_factor = scan_node.__isset.scale_factor ? scan_node.scale_factor : 1.0;
    param.options.start_row = _benchmark_scan_range.start_row;
    if (_has_scan_range && _benchmark_scan_range.__isset.row_count) {
        param.options.row_count = _benchmark_scan_range.row_count;
    } else {
        param.options.row_count = -1;
    }
    param.options.chunk_size = state->chunk_size();
    if (_read_limit != -1) {
        if (param.options.row_count < 0) {
            param.options.row_count = _read_limit;
        } else {
            param.options.row_count = std::min(param.options.row_count, _read_limit);
        }
    }

    _scanner = std::make_unique<BenchmarkScanner>(std::move(param), _tuple_desc);
    return Status::OK();
}

} // namespace starrocks::connector
