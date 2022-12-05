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

#include "connector/binlog_connector.h"
#include "exec/exec_node.h"
#include "exprs/expr.h"

namespace starrocks::connector {
using namespace vectorized;

// ================================

DataSourceProviderPtr BinlogConnector::create_data_source_provider(vectorized::ConnectorScanNode* scan_node,
                                                                  const TPlanNode& plan_node) const {
    return std::make_unique<BinlogDataSourceProvider>(scan_node, plan_node);
}

// ================================
BinlogDataSourceProvider::BinlogDataSourceProvider(vectorized::ConnectorScanNode* scan_node, const TPlanNode& plan_node)
        : _scan_node(scan_node) {}

DataSourcePtr BinlogDataSourceProvider::create_data_source(const TScanRange& scan_range) {
    return std::make_unique<BinlogDataSource>(this, scan_range);
}

// ================================

BinlogDataSource::BinlogDataSource(const BinlogDataSourceProvider* provider, const TScanRange& scan_range)
        : _provider(provider), _scan_range(scan_range.binlog_scan_range) {}

Status BinlogDataSource::open(RuntimeState* state) {
    // todo get binlog manager and init binlogReader
    // binlog_manager.create_reader(vectorized::VectorizedSchema& schema,int chunk_size)
    return Status::OK();
}

Status BinlogDataSource::get_next(RuntimeState* state, vectorized::ChunkPtr* chunk) {
    SCOPED_RAW_TIMER(&_cpu_time_ns);

    //Status _status = _binlog_reader->get_next(chunk, -1);
    //vectorized::Chunk* ck = chunk->get();
    _rows_read_number += (*chunk) -> num_rows();
    _bytes_read += (*chunk) -> bytes_usage();
    return Status::OK();
}

void BinlogDataSource::close(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    //_binlog_reader->reset()
}

Status BinlogDataSource::set_offset(int64_t table_version, int64_t changelog_id) {
   // todo return _binlog_reader->seek(table_version, changelog_id);
   return Status::OK();
}


int64_t BinlogDataSource::raw_rows_read() const {
    return _rows_read_number;
}
int64_t BinlogDataSource::num_rows_read() const {
    return _rows_read_number;
}
int64_t BinlogDataSource::num_bytes_read() const {
    return _bytes_read;
}
int64_t BinlogDataSource::cpu_time_spent() const {
    return _cpu_time_ns;
}

} // namespace starrocks::connector
