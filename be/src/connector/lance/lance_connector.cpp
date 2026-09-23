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

#include "connector/lance/lance_connector.h"

#include "base/testutil/sync_point.h"
#include "connector/lance/lance_native_reader.h"
#include "exprs/chunk_predicate_evaluator.h"
#include "runtime/descriptors_ext.h"
#include "runtime/runtime_state.h"

namespace starrocks::connector {

DataSourceProviderPtr LanceConnector::create_data_source_provider(ConnectorScanNode* scan_node,
                                                                  const TPlanNode& plan_node) const {
    return std::make_unique<LanceDataSourceProvider>(scan_node, plan_node);
}

LanceDataSourceProvider::LanceDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node)
        : _scan_node(scan_node), _hdfs_scan_node(plan_node.hdfs_scan_node) {}

DataSourcePtr LanceDataSourceProvider::create_data_source(const TScanRange& scan_range) {
    return std::make_unique<LanceDataSource>(this, scan_range);
}

const TupleDescriptor* LanceDataSourceProvider::tuple_descriptor(RuntimeState* state) const {
    return state->desc_tbl().get_tuple_descriptor(_hdfs_scan_node.tuple_id);
}

LanceDataSource::LanceDataSource(const LanceDataSourceProvider* provider, const TScanRange& scan_range)
        : _provider(provider), _scan_range(scan_range.hdfs_scan_range) {}

LanceDataSource::~LanceDataSource() = default;

std::string LanceDataSource::name() const {
    return "LanceDataSource";
}

Status LanceDataSource::open(RuntimeState* state) {
    const auto& hdfs_scan_node = _provider->_hdfs_scan_node;
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(hdfs_scan_node.tuple_id);
    if (_tuple_desc == nullptr) {
        return Status::InternalError("Failed to get tuple descriptor for Lance scan");
    }

    // The dataset URI lives on TLanceTable in the table descriptor (the per-split
    // payload only carries fragment metadata); read it from LanceTableDescriptor.
    const auto* lance_table = dynamic_cast<const LanceTableDescriptor*>(_tuple_desc->table_desc());
    if (lance_table == nullptr) {
        return Status::InternalError("Failed to resolve LanceTableDescriptor for Lance scan");
    }

    if (_scan_range.__isset.lance_split_info && !_scan_range.lance_split_info.empty()) {
        return Status::NotSupported("Lance fragment splits are not supported yet");
    }
    TCloudConfiguration cloud;
    cloud.__set_cloud_type(TCloudType::DEFAULT);
    if (hdfs_scan_node.__isset.cloud_configuration) cloud = hdfs_scan_node.cloud_configuration;
    if (cloud.cloud_type != TCloudType::DEFAULT && cloud.cloud_type != TCloudType::AWS &&
        cloud.cloud_type != TCloudType::AZURE) {
        return Status::NotSupported("Unsupported Lance catalog cloud configuration");
    }
    _scanner = std::make_unique<LanceNativeReader>();
    TEST_SYNC_POINT_CALLBACK("LanceDataSource::open:scanner", &_scanner);
    RETURN_IF_ERROR(_scanner->open(state, _tuple_desc, std::string(lance_table->lance_dataset_uri()), cloud));
    return Status::OK();
}

void LanceDataSource::close(RuntimeState* state) {
    if (_scanner != nullptr) {
        _scanner->close();
        _scanner.reset();
    }
}

Status LanceDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    RETURN_IF_CANCELLED(state);
    auto status = _scanner->get_next(state, chunk);
    _reader_cpu_time_ns = _scanner->cpu_time_spent();
    _io_time_ns = _scanner->io_time_spent();
    RETURN_IF_ERROR(status);
    _raw_rows_read += (*chunk)->num_rows();
    _bytes_read += (*chunk)->bytes_usage();
    // Evaluate each residual exactly once, after native decoding and before LIMIT.
    {
        SCOPED_RAW_TIMER(&_filter_time_ns);
        RETURN_IF_ERROR(ChunkPredicateEvaluator::eval_conjuncts(_conjunct_ctxs, chunk->get()));
    }
    _rows_read += (*chunk)->num_rows();
    return Status::OK();
}

int64_t LanceDataSource::raw_rows_read() const {
    return _raw_rows_read;
}

int64_t LanceDataSource::num_rows_read() const {
    return _rows_read;
}

int64_t LanceDataSource::num_bytes_read() const {
    return _bytes_read;
}

int64_t LanceDataSource::cpu_time_spent() const {
    return _reader_cpu_time_ns + _filter_time_ns;
}

int64_t LanceDataSource::io_time_spent() const {
    return _io_time_ns;
}

} // namespace starrocks::connector
