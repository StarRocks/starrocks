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

#include "exec/hdfs_scanner/jni_scanner.h"
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

std::string LanceDataSource::name() const {
    return "LanceDataSource";
}

Status LanceDataSource::open(RuntimeState* state) {
    const auto& hdfs_scan_node = _provider->_hdfs_scan_node;
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(hdfs_scan_node.tuple_id);
    if (_tuple_desc == nullptr) {
        return Status::InternalError("Failed to get tuple descriptor for Lance scan");
    }

    // Build JNI scanner params from the scan range
    std::map<std::string, std::string> jni_scanner_params;
    jni_scanner_params["lance_split_info"] = _scan_range.lance_split_info;
    jni_scanner_params["lance_dataset_uri"] = _scan_range.lance_dataset_uri;

    std::string scanner_factory_class = "com/starrocks/lance/reader/LanceSplitScannerFactory";
    _scanner = std::make_unique<JniScanner>(scanner_factory_class, jni_scanner_params);

    // Build minimal HdfsScannerParams needed by JniScanner
    HdfsScannerParams scanner_params;
    scanner_params.tuple_desc = _tuple_desc;
    for (auto* slot : _tuple_desc->slots()) {
        scanner_params.materialize_slots.push_back(slot);
    }
    scanner_params.scanner_conjunct_ctxs = _conjunct_ctxs;

    RETURN_IF_ERROR(_scanner->init(state, scanner_params));
    RETURN_IF_ERROR(_scanner->open(state));
    return Status::OK();
}

void LanceDataSource::close(RuntimeState* state) {
    if (_scanner != nullptr) {
        _scanner->close();
    }
}

Status LanceDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    RETURN_IF_ERROR(_init_chunk_if_needed(chunk, state->chunk_size()));
    Status status = _scanner->get_next(state, chunk);
    if (status.is_end_of_file()) {
        return status;
    }
    RETURN_IF_ERROR(status);
    _rows_read += (*chunk)->num_rows();
    _bytes_read += (*chunk)->bytes_usage();
    return Status::OK();
}

int64_t LanceDataSource::raw_rows_read() const {
    return _rows_read;
}

int64_t LanceDataSource::num_rows_read() const {
    return _rows_read;
}

int64_t LanceDataSource::num_bytes_read() const {
    return _bytes_read;
}

int64_t LanceDataSource::cpu_time_spent() const {
    return 0;
}

} // namespace starrocks::connector
