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
#include "connector/hive/scanner/jni_scanner.h"
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

    std::map<std::string, std::string> jni_scanner_params;
    if (_scan_range.__isset.lance_split_info && !_scan_range.lance_split_info.empty()) {
        // The Java reader validates REST metadata and rejects unsupported fragment payloads.
        jni_scanner_params["lance_split_info"] = _scan_range.lance_split_info;
    }
    jni_scanner_params["lance_dataset_uri"] = std::string(lance_table->lance_dataset_uri());

    if (hdfs_scan_node.__isset.cloud_configuration) {
        const auto& cloud = hdfs_scan_node.cloud_configuration;
        switch (cloud.cloud_type) {
        case TCloudType::DEFAULT:
            jni_scanner_params["lance.cloud_type"] = "DEFAULT";
            break;
        case TCloudType::AWS:
            jni_scanner_params["lance.cloud_type"] = "AWS";
            break;
        case TCloudType::AZURE:
            jni_scanner_params["lance.cloud_type"] = "AZURE";
            break;
        default:
            return Status::NotSupported("Unsupported Lance catalog cloud configuration");
        }
        for (const auto& [key, value] : cloud.cloud_properties) {
            jni_scanner_params["lance.cloud." + key] = value;
        }
    }

    std::string scanner_factory_class = "com/starrocks/lance/reader/LanceSplitScannerFactory";
    _scanner = std::make_unique<JniScanner>(scanner_factory_class, jni_scanner_params);

    _scanner_ctx.tuple_desc = _tuple_desc;
    _scanner_ctx.runtime_filter_collector = _runtime_filters;
    _scanner_ctx.format_scan_context.conjuncts.all_ctxs = _conjunct_ctxs;
    // Lance does not push predicates into its reader; evaluate every scan conjunct on the decoded chunk.
    _scanner_ctx.format_scan_context.conjuncts.scanner_ctxs = _conjunct_ctxs;
    for (int i = 0; i < _tuple_desc->slots().size(); i++) {
        auto* slot = _tuple_desc->slots()[i];
        _scanner_ctx.materialize_slots.push_back(slot);
        _scanner_ctx.materialize_index_in_chunk.push_back(i);
    }
    _scanner_ctx.scan_range = &_scan_range;

    TEST_SYNC_POINT_CALLBACK("LanceDataSource::open:scanner", &_scanner);
    RETURN_IF_ERROR(_scanner->init(state, &_scanner_ctx));
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
