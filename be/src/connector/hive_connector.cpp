// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "connector/hive_connector.h"

namespace starrocks {
namespace connector {

DataSourceProviderPtr HiveConnector::create_data_source_provider(vectorized::ConnectorScanNode* scan_node,
                                                                 const TPlanNode& plan_node) const {
    return nullptr;
}

Status HiveDataSourceProvider::init(RuntimeState* state) {
    return Status::OK();
}
DataSourcePtr HiveDataSourceProvider::create_data_source(const TScanRange& scan_range) {
    return nullptr;
}

HiveDataSource::HiveDataSource(HiveDataSourceProvider* provider, const TScanRange& scan_range) : _provider(provider) {}

Status HiveDataSource::do_open(RuntimeState* state) {
    return Status::OK();
}
void HiveDataSource::do_close(RuntimeState* state) {
    return;
}
Status HiveDataSource::do_get_next(RuntimeState* state, vectorized::ChunkPtr* chunk) {
    return Status::OK();
}

} // namespace connector
} // namespace starrocks