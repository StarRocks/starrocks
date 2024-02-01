//
// Created by Letian Jiang on 2024/2/1.
//

#include "iceberg_connector.h"
#include "connector_sink/iceberg_chunk_sink.h"

namespace starrocks::connector {

std::unique_ptr<ConnectorChunkSinkProvider> IcebergConnector::create_data_sink_provider() const {
    return std::make_unique<IcebergChunkSinkProvider>();
}

} // namespace starrocks::connector
