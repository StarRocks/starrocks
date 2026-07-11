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

#include "connector/iceberg/iceberg_connector.h"

#include "connector/iceberg/iceberg_chunk_sink.h"
#include "connector/iceberg/iceberg_delete_sink.h"
#include "connector/iceberg/iceberg_row_delta_sink.h"

namespace starrocks::connector {

StatusOr<std::unique_ptr<ConnectorSinkProvider>> IcebergConnector::create_sink_provider(
        ConnectorSinkProviderType type, std::shared_ptr<ConnectorSinkContext> context) const {
    switch (type) {
    case ConnectorSinkProviderType::DATA: {
        auto ctx = std::dynamic_pointer_cast<IcebergChunkSinkContext>(context);
        if (ctx == nullptr) {
            return Status::InternalError("Iceberg connector data sink requires IcebergChunkSinkContext");
        }
        std::unique_ptr<ConnectorSinkProvider> provider = std::make_unique<IcebergChunkSinkProvider>(std::move(ctx));
        return provider;
    }
    case ConnectorSinkProviderType::DELETE: {
        auto ctx = std::dynamic_pointer_cast<IcebergDeleteSinkContext>(context);
        if (ctx == nullptr) {
            return Status::InternalError("Iceberg connector delete sink requires IcebergDeleteSinkContext");
        }
        std::unique_ptr<ConnectorSinkProvider> provider = std::make_unique<IcebergDeleteSinkProvider>(std::move(ctx));
        return provider;
    }
    case ConnectorSinkProviderType::ROW_DELTA: {
        auto ctx = std::dynamic_pointer_cast<IcebergRowDeltaSinkContext>(context);
        if (ctx == nullptr) {
            return Status::InternalError("Iceberg connector row-delta sink requires IcebergRowDeltaSinkContext");
        }
        std::unique_ptr<ConnectorSinkProvider> provider = std::make_unique<IcebergRowDeltaSinkProvider>(std::move(ctx));
        return provider;
    }
    }
    return Status::NotSupported("Unknown Iceberg connector sink provider type");
}

} // namespace starrocks::connector
