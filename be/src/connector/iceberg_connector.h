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

#pragma once

#include "column/vectorized_fwd.h"
#include "connector/connector.h"
#include "exec/connector_scan_node.h"
#include "exec/hdfs_scanner.h"

namespace starrocks::connector {

class IcebergConnector final : public Connector {
public:
    ~IcebergConnector() override = default;

    ConnectorType connector_type() const override { return ConnectorType::HIVE; }

    std::unique_ptr<ConnectorChunkSinkProvider> create_data_sink_provider() const override;
};

} // namespace starrocks::connector
