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

#include "connector/connector.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <type_traits>

namespace starrocks::connector {

class TestConnector final : public Connector {
public:
    DataSourceProviderPtr create_data_source_provider(ConnectorScanNode* scan_node,
                                                      const TPlanNode& plan_node) const override {
        return nullptr;
    }

    std::unique_ptr<ConnectorChunkSinkProvider> create_data_sink_provider() const override { return nullptr; }

    std::unique_ptr<ConnectorChunkSinkProvider> create_delete_sink_provider() const override { return nullptr; }

    std::unique_ptr<ConnectorChunkSinkProvider> create_row_delta_sink_provider() const override { return nullptr; }

    ConnectorType connector_type() const override { return ConnectorType::FILE; }
};

TEST(ConnectorCoreTest, ConnectorContractHeadersCompileWithoutConcreteConnector) {
    static_assert(std::is_base_of_v<Connector, TestConnector>);
    static_assert(std::is_same_v<decltype(std::declval<const TestConnector&>().connector_type()), ConnectorType>);

    EXPECT_EQ("file", Connector::FILE);
}

} // namespace starrocks::connector
