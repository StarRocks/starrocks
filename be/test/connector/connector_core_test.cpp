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

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <type_traits>

#include "connector/connector.h"

namespace starrocks::connector {

class ScanOnlyConnector final : public Connector {
public:
    DataSourceProviderPtr create_data_source_provider(ConnectorScanNode* scan_node,
                                                      const TPlanNode& plan_node) const override {
        return nullptr;
    }

    ConnectorType connector_type() const override { return ConnectorType::FILE; }
};

TEST(ConnectorCoreTest, ConnectorContractHeadersCompileWithoutConcreteConnector) {
    static_assert(std::is_base_of_v<Connector, ScanOnlyConnector>);
    static_assert(std::is_same_v<decltype(std::declval<const ScanOnlyConnector&>().connector_type()), ConnectorType>);

    ScanOnlyConnector connector;
    EXPECT_EQ(ConnectorType::FILE, connector.connector_type());
    EXPECT_EQ("file", Connector::FILE);
}

} // namespace starrocks::connector
