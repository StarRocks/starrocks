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

#include "connector/mysql/mysql_connector.h"

#include <gtest/gtest.h>

#include <memory>

#include "common/config_exec_fwd.h"
#include "common/config_metrics_fwd.h"
#include "common/object_pool.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "types/type_descriptor.h"

namespace starrocks::connector {

class MySQLConnectorTest : public ::testing::Test {
public:
    void SetUp() override {
        config::enable_system_metrics = false;
        config::enable_metric_calculator = false;

        TUniqueId fragment_id;
        TQueryOptions query_options;
        TQueryGlobals query_globals;
        _runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals,
                                                        static_cast<const QueryExecutionServices*>(nullptr), nullptr);
        TUniqueId id;
        _runtime_state->init_mem_trackers(id);
        _pool = _runtime_state->obj_pool();
    }

protected:
    void _create_desc_tbl() {
        TDescriptorTableBuilder desc_tbl_builder;
        TTupleDescriptorBuilder tuple_desc_builder;
        TSlotDescriptorBuilder slot_desc_builder;
        slot_desc_builder.type(TYPE_INT).nullable(true).column_name("c1");
        tuple_desc_builder.add_slot(slot_desc_builder.build());
        tuple_desc_builder.build(&desc_tbl_builder);

        auto thrift_tbl = desc_tbl_builder.desc_tbl();
        auto* tbl = _pool->add(new DescriptorTbl());
        auto* tuple = _pool->add(new TupleDescriptor(thrift_tbl.tupleDescriptors[0]));
        tbl->_tuple_desc_map.emplace(tuple->id(), tuple);
        for (const auto& slot_desc : thrift_tbl.slotDescriptors) {
            auto* slot = _pool->add(new SlotDescriptor(slot_desc));
            tbl->_slot_desc_map.emplace(slot->id(), slot);
            tuple->add_slot(slot);
        }
        _runtime_state->set_desc_tbl(tbl);
    }

    std::shared_ptr<RuntimeState> _runtime_state = nullptr;
    ObjectPool* _pool = nullptr;
};

TEST_F(MySQLConnectorTest, ConnectorCreatesProviderForMySQLScan) {
    _create_desc_tbl();

    TMySQLScanNode scan_node;
    scan_node.__set_tuple_id(0);
    scan_node.__set_table_name("mysql_table");
    scan_node.__set_columns({"c1"});
    scan_node.__set_filters({});
    scan_node.__set_temporal_clause("FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 00:00:00'");

    TPlanNode plan_node;
    plan_node.__set_mysql_scan_node(scan_node);

    MySQLConnector connector;
    ASSERT_EQ(ConnectorType::MYSQL, connector.connector_type());

    auto provider = connector.create_data_source_provider(nullptr, plan_node);
    ASSERT_NE(provider, nullptr);
    ASSERT_TRUE(provider->insert_local_exchange_operator());
    ASSERT_FALSE(provider->accept_empty_scan_ranges());
    ASSERT_EQ(_runtime_state->desc_tbl().get_tuple_descriptor(0), provider->tuple_descriptor(_runtime_state.get()));

    TScanRange scan_range;
    auto data_source = provider->create_data_source(scan_range);
    ASSERT_NE(data_source, nullptr);
    ASSERT_EQ("MySQLDataSource", data_source->name());
    ASSERT_EQ(0, data_source->raw_rows_read());
    ASSERT_EQ(0, data_source->num_rows_read());
    ASSERT_EQ(0, data_source->num_bytes_read());
    ASSERT_EQ(0, data_source->cpu_time_spent());
}

} // namespace starrocks::connector
