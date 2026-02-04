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

#include "connector/benchmark_connector.h"

#include <arrow/status.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "benchgen/benchmark_suite.h"
#include "benchgen/record_batch_iterator_factory.h"
#include "benchgen/table.h"
#include "common/config.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"

namespace starrocks::connector {

class BenchmarkConnectorTest : public ::testing::Test {
public:
    void SetUp() override {
        config::enable_system_metrics = false;
        config::enable_metric_calculator = false;

        _exec_env = ExecEnv::GetInstance();
        _runtime_state = _create_runtime_state();
        _pool = _runtime_state->obj_pool();
    }

protected:
    std::shared_ptr<RuntimeState> _create_runtime_state() {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.batch_size = 16;
        TQueryGlobals query_globals;
        auto runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, _exec_env);
        TUniqueId id;
        runtime_state->init_mem_trackers(id);
        return runtime_state;
    }

    DescriptorTbl* _create_table_desc(const std::string& column_name, const TypeDescriptor& type_desc) {
        TDescriptorTableBuilder desc_tbl_builder;
        TTupleDescriptorBuilder tuple_desc_builder;
        TSlotDescriptorBuilder slot_desc_builder;
        slot_desc_builder.type(type_desc)
                .length(type_desc.len)
                .precision(type_desc.precision)
                .scale(type_desc.scale)
                .nullable(true)
                .column_name(column_name);
        tuple_desc_builder.add_slot(slot_desc_builder.build());
        tuple_desc_builder.build(&desc_tbl_builder);

        DescriptorTbl* tbl = nullptr;
        CHECK(DescriptorTbl::create(_runtime_state.get(), _pool, desc_tbl_builder.desc_tbl(), &tbl,
                                    config::vector_chunk_size)
                      .ok());
        _runtime_state->set_desc_tbl(tbl);
        return tbl;
    }

    static bool _map_arrow_type(const std::shared_ptr<arrow::DataType>& type, TypeDescriptor* type_desc) {
        switch (type->id()) {
        case arrow::Type::BOOL:
            *type_desc = TypeDescriptor(TYPE_BOOLEAN);
            return true;
        case arrow::Type::INT8:
            *type_desc = TypeDescriptor(TYPE_TINYINT);
            return true;
        case arrow::Type::INT16:
            *type_desc = TypeDescriptor(TYPE_SMALLINT);
            return true;
        case arrow::Type::INT32:
            *type_desc = TypeDescriptor(TYPE_INT);
            return true;
        case arrow::Type::INT64:
            *type_desc = TypeDescriptor(TYPE_BIGINT);
            return true;
        case arrow::Type::FLOAT:
            *type_desc = TypeDescriptor(TYPE_FLOAT);
            return true;
        case arrow::Type::DOUBLE:
            *type_desc = TypeDescriptor(TYPE_DOUBLE);
            return true;
        case arrow::Type::STRING:
        case arrow::Type::LARGE_STRING:
            *type_desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
            return true;
        default:
            return false;
        }
    }

    static bool _pick_supported_field(const std::shared_ptr<arrow::Schema>& schema, std::string* column_name,
                                      TypeDescriptor* type_desc) {
        for (const auto& field : schema->fields()) {
            if (_map_arrow_type(field->type(), type_desc)) {
                *column_name = field->name();
                return true;
            }
        }
        return false;
    }

    ExecEnv* _exec_env = nullptr;
    std::shared_ptr<RuntimeState> _runtime_state = nullptr;
    ObjectPool* _pool = nullptr;
};

TEST_F(BenchmarkConnectorTest, ProviderTupleDescriptor) {
    _create_table_desc("col", TypeDescriptor(TYPE_INT));

    TBenchmarkScanNode scan_node;
    scan_node.__set_tuple_id(0);
    scan_node.__set_db_name("tpcds");
    scan_node.__set_table_name("call_center");
    scan_node.__set_scale_factor(1.0);

    TPlanNode plan_node;
    plan_node.__set_benchmark_scan_node(scan_node);

    BenchmarkDataSourceProvider provider(nullptr, plan_node);
    const TupleDescriptor* tuple_desc = provider.tuple_descriptor(_runtime_state.get());
    ASSERT_NE(tuple_desc, nullptr);
    ASSERT_EQ(tuple_desc, _runtime_state->desc_tbl().get_tuple_descriptor(0));
    ASSERT_TRUE(provider.insert_local_exchange_operator());
    ASSERT_FALSE(provider.accept_empty_scan_ranges());
}

TEST_F(BenchmarkConnectorTest, DataSourceOpenAndGetNext) {
    benchgen::SuiteId suite = benchgen::SuiteId::kTpcds;
    std::string db_name(benchgen::SuiteIdToString(suite));
    std::string table_name(benchgen::tpcds::TableIdToString(benchgen::tpcds::TableId::kCallCenter));

    benchgen::GeneratorOptions options;
    options.row_count = 3;
    options.chunk_size = 8;

    std::unique_ptr<benchgen::RecordBatchIterator> iter;
    auto status = benchgen::MakeRecordBatchIterator(suite, table_name, options, &iter);
    ASSERT_TRUE(status.ok()) << status.ToString();
    auto schema = iter->schema();
    ASSERT_NE(schema, nullptr);

    std::string column_name;
    TypeDescriptor type_desc;
    ASSERT_TRUE(_pick_supported_field(schema, &column_name, &type_desc));
    _create_table_desc(column_name, type_desc);

    TBenchmarkScanNode scan_node;
    scan_node.__set_tuple_id(0);
    scan_node.__set_db_name(db_name);
    scan_node.__set_table_name(table_name);
    scan_node.__set_scale_factor(1.0);

    TPlanNode plan_node;
    plan_node.__set_benchmark_scan_node(scan_node);

    BenchmarkDataSourceProvider provider(nullptr, plan_node);
    TBenchmarkScanRange benchmark_range;
    benchmark_range.__set_start_row(0);
    benchmark_range.__set_row_count(3);

    TScanRange scan_range;
    scan_range.__set_benchmark_scan_range(benchmark_range);
    auto data_source = provider.create_data_source(scan_range);

    ASSERT_OK(data_source->open(_runtime_state.get()));
    ChunkPtr chunk;
    Status next_status = data_source->get_next(_runtime_state.get(), &chunk);
    ASSERT_OK(next_status);
    ASSERT_NE(chunk, nullptr);
    ASSERT_EQ(chunk->num_columns(), 1);
    ASSERT_GT(chunk->num_rows(), 0);
    ASSERT_EQ(data_source->raw_rows_read(), data_source->num_rows_read());
    ASSERT_EQ(data_source->num_rows_read(), chunk->num_rows());
    ASSERT_EQ(data_source->num_bytes_read(), chunk->bytes_usage());
    ASSERT_GT(data_source->num_bytes_read(), 0);
    ASSERT_EQ(data_source->cpu_time_spent(), 0);
    ASSERT_EQ(data_source->name(), "BenchmarkDataSource");

    data_source->close(_runtime_state.get());
}

} // namespace starrocks::connector
