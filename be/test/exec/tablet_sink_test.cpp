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

#include "exec/tablet_sink.h"

#include <gtest/gtest.h>

#include <fstream>
#include <sstream>

#include "base/string/slice.h"
#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "column/column.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "exec/tablet_info.h"
#include "runtime/decimalv2_value.h"
#include "runtime/descriptor_helper.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"

namespace starrocks {

class TabletSinkTest : public ::testing::Test {
public:
    void SetUp() override {
        _db_id = 1;
        _table_id = 2;
        _txn_id = 3;
        _exec_env = ExecEnv::GetInstance();
        _object_pool = std::make_unique<ObjectPool>();
        _desc_tbl = _build_descriptor_table();
        _data_sink = _build_data_sink();
    }

protected:
    std::unique_ptr<RuntimeState> _build_runtime_state() {
        TQueryOptions query_options;
        query_options.query_type = TQueryType::LOAD;
        TUniqueId fragment_id;
        TQueryGlobals query_globals;
        auto runtime_state = std::make_unique<RuntimeState>(fragment_id, query_options, query_globals, _exec_env);
        TUniqueId id;
        runtime_state->init_mem_trackers(id);
        runtime_state->set_db("test_db");
        runtime_state->set_load_label("test_label");
        runtime_state->set_txn_id(_txn_id);
        return runtime_state;
    }

    TDescriptorTable _build_descriptor_table() {
        TDescriptorTableBuilder dtb;
        TTupleDescriptorBuilder tuple_builder;
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_VARCHAR)
                                       .column_name("varchar_col")
                                       .column_pos(1)
                                       .length(10)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_DECIMALV2)
                                       .column_name("decimalv2_col")
                                       .column_pos(2)
                                       .precision(10)
                                       .scale(2)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_DECIMAL64)
                                       .column_name("decimal64_col")
                                       .column_pos(3)
                                       .precision(10)
                                       .scale(2)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder().type(TYPE_INT).column_name("int_col").column_pos(4).build());
        tuple_builder.build(&dtb);
        return dtb.desc_tbl();
    }

    TDataSink _build_data_sink() {
        TOlapTableSink table_sink;
        table_sink.load_id.hi = 0;
        table_sink.load_id.lo = 0;
        table_sink.db_id = _db_id;
        table_sink.db_name = "test";
        table_sink.table_id = _table_id;
        table_sink.table_name = "test";
        table_sink.txn_id = _txn_id;
        table_sink.num_replicas = 1;
        table_sink.keys_type = TKeysType::DUP_KEYS;
        table_sink.tuple_id = _desc_tbl.tupleDescriptors[0].id;

        TOlapTableSchemaParam& schema = table_sink.schema;
        schema.db_id = _db_id;
        schema.table_id = _table_id;
        schema.version = 0;
        schema.tuple_desc = _desc_tbl.tupleDescriptors[0];
        schema.slot_descs = _desc_tbl.slotDescriptors;
        schema.indexes.resize(1);
        schema.indexes[0].id = 0;
        schema.indexes[0].columns = {"varchar_col", "decimalv2_col", "decimal64_col", "int_col"};

        TOlapTablePartitionParam& partition = table_sink.partition;
        partition.db_id = _db_id;
        partition.table_id = _table_id;
        partition.version = 0;
        partition.distributed_columns.push_back("int_col");
        partition.partitions.resize(1);
        partition.partitions[0].id = 0;
        partition.partitions[0].indexes.resize(1);
        partition.partitions[0].indexes[0].index_id = 0;
        partition.partitions[0].indexes[0].tablet_ids.push_back(0);

        TOlapTableLocationParam& location = table_sink.location;
        location.db_id = _db_id;
        location.table_id = _table_id;
        location.version = 0;
        location.tablets.resize(1);
        location.tablets[0].tablet_id = 0;
        location.tablets[0].node_ids.push_back(0);

        TNodesInfo& nodes_info = table_sink.nodes_info;
        nodes_info.version = 0;
        nodes_info.nodes.resize(1);
        nodes_info.nodes[0].id = 0;
        nodes_info.nodes[0].option = 0;
        nodes_info.nodes[0].host = "127.0.0.1";
        nodes_info.nodes[0].async_internal_port = 8060;

        TDataSink data_sink;
        data_sink.__set_olap_table_sink(table_sink);
        return data_sink;
    }

    std::string _read_error_log_file(const std::string& relative_path, ExecEnv* exec_env) {
        if (relative_path.empty()) {
            return "";
        }
        std::string absolute_path = exec_env->load_path_mgr()->get_load_error_absolute_path(relative_path);
        std::ifstream file(absolute_path);
        if (!file.is_open()) {
            return "";
        }
        std::stringstream buffer;
        buffer << file.rdbuf();
        return buffer.str();
    }

    std::unique_ptr<OlapTableSink> _setup_sink(std::unique_ptr<RuntimeState>& runtime_state, DescriptorTbl*& desc_tbl) {
        runtime_state = _build_runtime_state();
        CHECK_OK(DescriptorTbl::create(runtime_state.get(), _object_pool.get(), _desc_tbl, &desc_tbl,
                                       config::vector_chunk_size));
        runtime_state->set_desc_tbl(desc_tbl);

        auto sink =
                std::make_unique<OlapTableSink>(_object_pool.get(), std::vector<TExpr>(), nullptr, runtime_state.get());
        CHECK_OK(sink->init(_data_sink, runtime_state.get()));
        CHECK_OK(sink->prepare(runtime_state.get()));
        return sink;
    }

    void _fill_chunk_base_data(ChunkPtr& chunk, const std::vector<SlotDescriptor*>& slots, size_t num_rows,
                               LogicalType skip_type) {
        for (size_t i = 0; i < slots.size(); ++i) {
            auto* slot = slots[i];
            if (slot->type().type == skip_type) {
                continue; // Skip the column being tested
            }
            auto* column = chunk->get_column_raw_ptr_by_slot_id(slot->id());
            if (slot->type().type == TYPE_INT) {
                for (size_t j = 0; j < num_rows; ++j) {
                    column->append_datum(Datum(static_cast<int32_t>(100 + j * 100)));
                }
            } else {
                // Fill other columns with default values
                for (size_t j = 0; j < num_rows; ++j) {
                    column->append_default();
                }
            }
        }
    }

    void _setup_chunk_slot_map(ChunkPtr& chunk, const std::vector<SlotDescriptor*>& slots) {
        chunk->reset_slot_id_to_index();
        for (size_t i = 0; i < slots.size(); ++i) {
            chunk->set_slot_id_to_index(slots[i]->id(), i);
        }
    }

    void _verify_error_log_contains_row_info(const std::string& error_log_path, ExecEnv* exec_env,
                                             const std::string& expected_row_debug, const std::string& error_keyword1,
                                             const std::string& error_keyword2) {
        std::string error_log_content = _read_error_log_file(error_log_path, exec_env);
        ASSERT_FALSE(error_log_content.empty())
                << "Error log file should not be empty. Relative path: " << error_log_path;
        ASSERT_NE(error_log_content.find("Row:"), std::string::npos) << "Error log should contain 'Row:' marker";
        ASSERT_NE(error_log_content.find(expected_row_debug), std::string::npos)
                << "Error log should contain the row debug information";
        ASSERT_NE(error_log_content.find(error_keyword1), std::string::npos)
                << "Error log should contain '" << error_keyword1 << "'";
        ASSERT_NE(error_log_content.find(error_keyword2), std::string::npos)
                << "Error log should contain '" << error_keyword2 << "'";
    }

    // Generic test helper for error message validation
    template <typename FillColumnFunc>
    void _test_error_log(LogicalType test_type, size_t slot_index, size_t num_rows, size_t error_row_index,
                         FillColumnFunc fill_column, const std::string& error_keyword1,
                         const std::string& error_keyword2) {
        std::unique_ptr<RuntimeState> runtime_state;
        DescriptorTbl* desc_tbl = nullptr;
        auto sink = _setup_sink(runtime_state, desc_tbl);

        ChunkPtr chunk(ChunkHelper::new_chunk(desc_tbl->get_tuple_descriptor(0)->slots(), num_rows).release());
        _fill_chunk_base_data(chunk, desc_tbl->get_tuple_descriptor(0)->slots(), num_rows, test_type);

        auto* slot = desc_tbl->get_tuple_descriptor(0)->slots()[slot_index];
        auto* column = chunk->get_column_raw_ptr_by_slot_id(slot->id());
        fill_column(column);

        chunk->materialized_nullable();
        _setup_chunk_slot_map(chunk, desc_tbl->get_tuple_descriptor(0)->slots());
        std::string expected_row_debug = chunk->debug_row(error_row_index);

        (void)sink->send_chunk(runtime_state.get(), chunk.get());

        std::string error_log_path = runtime_state->get_error_log_file_path();
        ExecEnv* exec_env = runtime_state->exec_env();
        // The destructor will close and flush the error log file
        runtime_state.reset();

        _verify_error_log_contains_row_info(error_log_path, exec_env, expected_row_debug, error_keyword1,
                                            error_keyword2);
    }

    int64_t _db_id;
    int64_t _table_id;
    int64_t _txn_id;
    ExecEnv* _exec_env;
    std::unique_ptr<ObjectPool> _object_pool;
    TDescriptorTable _desc_tbl;
    TDataSink _data_sink;
};

TEST_F(TabletSinkTest, test_varchar_error_log) {
    bool old_enable_check_string_lengths = config::enable_check_string_lengths;
    config::enable_check_string_lengths = true;
    DeferOp defer([&]() { config::enable_check_string_lengths = old_enable_check_string_lengths; });

    _test_error_log(
            TYPE_VARCHAR, 0, 3, 1,
            [](Column* col) {
                col->append_datum(Datum(Slice("short")));
                col->append_datum(Datum(Slice("this_is_a_very_long_string_that_exceeds_max_length")));
                col->append_datum(Datum(Slice("medium_str")));
            },
            "String", "too long");
}

TEST_F(TabletSinkTest, test_decimal_error_log) {
    _test_error_log(
            TYPE_DECIMALV2, 1, 2, 1,
            [](Column* col) {
                col->append_datum(Datum(DecimalV2Value(12345, 2)));
                col->append_datum(Datum(DecimalV2Value(100000000, 0)));
            },
            "Decimal", "out of range");
}

TEST_F(TabletSinkTest, test_decimalv3_error_log) {
    _test_error_log(
            TYPE_DECIMAL64, 2, 2, 1,
            [](Column* col) {
                col->append_datum(Datum(static_cast<int64_t>(12345)));
                col->append_datum(Datum(static_cast<int64_t>(10000000000LL)));
            },
            "Decimal", "out of range");
}

} // namespace starrocks
