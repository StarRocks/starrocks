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

#include "data_sink/tablet/olap_table_sink.h"

#include <gtest/gtest.h>

#include <fstream>
#include <sstream>

#include "base/string/slice.h"
#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "column/array_column.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/config_diagnostic_fwd.h"
#include "common/config_exec_fwd.h"
#include "common/config_scan_io_fwd.h"
#include "common/tracer.h"
#include "compute_env/global_dict/fragment_dict_state.h"
#include "compute_env/load_path/base_load_path_mgr.h"
#include "exec/exec_env.h"
#include "runtime/chunk_helper.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage_primitive/tablet_info.h"
#include "types/decimalv2_value.h"

namespace starrocks {

class OlapTableSinkTest : public ::testing::Test {
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
        auto runtime_state = std::make_unique<RuntimeState>(fragment_id, query_options, query_globals,
                                                            &_exec_env->query_execution_services(), _exec_env);
        auto* fragment_dict_state = runtime_state->obj_pool()->add(new FragmentDictState());
        runtime_state->set_fragment_dict_state(fragment_dict_state);
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
        table_sink.__set_table_name("test");
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

        ChunkPtr chunk(RuntimeChunkHelper::new_chunk(desc_tbl->get_tuple_descriptor(0)->slots(), num_rows).release());
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

TEST_F(OlapTableSinkTest, test_varchar_error_log) {
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

TEST_F(OlapTableSinkTest, test_decimal_error_log) {
    _test_error_log(
            TYPE_DECIMALV2, 1, 2, 1,
            [](Column* col) {
                col->append_datum(Datum(DecimalV2Value(12345, 2)));
                col->append_datum(Datum(DecimalV2Value(100000000, 0)));
            },
            "Decimal", "out of range");
}

TEST_F(OlapTableSinkTest, test_close_wait_twice_after_cancel) {
#ifdef __APPLE__
    GTEST_SKIP() << "OpenTelemetry tracing is disabled on macOS";
#else
    std::string old_jaeger_endpoint = config::jaeger_endpoint;
    config::jaeger_endpoint = "127.0.0.1:16831";
    Tracer::reinitialize_for_test();
    DeferOp defer([&]() {
        config::jaeger_endpoint = old_jaeger_endpoint;
        Tracer::reinitialize_for_test();
    });

    std::unique_ptr<RuntimeState> runtime_state;
    DescriptorTbl* desc_tbl = nullptr;
    auto sink = _setup_sink(runtime_state, desc_tbl);
    ASSERT_TRUE(sink->_span->IsRecording());

    // set_cancelled() then pending_finish() both close the sink when a load is cancelled
    Status first = sink->close_wait(runtime_state.get(), Status::Cancelled("Cancelled by pipeline engine"));
    ASSERT_TRUE(first.is_cancelled());
    Status second = sink->close_wait(runtime_state.get(), Status::OK());
    ASSERT_TRUE(second.is_cancelled());
#endif
}

TEST_F(OlapTableSinkTest, test_decimalv3_error_log) {
    _test_error_log(
            TYPE_DECIMAL64, 2, 2, 1,
            [](Column* col) {
                col->append_datum(Datum(static_cast<int64_t>(12345)));
                col->append_datum(Datum(static_cast<int64_t>(10000000000LL)));
            },
            "Decimal", "out of range");
}

class OlapTableSinkNestedValidateTest : public ::testing::Test {
public:
    void SetUp() override {
        _db_id = 1;
        _table_id = 2;
        _txn_id = 3;
        _exec_env = ExecEnv::GetInstance();
        _object_pool = std::make_unique<ObjectPool>();
        _desc_tbl = _build_descriptor_table();
        _data_sink = _build_data_sink();
        _old_enable_check_string_lengths = config::enable_check_string_lengths;
        config::enable_check_string_lengths = true;
    }

    void TearDown() override { config::enable_check_string_lengths = _old_enable_check_string_lengths; }

protected:
    static constexpr uint8_t kSelKept = 0x1;
    static constexpr uint8_t kSelFiltered = 0x0;

    TDescriptorTable _build_descriptor_table() {
        const auto varchar5 = TypeDescriptor::create_varchar_type(5);
        const auto dec32 = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 4, 2);
        TDescriptorTableBuilder dtb;
        TTupleDescriptorBuilder tuple_builder;
        int pos = 1;
        auto add = [&](const TypeDescriptor& type, const std::string& name) {
            tuple_builder.add_slot(
                    TSlotDescriptorBuilder().type(type).column_name(name).column_pos(pos++).nullable(true).build());
            _column_names.push_back(name);
        };
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_INT)
                                       .column_name("int_col")
                                       .column_pos(pos++)
                                       .nullable(false)
                                       .build());
        _column_names.push_back("int_col");
        add(TypeDescriptor::create_array_type(TypeDescriptor(TYPE_INT)), "arr_int");
        add(TypeDescriptor::create_array_type(varchar5), "arr_varchar");
        add(TypeDescriptor::create_array_type(dec32), "arr_dec32");
        add(TypeDescriptor::create_array_type(TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 10, 2)),
            "arr_dec64");
        add(TypeDescriptor::create_array_type(TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 30, 2)),
            "arr_dec128");
        add(TypeDescriptor::create_array_type(TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL256, 50, 2)),
            "arr_dec256");
        add(TypeDescriptor::create_array_type(TypeDescriptor::create_decimalv2_type(4, 2)), "arr_decv2");
        add(TypeDescriptor::create_array_type(TypeDescriptor::create_array_type(varchar5)), "arr_arr_varchar");
        add(TypeDescriptor::create_map_type(varchar5, varchar5), "map_col");
        add(TypeDescriptor::create_array_type(TypeDescriptor::create_map_type(varchar5, varchar5)), "arr_map");
        add(TypeDescriptor::create_map_type(TypeDescriptor::create_decimalv2_type(4, 2), varchar5), "map_decv2");
        add(TypeDescriptor::create_struct_type({"name", "price"}, {varchar5, dec32}), "struct_col");
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
        table_sink.__set_table_name("test");
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
        schema.indexes[0].columns = _column_names;

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

    std::unique_ptr<RuntimeState> _build_runtime_state(bool log_rejected_record) {
        TQueryOptions query_options;
        query_options.query_type = TQueryType::LOAD;
        if (log_rejected_record) {
            query_options.__set_log_rejected_record_num(-1);
        }
        TUniqueId fragment_id;
        TQueryGlobals query_globals;
        auto runtime_state = std::make_unique<RuntimeState>(fragment_id, query_options, query_globals,
                                                            &_exec_env->query_execution_services(), _exec_env);
        auto* fragment_dict_state = runtime_state->obj_pool()->add(new FragmentDictState());
        runtime_state->set_fragment_dict_state(fragment_dict_state);
        TUniqueId id;
        runtime_state->init_mem_trackers(id);
        runtime_state->set_db("test_db");
        runtime_state->set_load_label("test_label");
        runtime_state->set_txn_id(_txn_id);
        return runtime_state;
    }

    // Fills `tested_col_name` with `fill`, every other column with defaults, runs the sink data
    // validation, and asserts each row's keep bit (kSelKept = row survives, kSelFiltered = row is
    // marked VALID_SEL_FAILED). Only the keep bit is asserted because the exact selection byte of
    // surviving rows depends on which slot the validation visited last.
    void _run_case(const std::string& tested_col_name, const std::function<void(Column*)>& fill,
                   const std::vector<uint8_t>& expected_keep, bool log_rejected_record = false,
                   const std::function<void(const Column&)>& verify_column = nullptr) {
        const size_t num_rows = expected_keep.size();
        auto runtime_state = _build_runtime_state(log_rejected_record);
        DescriptorTbl* desc_tbl = nullptr;
        CHECK_OK(DescriptorTbl::create(runtime_state.get(), _object_pool.get(), _desc_tbl, &desc_tbl,
                                       config::vector_chunk_size));
        runtime_state->set_desc_tbl(desc_tbl);

        auto sink =
                std::make_unique<OlapTableSink>(_object_pool.get(), std::vector<TExpr>(), nullptr, runtime_state.get());
        CHECK_OK(sink->init(_data_sink, runtime_state.get()));
        CHECK_OK(sink->prepare(runtime_state.get()));

        const auto& slots = desc_tbl->get_tuple_descriptor(0)->slots();
        ChunkPtr chunk(RuntimeChunkHelper::new_chunk(slots, num_rows).release());
        for (auto* slot : slots) {
            auto* column = chunk->get_column_raw_ptr_by_slot_id(slot->id());
            if (slot->col_name() == tested_col_name) {
                fill(column);
            } else if (slot->type().type == TYPE_INT) {
                for (size_t j = 0; j < num_rows; ++j) {
                    column->append_datum(Datum(static_cast<int32_t>(j)));
                }
            } else {
                for (size_t j = 0; j < num_rows; ++j) {
                    column->append_default();
                }
            }
        }
        chunk->materialized_nullable();
        chunk->reset_slot_id_to_index();
        for (size_t i = 0; i < slots.size(); ++i) {
            chunk->set_slot_id_to_index(slots[i]->id(), i);
        }

        SlotId tested_slot_id = -1;
        for (auto* slot : slots) {
            if (slot->col_name() == tested_col_name) {
                tested_slot_id = slot->id();
            }
        }

        sink->_validate_selection.assign(num_rows, kSelKept);
        sink->_validate_data(runtime_state.get(), chunk.get());
        for (size_t i = 0; i < num_rows; ++i) {
            EXPECT_EQ(expected_keep[i], sink->_validate_selection[i] & 0x1) << "row " << i;
        }
        if (verify_column != nullptr) {
            ASSERT_NE(-1, tested_slot_id);
            verify_column(*chunk->get_column_raw_ptr_by_slot_id(tested_slot_id));
        }
    }

    int64_t _db_id;
    int64_t _table_id;
    int64_t _txn_id;
    ExecEnv* _exec_env;
    std::unique_ptr<ObjectPool> _object_pool;
    TDescriptorTable _desc_tbl;
    TDataSink _data_sink;
    std::vector<std::string> _column_names;
    bool _old_enable_check_string_lengths = false;
};

TEST_F(OlapTableSinkNestedValidateTest, test_array_varchar_too_long) {
    _run_case(
            "arr_varchar",
            [](Column* col) {
                col->append_datum(Datum(DatumArray{Datum(Slice("ok"))}));
                // Two oversize elements in one row: the second one hits the already-failed fast path.
                col->append_datum(Datum(DatumArray{Datum(Slice("toolongstring")), Datum(Slice("alsotoolong"))}));
                col->append_datum(Datum(DatumArray{Datum(Slice("fine")), Datum(Slice("x"))}));
            },
            {kSelKept, kSelFiltered, kSelKept}, /*log_rejected_record=*/true);
}

TEST_F(OlapTableSinkNestedValidateTest, test_array_varchar_disabled_by_config) {
    config::enable_check_string_lengths = false;
    _run_case("arr_varchar", [](Column* col) { col->append_datum(Datum(DatumArray{Datum(Slice("toolongstring"))})); },
              {kSelKept});
    // Decimal children are still validated when the string check is off: the oversize name
    // passes but the out-of-range price does not.
    _run_case(
            "struct_col",
            [](Column* col) {
                col->append_datum(Datum(DatumStruct{Datum(Slice("waytoolong")), Datum(static_cast<int32_t>(1))}));
                col->append_datum(Datum(DatumStruct{Datum(Slice("waytoolong")), Datum(static_cast<int32_t>(123456))}));
            },
            {kSelKept, kSelFiltered});
}

TEST_F(OlapTableSinkNestedValidateTest, test_array_decimal32_out_of_range) {
    _run_case(
            "arr_dec32",
            [](Column* col) {
                col->append_datum(Datum(DatumArray{Datum(static_cast<int32_t>(9999))}));          // 99.99
                col->append_datum(Datum(DatumArray{Datum(static_cast<int32_t>(1000000))}));       // 10000.00
                col->append_datum(Datum(DatumArray{Datum(), Datum(static_cast<int32_t>(1250))})); // [NULL, 12.50]
            },
            {kSelKept, kSelFiltered, kSelKept}, /*log_rejected_record=*/true);
}

TEST_F(OlapTableSinkNestedValidateTest, test_array_decimal_wider_types_out_of_range) {
    _run_case("arr_dec64",
              [](Column* col) {
                  col->append_datum(Datum(DatumArray{Datum(static_cast<int64_t>(999))}));
                  col->append_datum(Datum(DatumArray{Datum(static_cast<int64_t>(100000000000LL))})); // 10^11 > 10^10-1
              },
              {kSelKept, kSelFiltered});
    _run_case("arr_dec128",
              [](Column* col) {
                  int128_t in_range = 123;
                  int128_t out_of_range = 1;
                  for (int k = 0; k < 31; ++k) {
                      out_of_range *= 10; // 10^31 > 10^30-1
                  }
                  col->append_datum(Datum(DatumArray{Datum(in_range)}));
                  col->append_datum(Datum(DatumArray{Datum(out_of_range)}));
              },
              {kSelKept, kSelFiltered});
    _run_case("arr_dec256",
              [](Column* col) {
                  int256_t in_range = 123;
                  int256_t out_of_range = 1;
                  for (int k = 0; k < 51; ++k) {
                      out_of_range *= 10; // 10^51 > 10^50-1
                  }
                  col->append_datum(Datum(DatumArray{Datum(in_range)}));
                  col->append_datum(Datum(DatumArray{Datum(out_of_range)}));
              },
              {kSelKept, kSelFiltered});
}

TEST_F(OlapTableSinkNestedValidateTest, test_array_decimalv2_out_of_range) {
    _run_case(
            "arr_decv2",
            [](Column* col) {
                col->append_datum(Datum(DatumArray{Datum(DecimalV2Value(99, 990000000))}));     // 99.99
                col->append_datum(Datum(DatumArray{Datum(DecimalV2Value(123456, 780000000))})); // 123456.78
                // 10.1234: over-scale, rounds to 10.12 and stays in range.
                col->append_datum(Datum(DatumArray{Datum(DecimalV2Value(10, 123400000))}));
                col->append_datum(Datum(DatumArray{Datum(), Datum(DecimalV2Value(1, 0))})); // [NULL, 1]
            },
            {kSelKept, kSelFiltered, kSelKept, kSelKept}, /*log_rejected_record=*/true);
}

// Nested DECIMALV2 leaves deliberately validate against a rounded copy and leave the column data
// alone, unlike the top-level path which rounds in place. Rounding here would run *after*
// MapColumn::remove_duplicated_keys(), so two keys that differ before rounding but collapse
// afterwards would leave the map with duplicate keys — trading a scale mismatch for a broken
// key-uniqueness invariant. 1.234 and 1.233 both round to 1.23 for the range check and both pass;
// this pins that they stay distinct in the column.
TEST_F(OlapTableSinkNestedValidateTest, test_map_decimalv2_keys_are_not_normalized) {
    _run_case(
            "map_decv2",
            [](Column* col) {
                auto* nullable = down_cast<NullableColumn*>(col);
                auto* map_col = down_cast<MapColumn*>(ColumnHelper::get_data_column(col));
                auto* keys = map_col->keys_column_raw_ptr();
                auto* values = map_col->values_column_raw_ptr();
                auto& offsets = map_col->offsets_column_raw_ptr()->get_data();
                keys->append_datum(Datum(DecimalV2Value(1, 234000000))); // 1.234
                values->append_datum(Datum(Slice("a")));
                keys->append_datum(Datum(DecimalV2Value(1, 233000000))); // 1.233
                values->append_datum(Datum(Slice("b")));
                offsets.push_back(static_cast<uint32_t>(keys->size()));
                nullable->null_column_data().push_back(0);
            },
            {kSelKept}, /*log_rejected_record=*/false,
            [](const Column& col) {
                // get_data_column_by_type() peels the nullable/const wrappers and is const-friendly,
                // unlike get_data_column() which only takes a mutable Column*.
                const auto* map_col = ColumnHelper::get_data_column_by_type<TYPE_MAP>(&col);
                const auto* keys =
                        ColumnHelper::get_data_column_by_type<TYPE_DECIMALV2>(map_col->keys_column_raw_ptr());
                ASSERT_EQ(2, keys->get_data().size());
                // Assert the exact values, not just that they differ: a change that rewrote only
                // one of the two keys would still leave them unequal.
                EXPECT_EQ("1.234", keys->get_data()[0].to_string());
                EXPECT_EQ("1.233", keys->get_data()[1].to_string());
            });
}

// MapColumn::remove_duplicated_keys() keeps the last occurrence of a duplicated key and rewrites the
// offsets the row mapping is built from, so _validate_data must dedup before validating. These two
// rows pin that order: with validation running first, row 0 would be filtered on a value that
// dedup removes anyway. DatumMap is a std::map, so the duplicate has to be built column-side.
TEST_F(OlapTableSinkNestedValidateTest, test_map_duplicated_keys_validated_after_dedup) {
    _run_case("map_col",
              [](Column* col) {
                  auto* nullable = down_cast<NullableColumn*>(col);
                  auto* map_col = down_cast<MapColumn*>(ColumnHelper::get_data_column(col));
                  auto* keys = map_col->keys_column_raw_ptr();
                  auto* values = map_col->values_column_raw_ptr();
                  auto& offsets = map_col->offsets_column_raw_ptr()->get_data();
                  auto append_row = [&](const std::vector<std::pair<Slice, Slice>>& kvs) {
                      for (const auto& kv : kvs) {
                          keys->append_datum(Datum(kv.first));
                          values->append_datum(Datum(kv.second));
                      }
                      offsets.push_back(static_cast<uint32_t>(keys->size()));
                      nullable->null_column_data().push_back(0);
                  };
                  // Duplicated key, oversize value dropped by dedup: the row must survive.
                  append_row({{Slice("k"), Slice("toolongvalue")}, {Slice("k"), Slice("v")}});
                  // Duplicated key, oversize value kept by dedup: the row must be filtered.
                  append_row({{Slice("k"), Slice("v")}, {Slice("k"), Slice("toolongvalue")}});
              },
              {kSelKept, kSelFiltered});
}

// Cross-container recursion: array<map<varchar,varchar>> exercises an offsets-driven mapping nested
// inside another offsets-driven mapping, on both map sides.
TEST_F(OlapTableSinkNestedValidateTest, test_array_of_map_cross_container) {
    _run_case("arr_map",
              [](Column* col) {
                  col->append_datum(Datum(DatumArray{Datum(DatumMap{{Slice("k"), Datum(Slice("v"))}})}));
                  col->append_datum(Datum(DatumArray{Datum(DatumMap{{Slice("k"), Datum(Slice("v"))}}),
                                                     Datum(DatumMap{{Slice("k"), Datum(Slice("toolongvalue"))}})}));
                  col->append_datum(Datum(DatumArray{Datum(DatumMap{{Slice("verylongkey"), Datum(Slice("v"))}})}));
                  col->append_datum(Datum(DatumArray{Datum(), Datum(DatumMap{{Slice("k"), Datum(Slice("v"))}})}));
              },
              {kSelKept, kSelFiltered, kSelFiltered, kSelKept});
}

TEST_F(OlapTableSinkNestedValidateTest, test_nested_array_of_array_maps_to_row) {
    _run_case("arr_arr_varchar",
              [](Column* col) {
                  col->append_datum(Datum(DatumArray{Datum(DatumArray{Datum(Slice("ok"))})}));
                  col->append_datum(Datum(DatumArray{Datum(DatumArray{Datum(Slice("ok"))}),
                                                     Datum(DatumArray{Datum(Slice("bad_toolong"))})}));
                  col->append_datum(Datum(DatumArray{Datum(DatumArray{}), Datum(DatumArray{Datum(Slice("x"))})}));
              },
              {kSelKept, kSelFiltered, kSelKept});
}

TEST_F(OlapTableSinkNestedValidateTest, test_map_key_and_value_sides) {
    _run_case("map_col",
              [](Column* col) {
                  col->append_datum(Datum(DatumMap{{Slice("k"), Datum(Slice("v"))}}));
                  col->append_datum(Datum(DatumMap{{Slice("verylongkey"), Datum(Slice("v"))}}));
                  col->append_datum(Datum(DatumMap{{Slice("k"), Datum(Slice("verylongvalue"))}}));
              },
              {kSelKept, kSelFiltered, kSelFiltered});
}

TEST_F(OlapTableSinkNestedValidateTest, test_struct_fields) {
    _run_case("struct_col",
              [](Column* col) {
                  col->append_datum(Datum(DatumStruct{Datum(Slice("ok")), Datum(static_cast<int32_t>(9999))}));
                  col->append_datum(Datum(DatumStruct{Datum(Slice("waytoolong")), Datum(static_cast<int32_t>(1))}));
                  col->append_datum(Datum(DatumStruct{Datum(Slice("ok")), Datum(static_cast<int32_t>(123456))}));
                  col->append_datum(Datum()); // whole struct NULL: fields are masked, row is kept
              },
              {kSelKept, kSelFiltered, kSelFiltered, kSelKept});
}

TEST_F(OlapTableSinkNestedValidateTest, test_null_element_and_null_row) {
    _run_case("arr_varchar",
              [](Column* col) {
                  col->append_datum(Datum(DatumArray{Datum(), Datum(Slice("ok"))})); // NULL element is skipped
                  col->append_datum(Datum());                                        // whole array NULL
                  col->append_datum(Datum(DatumArray{}));                            // empty array
              },
              {kSelKept, kSelKept, kSelKept});
}

TEST_F(OlapTableSinkNestedValidateTest, test_only_middle_row_fails) {
    _run_case("arr_varchar",
              [](Column* col) {
                  col->append_datum(Datum(DatumArray{Datum(Slice("a"))}));
                  col->append_datum(Datum(DatumArray{Datum(Slice("bb"))}));
                  col->append_datum(Datum(DatumArray{Datum(Slice("ccc_toolong"))}));
                  col->append_datum(Datum(DatumArray{Datum(Slice("dd"))}));
                  col->append_datum(Datum(DatumArray{Datum(Slice("e"))}));
              },
              {kSelKept, kSelKept, kSelFiltered, kSelKept, kSelKept});
}

} // namespace starrocks
