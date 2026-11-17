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
#include <numeric>
#include <set>
#include <sstream>
#include <vector>

#include "base/string/slice.h"
#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "column/column.h"
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

TEST_F(OlapTableSinkTest, test_close_skipped_after_init_failure) {
    std::unique_ptr<RuntimeState> runtime_state = _build_runtime_state();
    DescriptorTbl* desc_tbl = nullptr;
    ASSERT_OK(DescriptorTbl::create(runtime_state.get(), _object_pool.get(), _desc_tbl, &desc_tbl,
                                    config::vector_chunk_size));
    runtime_state->set_desc_tbl(desc_tbl);

    auto& indexes = _data_sink.olap_table_sink.schema.indexes;
    indexes.push_back(indexes.front());
    indexes.back().id = 1;

    auto sink = std::make_unique<OlapTableSink>(_object_pool.get(), std::vector<TExpr>(), nullptr, runtime_state.get());
    Status init_status = sink->init(_data_sink, runtime_state.get());
    ASSERT_ERROR(init_status);
    ASSERT_FALSE(sink->_is_initialized);
    ASSERT_EQ(nullptr, sink->_span);
    ASSERT_EQ(nullptr, sink->ts_profile());
    ASSERT_EQ(nullptr, sink->_tablet_sink_sender);
    ASSERT_OK(sink->open(runtime_state.get()));
    ASSERT_OK(sink->try_open(runtime_state.get()));
    ASSERT_TRUE(sink->is_open_done());
    ASSERT_OK(sink->open_wait());
    ASSERT_FALSE(sink->is_full());
    ASSERT_OK(sink->try_close(runtime_state.get()));
    ASSERT_TRUE(sink->is_close_done());
    EXPECT_STATUS(init_status, sink->close_wait(runtime_state.get(), init_status));
    EXPECT_STATUS(init_status, sink->close_wait(runtime_state.get(), Status::OK()));
    EXPECT_STATUS(init_status, sink->close(runtime_state.get(), init_status));
}

TEST_F(OlapTableSinkTest, test_close_before_prepare) {
    std::unique_ptr<RuntimeState> runtime_state = _build_runtime_state();
    auto sink = std::make_unique<OlapTableSink>(_object_pool.get(), std::vector<TExpr>(), nullptr, runtime_state.get());
    ASSERT_OK(sink->init(_data_sink, runtime_state.get()));
    ASSERT_TRUE(sink->_is_initialized);
    ASSERT_NE(nullptr, sink->_span);
    ASSERT_EQ(nullptr, sink->ts_profile());
    ASSERT_EQ(nullptr, sink->_tablet_sink_sender);

    Status close_status = Status::InternalError("prepare failed");
    ASSERT_OK(sink->try_close(runtime_state.get()));
    ASSERT_TRUE(sink->is_close_done());
    EXPECT_STATUS(close_status, sink->close_wait(runtime_state.get(), close_status));
    EXPECT_STATUS(close_status, sink->close_wait(runtime_state.get(), Status::OK()));
    EXPECT_STATUS(close_status, sink->close(runtime_state.get(), close_status));
}

TEST_F(OlapTableSinkTest, test_close_after_prepare_failure) {
    std::unique_ptr<RuntimeState> runtime_state = _build_runtime_state();
    DescriptorTbl* desc_tbl = nullptr;
    ASSERT_OK(DescriptorTbl::create(runtime_state.get(), _object_pool.get(), _desc_tbl, &desc_tbl,
                                    config::vector_chunk_size));
    runtime_state->set_desc_tbl(desc_tbl);

    _data_sink.olap_table_sink.tuple_id = 999;
    auto sink = std::make_unique<OlapTableSink>(_object_pool.get(), std::vector<TExpr>(), nullptr, runtime_state.get());
    ASSERT_OK(sink->init(_data_sink, runtime_state.get()));
    Status prepare_status = sink->prepare(runtime_state.get());
    ASSERT_ERROR(prepare_status);
    ASSERT_NE(nullptr, sink->_span);
    ASSERT_NE(nullptr, sink->profile());
    ASSERT_NE(nullptr, sink->ts_profile());
    ASSERT_EQ(nullptr, sink->_tablet_sink_sender);

    EXPECT_STATUS(prepare_status, sink->close_wait(runtime_state.get(), prepare_status));
    EXPECT_STATUS(prepare_status, sink->close_wait(runtime_state.get(), Status::OK()));
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

// A schema change renames the columns it rewrites to `__starrocks_shadow_<name>` (FE's
// SchemaChangeHandler.SHADOW_NAME_PREFIX) by setting the NAME only, leaving the ColumnId alone. FE
// then emits the slot list under the shadow name and TColumn.column_name under the plain ColumnId,
// so multi-node write's key-slot resolution has to undo the rename to match the two. Getting this
// wrong does not degrade quietly -- it resolves a shadow KEY column to no slot at all, trips the
// "index exposes N of M key columns" check, and fails a load during a key-column schema change.
TEST(StripShadowColumnPrefixTest, strips_only_the_schema_change_prefix) {
    // The case that was broken: a key column being rewritten by a schema change.
    EXPECT_EQ("k1", strip_shadow_column_prefix("__starrocks_shadow_k1"));

    // Every ordinary column must come back byte-for-byte, or this "fix" would break the common path
    // it never had a problem with.
    EXPECT_EQ("k1", strip_shadow_column_prefix("k1"));
    EXPECT_EQ("", strip_shadow_column_prefix(""));
    EXPECT_EQ("__starrocks_k1", strip_shadow_column_prefix("__starrocks_k1"));

    // Only a genuine prefix counts; the same text further along the name is part of the name.
    EXPECT_EQ("k1___starrocks_shadow_x", strip_shadow_column_prefix("k1___starrocks_shadow_x"));

    // The bare prefix names nothing, so stripping it to the empty string would match every column
    // with an empty name rather than none. Leave it alone.
    EXPECT_EQ("__starrocks_shadow_", strip_shadow_column_prefix("__starrocks_shadow_"));
}

// A tablet's writer nodes must all be reachable. The key hash and the hash that picked the tablet are
// computed identically (crc32, seed 0, folded per column), so for `PRIMARY KEY(k) DISTRIBUTED BY
// HASH(k)` they are the same number h: taking the node as h % N against a tablet of h % T confines
// every row of tablet t to N / gcd(T, N) of the N writers -- 3 of 6 at two buckets, 1 of 6 at six --
// while the unreachable nodes still cost an open/close, a delta writer and an empty partial txn log,
// and MultiNodeWriteNodes still reports N.
//
// The raw arm is kept deliberately: it is what the bug looked like, so this test fails if the
// avalanche is ever dropped rather than silently passing on some other property.
TEST(MultiNodeWriteNodeSlotTest, every_writer_is_reachable_whatever_the_bucket_count) {
    constexpr size_t kNodes = 6;
    constexpr uint32_t kSamples = 60000;

    for (size_t buckets : std::vector<size_t>{1, 2, 3, 4, 6}) {
        std::vector<std::set<uint32_t>> mixed(buckets);
        std::vector<std::set<uint32_t>> raw(buckets);
        for (uint32_t h = 0; h < kSamples; ++h) {
            mixed[h % buckets].insert(multi_node_write_node_slot(h, kNodes));
            raw[h % buckets].insert(h % kNodes); // what the modulo on the unmixed hash did
        }
        for (size_t t = 0; t < buckets; ++t) {
            EXPECT_EQ(kNodes, mixed[t].size())
                    << "buckets=" << buckets << " tablet=" << t << " cannot reach every writer";
        }
        const size_t expected_raw = kNodes / std::gcd(buckets, kNodes);
        EXPECT_EQ(expected_raw, raw[0].size())
                << "buckets=" << buckets << ": the unmixed hash should show the N/gcd(T,N) collapse";
    }
}

// The routing exists so that all of a key's rows meet in ONE writer; the mix must not cost that.
TEST(MultiNodeWriteNodeSlotTest, same_key_always_picks_the_same_writer) {
    for (uint32_t h = 0; h < 10000; ++h) {
        EXPECT_EQ(multi_node_write_node_slot(h, 6), multi_node_write_node_slot(h, 6));
    }
    // And the slot must stay inside the node list.
    for (size_t n : std::vector<size_t>{1, 2, 3, 5, 6, 7}) {
        for (uint32_t h = 0; h < 2000; ++h) {
            EXPECT_LT(static_cast<size_t>(multi_node_write_node_slot(h, n)), n);
        }
    }
}

} // namespace starrocks
