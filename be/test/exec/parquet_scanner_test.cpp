// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/exec/parquet_scanner_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>
#include <time.h>

#include <map>
#include <string>
#include <vector>

#include "common/object_pool.h"
#include "exec/file_scan_node.h"
#include "exec/local_file_reader.h"
#include "exprs/cast_functions.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/user_function_cache.h"

namespace starrocks {

class ParquetSannerTest : public testing::Test {
public:
    ParquetSannerTest() : _runtime_state(TQueryGlobals()) {
        init();
        _runtime_state._instance_mem_tracker.reset(new MemTracker());
    }
    void init();
    static void SetUpTestCase() {}

<<<<<<< HEAD
protected:
    virtual void SetUp() {}
    virtual void TearDown() {}
=======
    std::vector<TBrokerRangeDesc> generate_split_ranges(const std::vector<std::string>& file_names,
                                                        const std::vector<int>& file_sizes,
                                                        int32_t num_columns_from_file,
                                                        const std::vector<std::string>& columns_from_path) {
        std::vector<TBrokerRangeDesc> ranges;
        int total_size = 0;
        for (auto s : file_sizes) {
            total_size += s;
        }
        int split_size = 128 * 1024;

        for (auto i = 0; i < file_names.size(); ++i) {
            TBrokerRangeDesc range;
            range.__set_num_of_columns_from_file(num_columns_from_file);
            range.__set_columns_from_path(columns_from_path);
            range.__set_path(file_names[i]);
            range.file_type = TFileType::FILE_LOCAL;

            for (auto offset = 0; offset < file_sizes[i]; offset += split_size) {
                range.start_offset = offset;
                range.size = split_size < file_sizes[i] - offset ? split_size : file_sizes[i] - offset;
                ranges.push_back(range);
            }
        }
        return ranges;
    }

    starrocks::TExpr create_column_ref(int32_t slot_id, const TypeDescriptor& type_desc, bool is_nullable) {
        starrocks::TExpr e = starrocks::TExpr();
        e.nodes.emplace_back(TExprNode());
        e.nodes[0].__set_type(type_desc.to_thrift());
        e.nodes[0].__set_node_type(TExprNodeType::SLOT_REF);
        e.nodes[0].__set_is_nullable(is_nullable);
        e.nodes[0].__set_slot_ref(TSlotRef());
        e.nodes[0].slot_ref.__set_slot_id((::starrocks::TSlotId)slot_id);
        return e;
    }

    starrocks::TExpr create_cast_expr(const starrocks::TExpr& child, const TypeDescriptor& type_desc) {
        starrocks::TExpr e = starrocks::TExpr();
        e.nodes.emplace_back(TExprNode());
        e.nodes.insert(e.nodes.end(), child.nodes.begin(), child.nodes.end());
        auto& to_expr = e.nodes[0];
        to_expr.__set_type(type_desc.to_thrift());
        to_expr.__set_child_type(child.nodes[0].type.types[0].scalar_type.type);
        to_expr.__set_node_type(TExprNodeType::CAST_EXPR);
        to_expr.__set_is_nullable(true);
        to_expr.__set_num_children(1);
        return e;
    }

    std::unique_ptr<ParquetScanner> create_parquet_scanner(
            const std::string& timezone, DescriptorTbl* desc_tbl,
            const std::unordered_map<size_t, ::starrocks::TExpr>& dst_slot_exprs,
            const std::vector<TBrokerRangeDesc>& ranges) {
        /// Init RuntimeState
        auto query_globals = TQueryGlobals();
        query_globals.time_zone = timezone;
        RuntimeState* state = _obj_pool.add(new RuntimeState(TUniqueId(), TQueryOptions(), query_globals, nullptr));
        state->set_desc_tbl(desc_tbl);
        state->init_instance_mem_tracker();

        /// TBrokerScanRangeParams
        TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());
        params->strict_mode = true;
        std::vector<TupleDescriptor*> tuples;
        desc_tbl->get_tuple_descs(&tuples);
        const auto num_tuples = tuples.size();
        params->src_tuple_id = 0;
        params->dest_tuple_id = num_tuples - 1;
        const auto* src_tuple = desc_tbl->get_tuple_descriptor(params->src_tuple_id);
        const auto* dst_tuple = desc_tbl->get_tuple_descriptor(params->dest_tuple_id);
        for (int i = 0; i < src_tuple->slots().size(); i++) {
            auto& src_slot = src_tuple->slots()[i];
            auto& dst_slot = dst_tuple->slots()[i];
            if (dst_slot_exprs.count(i)) {
                params->expr_of_dest_slot[dst_slot->id()] = dst_slot_exprs.at(i);
            } else {
                params->expr_of_dest_slot[dst_slot->id()] =
                        create_column_ref(src_slot->id(), src_slot->type(), src_slot->is_nullable());
            }
        }

        for (int i = 0; i < src_tuple->slots().size(); i++) {
            params->src_slot_ids.emplace_back(i);
        }

        RuntimeProfile* profile = _obj_pool.add(new RuntimeProfile("test_prof", true));
        ScannerCounter* counter = _obj_pool.add(new ScannerCounter());

        TBrokerScanRange* broker_scan_range = _obj_pool.add(new TBrokerScanRange());
        broker_scan_range->params = *params;
        broker_scan_range->ranges = ranges;

        return std::make_unique<ParquetScanner>(state, profile, *broker_scan_range, counter);
    }

    void validate(std::unique_ptr<ParquetScanner>& scanner, const size_t expect_num_rows,
                  const std::function<void(const ChunkPtr&)>& check_func) {
        ASSERT_OK(scanner->open());
        size_t num_rows = 0;
        while (true) {
            auto res = scanner->get_next();
            if (!res.ok() && res.status().is_end_of_file()) {
                ASSERT_EQ(expect_num_rows, num_rows);
                break;
            }
            if (!res.ok()) {
                std::cout << "Unexpected status:" << res.status().to_string() << std::endl;
            }
            ChunkPtr chunk = res.value();
            if (chunk == nullptr) {
                ASSERT_EQ(expect_num_rows, num_rows);
                break;
            }

            ASSERT_TRUE(chunk->num_rows() > 0);
            num_rows += chunk->num_rows();
            check_func(chunk);
        }
        scanner->close();
    }

    SlotTypeDescInfoArray select_columns(const std::vector<std::string>& column_names, bool is_nullable) {
        auto slot_map = std::unordered_map<std::string, TypeDescriptor>{
                {"col_date", TypeDescriptor::from_logical_type(TYPE_DATE)},
                {"col_datetime", TypeDescriptor::from_logical_type(TYPE_DATETIME)},
                {"col_char", TypeDescriptor::from_logical_type(TYPE_CHAR)},
                {"col_varchar", TypeDescriptor::from_logical_type(TYPE_VARCHAR)},
                {"col_boolean", TypeDescriptor::from_logical_type(TYPE_BOOLEAN)},
                {"col_tinyint", TypeDescriptor::from_logical_type(TYPE_TINYINT)},
                {"col_smallint", TypeDescriptor::from_logical_type(TYPE_SMALLINT)},
                {"col_int", TypeDescriptor::from_logical_type(TYPE_INT)},
                {"col_bigint", TypeDescriptor::from_logical_type(TYPE_BIGINT)},
                {"col_decimal_p6s2", TypeDescriptor::from_logical_type(TYPE_DECIMAL32, -1, 6, 2)},
                {"col_decimal_p14s5", TypeDescriptor::from_logical_type(TYPE_DECIMAL64, -1, 14, 5)},
                {"col_decimal_p27s9", TypeDescriptor::from_logical_type(TYPE_DECIMALV2, -1, 27, 9)},

                {"col_int_null", TypeDescriptor::from_logical_type(TYPE_INT)},
                {"col_string_null", TypeDescriptor::from_logical_type(TYPE_VARCHAR)},

                {"col_json_int8", TypeDescriptor::create_json_type()},
                {"col_json_int16", TypeDescriptor::create_json_type()},
                {"col_json_int32", TypeDescriptor::create_json_type()},
                {"col_json_int64", TypeDescriptor::create_json_type()},
                {"col_json_uint8", TypeDescriptor::create_json_type()},
                {"col_json_uint16", TypeDescriptor::create_json_type()},
                {"col_json_uint32", TypeDescriptor::create_json_type()},
                {"col_json_uint64", TypeDescriptor::create_json_type()},
                {"col_json_timestamp", TypeDescriptor::create_json_type()},

                {"col_json_float32", TypeDescriptor::create_json_type()},
                {"col_json_float64", TypeDescriptor::create_json_type()},

                {"col_json_bool", TypeDescriptor::create_json_type()},
                {"col_json_string", TypeDescriptor::create_json_type()},

                {"col_json_list", TypeDescriptor::create_json_type()},
                {"col_json_map", TypeDescriptor::create_json_type()},
                {"col_json_map_timestamp", TypeDescriptor::create_json_type()},
                {"col_json_struct", TypeDescriptor::create_json_type()},
                {"col_json_list_list", TypeDescriptor::create_json_type()},
                {"col_json_list_struct", TypeDescriptor::create_json_type()},
                {"col_json_map_list", TypeDescriptor::create_json_type()},
                {"col_json_struct_struct", TypeDescriptor::create_json_type()},

                // Convert struct->JSON->string
                {"col_json_struct_string", TypeDescriptor::from_logical_type(TYPE_VARCHAR)},
                {"col_json_json_string", TypeDescriptor::create_json_type()},
                {"issue_17693_c0", TypeDescriptor::create_array_type(TypeDescriptor::from_logical_type(TYPE_VARCHAR))}};
        SlotTypeDescInfoArray slot_infos;
        slot_infos.reserve(column_names.size());
        for (auto& name : column_names) {
            CHECK_EQ(slot_map.count(name), 1);
            slot_infos.emplace_back(name, slot_map[name], is_nullable);
        }
        return slot_infos;
    }

    template <bool is_nullable>
    void test_column_from_path(const std::vector<std::string>& columns_from_file,
                               const std::vector<std::string>& columns_from_path,
                               const std::vector<std::string>& column_values,
                               const std::unordered_map<size_t, ::starrocks::TExpr>& dst_slot_exprs) {
        std::vector<std::string> file_names;
        if constexpr (is_nullable) {
            file_names = _nullable_file_names;
        } else {
            file_names = _file_names;
        }
        std::vector<std::string> column_names;
        column_names.reserve(columns_from_file.size() + columns_from_path.size());
        column_names.template insert(column_names.end(), columns_from_file.begin(), columns_from_file.end());
        column_names.template insert(column_names.end(), columns_from_path.begin(), columns_from_path.end());

        auto src_slot_infos = select_columns(columns_from_file, is_nullable);
        for (const auto& i : columns_from_path) {
            src_slot_infos.template emplace_back(i, TypeDescriptor::from_logical_type(TYPE_VARCHAR), is_nullable);
        }

        auto dst_slot_infos = select_columns(column_names, is_nullable);

        auto ranges = generate_ranges(file_names, columns_from_file.size(), column_values);
        auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {src_slot_infos, dst_slot_infos});
        auto scanner = create_parquet_scanner("UTC", desc_tbl, dst_slot_exprs, ranges);
        auto check = [](const ChunkPtr& chunk) {
            auto& columns = chunk->columns();
            for (auto& col : columns) {
                if constexpr (is_nullable) {
                    ASSERT_TRUE(!col->only_null() || !col->is_constant());
                } else {
                    ASSERT_TRUE(!col->is_nullable() || !col->is_constant());
                }
            }
        };
        validate(scanner, 36865, check);
    }

    template <bool is_nullable>
    ChunkPtr test_json_column(const std::vector<std::string>& columns_from_file,
                              const std::unordered_map<size_t, ::starrocks::TExpr>& dst_slot_exprs,
                              std::string specific_file) {
        std::vector<std::string> file_names{std::move(specific_file)};
        const std::vector<std::string>& column_names = columns_from_file;

        auto src_slot_infos = select_columns(columns_from_file, is_nullable);
        auto dst_slot_infos = select_columns(column_names, is_nullable);

        auto ranges = generate_ranges(file_names, columns_from_file.size(), {});
        auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {src_slot_infos, dst_slot_infos});
        auto scanner = create_parquet_scanner("UTC", desc_tbl, dst_slot_exprs, ranges);

        ChunkPtr result;
        auto check = [&](const ChunkPtr& chunk) {
            auto& columns = chunk->columns();
            for (auto& col : columns) {
                if constexpr (is_nullable) {
                    ASSERT_TRUE(!col->only_null() || !col->is_constant());
                } else {
                    ASSERT_TRUE(!col->is_nullable() || !col->is_constant());
                }
            }
            result = chunk;
        };
        validate(scanner, 3, check);

        return result;
    }

    void SetUp() override {
        std::string starrocks_home = getenv("STARROCKS_HOME");
        test_exec_dir = starrocks_home + "/be/test/exec";
        _nullable_file_names =
                std::vector<std::string>{test_exec_dir + "/test_data/nullable_parquet_data/nullable_data_0.parquet",
                                         test_exec_dir + "/test_data/nullable_parquet_data/nullable_data_1.parquet",
                                         test_exec_dir + "/test_data/nullable_parquet_data/nullable_data_4095.parquet",
                                         test_exec_dir + "/test_data/nullable_parquet_data/nullable_data_4096.parquet",
                                         test_exec_dir + "/test_data/nullable_parquet_data/nullable_data_4097.parquet",
                                         test_exec_dir + "/test_data/nullable_parquet_data/nullable_data_8191.parquet",
                                         test_exec_dir + "/test_data/nullable_parquet_data/nullable_data_8192.parquet",
                                         test_exec_dir + "/test_data/nullable_parquet_data/nullable_data_8193.parquet"};
        _file_names = std::vector<std::string>{test_exec_dir + "/test_data/parquet_data/data_0.parquet",
                                               test_exec_dir + "/test_data/parquet_data/data_1.parquet",
                                               test_exec_dir + "/test_data/parquet_data/data_4095.parquet",
                                               test_exec_dir + "/test_data/parquet_data/data_4096.parquet",
                                               test_exec_dir + "/test_data/parquet_data/data_4097.parquet",
                                               test_exec_dir + "/test_data/parquet_data/data_8191.parquet",
                                               test_exec_dir + "/test_data/parquet_data/data_8192.parquet",
                                               test_exec_dir + "/test_data/parquet_data/data_8193.parquet"};
        _file_sizes = std::vector<int>{404,    /*"/test_data/parquet_data/data_0.parquet",   */
                                       2012,   /*"/test_data/parquet_data/data_1.parquet",*/
                                       386707, /*"/test_data/parquet_data/data_4095.parquet",*/
                                       388341, /*"/test_data/parquet_data/data_4096.parquet",*/
                                       388199, /*"/test_data/parquet_data/data_4097.parquet",*/
                                       773729, /*"/test_data/parquet_data/data_8191.parquet",*/
                                       772472, /*"/test_data/parquet_data/data_8192.parquet",*/
                                       775318 /*"/test_data/parquet_data/data_8193.parquet"*/};
        _runtime_state = _obj_pool.add(new RuntimeState(TQueryGlobals()));
        _issue_16475_file_names =
                std::vector<std::string>{test_exec_dir + "/test_data/parquet_data/issue_17693_1.parquet",
                                         test_exec_dir + "/test_data/parquet_data/issue_17693_2.parquet"};
    }
>>>>>>> 1ce2a8bc3 ([BugFix]  fix loading NA type from parquet file (#17780))

private:
    int create_src_tuple(TDescriptorTable& t_desc_table, int next_slot_id);
    int create_dst_tuple(TDescriptorTable& t_desc_table, int next_slot_id);
    void create_expr_info();
    void init_desc_table();
    RuntimeState _runtime_state;
    ObjectPool _obj_pool;
    std::map<std::string, SlotDescriptor*> _slots_map;
    TBrokerScanRangeParams _params;
    DescriptorTbl* _desc_tbl;
    TPlanNode _tnode;
};

#define TUPLE_ID_DST 0
#define TUPLE_ID_SRC 1
#define CLOMN_NUMBERS 20
#define DST_TUPLE_SLOT_ID_START 1
#define SRC_TUPLE_SLOT_ID_START 21
int ParquetSannerTest::create_src_tuple(TDescriptorTable& t_desc_table, int next_slot_id) {
    const char* clomnNames[] = {"log_version",       "log_time", "log_time_stamp", "js_version",
                                "vst_cookie",        "vst_ip",   "vst_user_id",    "vst_user_agent",
                                "device_resolution", "page_url", "page_refer_url", "page_yyid",
                                "page_type",         "pos_type", "content_id",     "media_id",
                                "spm_cnt",           "spm_pre",  "scm_cnt",        "partition_column"};
    for (int i = 0; i < CLOMN_NUMBERS; i++) {
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 1;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::VARCHAR);
            scalar_type.__set_len(65535);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = i;
        slot_desc.byteOffset = i * 16 + 8;
        slot_desc.nullIndicatorByte = i / 8;
        slot_desc.nullIndicatorBit = i % 8;
        slot_desc.colName = clomnNames[i];
        slot_desc.slotIdx = i + 1;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }

    {
        // TTupleDescriptor source
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = TUPLE_ID_SRC;
        t_tuple_desc.byteSize = CLOMN_NUMBERS * 16 + 8;
        t_tuple_desc.numNullBytes = 0;
        t_tuple_desc.tableId = 0;
        t_tuple_desc.__isset.tableId = true;
        t_desc_table.tupleDescriptors.push_back(t_tuple_desc);
    }
    return next_slot_id;
}

int ParquetSannerTest::create_dst_tuple(TDescriptorTable& t_desc_table, int next_slot_id) {
    int32_t byteOffset = 8;
    { //log_version
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 0;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::VARCHAR); //parquet::Type::BYTE
            scalar_type.__set_len(65535);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 0;
        slot_desc.byteOffset = byteOffset;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = 0;
        slot_desc.colName = "log_version";
        slot_desc.slotIdx = 1;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }
    byteOffset += 16;
    { // log_time
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 0;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::BIGINT); //parquet::Type::INT64
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 1;
        slot_desc.byteOffset = byteOffset;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = 1;
        slot_desc.colName = "log_time";
        slot_desc.slotIdx = 2;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }
    byteOffset += 8;
    { // log_time_stamp
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 0;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::BIGINT); //parquet::Type::INT32
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 2;
        slot_desc.byteOffset = byteOffset;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = 2;
        slot_desc.colName = "log_time_stamp";
        slot_desc.slotIdx = 3;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }
    byteOffset += 8;
    const char* clomnNames[] = {"log_version",       "log_time", "log_time_stamp", "js_version",
                                "vst_cookie",        "vst_ip",   "vst_user_id",    "vst_user_agent",
                                "device_resolution", "page_url", "page_refer_url", "page_yyid",
                                "page_type",         "pos_type", "content_id",     "media_id",
                                "spm_cnt",           "spm_pre",  "scm_cnt",        "partition_column"};
    for (int i = 3; i < CLOMN_NUMBERS; i++, byteOffset += 16) {
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 0;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::VARCHAR); //parquet::Type::BYTE
            scalar_type.__set_len(65535);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = i;
        slot_desc.byteOffset = byteOffset;
        slot_desc.nullIndicatorByte = i / 8;
        slot_desc.nullIndicatorBit = i % 8;
        slot_desc.colName = clomnNames[i];
        slot_desc.slotIdx = i + 1;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }

    t_desc_table.__isset.slotDescriptors = true;
    {
        // TTupleDescriptor dest
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = TUPLE_ID_DST;
        t_tuple_desc.byteSize = byteOffset + 8;
        t_tuple_desc.numNullBytes = 0;
        t_tuple_desc.tableId = 0;
        t_tuple_desc.__isset.tableId = true;
        t_desc_table.tupleDescriptors.push_back(t_tuple_desc);
    }
    return next_slot_id;
}

void ParquetSannerTest::init_desc_table() {
    TDescriptorTable t_desc_table;

    // table descriptors
    TTableDescriptor t_table_desc;

    t_table_desc.id = 0;
    t_table_desc.tableType = TTableType::BROKER_TABLE;
    t_table_desc.numCols = 0;
    t_table_desc.numClusteringCols = 0;
    t_desc_table.tableDescriptors.push_back(t_table_desc);
    t_desc_table.__isset.tableDescriptors = true;

    int next_slot_id = 1;

    next_slot_id = create_dst_tuple(t_desc_table, next_slot_id);

    next_slot_id = create_src_tuple(t_desc_table, next_slot_id);

    DescriptorTbl::create(&_obj_pool, t_desc_table, &_desc_tbl, config::vector_chunk_size);

    _runtime_state.set_desc_tbl(_desc_tbl);
}

void ParquetSannerTest::create_expr_info() {
    TTypeDesc varchar_type;
    {
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::VARCHAR);
        scalar_type.__set_len(5000);
        node.__set_scalar_type(scalar_type);
        varchar_type.types.push_back(node);
    }
    // log_version VARCHAR --> VARCHAR
    {
        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = varchar_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = SRC_TUPLE_SLOT_ID_START; // log_time id in src tuple
        slot_ref.slot_ref.tuple_id = 1;

        TExpr expr;
        expr.nodes.push_back(slot_ref);

        _params.expr_of_dest_slot.emplace(DST_TUPLE_SLOT_ID_START, expr);
        _params.src_slot_ids.push_back(SRC_TUPLE_SLOT_ID_START);
    }
    // log_time VARCHAR --> BIGINT
    {
        TTypeDesc int_type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::BIGINT);
            node.__set_scalar_type(scalar_type);
            int_type.types.push_back(node);
        }
        TExprNode cast_expr;
        cast_expr.node_type = TExprNodeType::CAST_EXPR;
        cast_expr.type = int_type;
        cast_expr.__set_opcode(TExprOpcode::CAST);
        cast_expr.__set_num_children(1);
        cast_expr.__set_output_scale(-1);
        cast_expr.__isset.fn = true;
        cast_expr.fn.name.function_name = "casttoint";
        cast_expr.fn.binary_type = TFunctionBinaryType::BUILTIN;
        cast_expr.fn.arg_types.push_back(varchar_type);
        cast_expr.fn.ret_type = int_type;
        cast_expr.fn.has_var_args = false;
        cast_expr.fn.__set_signature("casttoint(VARCHAR(*))");
        cast_expr.fn.__isset.scalar_fn = true;
        cast_expr.fn.scalar_fn.symbol = "starrocks::CastFunctions::cast_to_big_int_val";

        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = varchar_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = SRC_TUPLE_SLOT_ID_START + 1; // log_time id in src tuple
        slot_ref.slot_ref.tuple_id = 1;

        TExpr expr;
        expr.nodes.push_back(cast_expr);
        expr.nodes.push_back(slot_ref);

        _params.expr_of_dest_slot.emplace(DST_TUPLE_SLOT_ID_START + 1, expr);
        _params.src_slot_ids.push_back(SRC_TUPLE_SLOT_ID_START + 1);
    }
    // log_time_stamp VARCHAR --> BIGINT
    {
        TTypeDesc int_type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::BIGINT);
            node.__set_scalar_type(scalar_type);
            int_type.types.push_back(node);
        }
        TExprNode cast_expr;
        cast_expr.node_type = TExprNodeType::CAST_EXPR;
        cast_expr.type = int_type;
        cast_expr.__set_opcode(TExprOpcode::CAST);
        cast_expr.__set_num_children(1);
        cast_expr.__set_output_scale(-1);
        cast_expr.__isset.fn = true;
        cast_expr.fn.name.function_name = "casttoint";
        cast_expr.fn.binary_type = TFunctionBinaryType::BUILTIN;
        cast_expr.fn.arg_types.push_back(varchar_type);
        cast_expr.fn.ret_type = int_type;
        cast_expr.fn.has_var_args = false;
        cast_expr.fn.__set_signature("casttoint(VARCHAR(*))");
        cast_expr.fn.__isset.scalar_fn = true;
        cast_expr.fn.scalar_fn.symbol = "starrocks::CastFunctions::cast_to_big_int_val";

        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = varchar_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = SRC_TUPLE_SLOT_ID_START + 2;
        slot_ref.slot_ref.tuple_id = 1;

        TExpr expr;
        expr.nodes.push_back(cast_expr);
        expr.nodes.push_back(slot_ref);

        _params.expr_of_dest_slot.emplace(DST_TUPLE_SLOT_ID_START + 2, expr);
        _params.src_slot_ids.push_back(SRC_TUPLE_SLOT_ID_START + 2);
    }
    // could't convert type
    for (int i = 3; i < CLOMN_NUMBERS; i++) {
        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = varchar_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = SRC_TUPLE_SLOT_ID_START + i; // log_time id in src tuple
        slot_ref.slot_ref.tuple_id = 1;

        TExpr expr;
        expr.nodes.push_back(slot_ref);

        _params.expr_of_dest_slot.emplace(DST_TUPLE_SLOT_ID_START + i, expr);
        _params.src_slot_ids.push_back(SRC_TUPLE_SLOT_ID_START + i);
    }

    // _params.__isset.expr_of_dest_slot = true;
    _params.__set_dest_tuple_id(TUPLE_ID_DST);
    _params.__set_src_tuple_id(TUPLE_ID_SRC);
}

void ParquetSannerTest::init() {
    create_expr_info();
    init_desc_table();

    // Node Id
    _tnode.node_id = 0;
    _tnode.node_type = TPlanNodeType::SCHEMA_SCAN_NODE;
    _tnode.num_children = 0;
    _tnode.limit = -1;
    _tnode.row_tuples.push_back(0);
    _tnode.nullable_tuples.push_back(false);
    _tnode.file_scan_node.tuple_id = 0;
    _tnode.__isset.file_scan_node = true;
}

TEST_F(ParquetScannerTest, test_arrow_null) {
    std::vector<std::string> column_names{"col_int_null", "col_string_null"};
    std::string parquet_file_name = test_exec_dir + "/test_data/parquet_data/data_null.parquet";
    std::vector<std::string> file_names{parquet_file_name};

    auto slot_infos = select_columns(column_names, true);
    auto ranges = generate_ranges(file_names, column_names.size(), {});
    auto* desc_tbl = DescTblHelper::generate_desc_tbl(_runtime_state, _obj_pool, {slot_infos, slot_infos});
    auto scanner = create_parquet_scanner("UTC", desc_tbl, {}, ranges);
    auto check = [](const ChunkPtr& chunk) {};
    validate(scanner, 3, check);
}

} // namespace starrocks
