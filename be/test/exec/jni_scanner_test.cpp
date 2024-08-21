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

#include "exec/jni_scanner.h"

#include <gtest/gtest.h>

#include "runtime/descriptor_helper.h"
#include "runtime/runtime_state.h"
#include "util/thrift_util.h"

namespace starrocks {

namespace {
struct SlotDesc {
    string name;
    TypeDescriptor type;
};
} // namespace

class JniScannerTest : public ::testing::Test {
public:
    void SetUp() override { create_runtime_state(""); }

    std::string read_from_file(const std::string& path) {
        std::ifstream inputFile(path);
        std::stringstream buffer;
        buffer << inputFile.rdbuf();
        return buffer.str();
    }

    void thrift_read_from_file(::apache::thrift::TBase* base, const std::string& path) {
        std::string data = read_from_file(path);
        thrift_from_json_string(base, data);
    }

    void init_fs_options(FSOptions* options) {
        TCloudConfiguration* cloud_configuration = _pool.add(new TCloudConfiguration());
        options->cloud_configuration = cloud_configuration;
        std::map<std::string, std::string> kvs = {
                {"xxx", "xxx0"},
                {"yyy", "yyy0"},
                {"zzz", "zzz0"},
        };
        cloud_configuration->__set_cloud_properties(kvs);
    }

    void init_hdfs_scanner_context(HdfsScannerContext* ctx, TupleDescriptor* tuple_desc) {
        const auto& slots = tuple_desc->slots();
        for (int i = 0; i < slots.size(); i++) {
            SlotDescriptor* slot = slots[i];
            HdfsScannerContext::ColumnInfo info;
            info.idx_in_chunk = i;
            info.slot_desc = slot;
            ctx->materialized_columns.push_back(info);
        }
    }

    TupleDescriptor* create_tuple_desc(SlotDesc* descs) {
        TDescriptorTableBuilder table_desc_builder;
        TSlotDescriptorBuilder slot_desc_builder;
        TTupleDescriptorBuilder tuple_desc_builder;
        int slot_id = 0;
        while (descs->name != "") {
            slot_desc_builder.column_name(descs->name).type(descs->type).id(slot_id).nullable(true);
            tuple_desc_builder.add_slot(slot_desc_builder.build());
            descs += 1;
            slot_id += 1;
        }
        tuple_desc_builder.build(&table_desc_builder);
        std::vector<TTupleId> row_tuples = std::vector<TTupleId>{0};
        DescriptorTbl* tbl = nullptr;
        CHECK(DescriptorTbl::create(_runtime_state, &_pool, table_desc_builder.desc_tbl(), &tbl,
                                    config::vector_chunk_size)
                      .ok());
        auto* row_desc = _pool.add(new RowDescriptor(*tbl, row_tuples));
        auto* tuple_desc = row_desc->tuple_descriptors()[0];
        return tuple_desc;
    }

    TupleDescriptor* create_default_tuple_desc() {
        SlotDesc c0{"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)};
        SlotDesc c1{"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT)};
        c1.type.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
        c1.type.field_names.emplace_back("Cc1");

        SlotDesc slot_descs[] = {c0, c1, {""}};
        return create_tuple_desc(slot_descs);
    }

    void create_runtime_state(const std::string& timezone) {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        TQueryGlobals query_globals;
        if (timezone != "") {
            query_globals.__set_time_zone(timezone);
        }
        _runtime_state = _pool.add(new RuntimeState(fragment_id, query_options, query_globals, nullptr));
        _runtime_state->init_instance_mem_tracker();
    }

    ObjectPool _pool;
    RuntimeState* _runtime_state;
};

static void print_jni_scanner_params(const std::map<std::string, std::string>& params) {
    std::cout << "-------- params --------\n";
    for (const auto& kv : params) {
        std::cout << "{\"" << kv.first << "\", \"" << kv.second.substr(0, std::min<int>(kv.second.size(), 128))
                  << "\"},\n";
    }
}

static void check_jni_scanner_params(const std::map<std::string, std::string>& params,
                                     const std::map<std::string, std::string>& expected) {
    ASSERT_EQ(params.size(), expected.size());
    for (const auto& kv : params) {
        std::string act = kv.second.substr(0, std::min<int>(kv.second.size(), 128));
        const std::string exp = expected.at(kv.first);
        ASSERT_EQ(act, exp);
    }
}

TEST_F(JniScannerTest, test_create_paimon_jni_scanner) {
    // create jni scanner
    JniScanner::CreateOptions options;

    TTableDescriptor t_table_desc;
    thrift_read_from_file(&t_table_desc, "./be/test/exec/test_data/jni_scanner/test_create_scanner/paimon.table.json");
    PaimonTableDescriptor paimonTable(t_table_desc, &_pool);
    options.hive_table = &paimonTable;

    THdfsScanRange t_hdfs_scan_range;
    thrift_read_from_file(&t_hdfs_scan_range,
                          "./be/test/exec/test_data/jni_scanner/test_create_scanner/paimon.scan.json");
    options.scan_range = &t_hdfs_scan_range;

    FSOptions fs_options;
    init_fs_options(&fs_options);
    options.fs_options = &fs_options;

    auto scanner = create_paimon_jni_scanner(options);

    // update columns.
    TupleDescriptor* tuple_desc = create_default_tuple_desc();
    init_hdfs_scanner_context(&(scanner->_scanner_ctx), tuple_desc);
    scanner->update_jni_scanner_params();

    // check parameters.
    print_jni_scanner_params(scanner->_jni_scanner_params);

    std::map<std::string, std::string> expected = {
            {"native_table",
             "rO0ABXNyADBvcmcuYXBhY2hlLnBhaW1vbi50YWJsZS5BcHBlbmRPbmx5RmlsZVN0b3JlVGFibGUAAAAAAAAAAQIAAHhyAC5vcmcuYXBhY"
             "2hlLnBhaW1vbi50YWJsZS5B"},
            {"nested_fields", "c1.Cc1"},
            {"predicate_info", "rO0ABXNyABNqYXZhLnV0aWwuQXJyYXlMaXN0eIHSHZnHYZ0DAAFJAARzaXpleHAAAAAAdwQAAAAAeA"},
            {"required_fields", "c0,c1"},
            {"split_info",
             "rO0ABXNyAChvcmcuYXBhY2hlLnBhaW1vbi50YWJsZS5zb3VyY2UuRGF0YVNwbGl0AAAAAAAAAAUDAAdJAAZidWNrZXRaAAtpc1N0cmVhb"
             "WluZ0oACnNuYXBzaG90SWRM"},
            {"time_zone", "Asia/Shanghai"},
    };
    check_jni_scanner_params(scanner->_jni_scanner_params, expected);
}

TEST_F(JniScannerTest, test_create_hudi_jni_scanner) {
    // create jni scanner
    JniScanner::CreateOptions options;

    TTableDescriptor t_table_desc;
    thrift_read_from_file(&t_table_desc, "./be/test/exec/test_data/jni_scanner/test_create_scanner/hudi.table.json");
    HudiTableDescriptor hudiTable(t_table_desc, &_pool);
    options.hive_table = &hudiTable;

    THdfsScanRange t_hdfs_scan_range;
    thrift_read_from_file(&t_hdfs_scan_range,
                          "./be/test/exec/test_data/jni_scanner/test_create_scanner/hudi.scan.json");
    options.scan_range = &t_hdfs_scan_range;

    FSOptions fs_options;
    init_fs_options(&fs_options);
    options.fs_options = &fs_options;

    auto scanner = create_hudi_jni_scanner(options);

    // update columns.
    TupleDescriptor* tuple_desc = create_default_tuple_desc();
    init_hdfs_scanner_context(&(scanner->_scanner_ctx), tuple_desc);
    scanner->update_jni_scanner_params();

    // check parameters.
    print_jni_scanner_params(scanner->_jni_scanner_params);

    std::map<std::string, std::string> expected = {
            {"base_path",
             "hdfs://emr-header-1.cluster-49091:9000/user/hive/warehouse/hudi_db.db/hudi_mor_parquet_snappy"},
            {"data_file_length", "440794"},
            {"data_file_path",
             "hdfs://emr-header-1.cluster-49091:9000/user/hive/warehouse/hudi_db.db/hudi_mor_parquet_snappy/"
             "cdbd9f89-236c-457f-95e1-6acf2dfccf"},
            {"delta_file_paths", ""},
            {"fs_options_props", "xxx\x1xxx0\x2yyy\x1yyy0\x2zzz\x1zzz0"},
            {"hive_column_names",
             "_hoodie_commit_time,_hoodie_commit_seqno,_hoodie_record_key,_hoodie_partition_path,_hoodie_file_name,"
             "uuid,col_boolean,col_int,co"},
            {"hive_column_types",
             "string#string#string#string#string#int#boolean#int#bigint#float#double#decimal(38,18)#date#timestamp-"
             "micros#string#string#array<"},
            {"input_format", "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat"},
            {"instant_time", "20230810190954595"},
            {"nested_fields", "c1.Cc1"},
            {"required_fields", "c0,c1"},
            {"serde", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"},
            {"time_zone", "Asia/Shanghai"},
    };
    check_jni_scanner_params(scanner->_jni_scanner_params, expected);
}

TEST_F(JniScannerTest, test_create_hive_jni_scanner) {
    // create jni scanner
    JniScanner::CreateOptions options;

    TTableDescriptor t_table_desc;
    thrift_read_from_file(&t_table_desc, "./be/test/exec/test_data/jni_scanner/test_create_scanner/hive.table.json");
    HdfsTableDescriptor hdfsTable(t_table_desc, &_pool);
    options.hive_table = &hdfsTable;

    THdfsScanRange t_hdfs_scan_range;
    thrift_read_from_file(&t_hdfs_scan_range,
                          "./be/test/exec/test_data/jni_scanner/test_create_scanner/hive.scan.json");
    options.scan_range = &t_hdfs_scan_range;

    {
        FSOptions fs_options;
        init_fs_options(&fs_options);
        options.fs_options = &fs_options;

        auto scanner = create_hive_jni_scanner(options);
        // update columns.
        TupleDescriptor* tuple_desc = create_default_tuple_desc();
        init_hdfs_scanner_context(&(scanner->_scanner_ctx), tuple_desc);
        scanner->update_jni_scanner_params();

        // check parameters.
        print_jni_scanner_params(scanner->_jni_scanner_params);

        std::map<std::string, std::string> expected = {
                {"SerDe.serialization.format", "1"},
                {"block_length", "713"},
                {"block_offset", "0"},
                {"data_file_path",
                 "hdfs://emr-header-1.cluster-49091:9000/user/hive/warehouse/hive_extbl_test.db/"
                 "hive_hdfs_rcfile_snappy/part-00001-29762f87-098a-4"},
                {"fs_options_props", "xxx\x1xxx0\x2yyy\x1yyy0\x2zzz\x1zzz0"},
                {"hive_column_names",
                 "col_tinyint,col_smallint,col_int,col_integer,col_bigint,col_float,col_double,col_double_precision,"
                 "col_decimal,col_timestamp,col_"},
                {"hive_column_types",
                 "tinyint#smallint#int#int#bigint#float#double#double#decimal(10,2)#timestamp#date#string#varchar(100)#"
                 "binary#char(100)#boolean#ar"},
                {"input_format", "org.apache.hadoop.hive.ql.io.RCFileInputFormat"},
                {"nested_fields", "c1.Cc1"},
                {"required_fields", "c0,c1"},
                {"serde", "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe"},
                {"time_zone", "Asia/Shanghai"},
        };
        check_jni_scanner_params(scanner->_jni_scanner_params, expected);
    }

    {
        FSOptions fs_options;
        init_fs_options(&fs_options);
        options.fs_options = &fs_options;

        auto scanner = create_hive_jni_scanner(options);

        // update columns.
        TupleDescriptor* tuple_desc = create_default_tuple_desc();
        init_hdfs_scanner_context(&(scanner->_scanner_ctx), tuple_desc);
        scanner->update_jni_scanner_params();

        // check parameters.
        print_jni_scanner_params(scanner->_jni_scanner_params);
        std::map<std::string, std::string> expected = {
                {"SerDe.serialization.format", "1"},
                {"block_length", "713"},
                {"block_offset", "0"},
                {"data_file_path",
                 "hdfs://emr-header-1.cluster-49091:9000/user/hive/warehouse/hive_extbl_test.db/"
                 "hive_hdfs_rcfile_snappy/part-00001-29762f87-098a-4"},
                {"fs_options_props", "xxx\x1xxx0\x2yyy\x1yyy0\x2zzz\x1zzz0"},
                {"hive_column_names",
                 "col_tinyint,col_smallint,col_int,col_integer,col_bigint,col_float,col_double,col_double_precision,"
                 "col_decimal,col_timestamp,col_"},
                {"hive_column_types",
                 "tinyint#smallint#int#int#bigint#float#double#double#decimal(10,2)#timestamp#date#string#varchar(100)#"
                 "binary#char(100)#boolean#ar"},
                {"input_format", "org.apache.hadoop.hive.ql.io.RCFileInputFormat"},
                {"nested_fields", "c1.Cc1"},
                {"required_fields", "c0,c1"},
                {"serde", "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe"},
                {"time_zone", "Asia/Shanghai"},
        };
        check_jni_scanner_params(scanner->_jni_scanner_params, expected);
    }
}

TEST_F(JniScannerTest, test_create_hive_jni_scanner2) {
    // create jni scanner
    JniScanner::CreateOptions options;

    TTableDescriptor t_table_desc;
    thrift_read_from_file(&t_table_desc, "./be/test/exec/test_data/jni_scanner/test_create_scanner/hive.table.json");
    FileTableDescriptor fileTable(t_table_desc, &_pool);
    options.hive_table = &fileTable;

    THdfsScanRange t_hdfs_scan_range;
    thrift_read_from_file(&t_hdfs_scan_range,
                          "./be/test/exec/test_data/jni_scanner/test_create_scanner/hive.scan.json");
    options.scan_range = &t_hdfs_scan_range;

    {
        FSOptions fs_options;
        init_fs_options(&fs_options);
        options.fs_options = &fs_options;

        auto scanner = create_hive_jni_scanner(options);
        // update columns.
        TupleDescriptor* tuple_desc = create_default_tuple_desc();
        init_hdfs_scanner_context(&(scanner->_scanner_ctx), tuple_desc);
        scanner->update_jni_scanner_params();

        // check parameters.
        print_jni_scanner_params(scanner->_jni_scanner_params);

        std::map<std::string, std::string> expected = {
                {"block_length", "713"},
                {"block_offset", "0"},
                {"data_file_path", ""},
                {"fs_options_props", "xxx\x1xxx0\x2yyy\x1yyy0\x2zzz\x1zzz0"},
                {"hive_column_names", ""},
                {"hive_column_types", ""},
                {"input_format", ""},
                {"nested_fields", "c1.Cc1"},
                {"required_fields", "c0,c1"},
                {"serde", ""},
                {"time_zone", ""},
        };
        check_jni_scanner_params(scanner->_jni_scanner_params, expected);
    }
}

TEST_F(JniScannerTest, test_create_odps_jni_scanner) {
    // create jni scanner
    JniScanner::CreateOptions options;

    TTableDescriptor t_table_desc;
    thrift_read_from_file(&t_table_desc, "./be/test/exec/test_data/jni_scanner/test_create_scanner/odps.table.json");
    OdpsTableDescriptor odpsTable(t_table_desc, &_pool);
    options.hive_table = &odpsTable;

    THdfsScanRange t_hdfs_scan_range;
    thrift_read_from_file(&t_hdfs_scan_range,
                          "./be/test/exec/test_data/jni_scanner/test_create_scanner/odps.scan.json");
    options.scan_range = &t_hdfs_scan_range;

    FSOptions fs_options;
    init_fs_options(&fs_options);
    TCloudConfiguration* cloud_conf = const_cast<TCloudConfiguration*>(fs_options.cloud_configuration);
    cloud_conf->__set_cloud_type(TCloudType::ALIYUN);
    options.fs_options = &fs_options;

    auto scanner = create_odps_jni_scanner(options);
    // update columns.
    TupleDescriptor* tuple_desc = create_default_tuple_desc();
    init_hdfs_scanner_context(&(scanner->_scanner_ctx), tuple_desc);
    scanner->update_jni_scanner_params();

    // check parameters.
    print_jni_scanner_params(scanner->_jni_scanner_params);

    std::map<std::string, std::string> expected = {
            {"access_id", ""},
            {"access_key", ""},
            {"endpoint", ""},
            {"nested_fields", "c1.Cc1"},
            {"project_name", "hive_extbl_test"},
            {"required_fields", "c0,c1"},
            {"table_name", "hive_hdfs_rcfile_snappy"},
            {"time_zone", "Asia/Shanghai"},
    };
    check_jni_scanner_params(scanner->_jni_scanner_params, expected);
}

} // namespace starrocks
