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

struct SlotDesc {
    string name;
    TypeDescriptor type;
};

class JniScannerTest : public ::testing::Test {
public:
    void SetUp() override { create_runtime_state(""); }

    std::string read_from_file(const std::string& path) {
        std::ifstream inputFile(path);
        std::stringstream buffer;
        std::string jsonString;
        buffer << inputFile.rdbuf();
        jsonString = buffer.str();
        return jsonString;
    }

    void thrift_read_from_file(::apache::thrift::TBase* base, const std::string& path) {
        std::string data = read_from_file(path);
        thrift_from_json_string(base, data);
    }

    void init_fs_options(FSOptions* options, bool v2 = false) {
        TCloudConfiguration* cloud_configuration = _pool.add(new TCloudConfiguration());
        options->cloud_configuration = cloud_configuration;
        std::map<std::string, std::string> kvs = {
                {"xxx", "xxx0"},
                {"yyy", "yyy0"},
                {"zzz", "zzz0"},
        };
        if (v2) {
            cloud_configuration->__set_cloud_properties_v2(kvs);
        } else {
            std::vector<TCloudProperty> props;
            for (const auto& kv : kvs) {
                TCloudProperty prop;
                prop.key = kv.first;
                prop.value = kv.second;
                props.push_back(prop);
            }
            cloud_configuration->__set_cloud_properties(props);
        }
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
        std::vector<bool> nullable_tuples = std::vector<bool>{true};
        DescriptorTbl* tbl = nullptr;
        CHECK(DescriptorTbl::create(_runtime_state, &_pool, table_desc_builder.desc_tbl(), &tbl,
                                    config::vector_chunk_size)
                      .ok());
        auto* row_desc = _pool.add(new RowDescriptor(*tbl, row_tuples, nullable_tuples));
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

    JniScanner* scanner = create_paimon_jni_scanner(options);

    // update columns.
    TupleDescriptor* tuple_desc = create_default_tuple_desc();
    init_hdfs_scanner_context(&(scanner->_scanner_ctx), tuple_desc);
    scanner->update_jni_scanner_params();

    // check parameters.
    for (const auto& kv : scanner->_jni_scanner_params) {
        if (kv.second.size() < 128) {
            std::cout << "{\"" << kv.first << "\", \"" << kv.second << "\"},\n";
        }
    }
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

    JniScanner* scanner = create_hudi_jni_scanner(options);

    // update columns.
    TupleDescriptor* tuple_desc = create_default_tuple_desc();
    init_hdfs_scanner_context(&(scanner->_scanner_ctx), tuple_desc);
    scanner->update_jni_scanner_params();

    // check parameters.
    for (const auto& kv : scanner->_jni_scanner_params) {
        if (kv.second.size() < 128) {
            std::cout << "{\"" << kv.first << "\", \"" << kv.second << "\"},\n";
        }
    }
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

        JniScanner* scanner = create_hive_jni_scanner(options);
            // update columns.
    TupleDescriptor* tuple_desc = create_default_tuple_desc();
    init_hdfs_scanner_context(&(scanner->_scanner_ctx), tuple_desc);
    scanner->update_jni_scanner_params();

        // check parameters.
        for (const auto& kv : scanner->_jni_scanner_params) {
            if (kv.second.size() < 128) {
                std::cout << "{\"" << kv.first << "\", \"" << kv.second << "\"},\n";
            }
        }
    }


    {
        FSOptions fs_options;
        init_fs_options(&fs_options, true);
        options.fs_options = &fs_options;

        JniScanner* scanner = create_hive_jni_scanner(options);

            // update columns.
    TupleDescriptor* tuple_desc = create_default_tuple_desc();
    init_hdfs_scanner_context(&(scanner->_scanner_ctx), tuple_desc);
    scanner->update_jni_scanner_params();

        // check parameters.
        for (const auto& kv : scanner->_jni_scanner_params) {
            if (kv.second.size() < 128) {
                std::cout << "{\"" << kv.first << "\", \"" << kv.second << "\"},\n";
            }
        }
    }

}
} // namespace starrocks
