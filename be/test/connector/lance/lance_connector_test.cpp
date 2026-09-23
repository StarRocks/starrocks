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

#include "connector/lance/lance_connector.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "base/testutil/sync_point.h"
#include "compute_env/global_dict/fragment_dict_state.h"
#include "compute_env/query/fragment_runtime_state.h"
#include "connector/hive/scanner/jni_scanner.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors_ext.h"
#include "runtime/runtime_state.h"

namespace starrocks::connector {

// Exercise the real data source and HdfsScanner lifecycle without starting a JVM or using cloud credentials.
class StubLanceScanner final : public JniScanner {
public:
    StubLanceScanner(const std::map<std::string, std::string>& params, int* close_count)
            : JniScanner("unused", params), _close_count(close_count) {}
    Status do_init(RuntimeState*, const HdfsScannerContext&) override { return init_status; }
    Status do_open(RuntimeState*) override { return open_status; }
    Status do_get_next(RuntimeState*, ChunkPtr* chunk) override {
        if (!read_status.ok()) {
            return read_status;
        }
        (*chunk)->get_column_by_index(0)->as_mutable_ptr()->append_datum(Datum(int32_t(42)));
        return Status::OK();
    }
    void do_close(RuntimeState*) noexcept override { ++*_close_count; }

    Status init_status;
    Status open_status;
    Status read_status;

private:
    int* _close_count;
};

class LanceConnectorTest : public ::testing::Test {
public:
    void SetUp() override {
        _state = std::make_unique<RuntimeState>(TUniqueId(), TQueryOptions(), TQueryGlobals(),
                                                static_cast<ExecEnv*>(nullptr));
        _state->init_instance_mem_tracker();
        _state->set_fragment_runtime_state(&_fragment_state);
        _state->set_fragment_dict_state(&_dict_state);
        _fragment_state.set_pred_tree_params({true, true});
        TDescriptorTableBuilder builder;
        TTupleDescriptorBuilder tuple;
        TSlotDescriptorBuilder slot;
        slot.type(TYPE_INT).nullable(true).column_name("id");
        tuple.add_slot(slot.build());
        tuple.build(&builder);
        DescriptorTbl* descriptors = nullptr;
        ASSERT_OK(DescriptorTbl::create(_state.get(), _state->obj_pool(), builder.desc_tbl(), &descriptors, 4096));
        _state->set_desc_tbl(descriptors);
        _tuple = descriptors->get_tuple_descriptor(0);
        TTableDescriptor table;
        table.__set_id(7);
        table.__set_tableType(TTableType::LANCE_TABLE);
        TLanceTable lance;
        lance.__set_lance_dataset_uri("abfss://data@account.dfs.core.windows.net/rows.lance");
        table.__set_lanceTable(lance);
        _tuple->set_table_desc(_state->obj_pool()->add(new LanceTableDescriptor(table)));
        _plan.hdfs_scan_node.__set_tuple_id(0);
        _range.hdfs_scan_range.__set_offset(0);
        _range.hdfs_scan_range.__set_length(0);
        _range.hdfs_scan_range.__set_file_length(0);
        auto* sync = SyncPoint::GetInstance();
        sync->SetCallBack("LanceDataSource::open:scanner", [this](void* arg) {
            auto* scanner = static_cast<std::unique_ptr<JniScanner>*>(arg);
            _params = (*scanner)->_jni_scanner_params;
            _factory = (*scanner)->_jni_scanner_factory_class;
            auto stub = std::make_unique<StubLanceScanner>(_params, &_close_count);
            stub->init_status = _init_status;
            stub->open_status = _open_status;
            _scanner = stub.get();
            *scanner = std::move(stub);
        });
        sync->EnableProcessing();
    }

    void TearDown() override {
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearAllCallBacks();
    }

protected:
    pipeline::FragmentRuntimeState _fragment_state;
    FragmentDictState _dict_state;
    std::unique_ptr<RuntimeState> _state;
    TupleDescriptor* _tuple = nullptr;
    TPlanNode _plan;
    TScanRange _range;
    std::map<std::string, std::string> _params;
    std::string _factory;
    StubLanceScanner* _scanner = nullptr;
    Status _init_status;
    Status _open_status;
    int _close_count = 0;
};

TEST_F(LanceConnectorTest, ProviderAndFullDatasetScan) {
    LanceConnector connector;
    EXPECT_EQ(ConnectorType::LANCE, connector.connector_type());
    auto provider = connector.create_data_source_provider(nullptr, _plan);
    EXPECT_TRUE(provider->insert_local_exchange_operator());
    EXPECT_FALSE(provider->accept_empty_scan_ranges());
    EXPECT_EQ(_tuple, provider->tuple_descriptor(_state.get()));
    auto source = provider->create_data_source(_range);
    EXPECT_EQ("LanceDataSource", source->name());
    source->close(_state.get());
    ASSERT_OK(source->open(_state.get()));
    EXPECT_EQ("com/starrocks/lance/reader/LanceSplitScannerFactory", _factory);
    EXPECT_EQ("abfss://data@account.dfs.core.windows.net/rows.lance", _params.at("lance_dataset_uri"));
    EXPECT_EQ(0, _params.count("lance.cloud_type"));
    EXPECT_EQ(_tuple, _scanner->_scanner_ctx->tuple_desc);
    EXPECT_EQ(_tuple->slots(), _scanner->_scanner_ctx->materialize_slots);
    EXPECT_EQ(std::vector<int>{0}, _scanner->_scanner_ctx->materialize_index_in_chunk);
    EXPECT_EQ(0, source->num_rows_read());
    EXPECT_EQ(0, source->num_bytes_read());
    EXPECT_EQ(0, source->cpu_time_spent());
    ChunkPtr chunk;
    ASSERT_OK(source->get_next(_state.get(), &chunk));
    EXPECT_EQ(1, chunk->num_rows());
    EXPECT_EQ(42, chunk->get_column_by_index(0)->get(0).get_int32());
    EXPECT_EQ(1, source->raw_rows_read());
    EXPECT_EQ(1, source->num_rows_read());
    EXPECT_EQ(chunk->bytes_usage(), source->num_bytes_read());
    int64_t bytes = source->num_bytes_read();
    _scanner->read_status = Status::EndOfFile("done");
    EXPECT_TRUE(source->get_next(_state.get(), &chunk).is_end_of_file());
    _scanner->read_status = Status::IOError("read failed");
    EXPECT_TRUE(source->get_next(_state.get(), &chunk).is_io_error());
    EXPECT_EQ(1, source->num_rows_read());
    EXPECT_EQ(bytes, source->num_bytes_read());
    source->close(_state.get());
    source->close(_state.get());
    EXPECT_EQ(1, _close_count);
}

TEST_F(LanceConnectorTest, ForwardsCatalogCloudProperties) {
    for (auto type : {TCloudType::DEFAULT, TCloudType::AWS, TCloudType::AZURE}) {
        TCloudConfiguration cloud;
        cloud.__set_cloud_type(type);
        cloud.__set_cloud_properties({{"credential", "test-token"}});
        _plan.hdfs_scan_node.__set_cloud_configuration(cloud);
        LanceDataSourceProvider provider(nullptr, _plan);
        auto source = provider.create_data_source(_range);
        ASSERT_OK(source->open(_state.get()));
        EXPECT_EQ(type == TCloudType::AWS ? "AWS" : type == TCloudType::AZURE ? "AZURE" : "DEFAULT",
                  _params.at("lance.cloud_type"));
        EXPECT_EQ("test-token", _params.at("lance.cloud.credential"));
        source->close(_state.get());
    }
}

TEST_F(LanceConnectorTest, RejectsUnsupportedCloudBeforeOpeningReader) {
    TCloudConfiguration cloud;
    cloud.__set_cloud_type(TCloudType::HDFS);
    _plan.hdfs_scan_node.__set_cloud_configuration(cloud);
    LanceDataSourceProvider provider(nullptr, _plan);
    auto source = provider.create_data_source(_range);
    EXPECT_TRUE(source->open(_state.get()).is_not_supported());
    EXPECT_EQ(nullptr, _scanner);
    source->close(_state.get());
}

TEST_F(LanceConnectorTest, RejectsFragmentSplitsBeforeOpeningReader) {
    _range.hdfs_scan_range.__set_lance_split_info("fragment");
    LanceDataSourceProvider provider(nullptr, _plan);
    auto source = provider.create_data_source(_range);
    EXPECT_TRUE(source->open(_state.get()).is_not_supported());
    EXPECT_EQ(nullptr, _scanner);
    source->close(_state.get());
}

TEST_F(LanceConnectorTest, RejectsMissingTuple) {
    _plan.hdfs_scan_node.__set_tuple_id(999);
    LanceDataSourceProvider provider(nullptr, _plan);
    auto source = provider.create_data_source(_range);
    EXPECT_TRUE(source->open(_state.get()).is_internal_error());
    source->close(_state.get());
}

TEST_F(LanceConnectorTest, RejectsMissingLanceTable) {
    _tuple->set_table_desc(nullptr);
    LanceDataSourceProvider provider(nullptr, _plan);
    auto source = provider.create_data_source(_range);
    EXPECT_TRUE(source->open(_state.get()).is_internal_error());
    source->close(_state.get());
}

TEST_F(LanceConnectorTest, PreservesReaderInitFailure) {
    _init_status = Status::IOError("init failed");
    LanceDataSourceProvider provider(nullptr, _plan);
    auto source = provider.create_data_source(_range);
    EXPECT_EQ(_init_status.to_string(), source->open(_state.get()).to_string());
    source->close(_state.get());
    EXPECT_EQ(1, _close_count);
}

TEST_F(LanceConnectorTest, PreservesReaderOpenFailure) {
    _open_status = Status::IOError("open failed");
    LanceDataSourceProvider provider(nullptr, _plan);
    auto source = provider.create_data_source(_range);
    EXPECT_EQ(_open_status.to_string(), source->open(_state.get()).to_string());
    source->close(_state.get());
    EXPECT_EQ(1, _close_count);
}

} // namespace starrocks::connector
