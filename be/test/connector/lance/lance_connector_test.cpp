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

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "base/testutil/sync_point.h"
#include "column/fixed_length_column.h"
#include "compute_env/global_dict/fragment_dict_state.h"
#include "compute_env/query/fragment_runtime_state.h"
#include "connector/lance/lance_native_reader.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors_ext.h"
#include "runtime/runtime_state.h"

namespace starrocks::connector {

// Exercise the data-source lifecycle without cloud credentials or a JVM.
class StubLanceScanner final : public LanceNativeReader {
public:
    explicit StubLanceScanner(int* close_count) : _close_count(close_count) {}
    Status open(RuntimeState*, const TupleDescriptor* tuple, const std::string& uri,
                const TCloudConfiguration& cloud) override {
        this->tuple = tuple;
        this->uri = uri;
        this->cloud = cloud;
        return open_status;
    }
    Status get_next(RuntimeState*, ChunkPtr* chunk) override {
        if (!read_status.ok()) return read_status;
        auto data = Int32Column::create();
        data->append(42);
        *chunk = std::make_shared<Chunk>();
        (*chunk)->append_column(std::move(data), tuple->slots()[0]->id());
        return Status::OK();
    }
    void close() override { ++*_close_count; }
    const TupleDescriptor* tuple = nullptr;
    std::string uri;
    TCloudConfiguration cloud;
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
            auto* scanner = static_cast<std::unique_ptr<LanceNativeReader>*>(arg);
            auto stub = std::make_unique<StubLanceScanner>(&_close_count);
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
    const TupleDescriptor* tuple_for_type(const TypeDescriptor& type, const std::string& name = "id") {
        TDescriptorTableBuilder builder;
        TTupleDescriptorBuilder tuple;
        TSlotDescriptorBuilder slot;
        tuple.add_slot(slot.type(type).nullable(true).column_name(name).build());
        tuple.build(&builder);
        DescriptorTbl* descriptors = nullptr;
        auto status = DescriptorTbl::create(_state.get(), _state->obj_pool(), builder.desc_tbl(), &descriptors, 4096);
        EXPECT_OK(status);
        return status.ok() ? descriptors->get_tuple_descriptor(0) : nullptr;
    }
    pipeline::FragmentRuntimeState _fragment_state;
    FragmentDictState _dict_state;
    std::unique_ptr<RuntimeState> _state;
    TupleDescriptor* _tuple = nullptr;
    TPlanNode _plan;
    TScanRange _range;
    StubLanceScanner* _scanner = nullptr;
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
    EXPECT_EQ("abfss://data@account.dfs.core.windows.net/rows.lance", _scanner->uri);
    EXPECT_EQ(TCloudType::DEFAULT, _scanner->cloud.cloud_type);
    EXPECT_EQ(_tuple, _scanner->tuple);
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
        EXPECT_EQ(type, _scanner->cloud.cloud_type);
        EXPECT_EQ("test-token", _scanner->cloud.cloud_properties.at("credential"));
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

TEST_F(LanceConnectorTest, PreservesReaderOpenFailure) {
    _open_status = Status::IOError("open failed");
    LanceDataSourceProvider provider(nullptr, _plan);
    auto source = provider.create_data_source(_range);
    Status status = source->open(_state.get());
    EXPECT_TRUE(status.is_io_error());
    EXPECT_EQ(_open_status.message(), status.message());
    source->close(_state.get());
    EXPECT_EQ(1, _close_count);
}

TEST_F(LanceConnectorTest, ConvertsSlicedArrowBatchWithNulls) {
    arrow::Int32Builder builder;
    ASSERT_TRUE(builder.AppendValues({10, 20, 30}).ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());
    auto schema = arrow::schema({arrow::field("id", arrow::int32())});
    auto batch = arrow::RecordBatch::Make(schema, 4, {array})->Slice(1, 3);
    ChunkPtr chunk;
    ASSERT_OK(LanceNativeReader::convert_batch(_state.get(), _tuple, batch, &chunk));
    EXPECT_EQ(3, chunk->num_rows());
    EXPECT_EQ(20, chunk->get_column_by_index(0)->get(0).get_int32());
    EXPECT_EQ(30, chunk->get_column_by_index(0)->get(1).get_int32());
    EXPECT_TRUE(chunk->get_column_by_index(0)->is_null(2));
}

TEST_F(LanceConnectorTest, ConvertsFixedSizeVectorAndNull) {
    TypeDescriptor type(TYPE_ARRAY);
    type.children.push_back(TypeDescriptor(TYPE_FLOAT));
    const auto* tuple = tuple_for_type(type);
    ASSERT_NE(nullptr, tuple);
    auto values = std::make_shared<arrow::FloatBuilder>();
    arrow::FixedSizeListBuilder builder(arrow::default_memory_pool(), values, 2);
    ASSERT_TRUE(builder.Append().ok());
    ASSERT_TRUE(values->AppendValues({1.5f, 2.5f}).ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());
    auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("id", array->type())}), 2, {array});
    ChunkPtr chunk;
    ASSERT_OK(LanceNativeReader::convert_batch(_state.get(), tuple, batch, &chunk));
    auto result = chunk->get_column_by_index(0)->get(0).get_array();
    ASSERT_EQ(2, result.size());
    EXPECT_FLOAT_EQ(1.5f, result[0].get_float());
    EXPECT_FLOAT_EQ(2.5f, result[1].get_float());
    EXPECT_TRUE(chunk->get_column_by_index(0)->is_null(1));
}

TEST_F(LanceConnectorTest, PreservesUnsignedIntegerRange) {
    const auto* tuple = tuple_for_type(TypeDescriptor(TYPE_BIGINT));
    ASSERT_NE(nullptr, tuple);
    arrow::UInt32Builder builder;
    ASSERT_TRUE(builder.Append(UINT32_MAX).ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());
    auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("id", arrow::uint32())}), 1, {array});
    ChunkPtr chunk;
    ASSERT_OK(LanceNativeReader::convert_batch(_state.get(), tuple, batch, &chunk));
    EXPECT_EQ(int64_t(UINT32_MAX), chunk->get_column_by_index(0)->get(0).get_int64());
}

TEST_F(LanceConnectorTest, RejectsOverflowInsteadOfDroppingRows) {
    arrow::Int64Builder builder;
    ASSERT_TRUE(builder.Append(INT64_MAX).ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());
    auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("id", arrow::int64())}), 1, {array});
    ChunkPtr chunk;
    EXPECT_FALSE(LanceNativeReader::convert_batch(_state.get(), _tuple, batch, &chunk).ok());
}

TEST_F(LanceConnectorTest, RejectsMissingProjectedColumn) {
    arrow::Int32Builder builder;
    ASSERT_TRUE(builder.Append(10).ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());
    auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("other", arrow::int32())}), 1, {array});
    ChunkPtr chunk;
    EXPECT_TRUE(LanceNativeReader::convert_batch(_state.get(), _tuple, batch, &chunk).is_internal_error());
}

TEST_F(LanceConnectorTest, RejectsNarrowingNestedIntegerSchema) {
    TypeDescriptor type(TYPE_ARRAY);
    type.children.push_back(TypeDescriptor(TYPE_INT));
    const auto* tuple = tuple_for_type(type);
    ASSERT_NE(nullptr, tuple);
    auto values = std::make_shared<arrow::Int64Builder>();
    arrow::ListBuilder builder(arrow::default_memory_pool(), values);
    ASSERT_TRUE(builder.Append().ok());
    ASSERT_TRUE(values->Append(INT64_MAX).ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());
    auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("id", array->type())}), 1, {array});
    ChunkPtr chunk;
    EXPECT_TRUE(LanceNativeReader::convert_batch(_state.get(), tuple, batch, &chunk).is_data_quality_error());
}

TEST_F(LanceConnectorTest, CancelledQueryDoesNotRead) {
    LanceDataSourceProvider provider(nullptr, _plan);
    auto source = provider.create_data_source(_range);
    ASSERT_OK(source->open(_state.get()));
    _state->set_is_cancelled(true);
    ChunkPtr chunk;
    EXPECT_TRUE(source->get_next(_state.get(), &chunk).is_cancelled());
    EXPECT_EQ(0, source->raw_rows_read());
    source->close(_state.get());
}

TEST_F(LanceConnectorTest, ReadsNativeDatasetAcrossEveryFragment) {
    SyncPoint::GetInstance()->DisableProcessing();
    TTableDescriptor table;
    table.__set_id(7);
    table.__set_tableType(TTableType::LANCE_TABLE);
    TLanceTable lance;
    lance.__set_lance_dataset_uri(LANCE_TEST_DATASET);
    table.__set_lanceTable(lance);
    _tuple->set_table_desc(_state->obj_pool()->add(new LanceTableDescriptor(table)));
    LanceDataSourceProvider provider(nullptr, _plan);
    auto source = provider.create_data_source(_range);
    ASSERT_OK(source->open(_state.get()));
    std::vector<int32_t> ids;
    while (true) {
        ChunkPtr chunk;
        auto status = source->get_next(_state.get(), &chunk);
        if (status.is_end_of_file()) break;
        ASSERT_OK(status);
        for (size_t i = 0; i < chunk->num_rows(); ++i) {
            ids.push_back(chunk->get_column_by_index(0)->get(i).get_int32());
        }
    }
    source->close(_state.get());
    source->close(_state.get());
    std::sort(ids.begin(), ids.end());
    EXPECT_EQ((std::vector<int32_t>{0, 1, 2, 3, 4, 5, 6}), ids);
    EXPECT_EQ(7, source->raw_rows_read());
    EXPECT_EQ(7, source->num_rows_read());
}

TEST_F(LanceConnectorTest, ReopenResetsProjectionAndBufferedBatch) {
    TCloudConfiguration cloud;
    cloud.__set_cloud_type(TCloudType::DEFAULT);
    LanceNativeReader reader;
    ASSERT_OK(reader.open(_state.get(), _tuple, LANCE_TEST_DATASET, cloud));
    ChunkPtr chunk;
    ASSERT_OK(reader.get_next(_state.get(), &chunk));
    const auto* tuple = tuple_for_type(TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 20, 0), "u64");
    ASSERT_NE(nullptr, tuple);
    // Reopen before EOF: the old Arrow batch and projected field list must be replaced.
    ASSERT_OK(reader.open(_state.get(), tuple, LANCE_TEST_DATASET, cloud));
    size_t rows = 0;
    while (true) {
        auto status = reader.get_next(_state.get(), &chunk);
        if (status.is_end_of_file()) break;
        ASSERT_OK(status);
        ASSERT_EQ(1, chunk->num_columns());
        for (size_t i = 0; i < chunk->num_rows(); ++i) {
            const auto value = chunk->get_column_by_index(0)->get(i).get_int128();
            EXPECT_GE(value, static_cast<int128_t>(UINT64_MAX) - 6);
            EXPECT_LE(value, static_cast<int128_t>(UINT64_MAX));
        }
        rows += chunk->num_rows();
    }
    EXPECT_EQ(7, rows);
    reader.close();
    reader.close();
}

TEST_F(LanceConnectorTest, TracksAndReleasesNativeArrowMemory) {
    TCloudConfiguration cloud;
    cloud.__set_cloud_type(TCloudType::DEFAULT);
    LanceNativeReader reader;
    ASSERT_OK(reader.open(_state.get(), _tuple, LANCE_TEST_DATASET, cloud));
    const int64_t before = _state->instance_mem_tracker()->consumption();
    ChunkPtr chunk;
    ASSERT_OK(reader.get_next(_state.get(), &chunk));
    EXPECT_GT(_state->instance_mem_tracker()->consumption(), before);
    reader.close();
    EXPECT_EQ(before, _state->instance_mem_tracker()->consumption());
}

TEST_F(LanceConnectorTest, RejectsNativeBatchOverMemoryLimit) {
    TCloudConfiguration cloud;
    cloud.__set_cloud_type(TCloudType::DEFAULT);
    LanceNativeReader reader;
    ASSERT_OK(reader.open(_state.get(), _tuple, LANCE_TEST_DATASET, cloud));
    const int64_t before = _state->instance_mem_tracker()->consumption();
    _state->instance_mem_tracker()->set_limit(before);
    ChunkPtr chunk;
    EXPECT_TRUE(reader.get_next(_state.get(), &chunk).is_mem_limit_exceeded());
    reader.close();
    EXPECT_EQ(before, _state->instance_mem_tracker()->consumption());
    _state->instance_mem_tracker()->set_limit(-1);
}

TEST_F(LanceConnectorTest, NativeUnsigned64PreservesDecimalPrecision) {
    const auto* tuple = tuple_for_type(TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 20, 0), "u64");
    ASSERT_NE(nullptr, tuple);
    TCloudConfiguration cloud;
    cloud.__set_cloud_type(TCloudType::DEFAULT);
    LanceNativeReader reader;
    ASSERT_OK(reader.open(_state.get(), tuple, LANCE_TEST_DATASET, cloud));
    ChunkPtr chunk;
    ASSERT_OK(reader.get_next(_state.get(), &chunk));
    const auto value = chunk->get_column_by_index(0)->get(0).get_int128();
    EXPECT_GE(value, static_cast<int128_t>(UINT64_MAX) - 6);
    EXPECT_LE(value, static_cast<int128_t>(UINT64_MAX));
    reader.close();
}

TEST_F(LanceConnectorTest, PreservesUtcDateAndNegativeTimestampFractions) {
    const auto* date_tuple = tuple_for_type(TypeDescriptor(TYPE_DATE));
    ASSERT_NE(nullptr, date_tuple);
    arrow::Date64Builder dates;
    ASSERT_TRUE(dates.AppendValues({-1, -86400001, 0}).ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(dates.Finish(&array).ok());
    auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("id", array->type())}), 3, {array});
    ChunkPtr chunk;
    ASSERT_OK(LanceNativeReader::convert_batch(_state.get(), date_tuple, batch, &chunk));
    EXPECT_EQ("1969-12-31", chunk->get_column_by_index(0)->get(0).get_date().to_string());
    EXPECT_EQ("1969-12-30", chunk->get_column_by_index(0)->get(1).get_date().to_string());
    EXPECT_EQ("1970-01-01", chunk->get_column_by_index(0)->get(2).get_date().to_string());

    const auto* timestamp_tuple = tuple_for_type(TypeDescriptor(TYPE_DATETIME));
    ASSERT_NE(nullptr, timestamp_tuple);
    for (auto unit : {arrow::TimeUnit::SECOND, arrow::TimeUnit::MILLI, arrow::TimeUnit::MICRO, arrow::TimeUnit::NANO}) {
        arrow::TimestampBuilder timestamps(arrow::timestamp(unit, "Asia/Shanghai"), arrow::default_memory_pool());
        ASSERT_TRUE(timestamps.AppendValues({-1, 0}).ok());
        ASSERT_TRUE(timestamps.Finish(&array).ok());
        batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("id", array->type())}), 2, {array});
        ASSERT_OK(LanceNativeReader::convert_batch(_state.get(), timestamp_tuple, batch, &chunk));
        const auto timestamp = chunk->get_column_by_index(0)->get(0).get_timestamp();
        EXPECT_EQ(unit == arrow::TimeUnit::SECOND ? -1000000 : unit == arrow::TimeUnit::MILLI ? -1000 : -1,
                  timestamp.to_unix_microsecond());
        auto value = timestamp.to_string();
        EXPECT_EQ(0, value.find("1969-12-31 23:59:59"));
        EXPECT_EQ("1970-01-01 00:00:00", chunk->get_column_by_index(0)->get(1).get_timestamp().to_string());
    }
}

} // namespace starrocks::connector
