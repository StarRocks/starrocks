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

#include "connector/adbc_connector.h"

#include <arrow-adbc/adbc.h>
#include <arrow-adbc/adbc_driver_manager.h>
#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include "base/testutil/sync_point.h"
#include "exec/adbc_scanner.h"
#include "runtime/descriptors.h"
#include "runtime/descriptors_ext.h"
#include "types/date_value.h"

namespace starrocks::connector {

// Forward-declare the free function defined in adbc_connector.cpp for testing.
std::string get_adbc_sql(const std::string& table, const std::vector<std::string>& columns,
                         const std::vector<std::string>& filters, int64_t limit);

namespace {

// Exercise the real Driver Manager with an in-process driver; no installed database driver is needed.
class TestADBCDriver {
public:
    TestADBCDriver() {
        current = this;
        SyncPoint::GetInstance()->SetCallBack("ADBCScanner::init_driver", [](void* database) {
            AdbcError error = ADBC_ERROR_INIT;
            ASSERT_EQ(ADBC_STATUS_OK,
                      AdbcDriverManagerDatabaseSetInitFunc(static_cast<AdbcDatabase*>(database), init, &error));
        });
        SyncPoint::GetInstance()->EnableProcessing();
    }

    ~TestADBCDriver() {
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearAllCallBacks();
        current = nullptr;
    }

    std::string fail_stage;
    std::string throw_stage;
    std::string stream_failure;
    std::string query;
    std::map<std::string, std::string> options;
    std::vector<std::string> calls;
    int errors_released = 0;
    std::shared_ptr<arrow::Schema> schema = arrow::schema({arrow::field("value", arrow::int32())});
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

private:
    inline static thread_local TestADBCDriver* current = nullptr;

    AdbcStatusCode check(const std::string& stage, AdbcError* error) {
        calls.push_back(stage);
        if (stage == throw_stage) {
            throw std::runtime_error("test driver exception: " + stage);
        }
        if (stage != fail_stage) {
            return ADBC_STATUS_OK;
        }
        error->message = strdup(stage.c_str());
        error->release = [](AdbcError* value) {
            ++current->errors_released;
            free(const_cast<char*>(value->message));
            value->message = nullptr;
            value->release = nullptr;
        };
        return ADBC_STATUS_IO;
    }

    static AdbcStatusCode init(int, void* output, AdbcError*) {
        auto* driver = static_cast<AdbcDriver*>(output);
        driver->release = [](AdbcDriver*, AdbcError* error) { return current->check("DriverRelease", error); };
        driver->DatabaseNew = [](AdbcDatabase* database, AdbcError* error) {
            database->private_data = current;
            return current->check("DatabaseNew", error);
        };
        driver->DatabaseSetOption = [](AdbcDatabase*, const char* key, const char* value, AdbcError* error) {
            current->options[key] = value;
            return current->check("DatabaseSetOption", error);
        };
        driver->DatabaseInit = [](AdbcDatabase*, AdbcError* error) { return current->check("DatabaseInit", error); };
        driver->DatabaseRelease = [](AdbcDatabase* database, AdbcError* error) {
            database->private_data = nullptr;
            return current->check("DatabaseRelease", error);
        };
        driver->ConnectionNew = [](AdbcConnection* connection, AdbcError* error) {
            connection->private_data = current;
            return current->check("ConnectionNew", error);
        };
        driver->ConnectionInit = [](AdbcConnection*, AdbcDatabase*, AdbcError* error) {
            return current->check("ConnectionInit", error);
        };
        driver->ConnectionRelease = [](AdbcConnection* connection, AdbcError* error) {
            connection->private_data = nullptr;
            return current->check("ConnectionRelease", error);
        };
        driver->StatementNew = [](AdbcConnection*, AdbcStatement* statement, AdbcError* error) {
            statement->private_data = current;
            return current->check("StatementNew", error);
        };
        driver->StatementSetSqlQuery = [](AdbcStatement*, const char* query, AdbcError* error) {
            current->query = query;
            return current->check("StatementSetSqlQuery", error);
        };
        driver->StatementRelease = [](AdbcStatement* statement, AdbcError* error) {
            statement->private_data = nullptr;
            return current->check("StatementRelease", error);
        };
        driver->StatementExecuteQuery = [](AdbcStatement*, ArrowArrayStream* stream, int64_t*,
                                           AdbcError* error) -> AdbcStatusCode {
            auto status = current->check("StatementExecuteQuery", error);
            if (status != ADBC_STATUS_OK) {
                return status;
            }
            auto reader = arrow::RecordBatchReader::Make(current->batches, current->schema).ValueOrDie();
            if (!arrow::ExportRecordBatchReader(reader, stream).ok()) {
                return ADBC_STATUS_INTERNAL;
            }
            if (!current->stream_failure.empty()) {
                stream->get_last_error = [](ArrowArrayStream*) { return "test stream failure"; };
            }
            if (current->stream_failure == "schema") {
                stream->get_schema = [](ArrowArrayStream*, ArrowSchema*) { return EIO; };
            } else if (current->stream_failure == "read") {
                stream->get_next = [](ArrowArrayStream*, ArrowArray*) { return EIO; };
            } else if (current->stream_failure == "throw") {
                stream->get_next = [](ArrowArrayStream*, ArrowArray*) -> int {
                    throw std::runtime_error("test stream exception");
                };
            } else if (current->stream_failure == "unknown") {
                stream->get_next = [](ArrowArrayStream*, ArrowArray*) -> int { throw 1; };
            }
            return ADBC_STATUS_OK;
        };
        return ADBC_STATUS_OK;
    }
};

} // namespace

class ADBCConnectorTest : public ::testing::Test {
protected:
    static TSlotDescriptor int_slot(int id, const std::string& name, bool nullable) {
        TSlotDescriptor slot;
        slot.__set_id(id);
        slot.__set_colName(name);
        slot.__set_isMaterialized(true);
        slot.__set_isNullable(nullable);
        TypeDescriptor(TYPE_INT).to_thrift(&slot.slotType);
        return slot;
    }
};

TEST_F(ADBCConnectorTest, ConnectorType) {
    ADBCConnector connector;
    EXPECT_EQ(connector.connector_type(), ConnectorType::ADBC_CONN);
}

TEST_F(ADBCConnectorTest, DataSourceName) {
    TScanRange scan_range;
    ADBCDataSource ds(nullptr, scan_range);
    EXPECT_EQ(ds.name(), "ADBCDataSource");
}

TEST_F(ADBCConnectorTest, DriverManagerUsesLogicalNameDiscovery) {
    AdbcDatabase database{};
    AdbcError error = ADBC_ERROR_INIT;
    ASSERT_EQ(ADBC_STATUS_OK, AdbcDatabaseNew(&database, &error));
    ASSERT_EQ(ADBC_STATUS_OK, AdbcDriverManagerDatabaseSetLoadFlags(&database, ADBC_LOAD_FLAG_DEFAULT, &error));

    constexpr const char* kMissingDriver = "starrocks_adbc_missing_driver_for_test";
    ASSERT_EQ(ADBC_STATUS_OK, AdbcDatabaseSetOption(&database, "driver", kMissingDriver, &error));

    AdbcStatusCode status = AdbcDatabaseInit(&database, &error);
    EXPECT_NE(ADBC_STATUS_OK, status);
    std::string message = error.message == nullptr ? "" : error.message;
    EXPECT_FALSE(message.empty());
    EXPECT_NE(std::string::npos, message.find(kMissingDriver));
    if (error.release != nullptr) {
        error.release(&error);
    }

    error = ADBC_ERROR_INIT;
    EXPECT_EQ(ADBC_STATUS_OK, AdbcDatabaseRelease(&database, &error));
    if (error.release != nullptr) {
        error.release(&error);
    }
}

TEST_F(ADBCConnectorTest, DataSourceUsesDriverManagerAndPreservesRowsAndMetrics) {
    TestADBCDriver driver;
    arrow::Int32Builder builder;
    ASSERT_TRUE(builder.AppendValues({1, 2, 3}).ok());
    auto batch = arrow::RecordBatch::Make(driver.schema, 3, {builder.Finish().ValueOrDie()});
    driver.batches = {batch->Slice(0, 0), batch, batch->Slice(0, 0)};

    TTableDescriptor thrift_table;
    thrift_table.__set_id(1);
    thrift_table.adbcTable.__set_driver("test_driver");
    thrift_table.adbcTable.__set_adbc_options({{"uri", "grpc://test"},
                                               {"username", "reader"},
                                               {"password", "test-password"},
                                               {"adbc.test.option", "enabled"}});
    thrift_table.__set_adbcTable(thrift_table.adbcTable);
    ADBCTableDescriptor table(thrift_table, std::pmr::get_default_resource());
    TTupleDescriptor thrift_tuple;
    thrift_tuple.id = 0;
    TupleDescriptor tuple(thrift_tuple);
    tuple.set_table_desc(&table);
    SlotDescriptor slot(int_slot(0, "value", true));
    tuple.add_slot(&slot);
    DescriptorTbl descriptors;
    descriptors._tuple_desc_map.emplace(0, &tuple);
    RuntimeState state;
    state.set_desc_tbl(&descriptors);
    state.set_chunk_size(2);

    TPlanNode plan;
    plan.adbc_scan_node.__set_tuple_id(0);
    plan.adbc_scan_node.__set_table_name("\"sales\".\"events\"");
    plan.adbc_scan_node.__set_columns({"\"value\""});
    plan.adbc_scan_node.__set_filters({"\"value\" > 0"});
    plan.adbc_scan_node.__set_limit(3);
    ADBCConnector connector;
    auto provider = connector.create_data_source_provider(nullptr, plan);
    EXPECT_EQ(&tuple, provider->tuple_descriptor(&state));
    EXPECT_TRUE(provider->insert_local_exchange_operator());
    EXPECT_FALSE(provider->accept_empty_scan_ranges());
    auto source = provider->create_data_source(TScanRange{});
    RuntimeProfile profile("ADBC source");
    source->set_runtime_profile(&profile);
    ASSERT_TRUE(source->open(&state).ok());
    EXPECT_EQ("SELECT \"value\" FROM \"sales\".\"events\" WHERE (\"value\" > 0) LIMIT 3", driver.query);
    EXPECT_EQ("grpc://test", driver.options["uri"]);
    EXPECT_EQ("reader", driver.options["username"]);
    EXPECT_EQ("test-password", driver.options["password"]);
    EXPECT_EQ("enabled", driver.options["adbc.test.option"]);
    ChunkPtr chunk;
    ASSERT_TRUE(source->get_next(&state, &chunk).ok());
    ASSERT_EQ(2, chunk->num_rows());
    EXPECT_EQ(1, chunk->get_column_by_slot_id(0)->get(0).get_int32());
    EXPECT_EQ(2, chunk->get_column_by_slot_id(0)->get(1).get_int32());
    ASSERT_TRUE(source->get_next(&state, &chunk).ok());
    ASSERT_EQ(1, chunk->num_rows());
    EXPECT_EQ(3, chunk->get_column_by_slot_id(0)->get(0).get_int32());
    EXPECT_TRUE(source->get_next(&state, &chunk).is_end_of_file());
    EXPECT_EQ(3, source->raw_rows_read());
    EXPECT_EQ(3, source->num_rows_read());
    EXPECT_GT(source->num_bytes_read(), 0);
    EXPECT_EQ(0, source->cpu_time_spent());
    EXPECT_EQ(3, source->_runtime_profile->get_counter("RowsRead")->value());
    source->close(&state);
    source->close(&state);
    EXPECT_EQ(1, std::count(driver.calls.begin(), driver.calls.end(), "StatementRelease"));
    EXPECT_EQ(1, std::count(driver.calls.begin(), driver.calls.end(), "ConnectionRelease"));
    EXPECT_EQ(1, std::count(driver.calls.begin(), driver.calls.end(), "DatabaseRelease"));
}

TEST_F(ADBCConnectorTest, ScannerReportsDriverFailuresAndReleasesPartialState) {
    for (const std::string stage : {"DatabaseSetOption", "DatabaseInit", "ConnectionInit", "StatementNew",
                                    "StatementSetSqlQuery", "StatementExecuteQuery"}) {
        SCOPED_TRACE(stage);
        TestADBCDriver driver;
        driver.fail_stage = stage;
        ADBCScanContext context;
        context.driver = "test_driver";
        context.uri = "grpc://test";
        ADBCScanner scanner(context, nullptr, nullptr);
        auto status = scanner.open(nullptr);
        EXPECT_FALSE(status.ok());
        EXPECT_NE(std::string::npos, status.to_string().find(stage));
        scanner.close(nullptr);
        scanner.close(nullptr);
        EXPECT_EQ(1, driver.errors_released);
        EXPECT_EQ(1, std::count(driver.calls.begin(), driver.calls.end(), "DatabaseRelease"));
    }
}

TEST_F(ADBCConnectorTest, ScannerCatchesDriverAndStreamExceptions) {
    {
        TestADBCDriver driver;
        driver.throw_stage = "DatabaseInit";
        ADBCScanner scanner(ADBCScanContext{}, nullptr, nullptr);
        auto status = scanner.open(nullptr);
        EXPECT_FALSE(status.ok());
        EXPECT_NE(std::string::npos, status.to_string().find("test driver exception"));
    }
    for (const std::string failure : {"schema", "read", "throw", "unknown"}) {
        SCOPED_TRACE(failure);
        TestADBCDriver driver;
        driver.stream_failure = failure;
        RuntimeProfile profile("ADBC stream");
        ADBCScanner scanner(ADBCScanContext{}, nullptr, &profile);
        auto status = scanner.open(nullptr);
        if (failure == "schema") {
            EXPECT_FALSE(status.ok());
        } else {
            ASSERT_TRUE(status.ok());
            ChunkPtr chunk;
            bool eos = false;
            status = scanner.get_next(nullptr, &chunk, &eos);
            EXPECT_FALSE(status.ok());
        }
        EXPECT_NE(std::string::npos, status.to_string().find(failure == "unknown" ? "unknown" : "test"));
    }
}

TEST_F(ADBCConnectorTest, CancelledScanDoesNotReadFromDriver) {
    TestADBCDriver driver;
    ADBCScanner scanner(ADBCScanContext{}, nullptr, nullptr);
    RuntimeState state;
    ASSERT_TRUE(scanner.open(&state).ok());
    state.set_is_cancelled(true);
    ChunkPtr chunk;
    bool eos = false;
    EXPECT_TRUE(scanner.get_next(&state, &chunk, &eos).is_cancelled());
    EXPECT_FALSE(eos);
    EXPECT_EQ(0, scanner.rows_read());
}

TEST_F(ADBCConnectorTest, ScannerPreservesRowsAndNulls) {
    TTupleDescriptor thrift_tuple;
    thrift_tuple.id = 0;
    TupleDescriptor tuple(thrift_tuple);
    SlotDescriptor slot(int_slot(0, "value", true));
    tuple.add_slot(&slot);
    arrow::Int32Builder builder;
    ASSERT_TRUE(builder.Append(7).ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    auto array = builder.Finish().ValueOrDie();
    auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("value", arrow::int32())}), 2, {array});
    ADBCScanner scanner(ADBCScanContext{}, &tuple, nullptr);
    ChunkPtr chunk;
    ASSERT_TRUE(scanner._convert_batch_to_chunk(batch, &chunk).ok());
    ASSERT_EQ(2, chunk->num_rows());
    EXPECT_EQ(7, chunk->get_column_by_slot_id(0)->get(0).get_int32());
    EXPECT_TRUE(chunk->get_column_by_slot_id(0)->is_null(1));
}

TEST_F(ADBCConnectorTest, ScannerDecodesDictionaryColumns) {
    TTupleDescriptor thrift_tuple;
    thrift_tuple.id = 0;
    TupleDescriptor tuple(thrift_tuple);
    SlotDescriptor number_slot(int_slot(0, "number", true));
    auto string_thrift = int_slot(1, "text", true);
    string_thrift.slotType.types.clear();
    TypeDescriptor::create_varchar_type(20).to_thrift(&string_thrift.slotType);
    SlotDescriptor string_slot(string_thrift);
    tuple.add_slot(&number_slot);
    tuple.add_slot(&string_slot);

    arrow::Int8Builder index_builder;
    ASSERT_TRUE(index_builder.Append(1).ok());
    ASSERT_TRUE(index_builder.AppendNull().ok());
    ASSERT_TRUE(index_builder.Append(0).ok());
    auto indices = index_builder.Finish().ValueOrDie();
    arrow::Int32Builder number_builder;
    ASSERT_TRUE(number_builder.AppendValues({7, 8}).ok());
    arrow::StringBuilder string_builder;
    ASSERT_TRUE(string_builder.AppendValues({"first", "second"}).ok());
    auto numbers = arrow::DictionaryArray::FromArrays(arrow::dictionary(arrow::int8(), arrow::int32()), indices,
                                                      number_builder.Finish().ValueOrDie())
                           .ValueOrDie();
    auto strings = arrow::DictionaryArray::FromArrays(arrow::dictionary(arrow::int8(), arrow::utf8()), indices,
                                                      string_builder.Finish().ValueOrDie())
                           .ValueOrDie();
    auto batch = arrow::RecordBatch::Make(
            arrow::schema({arrow::field("number", numbers->type()), arrow::field("text", strings->type())}), 3,
            {numbers, strings});
    ADBCScanner scanner(ADBCScanContext{}, &tuple, nullptr);
    ChunkPtr chunk;
    ASSERT_TRUE(scanner._convert_batch_to_chunk(batch, &chunk).ok());
    EXPECT_EQ(8, chunk->get_column_by_slot_id(0)->get(0).get_int32());
    EXPECT_EQ("second", chunk->get_column_by_slot_id(1)->get(0).get_slice().to_string());
    EXPECT_TRUE(chunk->get_column_by_slot_id(0)->is_null(1));
    EXPECT_TRUE(chunk->get_column_by_slot_id(1)->is_null(1));
    EXPECT_EQ(7, chunk->get_column_by_slot_id(0)->get(2).get_int32());
    EXPECT_EQ("first", chunk->get_column_by_slot_id(1)->get(2).get_slice().to_string());
}

TEST_F(ADBCConnectorTest, ScannerRejectsNullInRequiredColumn) {
    TTupleDescriptor thrift_tuple;
    thrift_tuple.id = 0;
    TupleDescriptor tuple(thrift_tuple);
    SlotDescriptor first(int_slot(0, "required_value", false));
    SlotDescriptor second(int_slot(1, "other_value", false));
    tuple.add_slot(&first);
    tuple.add_slot(&second);
    arrow::Int32Builder null_builder;
    ASSERT_TRUE(null_builder.AppendNull().ok());
    auto null_array = null_builder.Finish().ValueOrDie();
    arrow::Int32Builder value_builder;
    ASSERT_TRUE(value_builder.Append(7).ok());
    auto value_array = value_builder.Finish().ValueOrDie();
    auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("required_value", arrow::int32()),
                                                         arrow::field("other_value", arrow::int32())}),
                                          1, {null_array, value_array});
    ADBCScanner scanner(ADBCScanContext{}, &tuple, nullptr);
    ChunkPtr chunk;
    auto status = scanner._convert_batch_to_chunk(batch, &chunk);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.to_string().find("required_value"));
}

TEST_F(ADBCConnectorTest, ScannerConvertsDate64ToDatetime) {
    date::init_date_cache();
    TTupleDescriptor thrift_tuple;
    thrift_tuple.id = 0;
    TupleDescriptor tuple(thrift_tuple);
    TSlotDescriptor thrift_slot = int_slot(0, "value", true);
    thrift_slot.slotType.types.clear();
    TypeDescriptor(TYPE_DATETIME).to_thrift(&thrift_slot.slotType);
    SlotDescriptor slot(thrift_slot);
    tuple.add_slot(&slot);
    arrow::Date64Builder builder;
    ASSERT_TRUE(builder.Append(-86400000).ok());
    ASSERT_TRUE(builder.Append(0).ok());
    ASSERT_TRUE(builder.Append(86400000).ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("value", arrow::date64())}), 4,
                                          {builder.Finish().ValueOrDie()});
    ADBCScanner scanner(ADBCScanContext{}, &tuple, nullptr);
    ChunkPtr chunk;
    ASSERT_TRUE(scanner._convert_batch_to_chunk(batch, &chunk).ok());
    ASSERT_EQ(4, chunk->num_rows());
    const auto& column = chunk->get_column_by_slot_id(0);
    EXPECT_EQ("1969-12-31 00:00:00", column->get(0).get_timestamp().to_string());
    EXPECT_EQ("1970-01-01 00:00:00", column->get(1).get_timestamp().to_string());
    EXPECT_EQ("1970-01-02 00:00:00", column->get(2).get_timestamp().to_string());
    EXPECT_TRUE(column->is_null(3));
}

TEST_F(ADBCConnectorTest, ScannerRechunksAndPreservesProfileCounters) {
    TTupleDescriptor thrift_tuple;
    thrift_tuple.id = 0;
    TupleDescriptor tuple(thrift_tuple);
    SlotDescriptor slot(int_slot(0, "value", true));
    tuple.add_slot(&slot);
    arrow::Int32Builder builder;
    ASSERT_TRUE(builder.AppendValues({1, 2, 3, 4, 5}).ok());
    auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("value", arrow::int32())}), 5,
                                          {builder.Finish().ValueOrDie()});
    auto reader = arrow::RecordBatchReader::Make({batch}).ValueOrDie();
    RuntimeProfile profile("ADBC scan");
    ADBCScanner scanner(ADBCScanContext{}, &tuple, &profile);
    ASSERT_TRUE(arrow::ExportRecordBatchReader(reader, &scanner._c_stream).ok());
    scanner._arrow_schema = batch->schema();
    scanner._max_chunk_size = 2;
    ChunkPtr chunk;
    bool eos = false;
    int value = 1;
    for (int expected_size : {2, 2, 1}) {
        ASSERT_TRUE(scanner.get_next(nullptr, &chunk, &eos).ok());
        ASSERT_FALSE(eos);
        ASSERT_EQ(expected_size, chunk->num_rows());
        for (int row = 0; row < expected_size; ++row) {
            EXPECT_EQ(value++, chunk->get_column_by_slot_id(0)->get(row).get_int32());
        }
    }
    ASSERT_TRUE(scanner.get_next(nullptr, &chunk, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(5, profile.get_counter("RowsRead")->value());
    EXPECT_EQ(2, profile.get_counter("IOCounter")->value());
    scanner.close(nullptr);
    scanner.close(nullptr);
}

TEST_F(ADBCConnectorTest, SqlAssemblyBasic) {
    std::vector<std::string> columns = {"col1", "col2", "col3"};
    std::vector<std::string> filters;
    std::string sql = get_adbc_sql("schema1.table1", columns, filters, -1);
    EXPECT_EQ(sql, "SELECT col1, col2, col3 FROM schema1.table1");
}

TEST_F(ADBCConnectorTest, SqlAssemblyWithFilters) {
    std::vector<std::string> columns = {"id", "name"};
    std::vector<std::string> filters = {"id > 10", "name = 'test'"};
    std::string sql = get_adbc_sql("db.users", columns, filters, -1);
    EXPECT_EQ(sql, "SELECT id, name FROM db.users WHERE (id > 10) AND (name = 'test')");
}

TEST_F(ADBCConnectorTest, SqlAssemblyWithLimit) {
    std::vector<std::string> columns = {"*"};
    std::vector<std::string> filters;
    std::string sql = get_adbc_sql("t", columns, filters, 100);
    EXPECT_EQ(sql, "SELECT * FROM t LIMIT 100");
}

TEST_F(ADBCConnectorTest, SqlAssemblyWithFiltersAndLimit) {
    std::vector<std::string> columns = {"a", "b"};
    std::vector<std::string> filters = {"a > 0"};
    std::string sql = get_adbc_sql("schema.tbl", columns, filters, 5);
    EXPECT_EQ(sql, "SELECT a, b FROM schema.tbl WHERE (a > 0) LIMIT 5");
}

TEST_F(ADBCConnectorTest, SqlAssemblySingleColumn) {
    std::vector<std::string> columns = {"col1"};
    std::vector<std::string> filters;
    std::string sql = get_adbc_sql("t", columns, filters, -1);
    EXPECT_EQ(sql, "SELECT col1 FROM t");
}

} // namespace starrocks::connector
