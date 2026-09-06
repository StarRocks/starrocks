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

#include <string>
#include <vector>

#include "exec/adbc_scanner.h"
#include "runtime/descriptors.h"
#include "types/date_value.h"

namespace starrocks::connector {

// Forward-declare the free function defined in adbc_connector.cpp for testing.
std::string get_adbc_sql(const std::string& table, const std::vector<std::string>& columns,
                         const std::vector<std::string>& filters, int64_t limit);

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
