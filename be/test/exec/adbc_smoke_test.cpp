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

// Throwaway ADBC smoke tests — deleted before PR push (per D-16).
// Proves the ADBC scanner stack works E2E against the SQLite ADBC driver.

#include <gtest/gtest.h>

#include <arrow-adbc/adbc.h>
#include <arrow-adbc/adbc_driver_manager.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <unistd.h>
#include <vector>

#include "exec/adbc_arrow_raii.h"
#include "exec/adbc_driver_registry.h"

namespace starrocks {

#define ASSERT_ADBC_OK(status, error)                                                       \
    do {                                                                                    \
        if ((status) != ADBC_STATUS_OK) {                                                   \
            std::string msg = (error).message ? (error).message : "Unknown";                \
            if ((error).release) (error).release(&(error));                                 \
            FAIL() << "ADBC error: " << msg;                                                \
        }                                                                                   \
    } while (0)

static std::string get_sqlite_driver_path() {
    // 1. Env var override
    const char* env = std::getenv("ADBC_SQLITE_DRIVER_PATH");
    if (env && std::strlen(env) > 0) return env;

    // 2. Vendored build output
    std::string vendored = "thirdparty/installed/lib64/libadbc_driver_sqlite.so";
    if (access(vendored.c_str(), R_OK) == 0) return vendored;

    // 3. Phase 1 spike install
    const char* home_env = std::getenv("HOME");
    std::string home = home_env ? home_env : "";
    std::string spike = home + "/.config/adbc/drivers/sqlite_linux_amd64_v1.10.0/libadbc_driver_sqlite.so";
    if (access(spike.c_str(), R_OK) == 0) return spike;

    return "";
}

// Helper: execute a SQL statement that doesn't return results (DDL/DML)
static void exec_sql(AdbcConnection* conn, const char* sql) {
    AdbcStatement stmt{};
    AdbcError error = ADBC_ERROR_INIT;
    ASSERT_ADBC_OK(AdbcStatementNew(conn, &stmt, &error), error);

    error = ADBC_ERROR_INIT;
    ASSERT_ADBC_OK(AdbcStatementSetSqlQuery(&stmt, sql, &error), error);

    ArrowArrayStreamHolder stream_holder;
    int64_t rows_affected = -1;
    error = ADBC_ERROR_INIT;
    ASSERT_ADBC_OK(AdbcStatementExecuteQuery(&stmt, &stream_holder.stream, &rows_affected, &error), error);

    error = ADBC_ERROR_INIT;
    AdbcStatementRelease(&stmt, &error);
    if (error.release) error.release(&error);
}

TEST(ADBCSmokeTest, DriverRegistrySingleton) {
    std::string driver_path = get_sqlite_driver_path();
    if (driver_path.empty()) {
        GTEST_SKIP() << "SQLite ADBC driver not found; set ADBC_SQLITE_DRIVER_PATH";
    }

    auto result1 = ADBCDriverRegistry::instance().get_or_load(driver_path);
    ASSERT_TRUE(result1.ok()) << result1.status().message();
    const AdbcDriver* ptr1 = result1.value();

    auto result2 = ADBCDriverRegistry::instance().get_or_load(driver_path);
    ASSERT_TRUE(result2.ok()) << result2.status().message();
    const AdbcDriver* ptr2 = result2.value();

    EXPECT_EQ(ptr1, ptr2) << "get_or_load must return same pointer for same driver";
    EXPECT_GE(ADBCDriverRegistry::instance().loaded_count(), 1u);
}

TEST(ADBCSmokeTest, ScannerInitViaDriverManager) {
    std::string driver_path = get_sqlite_driver_path();
    if (driver_path.empty()) {
        GTEST_SKIP() << "SQLite ADBC driver not found; set ADBC_SQLITE_DRIVER_PATH";
    }

    AdbcDatabase db{};
    AdbcError error = ADBC_ERROR_INIT;
    ASSERT_ADBC_OK(AdbcDatabaseNew(&db, &error), error);

    error = ADBC_ERROR_INIT;
    ASSERT_ADBC_OK(AdbcDatabaseSetOption(&db, "driver", driver_path.c_str(), &error), error);

    error = ADBC_ERROR_INIT;
    ASSERT_ADBC_OK(AdbcDatabaseSetOption(&db, "uri", ":memory:", &error), error);

    error = ADBC_ERROR_INIT;
    ASSERT_ADBC_OK(AdbcDatabaseInit(&db, &error), error);

    AdbcConnection conn{};
    error = ADBC_ERROR_INIT;
    ASSERT_ADBC_OK(AdbcConnectionNew(&conn, &error), error);

    error = ADBC_ERROR_INIT;
    ASSERT_ADBC_OK(AdbcConnectionInit(&conn, &db, &error), error);

    AdbcStatement stmt{};
    error = ADBC_ERROR_INIT;
    ASSERT_ADBC_OK(AdbcStatementNew(&conn, &stmt, &error), error);

    error = ADBC_ERROR_INIT;
    ASSERT_ADBC_OK(AdbcStatementSetSqlQuery(&stmt, "SELECT 1 AS val", &error), error);

    ArrowArrayStreamHolder stream_holder;
    int64_t rows_affected = -1;
    error = ADBC_ERROR_INIT;
    ASSERT_ADBC_OK(AdbcStatementExecuteQuery(&stmt, &stream_holder.stream, &rows_affected, &error), error);

    // Verify schema
    struct ArrowSchema c_schema {};
    ASSERT_EQ(stream_holder.stream.get_schema(&stream_holder.stream, &c_schema), 0);
    EXPECT_GE(c_schema.n_children, 1);
    if (c_schema.release) c_schema.release(&c_schema);

    // Cleanup
    error = ADBC_ERROR_INIT;
    AdbcStatementRelease(&stmt, &error);
    if (error.release) error.release(&error);
    error = ADBC_ERROR_INIT;
    AdbcConnectionRelease(&conn, &error);
    if (error.release) error.release(&error);
    error = ADBC_ERROR_INIT;
    AdbcDatabaseRelease(&db, &error);
    if (error.release) error.release(&error);
}

TEST(ADBCSmokeTest, EndToEndSelect) {
    std::string driver_path = get_sqlite_driver_path();
    if (driver_path.empty()) {
        GTEST_SKIP() << "SQLite ADBC driver not found; set ADBC_SQLITE_DRIVER_PATH";
    }

    AdbcDatabase db{};
    AdbcError error = ADBC_ERROR_INIT;
    ASSERT_ADBC_OK(AdbcDatabaseNew(&db, &error), error);
    error = ADBC_ERROR_INIT;
    ASSERT_ADBC_OK(AdbcDatabaseSetOption(&db, "driver", driver_path.c_str(), &error), error);
    error = ADBC_ERROR_INIT;
    ASSERT_ADBC_OK(AdbcDatabaseSetOption(&db, "uri", ":memory:", &error), error);
    error = ADBC_ERROR_INIT;
    ASSERT_ADBC_OK(AdbcDatabaseInit(&db, &error), error);

    AdbcConnection conn{};
    error = ADBC_ERROR_INIT;
    ASSERT_ADBC_OK(AdbcConnectionNew(&conn, &error), error);
    error = ADBC_ERROR_INIT;
    ASSERT_ADBC_OK(AdbcConnectionInit(&conn, &db, &error), error);

    // Create table and insert rows
    exec_sql(&conn, "CREATE TABLE test_t (id INTEGER, name TEXT)");
    exec_sql(&conn, "INSERT INTO test_t VALUES (1, 'alice'), (2, 'bob')");

    // Select and verify
    AdbcStatement stmt{};
    error = ADBC_ERROR_INIT;
    ASSERT_ADBC_OK(AdbcStatementNew(&conn, &stmt, &error), error);
    error = ADBC_ERROR_INIT;
    ASSERT_ADBC_OK(AdbcStatementSetSqlQuery(&stmt, "SELECT id, name FROM test_t ORDER BY id", &error), error);

    ArrowArrayStreamHolder stream_holder;
    int64_t rows_affected = -1;
    error = ADBC_ERROR_INIT;
    ASSERT_ADBC_OK(AdbcStatementExecuteQuery(&stmt, &stream_holder.stream, &rows_affected, &error), error);

    // Import via Arrow C bridge and verify data
    auto reader_result = arrow::ImportRecordBatchReader(&stream_holder.stream);
    ASSERT_TRUE(reader_result.ok()) << reader_result.status().ToString();
    auto reader = std::move(reader_result).ValueUnsafe();

    int64_t total_rows = 0;
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        auto status = reader->ReadNext(&batch);
        ASSERT_TRUE(status.ok()) << status.ToString();
        if (!batch) break;
        total_rows += batch->num_rows();
    }
    EXPECT_EQ(total_rows, 2);

    // Cleanup
    error = ADBC_ERROR_INIT;
    AdbcStatementRelease(&stmt, &error);
    if (error.release) error.release(&error);
    error = ADBC_ERROR_INIT;
    AdbcConnectionRelease(&conn, &error);
    if (error.release) error.release(&error);
    error = ADBC_ERROR_INIT;
    AdbcDatabaseRelease(&db, &error);
    if (error.release) error.release(&error);
}

TEST(ADBCSmokeTest, ConcurrentScan) {
    std::string driver_path = get_sqlite_driver_path();
    if (driver_path.empty()) {
        GTEST_SKIP() << "SQLite ADBC driver not found; set ADBC_SQLITE_DRIVER_PATH";
    }

    // Set up shared database
    AdbcDatabase db{};
    AdbcError error = ADBC_ERROR_INIT;
    ASSERT_ADBC_OK(AdbcDatabaseNew(&db, &error), error);
    error = ADBC_ERROR_INIT;
    ASSERT_ADBC_OK(AdbcDatabaseSetOption(&db, "driver", driver_path.c_str(), &error), error);
    error = ADBC_ERROR_INIT;
    ASSERT_ADBC_OK(AdbcDatabaseSetOption(&db, "uri", ":memory:", &error), error);
    error = ADBC_ERROR_INIT;
    ASSERT_ADBC_OK(AdbcDatabaseInit(&db, &error), error);

    // Create table and insert 100 rows
    {
        AdbcConnection setup_conn{};
        error = ADBC_ERROR_INIT;
        ASSERT_ADBC_OK(AdbcConnectionNew(&setup_conn, &error), error);
        error = ADBC_ERROR_INIT;
        ASSERT_ADBC_OK(AdbcConnectionInit(&setup_conn, &db, &error), error);

        exec_sql(&setup_conn, "CREATE TABLE concurrent_t (id INTEGER, val TEXT)");
        for (int i = 0; i < 100; i++) {
            std::string sql = "INSERT INTO concurrent_t VALUES (" + std::to_string(i) + ", 'row" +
                              std::to_string(i) + "')";
            exec_sql(&setup_conn, sql.c_str());
        }

        error = ADBC_ERROR_INIT;
        AdbcConnectionRelease(&setup_conn, &error);
        if (error.release) error.release(&error);
    }

    // Launch 8 threads, each with own connection
    constexpr int NUM_THREADS = 8;
    std::atomic<int> success_count{0};
    std::vector<std::thread> threads;

    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back([&db, &success_count]() {
            AdbcConnection conn{};
            AdbcError err = ADBC_ERROR_INIT;
            if (AdbcConnectionNew(&conn, &err) != ADBC_STATUS_OK) {
                if (err.release) err.release(&err);
                return;
            }
            err = ADBC_ERROR_INIT;
            if (AdbcConnectionInit(&conn, &db, &err) != ADBC_STATUS_OK) {
                if (err.release) err.release(&err);
                AdbcError re = ADBC_ERROR_INIT;
                AdbcConnectionRelease(&conn, &re);
                if (re.release) re.release(&re);
                return;
            }

            AdbcStatement stmt{};
            err = ADBC_ERROR_INIT;
            if (AdbcStatementNew(&conn, &stmt, &err) != ADBC_STATUS_OK) {
                if (err.release) err.release(&err);
                AdbcError re = ADBC_ERROR_INIT;
                AdbcConnectionRelease(&conn, &re);
                if (re.release) re.release(&re);
                return;
            }
            err = ADBC_ERROR_INIT;
            if (AdbcStatementSetSqlQuery(&stmt, "SELECT * FROM concurrent_t", &err) != ADBC_STATUS_OK) {
                if (err.release) err.release(&err);
                AdbcError re = ADBC_ERROR_INIT;
                AdbcStatementRelease(&stmt, &re);
                if (re.release) re.release(&re);
                re = ADBC_ERROR_INIT;
                AdbcConnectionRelease(&conn, &re);
                if (re.release) re.release(&re);
                return;
            }

            ArrowArrayStreamHolder stream_holder;
            int64_t rows_affected = -1;
            err = ADBC_ERROR_INIT;
            if (AdbcStatementExecuteQuery(&stmt, &stream_holder.stream, &rows_affected, &err) != ADBC_STATUS_OK) {
                if (err.release) err.release(&err);
                AdbcError re = ADBC_ERROR_INIT;
                AdbcStatementRelease(&stmt, &re);
                if (re.release) re.release(&re);
                re = ADBC_ERROR_INIT;
                AdbcConnectionRelease(&conn, &re);
                if (re.release) re.release(&re);
                return;
            }

            // Read all rows
            auto reader_result = arrow::ImportRecordBatchReader(&stream_holder.stream);
            if (!reader_result.ok()) {
                AdbcError re = ADBC_ERROR_INIT;
                AdbcStatementRelease(&stmt, &re);
                if (re.release) re.release(&re);
                re = ADBC_ERROR_INIT;
                AdbcConnectionRelease(&conn, &re);
                if (re.release) re.release(&re);
                return;
            }
            auto reader = std::move(reader_result).ValueUnsafe();

            int64_t total_rows = 0;
            while (true) {
                std::shared_ptr<arrow::RecordBatch> batch;
                auto status = reader->ReadNext(&batch);
                if (!status.ok()) break;
                if (!batch) break;
                total_rows += batch->num_rows();
            }

            if (total_rows == 100) {
                success_count.fetch_add(1);
            }

            // Cleanup
            AdbcError re = ADBC_ERROR_INIT;
            AdbcStatementRelease(&stmt, &re);
            if (re.release) re.release(&re);
            re = ADBC_ERROR_INIT;
            AdbcConnectionRelease(&conn, &re);
            if (re.release) re.release(&re);
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(success_count.load(), NUM_THREADS) << "All 8 threads should complete successfully";

    // Release database
    error = ADBC_ERROR_INIT;
    AdbcDatabaseRelease(&db, &error);
    if (error.release) error.release(&error);
}

} // namespace starrocks
