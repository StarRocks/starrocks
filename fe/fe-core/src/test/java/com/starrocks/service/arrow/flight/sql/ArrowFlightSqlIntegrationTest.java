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

package com.starrocks.service.arrow.flight.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Integration test for Arrow Flight SQL JDBC driver with StarRocks.
 *
 * This test covers:
 *  - Basic DDL and DML operations
 *  - Query execution and result validation
 *  - Error handling for invalid SQL
 *  - Query cancellation (simulated with a long-running query)
 */
public class ArrowFlightSqlIntegrationTest {

    private static final String JDBC_URL = "jdbc:arrow-flight-sql://127.0.0.1:9408"
            + "?useEncryption=false"
            + "&useServerPrepStmts=false"
            + "&useSSL=false"
            + "&useArrowFlightSql=true";

    private static final String USER = "root";
    private static final String PASSWORD = "";

    private static int testCaseNum = 1;

    public static void main(String[] args) {
        try {
            // Load Arrow Flight SQL JDBC driver
            Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");

            try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
                    Statement stmt = conn.createStatement()) {

                // Basic DDL and DML operations
                testUpdate(stmt, "DROP DATABASE IF EXISTS arrow_demo FORCE;");
                testQuery(stmt, "SHOW PROCESSLIST;");
                testUpdate(stmt, "CREATE DATABASE arrow_demo;");
                testQuery(stmt, "SHOW DATABASES;");
                testUpdate(stmt, "USE arrow_demo;");
                testUpdate(stmt, "CREATE TABLE test (id INT, name STRING) ENGINE=OLAP PRIMARY KEY (id) " +
                        "DISTRIBUTED BY HASH(id) BUCKETS 1 " +
                        "PROPERTIES ('replication_num' = '1');");
                testUpdate(stmt, "INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob');");
                testQuery(stmt, "SELECT * FROM test;");
                testUpdate(stmt, "UPDATE test SET name = 'Charlie' WHERE id = 1;");
                testQuery(stmt, "SELECT * FROM test;");
                testUpdate(stmt, "DELETE FROM test WHERE id = 2;");
                testQuery(stmt, "SELECT * FROM test;");
                testQuery(stmt, "SHOW CREATE TABLE test;");

                // Error handling: query on non-existent table
                try {
                    testQuery(stmt, "SELECT * FROM not_exist_table;");
                } catch (Exception e) {
                    System.out.println("✅ Expected error (table not exist): " + e.getMessage());
                }

                // Error handling: SQL syntax error
                try {
                    testQuery(stmt, "SELECT * FROM arrow_demo.test WHERE id = ;");
                } catch (Exception e) {
                    System.out.println("✅ Expected error (syntax error): " + e.getMessage());
                }

                // Query cancellation test
                try {
                    System.out.println("Test Case: " + testCaseNum);
                    System.out.println("▶ Executing long-running query (SELECT SLEEP(10)) and canceling after 1s");

                    try (Statement longStmt = conn.createStatement()) {
                        Thread cancelThread = new Thread(() -> {
                            try {
                                Thread.sleep(1000);
                                longStmt.cancel();
                                System.out.println("✅ Query cancel() called.");
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });
                        cancelThread.start();

                        testQuery(longStmt, "SELECT * FROM information_schema.columns;");
                    }
                } catch (Exception e) {
                    System.out.println("✅ Expected error (query cancelled): " + e.getMessage());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Executes a query and prints the result to the console.
     */
    private static void testQuery(Statement stmt, String sql) throws Exception {
        System.out.println("Test Case: " + testCaseNum);
        System.out.println("▶ Executing query: " + sql);
        try (ResultSet rs = stmt.executeQuery(sql)) {
            System.out.println("Result:");
            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(rs.getString(i) + "\t");
                }
                System.out.println();
            }
        }
        testCaseNum++;
        System.out.println();
    }

    /**
     * Executes an update (DDL or DML) and prints the result to the console.
     */
    private static void testUpdate(Statement stmt, String sql) throws Exception {
        System.out.println("Test Case: " + testCaseNum);
        System.out.println("▶ Executing update: " + sql);
        stmt.executeUpdate(sql);
        System.out.println("Result: ✅ Success");
        testCaseNum++;
        System.out.println();
    }
}
