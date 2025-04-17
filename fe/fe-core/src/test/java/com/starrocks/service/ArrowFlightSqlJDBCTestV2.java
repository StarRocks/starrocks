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


package com.starrocks.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ArrowFlightSqlJDBCTestV2 {

    static final String JDBC_URL = "jdbc:arrow-flight-sql://127.0.0.1:9408"
            + "?useEncryption=false"
            + "&useServerPrepStmts=false"
            + "&useSSL=false"
            + "&useArrowFlightSql=true";

    static final String USER = "root";
    static final String PASSWORD = "";

    static int testCaseNum = 1;

    public static void main(String[] args) {
        try {
            Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");

            try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
                    Statement stmt = conn.createStatement()) {

                testUpdate(stmt, "DROP DATABASE IF EXISTS arrow_demo FORCE;");
                testQuery(stmt, "SHOW DATABASES;");
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
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void testQuery(Statement stmt, String sql) throws Exception {
        System.out.println("Test Case:" + testCaseNum);
        System.out.println("Executing query:" + sql);
        ResultSet rs = stmt.executeQuery(sql);
        System.out.println("Result:" + sql);
        while (rs.next()) {
            int columnCount = rs.getMetaData().getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(rs.getString(i) + "\t");
            }
            System.out.println();
        }
        rs.close();
        testCaseNum++;
        System.out.println();
    }

    private static void testUpdate(Statement stmt, String sql) throws Exception {
        System.out.println("Test Case:" + testCaseNum);
        System.out.println("Executing query:" + sql);
        stmt.executeUpdate(sql);
        System.out.println("Result: Success");
        testCaseNum++;
        System.out.println();
    }
}
