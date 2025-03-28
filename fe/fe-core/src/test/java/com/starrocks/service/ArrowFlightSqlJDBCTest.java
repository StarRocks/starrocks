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

public class ArrowFlightSqlJDBCTest {
    static final String DB_URL = "jdbc:arrow-flight-sql://127.0.0.1:9408?useServerPrepStmts=false"
            + "&cachePrepStmts=true&useSSL=false&useEncryption=false";
    static final String USER = "root";
    static final String PASSWORD = "";
    static int testCaseNum = 1;

    public static void main(String[] args) {
        try {
            Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");
            Connection connection = DriverManager.getConnection(DB_URL, USER, PASSWORD);
            Statement stmt = connection.createStatement();

            executeUpdate(stmt, "DROP DATABASE IF EXISTS arrow_demo FORCE;");
            executeQuery(stmt, "SHOW DATABASES;");
            executeUpdate(stmt, "CREATE DATABASE arrow_demo;");
            executeQuery(stmt, "SHOW DATABASES;");
            executeUpdate(stmt, "USE arrow_demo;");

            executeUpdate(stmt,
                    "CREATE TABLE students (" +
                            "    id INT NOT NULL," +
                            "    name VARCHAR(255) NOT NULL," +
                            "    age INT NOT NULL," +
                            "    enrollment_date DATE " +
                            ") ENGINE=OLAP " +
                            " PRIMARY KEY (id) " +
                            "DISTRIBUTED BY HASH(id) BUCKETS 10 " +
                            "PROPERTIES (" +
                            "    'replication_num' = '1'," +
                            "    'in_memory' = 'false'," +
                            "    'enable_persistent_index' = 'true'," +
                            "    'replicated_storage' = 'true'," +
                            "    'fast_schema_evolution' = 'true'," +
                            "    'compression' = 'LZ4'" +
                            ");"
            );

            executeUpdate(stmt,
                    "INSERT INTO students (id, name, age, enrollment_date) VALUES " +
                            "(1, 'John Doe', 20, '2024-01-10')," +
                            "(2, 'Jane Smith', 22, '2024-01-11')," +
                            "(3, 'Emily Davis', 21, '2024-01-12');"
            );

            executeQuery(stmt, "SELECT * FROM students;");
            executeUpdate(stmt, "UPDATE students SET age = 23 WHERE id = 1;");
            executeQuery(stmt, "SELECT * FROM students;");
            executeUpdate(stmt, "DELETE FROM students WHERE id = 3;");
            executeQuery(stmt, "SELECT * FROM students;");
            executeUpdate(stmt, "SET time_zone = 'Asia/Shanghai';");
            executeQuery(stmt, "SHOW CREATE TABLE students;");

            stmt.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void executeQuery(Statement stmt, String sql) throws Exception {
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

    private static void executeUpdate(Statement stmt, String sql) throws Exception {
        System.out.println("Test Case:" + testCaseNum);
        System.out.println("Executing query:" + sql);
        stmt.executeUpdate(sql);
        System.out.println("Result: Success");
        testCaseNum++;
        System.out.println();
    }
}
