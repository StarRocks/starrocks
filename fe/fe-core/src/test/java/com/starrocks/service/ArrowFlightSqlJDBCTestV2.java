package com.starrocks.service;

import java.sql.*;

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

                // testUpdate(stmt, "DROP DATABASE IF EXISTS arrow_demo FORCE;");
                // testQuery(stmt, "SHOW DATABASES;");
                // testUpdate(stmt, "CREATE DATABASE arrow_demo;");
                // testQuery(stmt, "SHOW DATABASES;");
                testUpdate(stmt, "USE information_schema;");
                testQuery(stmt, "SELECT * FROM tables;");
                // testUpdate(stmt, "CREATE TABLE test (id INT, name STRING) ENGINE=OLAP PRIMARY KEY (id) " +
                //         "DISTRIBUTED BY HASH(id) BUCKETS 1 " +
                //         "PROPERTIES ('replication_num' = '1');");
                // testUpdate(stmt, "INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob');");
                // testQuery(stmt, "SELECT * FROM test;");
                // testUpdate(stmt, "UPDATE test SET name = 'Charlie' WHERE id = 1;");
                // testQuery(stmt, "SELECT * FROM test;");
                // testUpdate(stmt, "DELETE FROM test WHERE id = 2;");
                // testQuery(stmt, "SELECT * FROM test;");
                // testQuery(stmt, "SHOW CREATE TABLE test;");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void testQuery(Statement stmt, String sql) throws SQLException {
        System.out.println("TestCase " + testCaseNum + " - QUERY: " + sql);
        try (ResultSet rs = stmt.executeQuery(sql)) {
            int columnCount = rs.getMetaData().getColumnCount();
            System.out.println("Result:");
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(rs.getString(i) + "\t");
                }
                System.out.println();
            }
        }
        System.out.println();
        testCaseNum++;
    }

    private static void testUpdate(Statement stmt, String sql) throws SQLException {
        System.out.println("TestCase " + testCaseNum + " - UPDATE: " + sql);
        int result = stmt.executeUpdate(sql);
        System.out.println("Result: " + result + " rows affected");
        System.out.println();
        testCaseNum++;
    }
}
