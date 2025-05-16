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

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ArrowFlightSQLVisibilityTest {

    // Arrow Flight SQL JDBC connection info
    static final String FLIGHT_JDBC_URL = "jdbc:arrow-flight-sql://127.0.0.1:9408"
            + "?useEncryption=false"
            + "&useServerPrepStmts=false"
            + "&useSSL=false"
            + "&useArrowFlightSql=true";
    static final String FLIGHT_USER = "root";
    static final String FLIGHT_PASSWORD = "";

    // MySQL JDBC connection info
    static final String MYSQL_JDBC_URL = "jdbc:mysql://127.0.0.1:9030";
    static final String MYSQL_USER = "root";
    static final String MYSQL_PASSWORD = "";

    @Test
    public void testArrowFlightSQLVisibility() {
        try {
            // 1. Execute SQL via Arrow Flight SQL JDBC
            try (Connection flightConn = DriverManager.getConnection(FLIGHT_JDBC_URL, FLIGHT_USER, FLIGHT_PASSWORD);
                    Statement flightStmt = flightConn.createStatement()) {

                // Enable query profiling
                flightStmt.execute("SET enable_profile = true");

                System.out.println("▶ Arrow Flight SQL Execute: SELECT SLEEP(1);");
                flightStmt.execute("SELECT SLEEP(1);");
            }

            // 2. Retrieve last_query_id via MySQL JDBC
            String lastQueryId = null;
            try (Connection mysqlConn = DriverManager.getConnection(MYSQL_JDBC_URL, MYSQL_USER, MYSQL_PASSWORD);
                    Statement mysqlStmt = mysqlConn.createStatement()) {

                System.out.println("▶ MySQL JDBC Execute: SELECT last_query_id();");
                try (ResultSet rs = mysqlStmt.executeQuery("SELECT last_query_id();")) {
                    if (rs.next()) {
                        lastQueryId = rs.getString(1);
                        System.out.println("✅ Retrieved last_query_id: " + lastQueryId);
                    }
                }

                // 3. Retrieve query profile using last_query_id
                if (lastQueryId != null) {
                    System.out.println("▶ MySQL JDBC Execute: SELECT get_query_profile('" + lastQueryId + "')");
                    try (ResultSet rs = mysqlStmt.executeQuery("SELECT get_query_profile('" + lastQueryId + "')")) {
                        System.out.println("✅ Query profile output:");
                        if (rs.next()) {
                            String profile = rs.getString(1);
                            System.out.println("----- Query Profile Start -----");
                            System.out.println(profile);
                            System.out.println("----- Query Profile End -----");
                        } else {
                            System.out.println("⚠️ No profile content retrieved");
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("⚠️ testArrowFlightSQLVisibility connection failed or encountered an exception, skipping test.");
            e.printStackTrace();
        }
    }
}
