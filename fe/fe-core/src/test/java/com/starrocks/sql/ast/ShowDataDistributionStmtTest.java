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

package com.starrocks.sql.ast;

import com.starrocks.pseudocluster.PseudoCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class ShowDataDistributionStmtTest {
    @BeforeAll
    public static void setUp() throws Exception {
        PseudoCluster.getOrCreateWithRandomPort(true, 1);
        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        Statement stmt = connection.createStatement();
        stmt.execute("create database IF NOT EXISTS show_data_distribution_test_db");
    }

    @AfterAll
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
    }

    @Test
    public void testShowDataDistributionFromPartitionTable() throws Exception {
        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        Statement stmt = connection.createStatement();

        try {
            // 1.init env: create table、insert data
            stmt.execute("use show_data_distribution_test_db");
            stmt.execute("CREATE TABLE IF NOT EXISTS partition_table " +
                    "(`col1` varchar(65533),`col2` varchar(65533),`ds` date) ENGINE=OLAP " +
                    "DUPLICATE KEY(`col1`) PARTITION BY RANGE(`ds`)" +
                    "(START (\"2024-09-20\") END (\"2024-09-22\") EVERY (INTERVAL 1 DAY))" +
                    "DISTRIBUTED BY HASH(`col1`) BUCKETS 2 " +
                    "PROPERTIES (\"replication_num\" = \"1\")");
            stmt.execute("insert into partition_table(col1,col2,ds) " +
                    "values('a','a','2024-09-20'),('a','a','2024-09-20'),('b','b','2024-09-20')," +
                    "('c','c','2024-09-21'),('c','c','2024-09-21'),('d','d','2024-09-21')");

            // 2.check
            // 2.0 check: valid sql
            List<String> validSqls = Arrays.asList(
                    "show data distribution from partition_table;",
                    "show data distribution from partition_table partition(p20240920);",
                    "show data distribution from partition_table partition(p20240920,p20240921);"
            );
            for (String sql : validSqls) {
                Assertions.assertTrue(stmt.execute(sql));
            }
            System.out.println("testShowDataDistributionFromPartitionTable: 2.0 check valid sql done!");

            // 2.1 check value: partition table
            stmt.execute("select count(*) from partition_table;");
            if (stmt.getResultSet().next()) {
                System.out.println("testShowDataDistributionFromPartitionTable: begin check value: partition table");
                //check insert data success and wait table meta update
                int count = stmt.getResultSet().getInt(1);
                System.out.println("testShowDataDistributionFromPartitionTable: partition_table row count = " + count);
                Assertions.assertEquals(count, 6);
                Thread.sleep(60000);

                //2.1.1 entire table
                stmt.execute("show data distribution from partition_table;");
                checkExpAndActValPartitionTable(stmt.getResultSet());
                //2.1.2 single partition
                stmt.execute("show data distribution from partition_table partition(p20240920);");
                checkExpAndActValPartitionTable(stmt.getResultSet());
                //2.1.3 several partition
                stmt.execute("show data distribution from partition_table partition(p20240920,p20240921);");
                checkExpAndActValPartitionTable(stmt.getResultSet());
            }
            System.out.println("testShowDataDistributionFromPartitionTable: 2.1 check partition table done!");

            // 2.2 check: db not exist
            try {
                stmt.execute("show data distribution from no_exist_db.partition_table;");
            } catch (Exception e) {
                String exp = "Database no_exist_db does not exist";
                Assertions.assertTrue(e.getMessage().contains(exp));
            }
            System.out.println("testShowDataDistributionFromPartitionTable: 2.2 check db not exist done!");

            // 2.3 check: table not exist
            try {
                stmt.execute("show data distribution from no_exist_table;");
            } catch (Exception e) {
                String exp = "Table does not exist";
                Assertions.assertTrue(e.getMessage().contains(exp));
            }
            System.out.println("testShowDataDistributionFromPartitionTable: 2.3 check table not exist done!");

            // 2.4 check: partition not exist
            try {
                stmt.execute("show data distribution from partition_table partition(p20240929);");
            } catch (Exception e) {
                String exp = "Partition does not exist";
                Assertions.assertTrue(e.getMessage().contains(exp));
            }
            System.out.println("testShowDataDistributionFromPartitionTable: 2.4 check partition not exist done!");

            // 2.5 check: privilege
            // create user and grant select privilege on other db
            stmt.execute("CREATE USER IF NOT EXISTS test IDENTIFIED BY 'test';");
            stmt.execute("create database IF NOT EXISTS show_data_distribution_test_privilege_db");
            stmt.execute("GRANT SELECT ON ALL TABLES IN database show_data_distribution_test_privilege_db TO USER test@'%';");
            // transfer to test
            stmt.execute("EXECUTE AS test WITH NO REVERT;");
            try {
                stmt.execute("show data distribution from show_data_distribution_test_db.partition_table;");
            } catch (Exception e) {
                String exp = "Access denied; you need (at least one of) the ANY privilege(s) " +
                        "on TABLE partition_table for this operation";
                Assertions.assertTrue(e.getMessage().contains(exp));
            }
            System.out.println("testShowDataDistributionFromPartitionTable: 2.5 check privilege done!");

            // 2.6 check: invalid sql
            List<String> invalidSqls = Arrays.asList("show data distribution from partition_table partition1(p20240920);");
            for (String sql : invalidSqls) {
                try {
                    stmt.execute(sql);
                } catch (Exception e) {
                    String exp = "Getting syntax error";
                    Assertions.assertTrue(e.getMessage().contains(exp));
                }
            }
            System.out.println("testShowDataDistributionFromPartitionTable: 2.6 check invalid sql done!");
        } finally {
            System.out.println("testShowDataDistributionFromPartitionTable: 2.check done!");
            stmt.close();
            connection.close();
        }
    }

    @Test
    public void testShowDataDistributionFromUnPartitionTable() throws Exception {
        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        Statement stmt = connection.createStatement();

        try {
            // 1.init env: create table、insert data
            stmt.execute("use show_data_distribution_test_db");
            stmt.execute("CREATE TABLE IF NOT EXISTS unpartition_table " +
                    "(`col1` varchar(65533),`col2` varchar(65533),`ds` date) ENGINE=OLAP " +
                    "DUPLICATE KEY(`col1`) " +
                    "DISTRIBUTED BY HASH(`col1`) BUCKETS 2 " +
                    "PROPERTIES (\"replication_num\" = \"1\")");
            stmt.execute("insert into unpartition_table(col1,col2,ds) " +
                    "values('c','c','2024-09-21'),('c','c','2024-09-21'),('d','d','2024-09-21')");

            // 2.check
            // 2.0 check: valid sql
            List<String> validSqls = Arrays.asList(
                    "show data distribution from unpartition_table;",
                    "show data distribution from unpartition_table partition(unpartition_table);"
            );
            for (String sql : validSqls) {
                Assertions.assertTrue(stmt.execute(sql));
            }
            System.out.println("testShowDataDistributionFromUnPartitionTable: 2.0 check valid sql done!");

            // 2.1 check value: unpartition table
            stmt.execute("select count(*) from unpartition_table;");
            if (stmt.getResultSet().next()) {
                System.out.println("testShowDataDistributionFromUnPartitionTable: begin check value: unpartition table");
                //check insert data success and wait table meta update
                int count = stmt.getResultSet().getInt(1);
                System.out.println("testShowDataDistributionFromUnPartitionTable: unpartition_table row count = " + count);
                Assertions.assertEquals(count, 3);
                Thread.sleep(60000);

                stmt.execute("show data distribution from unpartition_table;");
                checkExpAndActValUnPartitionTable(stmt.getResultSet());
                stmt.execute("show data distribution from unpartition_table partition(unpartition_table);");
                checkExpAndActValUnPartitionTable(stmt.getResultSet());
            }
            System.out.println("testShowDataDistributionFromUnPartitionTable: 2.1 check unpartition table done!");

            // 2.2 check: invalid sql
            List<String> invalidSqls = Arrays.asList(
                    "show data distribution unpartition_table;",
                    "show data distribution1 from unpartition_table;",
                    "show data1 distribution from unpartition_table;",
                    "show1 data distribution from unpartition_table;");
            for (String sql : invalidSqls) {
                try {
                    stmt.execute(sql);
                } catch (Exception e) {
                    String exp = "Getting syntax error";
                    Assertions.assertTrue(e.getMessage().contains(exp));
                }
            }
            System.out.println("testShowDataDistributionFromUnPartitionTable: 2.2 check invalid sql done!");
        } finally {
            System.out.println("testShowDataDistributionFromUnPartitionTable: 2.check done!");
            stmt.close();
            connection.close();
        }
    }

    public void checkExpAndActValPartitionTable(ResultSet rs) throws Exception {
        List<List<String>> expList = Arrays.asList(
                Arrays.asList("p20240920", "0", "0", "00.00 %"),
                Arrays.asList("p20240920", "1", "3", "100.00 %"),
                Arrays.asList("p20240921", "0", "1", "33.33 %"),
                Arrays.asList("p20240921", "1", "2", "66.67 %")
        );
        int idx = 0;
        while (rs.next()) {
            //PartitionName
            Assertions.assertEquals(rs.getString(1), expList.get(idx).get(0));
            //BucketId
            Assertions.assertEquals(rs.getString(2), expList.get(idx).get(1));
            //RowCount
            Assertions.assertEquals(rs.getString(3), expList.get(idx).get(2));
            //RowCount%
            Assertions.assertEquals(rs.getString(4), expList.get(idx).get(3));
            //DataSize & DataSize%
            //because DataSize is not easy estimate, so check RowCount only
            idx++;
        }
    }

    public void checkExpAndActValUnPartitionTable(ResultSet rs) throws Exception {
        List<List<String>> expList = Arrays.asList(
                Arrays.asList("unpartition_table", "0", "1", "33.33 %"),
                Arrays.asList("unpartition_table", "1", "2", "66.67 %")
        );
        int idx = 0;
        while (rs.next()) {
            //PartitionName
            Assertions.assertEquals(rs.getString(1), expList.get(idx).get(0));
            //BucketId
            Assertions.assertEquals(rs.getString(2), expList.get(idx).get(1));
            //RowCount
            Assertions.assertEquals(rs.getString(3), expList.get(idx).get(2));
            //RowCount%
            Assertions.assertEquals(rs.getString(4), expList.get(idx).get(3));
            //DataSize & DataSize%
            //because DataSize is not easy estimate, so check RowCount only
            idx++;
        }
    }
}
