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
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class ShowDataDistributionStmtTest {

    @Test
    public void testShowDataDistribution() throws Exception {
        PseudoCluster.getOrCreateWithRandomPort(true, 1);
        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        Statement stmt = connection.createStatement();

        try {
            //1.init env: create table、insert data
            stmt.execute("create database IF NOT EXISTS show_data_distribution_test_db");
            stmt.execute("use show_data_distribution_test_db");
            stmt.execute("CREATE TABLE IF NOT EXISTS partition_table " +
                    "(`col1` varchar(65533),`col2` varchar(65533),`ds` date) ENGINE=OLAP " +
                    "DUPLICATE KEY(`col1`) PARTITION BY RANGE(`ds`)" +
                    "(START (\"2024-09-20\") END (\"2024-09-22\") EVERY (INTERVAL 1 DAY))" +
                    "DISTRIBUTED BY HASH(`col1`) BUCKETS 2 " +
                    "PROPERTIES (\"replication_num\" = \"1\")");
            stmt.execute("CREATE TABLE IF NOT EXISTS unpartition_table " +
                    "(`col1` varchar(65533),`col2` varchar(65533),`ds` date) ENGINE=OLAP " +
                    "DUPLICATE KEY(`col1`) " +
                    "DISTRIBUTED BY HASH(`col1`) BUCKETS 2 " +
                    "PROPERTIES (\"replication_num\" = \"1\")");
            stmt.execute("insert into partition_table(col1,col2,ds) " +
                    "values('a','a','2024-09-20'),('a','a','2024-09-20'),('b','b','2024-09-20')," +
                    "('c','c','2024-09-21'),('c','c','2024-09-21'),('d','d','2024-09-21')");
            stmt.execute("insert into unpartition_table(col1,col2,ds) " +
                    "values('c','c','2024-09-21'),('c','c','2024-09-21'),('d','d','2024-09-21')");

            //2.check
            //2.0 check: vaild sql
            List<String> vaildSql = Arrays.asList(
                    "show data distribution from partition_table;",
                    "show data distribution from partition_table partition(p20240920);",
                    "show data distribution from partition_table partition(p20240920,p20240921);",
                    "show data distribution from unpartition_table;",
                    "show data distribution from unpartition_table partition(unpartition_table);"
            );
            for (String sql : vaildSql) {
                Assert.assertTrue(stmt.execute(sql));
            }
            System.out.println("ShowDataDistributionStmtTest: 2.0check vaild sql done!");

            //2.1 check value: partition table
            stmt.execute("select count(*) from partition_table;");
            if (stmt.getResultSet().next()) {
                System.out.println("ShowDataDistributionStmtTest: begin check value: partition table");
                //check insert data success and wait table meta update
                int count = stmt.getResultSet().getInt(1);
                System.out.println("ShowDataDistributionStmtTest: partition_table row count = " + count);
                Assert.assertEquals(count, 6);
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
            System.out.println("ShowDataDistributionStmtTest: 2.1check partition table done!");

            //2.2 check value: unpartition table
            stmt.execute("select count(*) from unpartition_table;");
            if (stmt.getResultSet().next()) {
                System.out.println("ShowDataDistributionStmtTest: begin check value: unpartition table");
                //check insert data success and wait table meta update
                int count = stmt.getResultSet().getInt(1);
                System.out.println("ShowDataDistributionStmtTest: unpartition_table row count = " + count);
                Assert.assertEquals(count, 3);
                Thread.sleep(60000);

                stmt.execute("show data distribution from unpartition_table;");
                checkExpAndActValUnPartitionTable(stmt.getResultSet());
                stmt.execute("show data distribution from unpartition_table partition(unpartition_table);");
                checkExpAndActValUnPartitionTable(stmt.getResultSet());
            }
            System.out.println("ShowDataDistributionStmtTest: 2.2check unpartition table done!");

            //2.3 check: db not exist
            try {
                stmt.execute("show data distribution from no_exist_db.unpartition_table;");
            } catch (Exception e) {
                String exp = "Database no_exist_db does not exsit";
                Assert.assertTrue(e.getMessage().contains(exp));
            }
            System.out.println("ShowDataDistributionStmtTest: 2.3check db not exist done!");

            //2.4 check: table not exist
            try {
                stmt.execute("show data distribution from no_exist_table;");
            } catch (Exception e) {
                String exp = "Table does not exist";
                Assert.assertTrue(e.getMessage().contains(exp));
            }
            System.out.println("ShowDataDistributionStmtTest: 2.4check table not exist done!");

            //2.5 check: partition not exist
            try {
                stmt.execute("show data distribution from partition_table partition(p20240929);");
            } catch (Exception e) {
                String exp = "Partition does not exist";
                Assert.assertTrue(e.getMessage().contains(exp));
            }
            System.out.println("ShowDataDistributionStmtTest: 2.5check partition not exist done!");

            //2.6 check: privilege
            //create user and grant select privilege on other db
            stmt.execute("CREATE USER IF NOT EXISTS test IDENTIFIED BY 'test';");
            stmt.execute("create database IF NOT EXISTS show_data_distribution_test_privilege_db");
            stmt.execute("GRANT SELECT ON ALL TABLES IN database show_data_distribution_test_privilege_db TO USER test@'%';");
            //transfer to test
            stmt.execute("EXECUTE AS test WITH NO REVERT;");
            try {
                stmt.execute("show data distribution from show_data_distribution_test_db.partition_table;");
            } catch (Exception e) {
                String exp = "Access denied; you need (at least one of) the ANY privilege(s) " +
                        "on TABLE partition_table for this operation";
                Assert.assertTrue(e.getMessage().contains(exp));
            }
            System.out.println("ShowDataDistributionStmtTest: 2.6check privilege done!");

            //2.7 check: invaild sql
            List<String> invaildSql = Arrays.asList(
                    "show data distribution unpartition_table;",
                    "show data distribution1 from unpartition_table;",
                    "show data1 distribution from unpartition_table;",
                    "show1 data distribution from unpartition_table;",
                    "show data distribution from partition_table partition1(p20240920);");
            for (String sql : invaildSql) {
                try {
                    stmt.execute(sql);
                } catch (Exception e) {
                    String exp = "Getting syntax error";
                    Assert.assertTrue(e.getMessage().contains(exp));
                }
            }
            System.out.println("ShowDataDistributionStmtTest: 2.7check invaild sql done!");
        } finally {
            System.out.println("ShowDataDistributionStmtTest: 2.check done!");
            stmt.close();
            connection.close();
            PseudoCluster.getInstance().shutdown(true);
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
            Assert.assertEquals(rs.getString(1), expList.get(idx).get(0));
            //BucketId
            Assert.assertEquals(rs.getString(2), expList.get(idx).get(1));
            //RowCount
            Assert.assertEquals(rs.getString(3), expList.get(idx).get(2));
            //RowCount%
            Assert.assertEquals(rs.getString(4), expList.get(idx).get(3));
            //DataSize & DataSize%
            //because DataSize is not easy estimate, so check RowCount only
            idx++;
        };
    }

    public void checkExpAndActValUnPartitionTable(ResultSet rs) throws Exception {
        List<List<String>> expList = Arrays.asList(
                Arrays.asList("unpartition_table", "0", "1", "33.33 %"),
                Arrays.asList("unpartition_table", "1", "2", "66.67 %")
        );
        int idx = 0;
        while (rs.next()) {
            //PartitionName
            Assert.assertEquals(rs.getString(1), expList.get(idx).get(0));
            //BucketId
            Assert.assertEquals(rs.getString(2), expList.get(idx).get(1));
            //RowCount
            Assert.assertEquals(rs.getString(3), expList.get(idx).get(2));
            //RowCount%
            Assert.assertEquals(rs.getString(4), expList.get(idx).get(3));
            //DataSize & DataSize%
            //because DataSize is not easy estimate, so check RowCount only
            idx++;
        };
    }
}
