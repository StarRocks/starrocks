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

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.pseudocluster.PseudoCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Kept in a separate class from {@link PartitionPruneRuleTest}: PseudoCluster.shutdown() only removes
 * the run dir, the pseudo FE/BE daemon threads keep running for the rest of the JVM's life. Those threads
 * keep touching OlapTable, which collides with the JVM-wide class mocking done by the JMockit based tests
 * in PartitionPruneRuleTest.
 */
public class OlapPartitionScanLimitTest {

    @Test
    public void testOlapPartitionScanLimit() throws Exception {
        PseudoCluster.getOrCreateWithRandomPort(true, 1);
        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        try (connection; Statement stmt = connection.createStatement()) {
            stmt.execute("create database olap_partition_scan_limit_test_db");
            stmt.execute("use olap_partition_scan_limit_test_db");
            stmt.execute("CREATE TABLE olap_partition_scan_limit_test_table " +
                    "(`a` varchar(65533),`b` varchar(65533),`ds` date) ENGINE=OLAP " +
                    "DUPLICATE KEY(`a`) PARTITION BY RANGE(`ds`)" +
                    "(START (\"2024-09-20\") END (\"2024-09-27\") EVERY (INTERVAL 1 DAY))" +
                    "DISTRIBUTED BY HASH(`a`)" +
                    "PROPERTIES (\"replication_num\" = \"1\")");
            stmt.execute("insert into olap_partition_scan_limit_test_table(a,b,ds) " +
                    "values('1','a','2024-09-20'),('2','a','2024-09-21')," +
                    "('3','a','2024-09-22'),('4','a','2024-09-23'),('5','a','2024-09-24')," +
                    "('6','a','2024-09-25'),('7','a','2024-09-26')");
            // The limit is enforced on the FE while planning, which is all this test can observe:
            // PseudoBackend never produces result rows (QueryProgress#getFetchDataResult returns eos
            // with no row batch), so a select always comes back empty here and the returned count
            // cannot be asserted. What is asserted is whether the query is accepted or rejected.
            String query = "select count(*) from olap_partition_scan_limit_test_table where ds>='2024-09-22';";

            //check default value 0, means no limit
            stmt.execute(query);
            //check set value -1, means no limit
            stmt.execute("set scan_olap_partition_num_limit=-1;");
            stmt.execute(query);
            //check set value 3, 5 partitions have to be scanned so the query must be rejected
            stmt.execute("set scan_olap_partition_num_limit=3;");
            SQLException e = Assertions.assertThrows(SQLException.class, () -> stmt.execute(query));
            String exp = "Exceeded the limit of number of olap table partitions to be scanned. Number of partitions " +
                    "allowed: 3, number of partitions to be scanned: 5. Please adjust the SQL or " +
                    "change the limit by set variable scan_olap_partition_num_limit.";
            Assertions.assertTrue(e.getMessage().contains(exp), e.getMessage());
            //check set invalid value abc
            e = Assertions.assertThrows(SQLException.class,
                    () -> stmt.execute("set scan_olap_partition_num_limit=abc;"));
            exp = "Incorrect argument type to variable 'scan_olap_partition_num_limit'";
            Assertions.assertTrue(e.getMessage().contains(exp), e.getMessage());
        } finally {
            PseudoCluster.getInstance().shutdown(true);
        }
    }
}