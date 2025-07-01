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

package com.starrocks.qe;

import com.starrocks.pseudocluster.PseudoCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class ExecuteScriptTest {
    @BeforeAll
    public static void setUp() throws Exception {
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
    }

    @Test
    public void testFEScript() throws Exception {
        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        Statement stmt = connection.createStatement();
        try {
            Assertions.assertTrue(stmt.execute("admin execute on frontend 'out.append(\"aaa\\\\nbbb\")'"));
            ResultSet result = stmt.getResultSet();
            Assertions.assertTrue(result.next());
            Assertions.assertEquals("aaa", result.getString(1));
            Assertions.assertTrue(result.next());
            Assertions.assertEquals("bbb", result.getString(1));
            Assertions.assertFalse(result.next());
        } finally {
            stmt.close();
            connection.close();
        }
    }

    @Test
    public void testBEScript() throws Exception {
        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        Statement stmt = connection.createStatement();
        try {
            Assertions.assertTrue(stmt.execute("admin execute on 10001 'System.print(\"aaa\")'"));
            ResultSet result = stmt.getResultSet();
            Assertions.assertTrue(result.next());
        } finally {
            stmt.close();
            connection.close();
        }
    }

}
