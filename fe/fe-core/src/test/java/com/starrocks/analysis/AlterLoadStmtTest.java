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


package com.starrocks.analysis;

import com.google.common.collect.Maps;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterLoadStmt;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.LabelName;
import com.starrocks.sql.ast.LoadStmt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class AlterLoadStmtTest {

    @BeforeEach
    public void setUp() {
    }

    @Test
    public void testNormal() {
        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(LoadStmt.PRIORITY, "NORMAL");
            AlterLoadStmt stmt = new AlterLoadStmt(new LabelName("db1", "label1"),
                    jobProperties);

            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, new ConnectContext());
            Assertions.assertEquals(1, stmt.getAnalyzedJobProperties().size());
            Assertions.assertTrue(
                    stmt.getAnalyzedJobProperties().containsKey(LoadStmt.PRIORITY));
        }
        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(LoadStmt.PRIORITY, "HIGH");
            AlterLoadStmt stmt = new AlterLoadStmt(new LabelName("db1", "label1"),
                    jobProperties);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, new ConnectContext());

            Assertions.assertEquals(1, stmt.getAnalyzedJobProperties().size());
            Assertions.assertTrue(
                    stmt.getAnalyzedJobProperties().containsKey(LoadStmt.PRIORITY));
        }
        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(LoadStmt.PRIORITY, "HIGHEST");
            AlterLoadStmt stmt = new AlterLoadStmt(new LabelName("db1", "label1"),
                    jobProperties);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, new ConnectContext());

            Assertions.assertEquals(1, stmt.getAnalyzedJobProperties().size());
            Assertions.assertTrue(
                    stmt.getAnalyzedJobProperties().containsKey(LoadStmt.PRIORITY));
        }
        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(LoadStmt.PRIORITY, "LOW");
            AlterLoadStmt stmt = new AlterLoadStmt(new LabelName("db1", "label1"),
                    jobProperties);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, new ConnectContext());

            Assertions.assertEquals(1, stmt.getAnalyzedJobProperties().size());
            Assertions.assertTrue(
                    stmt.getAnalyzedJobProperties().containsKey(LoadStmt.PRIORITY));
        }
        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(LoadStmt.PRIORITY, "LOWEST");
            AlterLoadStmt stmt = new AlterLoadStmt(new LabelName("db1", "label1"),
                    jobProperties);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, new ConnectContext());

            Assertions.assertEquals(1, stmt.getAnalyzedJobProperties().size());
            Assertions.assertTrue(
                    stmt.getAnalyzedJobProperties().containsKey(LoadStmt.PRIORITY));
        }
    }

    @Test
    public void testNoProperties() {
        assertThrows(SemanticException.class, () -> {
            AlterLoadStmt stmt = new AlterLoadStmt(new LabelName("db1", "label1"), null);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, new ConnectContext());
        });
    }

    @Test
    public void testUnsupportedProperties() {
        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(CreateRoutineLoadStmt.FORMAT, "csv");
            AlterLoadStmt stmt = new AlterLoadStmt(new LabelName("db1", "label1"),
                    jobProperties);
            try {
                com.starrocks.sql.analyzer.Analyzer.analyze(stmt, new ConnectContext());
                Assertions.fail();
            } catch (SemanticException e) {
                Assertions.assertTrue(e.getMessage().contains("Unsupported properties 'format'"));
            } catch (Exception e) {
                Assertions.fail();
            }
        }

        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(LoadStmt.PRIORITY, "abc");
            AlterLoadStmt stmt = new AlterLoadStmt(new LabelName("db1", "label1"),
                    jobProperties);

            try {
                com.starrocks.sql.analyzer.Analyzer.analyze(stmt, new ConnectContext());
                Assertions.fail();
            } catch (SemanticException e) {
                Assertions.assertTrue(e.getMessage().contains("priority"));
            } catch (Exception e) {
                Assertions.fail();
            }
        }
    }
}
